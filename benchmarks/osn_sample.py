"""
Pull a local sample of OpenSky state vectors for benchmarking.

Reads the hourly ``tables_v4/state_vectors`` partitions straight from the
OpenSky S3 bucket, applies the same bounding box and decimation the production
ingestion applies (``ingestion/osn_statevectors.py:_apply_filters``), and
writes day-partitioned parquet locally.

Filtering happens *at read time*, before anything is persisted. The raw feed is
~21 GB/day globally; the Europe bbox plus 5 s decimation brings that down to
~1.5 GB/day.

Output lands in S3, not on local disk -- dev boxes are scratch, S3 is reachable
from the OSN server and the work laptop. Every dataset written here must be
recorded in ``benchmarks/DATASETS.md``.

    python benchmarks/osn_sample.py 2025-06-05 2025-06-08     # [start, end)

Credentials come from ``.env`` (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).
Never run this on the OSN server -- there the data is already local.
"""

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

S3_ENDPOINT = "https://s3.opensky-network.org"
S3_BASE = "s3a://opensky-hdfs-backup/tables_v4/state_vectors"
# Research output goes to S3, never local disk. A separate prefix from the
# production tables under opdi/ -- those must never be overwritten.
OUT_BASE = "s3a://eurocontrol/opdi/research/statevectors"

# Matches StateVectorIngestion.DEFAULT_BBOX exactly. Do not drift from it: the
# benchmark is meaningless if it evaluates a different spatial population than
# the pipeline ingests.
BBOX = (-25.86653, 26.74617, 49.65699, 70.25976)  # min_lon, min_lat, max_lon, max_lat
TIME_INTERVAL = 5  # seconds; keep rows where event_time % 5 == 0

HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.5.0"


def load_dotenv() -> None:
    env = REPO / ".env"
    if not env.is_file():
        sys.exit(f"No .env at {env}; AWS credentials are required.")
    for line in env.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def build_spark(cores: int, driver_memory: str):
    from pyspark.sql import SparkSession

    key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if not key or not secret:
        sys.exit("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set.")

    return (
        SparkSession.builder.master(f"local[{cores}]")
        .appName("opdi-osn-sample")
        .config("spark.jars.packages", HADOOP_AWS)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", key)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # -- S3 read robustness -------------------------------------------
        # The hourly objects are ~1 GB. With many readers in parallel the
        # default 300 s vectored-range timeout is hit and the whole job aborts
        # with FAILED_READ_FILE. Parquet's vectored IO issues many small
        # concurrent range requests, which is the pattern that starves; plain
        # sequential reads are what we actually want for a full-file scan.
        .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false")
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
        .config("spark.hadoop.fs.s3a.readahead.range", "2M")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "256")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.hadoop.fs.s3a.retry.interval", "1000")
        .config("spark.hadoop.fs.s3a.create.performance", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )


def hour_partitions(start: dt.date, end: dt.date):
    cur = dt.datetime(start.year, start.month, start.day, tzinfo=dt.timezone.utc)
    stop = dt.datetime(end.year, end.month, end.day, tzinfo=dt.timezone.utc)
    while cur < stop:
        yield f"{S3_BASE}/hour={int(cur.timestamp())}"
        cur += dt.timedelta(hours=1)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("start", help="first day, YYYY-MM-DD (inclusive)")
    ap.add_argument("end", help="last day, YYYY-MM-DD (exclusive)")
    ap.add_argument("--cores", type=int, default=12)
    ap.add_argument("--driver-memory", default="64g")
    ap.add_argument("--interval", type=int, default=TIME_INTERVAL)
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    if end <= start:
        sys.exit("end must be after start")

    load_dotenv()
    spark = build_spark(args.cores, args.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")

    from pyspark.sql import functions as F

    # One day at a time. A whole month in one job means any single S3 timeout
    # discards every partition read so far; per-day output is resumable and
    # each day is independently usable.
    day = start
    while day < end:
        out_day = f"{OUT_BASE}/day={day:%Y-%m-%d}"
        if _already_written(spark, out_day):
            print(f"{day}: already in S3, skipping")
            day += dt.timedelta(days=1)
            continue
        print(f"--- {day} ---")
        _pull_day(spark, day, out_day, args.interval)
        day += dt.timedelta(days=1)

    print("\nAll days complete.")
    spark.stop()


def _already_written(spark, path: str) -> bool:
    """True if a completed write already exists at *path* (idempotent resume)."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path + "/_SUCCESS")
    return p.getFileSystem(hconf).exists(p)


def _pull_day(spark, day: dt.date, out_day: str, interval: int) -> None:
    from pyspark.sql import functions as F

    paths = list(hour_partitions(day, day + dt.timedelta(days=1)))
    df = spark.read.option("mergeSchema", "true").parquet(*paths)

    # Column names differ between the S3 backup (camelCase) and the OPDI schema.
    renames = {
        "lastPosUpdate": "last_pos_update",
        "lastContact": "last_contact",
        "baroAltitude": "baro_altitude",
        "geoAltitude": "geo_altitude",
        "vertRate": "vert_rate",
        "onGround": "on_ground",
        "time": "event_time",
    }
    for src, dst in renames.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)

    min_lon, min_lat, max_lon, max_lat = BBOX
    # Filter BEFORE the timestamp cast, exactly as the pipeline does -- the
    # decimation is modular arithmetic on the raw unix seconds.
    df = df.filter(
        (F.col("lon") >= min_lon)
        & (F.col("lon") <= max_lon)
        & (F.col("lat") >= min_lat)
        & (F.col("lat") <= max_lat)
    )
    if interval > 1:
        df = df.filter((F.col("event_time") % interval) == 0)

    df = df.withColumn(
        "event_time", F.from_unixtime(F.col("event_time")).cast("timestamp")
    )

    df.write.mode("overwrite").parquet(out_day)
    n = spark.read.parquet(out_day).count()
    print(f"{day}: {n:,} state vectors -> {out_day}")


if __name__ == "__main__":
    main()
