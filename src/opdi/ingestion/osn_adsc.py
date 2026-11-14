"""Step 01b: ingestion of ADS-C position reports (off by default).

ADS-C reports are mapped to the state-vector columns, with a ``source``
column of ``"ADS-C"``, and written to ``config.adsc.table``; step 02 merges
them with the ADS-B state vectors when :attr:`opdi.config.AdscConfig.enabled`
is set.

OpenSky publishes a ``readsb`` ADS-C topic beside the ADS-B archive:
``s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro``, hourly
partitions named ``hour=<unix seconds>``, snappy **parquet** despite the
``.avro`` in the topic name, carrying the Avro schema
``org.opensky.avro.readsb.Aircraft`` in the footer.

**What arrives.** A few hundred to a few tens of thousands of reports a day
worldwide, one every few minutes per aircraft. The fields filled are
``time``, ``hex``, ``lat``, ``lon``, ``alt_baro_ft``, ``track``, ``flight``
and, for part of the reports, ``gs``, ``baro_rate`` and ``squawk``.

These fields are always empty and are not carried: ``alt_geom``, ``ias``,
``tas``, ``mach``, ``mag_heading``, ``true_heading``, ``roll``, ``track_rate``,
``geom_rate``, ``oat``, ``tat``, ``wd``, ``ws``, ``nav_qnh``,
``nav_altitude_mcp``, ``nav_altitude_fms``, ``nav_heading``, ``registration``,
``ac_type``, ``ac_desc``, ``category``. Constant placeholders rather than
measurements: ``nic``=0, ``rc``=0, ``seen``=0, ``seen_pos``=0, ``rssi``=-49.5,
``mlat``=[], ``tisb``=[].

``benchmarks/adsc_census.py`` counts what the feed holds.

**The feed's schema has grown.** ``track``, ``gs`` and ``baro_rate`` are empty
before roughly 2025-09, and ``flight``, ``squawk`` and ``on_ground`` before
roughly 2026-06. Ingesting a 2023 or 2024 day therefore writes rows with NULL
``velocity`` and ``vert_rate``; that is the feed, not a mapping fault.

**Units.** The feed is in aviation units (ft, kt, ft/min); the state
vectors are in SI units, as OpenSky delivers ADS-B. The reports are converted
on the way in, with the factors used throughout the pipeline.
"""

from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_unixtime, lit, to_date

from opdi.config import OPDIConfig
from opdi.utils.storage import StorageManager

__all__ = [
    "ADSB_SOURCE",
    "ADSC_SOURCE",
    "AdscIngestion",
    "shadow_filter",
    "to_opdi_schema",
]

#: Values of the ``source`` column, named once because the cleaning exemption,
#: the merge and other readers must all spell them the same way.
ADSB_SOURCE = "ADS-B"
ADSC_SOURCE = "ADS-C"

#: Aviation -> SI. Reciprocals of the factors in ``events.py``.
_FT_TO_M = 1.0 / 3.28084
_KT_TO_MS = 1.0 / 1.94384
_FT_MIN_TO_MS = 1.0 / 196.850394

#: Distinguishes "the caller said nothing" from "the caller said worldwide",
#: matching :class:`~opdi.ingestion.osn_statevectors.StateVectorIngestion`.
_UNSET = object()


def to_opdi_schema(df: DataFrame) -> DataFrame:
    """Raw ADS-C rows in, ``osn_statevectors_v2`` columns plus ``source`` out.

    ``event_time`` is left as a **double** of unix seconds, because the bbox
    filter runs before the timestamp cast -- the same ordering
    ``StateVectorIngestion._apply_filters`` documents and depends on.

    Three fields are deliberately NULL rather than carried:

    * ``callsign`` -- the feed's ``flight`` is the IATA number (``LH133``), not
      the ICAO callsign ADS-B broadcasts (``DLH133``). The default track rule
      starts a new track when the call sign changes, so carrying it would
      split every merged track at its first ADS-C report. The rule skips
      NULL call signs, so a NULL loses nothing.
    * ``alert``, ``spi`` -- integer 0 on every row, a readsb default rather
      than an observation of an ATC indicator.
    * ``serials`` -- the receiving-station list of a terrestrial network has no
      counterpart on a satellite link.
    """
    return df.select(
        col("time").cast("double").alias("event_time"),
        F.lower(F.trim(col("hex"))).alias("icao24"),
        col("lat").cast("double").alias("lat"),
        col("lon").cast("double").alias("lon"),
        (col("gs").cast("double") * lit(_KT_TO_MS)).alias("velocity"),
        col("track").cast("double").alias("heading"),
        (col("baro_rate").cast("double") * lit(_FT_MIN_TO_MS)).alias("vert_rate"),
        lit(None).cast("string").alias("callsign"),
        F.coalesce(col("on_ground").cast("boolean"), lit(False)).alias("on_ground"),
        lit(None).cast("boolean").alias("alert"),
        lit(None).cast("boolean").alias("spi"),
        col("squawk").cast("string").alias("squawk"),
        (col("alt_baro_ft").cast("double") * lit(_FT_TO_M)).alias("baro_altitude"),
        (col("alt_geom").cast("double") * lit(_FT_TO_M)).alias("geo_altitude"),
        (col("time").cast("double") - F.coalesce(col("seen_pos").cast("double"), lit(0.0))).alias(
            "last_pos_update"
        ),
        (col("time").cast("double") - F.coalesce(col("seen").cast("double"), lit(0.0))).alias(
            "last_contact"
        ),
        lit(None).cast("array<int>").alias("serials"),
        lit(ADSC_SOURCE).alias("source"),
    )


def shadow_filter(
    adsc: DataFrame, adsb: DataFrame, before_seconds: int, after_seconds: int
) -> DataFrame:
    """ADS-C rows with no ADS-B row for the same airframe inside the window.

    The window is ``[t - before_seconds, t + after_seconds]`` around each ADS-C
    report. The two sides can differ because the clocks of the two feeds are
    not aligned: ADS-C reports are on average about 1.5 s ahead of ADS-B, by
    an amount that differs between aircraft.

    Both frames carry ``icao24`` and a **timestamp** ``event_time``; the
    returned frame has exactly ``adsc``'s columns.

    Two stages, because the exact test is a range predicate and a range join
    over a day of state vectors is not affordable:

    1. Bin both sides to ``floor(t / w)`` where ``w`` is the **larger**
       half-window, and join the ADS-C side's three candidate bins (``b-1``,
       ``b``, ``b+1``) against the ADS-B side's. Three bins is what makes the
       prefilter *complete*: with a bin at least as wide as either half-window,
       a partner inside the window can only be one bin away.
    2. Apply the exact asymmetric test to the survivors, which is a handful of
       rows per airframe.

    The ADS-C side is the broadcast side -- a few thousand rows a day against
    hundreds of millions -- so this is one broadcast hash join, not a shuffle
    of the state vectors.
    """
    before, after = int(before_seconds), int(after_seconds)
    if before <= 0 or after <= 0:
        raise ValueError(
            f"both half-windows must be positive, got before={before!r} " f"after={after!r}"
        )
    s = max(before, after)

    def bin_of(t):
        return F.floor(t / F.lit(s)).cast("long")

    adsc_t = F.unix_timestamp("event_time").cast("double")
    adsc_keys = adsc.select(
        col("icao24"),
        col("event_time"),
        adsc_t.alias("_t_adsc"),
        F.explode(
            F.array(
                bin_of(adsc_t) - 1,
                bin_of(adsc_t),
                bin_of(adsc_t) + 1,
            )
        ).alias("_bin"),
    )

    adsb_t = F.unix_timestamp("event_time").cast("double")
    adsb_keys = adsb.select(
        col("icao24").alias("_icao24"),
        adsb_t.alias("_t_adsb"),
        bin_of(adsb_t).alias("_bin"),
    )

    shadowed = (
        adsb_keys.join(
            F.broadcast(adsc_keys),
            (col("_icao24") == col("icao24")) & (adsb_keys["_bin"] == adsc_keys["_bin"]),
            "inner",
        )
        # `t_adsb - t_adsc` in `[-before, +after]`: an ADS-B sample up to
        # `before` seconds behind the report, or up to `after` ahead of it.
        .where((col("_t_adsb") - col("_t_adsc")) >= F.lit(-float(before)))
        .where((col("_t_adsb") - col("_t_adsc")) <= F.lit(float(after)))
        # Distinct before the anti-join: a key set with repeats turns a filter
        # into a fan-out, and an ADS-C row near a dense stretch of ADS-B has
        # dozens of partners.
        .select(col("icao24"), col("event_time"))
        .distinct()
    )

    return adsc.join(shadowed, ["icao24", "event_time"], "left_anti")


class AdscIngestion:
    """Step 01b: read the ADS-C feed and write ``config.adsc.table``.

    Not a subclass of
    :class:`~opdi.ingestion.osn_statevectors.StateVectorIngestion`, and
    deliberately not routed through its ``_apply_filters``: that method's job
    includes the 5 s thinning, and both of its rules are wrong here. ``modulo``
    keeps only rows whose timestamp is congruent to zero -- on a feed whose
    instants are arbitrary doubles with millisecond resolution that deletes
    nearly everything -- and ``bucket`` keeps one row per 5 s bin, which at a
    600 s cadence is every row and therefore a shuffle for nothing. ADS-C is
    ingested unthinned.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        bbox: Optional[Tuple[float, float, float, float]] = _UNSET,
    ):
        self.spark = spark
        self.config = config
        self.storage = StorageManager(spark, config)
        self.bbox = config.coverage.bbox if bbox is _UNSET else bbox
        self.base = config.adsc.s3_base
        self.table = config.adsc.table

    def hour_paths(self, start_date: date, end_date: date) -> List[str]:
        """Leaf partition paths for ``[start_date, end_date)``, in order.

        Leaf directories, not a parent root plus a filter: Spark's partition
        discovery only looks *below* the path it is given, so naming the leaf
        yields no ``hour`` partition column -- which matters because the Avro
        schema already carries a data column called ``hour`` and the two would
        collide with "Found duplicate column(s)".
        """
        current = datetime(start_date.year, start_date.month, start_date.day)
        end_dt = datetime(end_date.year, end_date.month, end_date.day)
        out = []
        while current < end_dt:
            out.append(f"{self.base}/hour={int(current.timestamp())}")
            current += timedelta(hours=1)
        return out

    def existing(self, paths: List[str]) -> List[str]:
        """The subset of *paths* that exists.

        Missing hours, and whole missing days, are normal in the ADS-C archive.
        ``spark.read.parquet`` raises PATH_NOT_FOUND on the first missing
        path, which would make a normal gap look like a storage or credentials
        failure.
        """
        jvm = self.spark._jvm
        hconf = self.spark._jsc.hadoopConfiguration()
        out = []
        for p in paths:
            path = jvm.org.apache.hadoop.fs.Path(p)
            if path.getFileSystem(hconf).exists(path):
                out.append(p)
        return out

    def ingest_from_s3(self, start_date: date, end_date: date) -> int:
        """Ingest ``[start_date, end_date)`` into ``config.adsc.table``.

        Returns the number of rows written. Unlike the ADS-B ingest this counts
        unconditionally: the whole day is a few megabytes, so the second action
        costs nothing worth hiding behind an environment variable.
        """
        wanted = self.hour_paths(start_date, end_date)
        paths = self.existing(wanted)
        print(
            f"ADS-C: {len(paths)}/{len(wanted)} hourly partitions present "
            f"({start_date} to {end_date})"
        )
        if not paths:
            print("ADS-C: nothing to ingest; table left untouched")
            return 0

        df = to_opdi_schema(self.spark.read.option("mergeSchema", "true").parquet(*paths))

        if self.bbox:
            min_lon, min_lat, max_lon, max_lat = self.bbox
            df = df.filter(
                (col("lon") >= min_lon)
                & (col("lon") <= max_lon)
                & (col("lat") >= min_lat)
                & (col("lat") <= max_lat)
            )

        # Exact duplicates only -- same airframe, instant and position. The
        # topic is Kafka-sourced and at-least-once, so a redelivered report
        # appears twice; two genuinely different positions at the same instant
        # are a data problem for step 02a's dedup to arbitrate, not something
        # to discard here.
        #
        # Measured: **zero** duplicates on each of the eleven census days
        # (2023-09 to 2026-10), so this has never had anything to remove. Kept
        # because the delivery guarantee is at-least-once whatever today's data
        # shows, and it is one hash aggregate over a few thousand rows -- but
        # noted, so a later reader does not infer a problem that was never
        # observed.
        df = df.dropDuplicates(["icao24", "event_time", "lat", "lon"])

        # The global time correction, normally a no-op. Applied here rather
        # than at merge time so the written table is self-describing: whatever
        # `osn_adsc_statevectors` holds is what the merge will union, and a run
        # cannot silently mean two different things by one timestamp.
        offset = float(self.config.adsc.time_offset_seconds)
        if offset:
            print(
                f"ADS-C: applying a global time offset of {offset:+.3f} s. "
                f"The measured skew is per-airframe -- see AdscConfig."
            )
            df = df.withColumn("event_time", col("event_time") + lit(offset))

        df = df.withColumn("event_time", from_unixtime(col("event_time")).cast("timestamp"))
        df = df.withColumn("event_time_day", to_date(col("event_time")))

        rows = df.count()
        days = [r[0] for r in df.select("event_time_day").distinct().collect()]
        self.storage.write_table(
            df,
            self.table,
            mode="overwrite",
            partition_by=["event_time_day"],
            partition_values=[{"event_time_day": d} for d in days],
        )
        print(f"ADS-C: written {rows:,} rows to {self.table} " f"({len(days)} day partition(s))")
        return rows
