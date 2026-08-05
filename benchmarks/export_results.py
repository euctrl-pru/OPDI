"""
Export research results from S3 to the CSV cache the Quarto paper renders from.

The portal guarantees that ``quarto render`` needs no credentials and no
database -- every figure reads a committed cache. This script is the only thing
that crosses that line: it runs here, where the S3 credentials live, and writes
small CSVs into the paper's ``data/`` directory to be committed alongside it.

    python benchmarks/export_results.py

No Spark. The result tables are tens of rows; boto3 plus pyarrow reads them in
a second and does not need the cluster, which matters because the cluster is
usually busy with the job that produced them.
"""

import argparse
import io
import os
import sys
from pathlib import Path

import boto3
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from osn_sample import load_dotenv

BUCKET = "eurocontrol"
S3_ENDPOINT = "https://s3.opensky-network.org"

ROOT = "opdi/research/adep_ades"

#: Default run tag. `adep_ades.py` encodes the sample in it -- `3d-2025-06-05`
#: rather than a bare parameter string -- specifically so a longer run cannot
#: overwrite the shorter one it is meant to be compared against. Which means
#: the exporter has to be told which run it is publishing.
#: Run `--list` to see what is actually present. Prefixes without a `<n>d-`
#: sample component predate that scheme and were written by the first 1-day
#: run; they are the provenance of the currently committed CSVs.
DEFAULT_TAG = "3d-2025-06-05_large_medium_r30_fl40"


def exports(tag: str) -> dict:
    """S3 prefix (under the bucket) -> CSV filename in the paper's cache."""
    return {
        f"{ROOT}/results/{tag}": "method_comparison.csv",
        f"{ROOT}/abstain_sweep/{tag}": "abstention_sweep.csv",
        f"{ROOT}/cascade_diag/{tag}_m0-m3-m2-m1-m5_vs_m1": "cascade_attribution.csv",
        f"{ROOT}/per_airport/{tag}_M7_abstain": "per_airport.csv",
    }


#: OPDI Europe bounding box, as used at ingestion.
BBOX = (-25.86653, 26.74617, 49.65699, 70.25976)
AIRPORTS_PREFIX = "opdi/oa_airports"


def annotate_airports(df, s3):
    """Mark which aerodromes the method could ever have predicted.

    Ground truth names every destination a flight actually had -- small fields,
    and airports on other continents. Predictions are restricted to European
    large and medium airports, so an unrestricted median over all 1,200-odd
    aerodromes measures the mismatch between those two universes and nothing
    about the method. `in_scope` is the only population the comparison is
    meaningful over.
    """
    import pandas as pd

    apt = read_prefix(s3, AIRPORTS_PREFIX)
    if apt is None:
        return df
    a = apt.to_pandas()[["ident", "type", "latitude_deg", "longitude_deg"]]
    a = a.rename(columns={"ident": "airport", "type": "apt_type"})
    a["latitude_deg"] = pd.to_numeric(a["latitude_deg"], errors="coerce")
    a["longitude_deg"] = pd.to_numeric(a["longitude_deg"], errors="coerce")
    out = df.merge(a, on="airport", how="left")
    min_lon, min_lat, max_lon, max_lat = BBOX
    out["in_bbox"] = (
        out.longitude_deg.between(min_lon, max_lon)
        & out.latitude_deg.between(min_lat, max_lat)
    )
    out["in_set"] = out.apt_type.isin(["large_airport", "medium_airport"])
    out["in_scope"] = out.in_bbox & out.in_set
    return out

DEFAULT_OUT = REPO.parent / "opdi-portal" / "papers" / "adep-ades-detection" / "data"


def read_prefix(s3, prefix: str):
    """Concatenate every parquet part under *prefix* into one table."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    if not keys:
        return None
    tables = []
    for k in sorted(keys):
        body = s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    import pyarrow as pa

    return pa.concat_tables(tables)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--tag", default=DEFAULT_TAG,
                    help=f"run tag to publish (default {DEFAULT_TAG})")
    ap.add_argument("--list", action="store_true",
                    help="list the run tags present in S3 and exit")
    args = ap.parse_args()

    load_dotenv()
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    if args.list:
        for kind in ("results", "abstain_sweep", "cascade_diag", "per_airport"):
            print(f"\n{kind}:")
            seen = set()
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=BUCKET, Prefix=f"{ROOT}/{kind}/", Delimiter="/"
            ):
                for p in page.get("CommonPrefixes", []):
                    seen.add(p["Prefix"].rstrip("/").rsplit("/", 1)[-1])
            for t in sorted(seen):
                print(f"  {t}")
        return

    args.out.mkdir(parents=True, exist_ok=True)

    missing = []
    for prefix, name in exports(args.tag).items():
        tbl = read_prefix(s3, prefix)
        if tbl is None:
            missing.append(prefix)
            print(f"  MISSING  {prefix}")
            continue
        dest = args.out / name
        frame = tbl.to_pandas()
        if name == "per_airport.csv":
            frame = annotate_airports(frame, s3)
        frame.to_csv(dest, index=False)
        print(f"  {tbl.num_rows:4d} rows -> {dest.relative_to(REPO.parent)}")

    if missing:
        sys.exit(
            f"\n{len(missing)} prefix(es) absent -- run adep_ades.py to produce them first."
        )


if __name__ == "__main__":
    main()
