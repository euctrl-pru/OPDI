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

#: S3 prefix (under the bucket) -> CSV filename in the paper's cache.
EXPORTS = {
    "opdi/research/adep_ades/results/large_medium_r30_fl40": "method_comparison.csv",
    "opdi/research/adep_ades/abstain_sweep/large_medium": "abstention_sweep.csv",
    "opdi/research/adep_ades/cascade_diag/large_medium_r30_fl40_m0-m3-m2-m1-m5_vs_m1":
        "cascade_attribution.csv",
}

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
    args = ap.parse_args()

    load_dotenv()
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    args.out.mkdir(parents=True, exist_ok=True)

    missing = []
    for prefix, name in EXPORTS.items():
        tbl = read_prefix(s3, prefix)
        if tbl is None:
            missing.append(prefix)
            print(f"  MISSING  {prefix}")
            continue
        dest = args.out / name
        tbl.to_pandas().to_csv(dest, index=False)
        print(f"  {tbl.num_rows:4d} rows -> {dest.relative_to(REPO.parent)}")

    if missing:
        sys.exit(
            f"\n{len(missing)} prefix(es) absent -- run adep_ades.py to produce them first."
        )


if __name__ == "__main__":
    main()
