"""Do OPDI and APDF agree on how many movements there were?

OPDI counts more, and the excess is not spread evenly: over 2026-06-01..03 it
reported **1.23x** APDF's departures against **1.03x** its arrivals. The cause
is track splitting, not aerodrome assignment -- ``ADEP_SOURCE`` is 100%
``aerodrome``. A real departure is routinely cut into two tracks, a ground
fragment that never leaves the stand and then the flight; both begin at the
aerodrome, so both are given an ADEP and both count.

``SUPERSEDED_DEP``/``SUPERSEDED_ARR`` mark the fragments. This script is the
measurement that justified them and the regression check that keeps them
honest: run it after a campaign and the ratios should sit near the figures
below, per aerodrome as well as in total.

Reads committed reference data and the published flight list. No Spark.
"""
from __future__ import annotations

import argparse
import io
import os
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent

#: What the rule achieved when it was introduced, measured over three days.
#: A later run landing far from these has either changed the rule or changed
#: the segmentation feeding it, and either is worth knowing about.
BASELINE = {"dep_raw": 1.23, "arr_raw": 1.03, "dep_kept": 1.11, "arr_kept": 1.02}


def _s3():
    import boto3

    for line in (REPO / ".env").read_text().splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
    return boto3.client(
        "s3",
        endpoint_url="https://s3.opensky-network.org",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def flight_list(days, bucket="eurocontrol", warehouse="opdi-prod") -> pd.DataFrame:
    s3 = _s3()
    cols = ["ID", "ADEP", "ADES", "ATOT", "ALDT", "SUPERSEDED_DEP", "SUPERSEDED_ARR"]
    frames = []
    for day in days:
        prefix = f"{warehouse}/opdi_flight_list/DOF={day}/"
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith(".parquet"):
                    continue
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                frames.append(pd.read_parquet(io.BytesIO(body), columns=cols))
    if not frames:
        raise SystemExit(f"no flight list found for {days}")
    # The ID is the primary key; a duplicate would inflate every count here in
    # exactly the way this script exists to detect elsewhere.
    return pd.concat(frames, ignore_index=True).drop_duplicates("ID")


def apdf_movements(days, month: str) -> pd.DataFrame:
    path = REPO / "reference" / f"apdf_{month}.parquet"
    if not path.exists():
        raise SystemExit(f"{path} missing -- extract it with reference/extract.R")
    df = pd.read_parquet(path, columns=["ADEP_ICAO", "ADES_ICAO", "SRC_PHASE", "MVT_TIME_UTC"])
    df["day"] = pd.to_datetime(df["MVT_TIME_UTC"]).dt.strftime("%Y-%m-%d")
    return df[df["day"].isin(days)]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", required=True, help="YYYY-MM-DD ...")
    ap.add_argument("--month", default=None, help="APDF month, YYYYMM (default: from --days)")
    ap.add_argument("--warehouse", default="opdi-prod")
    args = ap.parse_args()
    month = args.month or args.days[0][:7].replace("-", "")

    opdi = flight_list(args.days, warehouse=args.warehouse)
    apdf = apdf_movements(args.days, month)

    ref_dep = apdf[apdf["SRC_PHASE"] == "DEP"]["ADEP_ICAO"].value_counts()
    ref_arr = apdf[apdf["SRC_PHASE"] == "ARR"]["ADES_ICAO"].value_counts()
    # APDF reports a subset of aerodromes; OPDI sees every one ADS-B covers, so
    # a comparison over all of them measures scope, not accuracy.
    covered = set(ref_dep.index) | set(ref_arr.index)

    def count(dep_mask, arr_mask):
        d = (dep_mask & opdi["ADEP"].isin(covered)).sum()
        a = (arr_mask & opdi["ADES"].isin(covered)).sum()
        return d, a

    has_dep, has_arr = opdi["ADEP"].notna(), opdi["ADES"].notna()
    raw = count(has_dep, has_arr)
    kept = count(has_dep & ~opdi["SUPERSEDED_DEP"].fillna(False),
                 has_arr & ~opdi["SUPERSEDED_ARR"].fillna(False))
    nd, na = ref_dep.sum(), ref_arr.sum()

    print(f"days {', '.join(args.days)}   aerodromes reported by APDF: {len(covered):,}")
    print(f"  APDF                       dep {nd:8,}          arr {na:8,}")
    print(f"  OPDI, every row            dep {raw[0]:8,} ({raw[0]/nd:5.2f}x)  "
          f"arr {raw[1]:8,} ({raw[1]/na:5.2f}x)   baseline {BASELINE['dep_raw']}x / {BASELINE['arr_raw']}x")
    print(f"  OPDI, fragments removed    dep {kept[0]:8,} ({kept[0]/nd:5.2f}x)  "
          f"arr {kept[1]:8,} ({kept[1]/na:5.2f}x)   baseline {BASELINE['dep_kept']}x / {BASELINE['arr_kept']}x")

    # The rule's central claim, restated as a check rather than a memory.
    flown_dep = (opdi["ATOT"].notna() & opdi["SUPERSEDED_DEP"].fillna(False)).sum()
    flown_arr = (opdi["ALDT"].notna() & opdi["SUPERSEDED_ARR"].fillna(False)).sum()
    verdict = "OK" if flown_dep == flown_arr == 0 else "REGRESSION"
    print(f"\n  flights with a detected take-off marked superseded: {flown_dep:,}")
    print(f"  flights with a detected landing marked superseded : {flown_arr:,}   [{verdict}]")
    print("  (the rule's whole claim is that these are zero; a non-zero value "
          "means it is\n   now discarding real movements and should be revisited)")


if __name__ == "__main__":
    main()
