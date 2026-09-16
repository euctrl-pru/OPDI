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

**The debounce guard (Task 5).** ``--warehouses`` takes one or more
``LABEL=WAREHOUSE`` pairs and measures each against the same APDF days, so one
invocation produces both the shipped baseline and the debounced arm's numbers
on a shared yardstick. The default pair is the one Task 5 used:
``recommended=opdi-prod`` (the shipped arm, already in production) and
``debounced=opdi/research/a9`` (the self-contained warehouse
``run_debounced_day.py`` builds for 2026-06-03, read-only here). With
``--out-name`` and ``--results-dir`` the per-label row is staged as a CSV, one
row per label, so ``regenerate_track_v3.py`` can declare this as a job.

Reads committed reference data and the published flight list. No Spark.
"""
from __future__ import annotations

import argparse
import csv
import io
import os
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent

#: What the rule achieved when it was introduced, measured over three days.
#: A later run landing far from these has either changed the rule or changed
#: the segmentation feeding it, and either is worth knowing about.
#: Measured on the shipped rules over 2026-06-01..03. ``dep_kept`` is 1.03
#: rather than the 1.02 of the analysis that designed the rule, because the
#: shipped version coalesces across every ring while the analysis used C40
#: alone -- so it keeps a few more departures, deliberately.
BASELINE = {"dep_raw": 1.23, "arr_raw": 1.03, "dep_kept": 1.03, "arr_kept": 1.02}

#: Task 5's pair, matching ``run_debounced_day.py``'s own "downstream" note:
#: the shipped arm reads straight from production, and the debounced arm
#: reads the self-contained research warehouse that script populated for
#: 2026-06-03 -- both read-only, neither touches production.
DEFAULT_WAREHOUSES = ["recommended=opdi-prod", "debounced=opdi/research/a9"]


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
    cols = ["ID", "ADEP", "ADES", "ATOT", "ALDT", "SUPERSEDED_DEP", "SUPERSEDED_ARR",
            "MOVEMENT_DEP", "MOVEMENT_ARR"]
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


def measure(label: str, warehouse: str, days, month: str) -> dict:
    """Score one warehouse's flight list against APDF, and return one row.

    Printed in full (the per-aerodrome breakdown included) and also returned
    as the flat dict a CSV row wants -- the two must not drift, so the row is
    read out of the same variables the prints use, not recomputed.
    """
    opdi = flight_list(days, warehouse=warehouse)
    apdf = apdf_movements(days, month)

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
    # MOVEMENT_DEP/ARR compose every rule; reading them here rather than
    # re-deriving keeps this check measuring the shipped definition rather than
    # a second copy of it that can drift.
    kept = count(opdi["MOVEMENT_DEP"].fillna(False), opdi["MOVEMENT_ARR"].fillna(False))
    nd, na = ref_dep.sum(), ref_arr.sum()

    print(f"\n=== {label}  (warehouse={warehouse}) ===")
    print(f"days {', '.join(days)}   aerodromes reported by APDF: {len(covered):,}")
    print(f"  APDF                       dep {nd:8,}          arr {na:8,}")
    print(f"  OPDI, every row            dep {raw[0]:8,} ({raw[0]/nd:5.2f}x)  "
          f"arr {raw[1]:8,} ({raw[1]/na:5.2f}x)   baseline {BASELINE['dep_raw']}x / {BASELINE['arr_raw']}x")
    print(f"  OPDI, fragments removed    dep {kept[0]:8,} ({kept[0]/nd:5.2f}x)  "
          f"arr {kept[1]:8,} ({kept[1]/na:5.2f}x)   baseline {BASELINE['dep_kept']}x / {BASELINE['arr_kept']}x")

    # The rule's central claim, restated as a check rather than a memory.
    # The claim under test is narrow and must stay narrow: *the rules* discard
    # no flight that flew. A flight with a take-off but no ADEP is not counted
    # either, but nothing here excluded it -- there is simply no aerodrome to
    # count it at, which is an upstream gap and a different problem. Conflating
    # the two made this print REGRESSION for 50 departures the rules never
    # touched.
    rule_dep = (opdi["ATOT"].notna() & opdi["ADEP"].notna()
                & ~opdi["MOVEMENT_DEP"].fillna(False)).sum()
    rule_arr = (opdi["ALDT"].notna() & opdi["ADES"].notna()
                & ~opdi["MOVEMENT_ARR"].fillna(False)).sum()
    verdict = "OK" if rule_dep == rule_arr == 0 else "REGRESSION"
    print(f"\n  flew, has an aerodrome, excluded by a rule: dep {rule_dep:,}  "
          f"arr {rule_arr:,}   [{verdict}]")
    print("  (the rules' whole claim is that these are zero; a non-zero value "
          "means they are\n   now discarding real movements and should be revisited)")

    # Surfaced separately rather than folded in: these are movements OPDI saw
    # happen and cannot attribute to an aerodrome, so they are missing from
    # every per-aerodrome total and no filter will bring them back.
    orphan_dep = (opdi["ATOT"].notna() & opdi["ADEP"].isna()).sum()
    orphan_arr = (opdi["ALDT"].notna() & opdi["ADES"].isna()).sum()
    # The aggregate cancelled a 0.39x aerodrome against over-counts elsewhere
    # and still read 0.96x. Per-aerodrome is not an optional extra view here;
    # it is the only one that can see that.
    kept_dep = opdi[opdi["MOVEMENT_DEP"].fillna(False)]["ADEP"].value_counts()
    worst = []
    for apt in ref_dep.head(40).index:
        r = kept_dep.get(apt, 0) / ref_dep[apt]
        worst.append((abs(r - 1.0), apt, ref_dep[apt], kept_dep.get(apt, 0), r))
    worst.sort(reverse=True)
    print("\n  furthest from APDF, of the 40 busiest departure aerodromes:")
    for _, apt, ref, got, r in worst[:6]:
        print(f"    {apt}  APDF {ref:6,}  OPDI {got:6,}  {r:5.2f}x")

    print(f"\n  flew but no aerodrome named  : dep {orphan_dep:,}  arr {orphan_arr:,}")
    print("  (an upstream ADEP/ADES gap, not a counting rule -- these movements "
          "are real\n   and are absent from every per-aerodrome figure)")

    max_abs_err = max((w[4] for w in worst), default=float("nan"))
    return {
        "label": label,
        "warehouse": warehouse,
        "days": ";".join(days),
        "apdf_dep": int(nd),
        "apdf_arr": int(na),
        "n_aerodromes_covered": len(covered),
        "opdi_raw_dep": int(raw[0]),
        "opdi_raw_dep_ratio": round(raw[0] / nd, 4),
        "opdi_raw_arr": int(raw[1]),
        "opdi_raw_arr_ratio": round(raw[1] / na, 4),
        "opdi_kept_dep": int(kept[0]),
        "opdi_kept_dep_ratio": round(kept[0] / nd, 4),
        "opdi_kept_arr": int(kept[1]),
        "opdi_kept_arr_ratio": round(kept[1] / na, 4),
        "rule_dep_excluded": int(rule_dep),
        "rule_arr_excluded": int(rule_arr),
        "guard_verdict": verdict,
        "orphan_dep": int(orphan_dep),
        "orphan_arr": int(orphan_arr),
        "worst_dep_aerodrome_ratio": round(max_abs_err, 4) if worst else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", required=True, help="YYYY-MM-DD ...")
    ap.add_argument("--month", default=None, help="APDF month, YYYYMM (default: from --days)")
    ap.add_argument("--warehouses", nargs="+", default=DEFAULT_WAREHOUSES,
                     help="one or more LABEL=WAREHOUSE pairs, each scored "
                          "against the same APDF days")
    ap.add_argument("--results-dir", type=Path, default=None,
                     help="stage one row per label here as a CSV, if "
                          "--out-name is also given")
    ap.add_argument("--out-name", default=None,
                     help="CSV filename under --results-dir")
    args = ap.parse_args()
    month = args.month or args.days[0][:7].replace("-", "")

    rows = []
    for pair in args.warehouses:
        if "=" not in pair:
            raise SystemExit(f"--warehouses entry {pair!r} is not LABEL=WAREHOUSE")
        label, warehouse = pair.split("=", 1)
        rows.append(measure(label, warehouse, args.days, month))

    if args.results_dir and args.out_name:
        args.results_dir.mkdir(parents=True, exist_ok=True)
        out = args.results_dir / args.out_name
        fieldnames = sorted({k for row in rows for k in row})
        with out.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"\nstaged {out}")


if __name__ == "__main__":
    main()
