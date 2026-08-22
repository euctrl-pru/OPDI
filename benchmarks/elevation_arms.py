"""Arm C: the datum's effect, banded by field elevation.

The study's discriminating measurement. A headline gain is consistent with
several explanations; a gain concentrated at elevated aerodromes and absent at
sea-level ones is consistent with almost none but the datum.

**Why the leave-one-out column exists.** The feasibility census
(`elevation_census.py`, run before any of this was built) showed each treatment
band rests on a single aerodrome: LEMD is 40% of `1500-3000`, LTAC 54% of
`>3000`. A band mean alone therefore cannot separate an elevation effect from a
Madrid effect. `delta_correct_loo` re-computes each band with its largest
contributor removed, which is what makes the claim capable of failing.

This reads what `flight_list_v61.py` already wrote -- `per_airport_v61.csv`,
produced by the same `per_airport_counts` the rest of the study is scored with
-- rather than re-scoring. The arithmetic is therefore pandas, and it costs a
Spark session only to read the aerodrome elevations.

    python benchmarks/elevation_arms.py --per-airport <dir>/per_airport_v61.csv \\
        --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import pandas as pd

from elevation_bands import BANDS

#: The two runs compared. Named here rather than passed, because reversing them
#: silently inverts every sign in the output.
ARM_MSL = "datum_msl"
ARM_FIELD = "datum_field"


def _band_of(elev):
    """Python twin of `elevation_bands.elevation_band`, for pandas frames."""
    if pd.isna(elev):
        return "unknown"
    for label, lo, hi in BANDS:
        if lo <= elev < hi:
            return label
    return "unknown"


def per_airport_delta(per_airport: pd.DataFrame, elevations: pd.DataFrame) -> pd.DataFrame:
    """One row per (aerodrome, role): correct on each datum, and the change.

    The delta is **field minus sea level**, so a positive number means the
    change helped. Stated here because the sign is the whole result and a
    convention nobody wrote down is a convention somebody will reverse.
    """
    missing = {ARM_MSL, ARM_FIELD} - set(per_airport["run"].unique())
    if missing:
        raise ValueError(
            f"per-airport table is missing run(s) {sorted(missing)}. Both arms "
            f"must be present: a band seen in only one would report a delta "
            f"equal to the whole of the other, which looks like a result."
        )

    keep = ["airport", "role", "n_truth", "n_correct"]
    msl = per_airport[per_airport.run == ARM_MSL][keep].rename(
        columns={"n_correct": "n_correct_msl"}
    )
    field = per_airport[per_airport.run == ARM_FIELD][keep].rename(
        columns={"n_correct": "n_correct_field"}
    ).drop(columns=["n_truth"])

    out = msl.merge(field, on=["airport", "role"], how="outer")
    out["n_correct_msl"] = out["n_correct_msl"].fillna(0).astype(int)
    out["n_correct_field"] = out["n_correct_field"].fillna(0).astype(int)
    out["delta_correct"] = out["n_correct_field"] - out["n_correct_msl"]

    if len(elevations):
        elev = elevations.drop_duplicates("airport")
        out = out.merge(elev, on="airport", how="left")
    else:
        # No reference at all. Every aerodrome bands as `unknown`, which is
        # visible in the output rather than being silently folded into the
        # control band -- the same convention `elevation_bands` uses.
        out["elevation_ft"] = pd.NA
    out["band"] = out["elevation_ft"].map(_band_of)
    return out.sort_values("delta_correct", ascending=False).reset_index(drop=True)


def band_summary(per_airport: pd.DataFrame, elevations: pd.DataFrame) -> pd.DataFrame:
    """Per (band, role) totals, with two independent robustness controls.

    ``delta_correct`` is the band's total change. Two columns then ask whether
    that total is really the band's, or one aerodrome's:

    ``delta_correct_loo``
        The total with ``largest_mover`` removed -- the aerodrome whose delta is
        largest in absolute value. This asks: *does the effect survive dropping
        whichever field moved most?* It is the stronger of the two controls,
        because it removes the single best case by construction.

    ``delta_correct_ex_busiest``
        The total with ``busiest`` removed -- the aerodrome carrying the most
        ground-truth movements. This is the control the feasibility census
        specifically called for: LEMD is 40% of `1500-3000` and LTAC 54% of
        `>3000`, so "the datum helped at 1,500-3,000 ft" and "the datum helped
        at Madrid" would otherwise be the same sentence.

    They answer different questions and can disagree; report both.
    """
    detail = per_airport_delta(per_airport, elevations)

    rows = []
    for (band, role), grp in detail.groupby(["band", "role"], sort=False):
        total = int(grp["delta_correct"].sum())
        # By absolute value: an aerodrome that loses heavily is as capable of
        # carrying a band as one that gains heavily, and taking the largest
        # positive contributor only would hide that case.
        mover = grp.loc[grp["delta_correct"].abs().idxmax()]
        busiest = grp.loc[grp["n_truth"].fillna(0).idxmax()]
        rows.append({
            "band": band,
            "role": role,
            "aerodromes": int(grp["airport"].nunique()),
            "n_truth": int(grp["n_truth"].fillna(0).sum()),
            "n_correct_msl": int(grp["n_correct_msl"].sum()),
            "n_correct_field": int(grp["n_correct_field"].sum()),
            "delta_correct": total,
            "largest_mover": mover["airport"],
            "largest_mover_delta": int(mover["delta_correct"]),
            "delta_correct_loo": total - int(mover["delta_correct"]),
            "busiest": busiest["airport"],
            "busiest_n_truth": int(busiest["n_truth"] or 0),
            "delta_correct_ex_busiest": total - int(busiest["delta_correct"]),
        })

    order = [label for label, _, _ in BANDS] + ["unknown"]
    out = pd.DataFrame(rows)
    out["_o"] = out["band"].map({b: i for i, b in enumerate(order)})
    return out.sort_values(["_o", "role"]).drop(columns="_o").reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--per-airport", type=Path, required=True,
                    help="per_airport_v61.csv from flight_list_v61.py, carrying "
                         "both datum arms")
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--executors", type=int, default=2)
    ap.add_argument("--ui-port", type=int, default=4047)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    out = args.results_dir
    out.mkdir(parents=True, exist_ok=True)

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    from elevation_bands import airport_elevations

    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(2, "4g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")

    elev = (
        airport_elevations(spark)
        .toPandas()
        .rename(columns={"_apt": "airport", "_elev_ft": "elevation_ft"})
    )
    spark.stop()

    per_apt = pd.read_csv(args.per_airport)
    detail = per_airport_delta(per_apt, elev)
    bands = band_summary(per_apt, elev)

    detail.to_csv(out / "elevation_per_airport.csv", index=False)
    bands.to_csv(out / "elevation_bands.csv", index=False)

    print("\n=== Arm C: correct answers per elevation band, field minus MSL ===")
    print(bands.to_string(index=False))
    print("\n=== the elevated aerodromes that moved most ===")
    elevated = detail[detail.band.isin(("1500-3000", ">3000"))]
    print(elevated.head(20).to_string(index=False))
    print(f"\nwritten to {out}")


if __name__ == "__main__":
    main()
