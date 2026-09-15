"""APDF ground truth reshaped to one row per flight.

``opdi_flight_list`` under ``events_v0.3.0`` is flight-shaped: ATOT, ALDT,
AOBT, AIBT, the two runways, the two stands and the ring crossings side by
side. APDF is movement-shaped -- one row per (flight, phase), with no literal
AOBT or ATOT column at all. ``SRC_PHASE`` is what decides which milestone a row
carries. Comparing the two therefore begins with a pivot, and doing it once
here beats doing it differently in every analysis that needs it.

This reads the committed monthly extracts under ``reference/``. It needs no
database: ``reference/extract_flight_list_truth.R`` is what fetches those, and
only that needs the work laptop.

**What APDF can and cannot check.** Crossings exist at 40 NM and 100 NM only,
so ``C50``, ``C60``, ``C110`` and ``C120`` in the flight list have no ground
truth whatsoever and must never be reported as validated.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
REFERENCE = REPO / "reference"

#: What each flight-list column is checked against. The left side is the
#: flight-list column, the right the APDF column on the row of that phase.
DEPARTURE_COLUMNS = {
    "AOBT": "BLOCK_TIME_UTC",
    "ATOT": "MVT_TIME_UTC",
    "RWY_DEP": "AP_C_RWY",
    "STND_DEP": "AP_C_STND",
    "C40_DEP": "C40_CROSS_TIME",
    "C100_DEP": "C100_CROSS_TIME",
}
ARRIVAL_COLUMNS = {
    "AIBT": "BLOCK_TIME_UTC",
    "ALDT": "MVT_TIME_UTC",
    "RWY_ARR": "AP_C_RWY",
    "STND_ARR": "AP_C_STND",
    "C40_ARR": "C40_CROSS_TIME",
    "C100_ARR": "C100_CROSS_TIME",
}

#: Flight-list columns with no APDF counterpart. Named so a comparison can
#: exclude them deliberately rather than silently scoring them as all-missing.
UNCHECKABLE = tuple(
    f"C{n}_{leg}" for leg in ("ARR", "DEP") for n in (50, 60, 110, 120)
)


def _phase(apdf: pd.DataFrame, phase: str, mapping: dict, extra: dict) -> pd.DataFrame:
    rows = apdf[apdf["SRC_PHASE"] == phase]
    out = rows[["ID"]].copy()
    for target, source in {**extra, **mapping}.items():
        out[target] = rows[source].values
    return out


def flight_truth(month: str = "202606", reference: Path | None = None) -> pd.DataFrame:
    """One row per flight, columns named as ``opdi_flight_list`` names them.

    ``month`` is ``YYYYMM``. Raises if the extract is not present rather than
    returning an empty frame -- an empty truth table scores every milestone as
    a total miss, which reads as a catastrophic pipeline failure instead of a
    missing file.
    """
    ref = reference or REFERENCE
    apdf_path = ref / f"apdf_{month}.parquet"
    flights_path = ref / f"flights_{month}.parquet"
    for p in (apdf_path, flights_path):
        if not p.exists():
            raise FileNotFoundError(
                f"{p} is missing. Extract it on the work laptop with "
                f"reference/extract_flight_list_truth.R {month[:4]}-{month[4:]}"
            )

    apdf = pd.read_parquet(apdf_path)
    flights = pd.read_parquet(flights_path, columns=["ID", "AIRCRAFT_ADDRESS"])

    # A null ID cannot be joined to anything; a duplicated one fans the join
    # out and turns one flight into several, inflating every count computed
    # downstream. Both are measured rather than assumed -- ID is the SAM ID
    # (IM_SAMAD_ID) and is not fully populated.
    keyed = apdf[apdf["ID"].notna()]
    dropped = len(apdf) - len(keyed)

    dep = _phase(keyed, "DEP", DEPARTURE_COLUMNS, {
        "ADEP": "ADEP_ICAO", "ADES": "ADES_ICAO",
        "FLTID": "AP_C_FLTID", "REG": "AP_C_REG", "ARCTYP": "ARCTYP",
    })
    arr = _phase(keyed, "ARR", ARRIVAL_COLUMNS, {})

    # A duplicated ID within one phase is ambiguous truth: two movement rows
    # claim to be the same flight's departure and there is no basis here for
    # preferring one. Such flights are removed from the table entirely rather
    # than kept and joined -- keeping them would fan the join out, turning one
    # flight into several and inflating every count computed downstream, and
    # picking one arbitrarily would publish a coin toss as a reference value.
    # It is a handful of rows in a million; the reason to handle it is that
    # the failure mode is silent, not that it is large.
    fanout = {
        "DEP": int(dep["ID"].duplicated().sum()),
        "ARR": int(arr["ID"].duplicated().sum()),
    }
    ambiguous = set(dep.loc[dep["ID"].duplicated(keep=False), "ID"]) | set(
        arr.loc[arr["ID"].duplicated(keep=False), "ID"]
    )
    if ambiguous:
        dep = dep[~dep["ID"].isin(ambiguous)]
        arr = arr[~arr["ID"].isin(ambiguous)]

    # An outer join, not inner: a flight seen at only one end is a real flight
    # and a real test of coverage. An inner join would delete precisely the
    # cases where coverage is worst.
    truth = dep.merge(arr, on="ID", how="outer")

    bridge = flights[flights["ID"].notna()].drop_duplicates("ID")
    truth = truth.merge(
        bridge.rename(columns={"AIRCRAFT_ADDRESS": "ICAO24"}), on="ID", how="left"
    )

    truth.attrs["dropped_no_id"] = dropped
    truth.attrs["duplicate_ids"] = fanout
    truth.attrs["dropped_ambiguous"] = len(ambiguous)
    truth.attrs["month"] = month
    return truth


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--month", default="202606", help="YYYYMM, default 202606")
    ap.add_argument("--write", type=Path, default=None,
                    help="optional parquet output path")
    args = ap.parse_args()

    truth = flight_truth(args.month)
    n = len(truth)
    print(f"APDF flight-shaped truth for {args.month}: {n:,} flights")
    print(f"  movement rows with no ID, dropped: {truth.attrs['dropped_no_id']:,}")
    print(f"  duplicate IDs within a phase     : {truth.attrs['duplicate_ids']}"
          f" -> {truth.attrs['dropped_ambiguous']} flight(s) removed as ambiguous")
    icao = truth["ICAO24"].notna().mean() * 100
    print(f"  ICAO24 present (the ADS-B join key): {icao:.1f}%")

    both = (truth["ATOT"].notna() & truth["ALDT"].notna()).mean() * 100
    dep_only = (truth["ATOT"].notna() & truth["ALDT"].isna()).mean() * 100
    arr_only = (truth["ATOT"].isna() & truth["ALDT"].notna()).mean() * 100
    print(f"\n  both ends {both:.1f}%   departure only {dep_only:.1f}%   "
          f"arrival only {arr_only:.1f}%")
    print("  APDF is airport-reported, so a flight leaving the reporting area "
          "has\n  a departure row and no arrival row. That is the shape of the "
          "truth,\n  not a gap in it.")

    print("\n  column coverage -- the ceiling on what can be validated:")
    for col in list(DEPARTURE_COLUMNS) + list(ARRIVAL_COLUMNS):
        print(f"    {col:10s} {truth[col].notna().mean() * 100:5.1f}%")
    print(f"\n  no APDF counterpart at all: {', '.join(UNCHECKABLE)}")

    if args.write:
        truth.to_parquet(args.write, index=False)
        print(f"\n  written -> {args.write}")


if __name__ == "__main__":
    main()
