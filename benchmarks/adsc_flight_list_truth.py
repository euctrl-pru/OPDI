#!/usr/bin/env python
"""Does merging ADS-C name the far end of flights to and from Europe?

Scores both arms of the one-day worldwide airframe subset against Network
Manager (``reference/flights_202606.parquet``), per flight and paired: the same
truth flight is looked up in each arm, so the result is a count of flights that
changed answer rather than two rates that happen to differ.

**Why not ``adep_ades.score``.** Two of its choices hide this question.

* ``label_ground_truth`` collapses every aerodrome outside the European box to
  ``OOA``. Here the far end's real ICAO code is the thing being tested.
* It joins on ``(icao24, callsign, day)``, and an ADS-C sample has no callsign:
  a track made only of ADS-C could never score, whatever it named.

So there are three views over the rows overlapping the flight's block window.
``callsign`` needs the callsign to match, ``overlap`` does not, which is the best
case for ADS-C because a fragment can answer for the flight it belongs to. Both
take ADEP from the earliest row and ADES from the latest.

``both_ends_table`` asks the stricter question: does any one row name both ends?

Scoring is pandas on the driver: both inputs are small once read, and this is a
one-off comparison rather than pipeline code.

    python benchmarks/adsc_flight_list_truth.py --day 2026-06-05 \
        --results-dir benchmarks/out/adsc_worldwide_subset
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

PREFIX = "s3a://eurocontrol/opdi-world/research/adsc_airframes"
ARMS = ("base", "merged")

#: Slack around the NM block window when matching a track to a flight. The NM
#: times are block times and a track starts and ends with reception.
OVERLAP_SLACK = pd.Timedelta(minutes=30)

SIDES = ("adep", "ades")


def load_truth(path, day: str, airframes) -> pd.DataFrame:
    """NM flights of ``airframes`` with off-block on ``day``, UTC, one row each.

    ``flight_key`` is the row number, so two legs of one callsign stay separate.
    """
    cols = ["AIRCRAFT_ADDRESS", "AIRCRAFT_ID", "ADEP", "ADES", "AOBT_3", "ARVT_3"]
    t = pd.read_parquet(path, columns=cols).dropna(
        subset=["AIRCRAFT_ADDRESS", "ADEP", "ADES", "AOBT_3", "ARVT_3"]
    )
    t = pd.DataFrame(
        {
            "icao24": t["AIRCRAFT_ADDRESS"].str.lower().str.strip(),
            "callsign": t["AIRCRAFT_ID"].str.strip(),
            "gt_adep": t["ADEP"].str.strip(),
            "gt_ades": t["ADES"].str.strip(),
            "t_off": pd.to_datetime(t["AOBT_3"], utc=True).dt.tz_localize(None),
            "t_on": pd.to_datetime(t["ARVT_3"], utc=True).dt.tz_localize(None),
        }
    )
    t = t[(t["t_off"].dt.strftime("%Y-%m-%d") == day) & t["icao24"].isin(set(airframes))]
    return t.reset_index(drop=True).assign(flight_key=lambda d: range(len(d)))


def label_far_end(truth: pd.DataFrame, in_box) -> pd.DataFrame:
    """Which end of each flight lies outside the European box.

    ``in_box`` is the set of aerodrome idents inside ``EUROPE_BBOX``. An ident
    it does not hold counts as outside: NM's ZZZZ and AFIL entries have no
    location at all, and they are outside anything OPDI can name.
    """
    d = truth["gt_adep"].isin(in_box)
    a = truth["gt_ades"].isin(in_box)
    cls = pd.Series("neither_in_box", index=truth.index)
    cls[d & a] = "intra_europe"
    cls[d & ~a] = "europe_to_outside"
    cls[~d & a] = "outside_to_europe"
    return truth.assign(flow=cls)


def _overlapping(truth: pd.DataFrame, flights: pd.DataFrame) -> pd.DataFrame:
    """Every (flight, row) pair where the row overlaps the flight's block window.

    One flight can hold several rows: a long-haul flight is normally two tracks,
    one each side of an oceanic gap, so its ADEP and ADES sit on different rows.
    """
    j = truth.merge(flights, on="icao24", how="left", suffixes=("", "_fl"))
    hit = (j["first_seen"] <= j["t_on"] + OVERLAP_SLACK) & (
        j["last_seen"] >= j["t_off"] - OVERLAP_SLACK
    )
    return j[hit]


def _ends(j: pd.DataFrame, truth: pd.DataFrame) -> pd.DataFrame:
    """ADEP from the earliest row naming one, ADES from the latest."""
    # The name as a tie-break: row order from Spark is not stable.
    adep = (
        j.dropna(subset=["adep"])
        .sort_values(["first_seen", "adep"])
        .drop_duplicates("flight_key")
        .set_index("flight_key")["adep"]
    )
    ades = (
        j.dropna(subset=["ades"])
        .sort_values(["last_seen", "ades"], ascending=[False, True])
        .drop_duplicates("flight_key")
        .set_index("flight_key")["ades"]
    )
    return pd.DataFrame({"adep": adep, "ades": ades}).reindex(truth["flight_key"])


def match_callsign(truth: pd.DataFrame, flights: pd.DataFrame) -> pd.DataFrame:
    """Rows of the same airframe and callsign. A row with no callsign is out."""
    j = _overlapping(truth, flights)
    return _ends(j[j["callsign"] == j["callsign_fl"]], truth)


def match_overlap(truth: pd.DataFrame, flights: pd.DataFrame) -> pd.DataFrame:
    """Rows of the airframe, callsign or not.

    Best case for ADS-C: a fragment with no callsign can answer.
    """
    return _ends(_overlapping(truth, flights), truth)


def match_one_record(truth: pd.DataFrame, flights: pd.DataFrame) -> pd.DataFrame:
    """The best row any selector could pick. An upper bound, for both ends only.

    It chooses using the truth, which no consumer has, so it is fair only for
    "does one row name both ends", where an extra row cannot score by luck. Do not
    read a side from it: a flight split into an ADEP row and an ADES row ties, the
    earlier row wins, and ADES reads as missing whichever way the flight flew.
    """
    j = _overlapping(truth, flights).copy()
    j["_hits"] = (j["adep"] == j["gt_adep"]).astype(int) + (j["ades"] == j["gt_ades"]).astype(int)
    j = j.sort_values(
        ["flight_key", "_hits", "first_seen", "adep", "ades"],
        ascending=[True, False, True, True, True],
    ).drop_duplicates("flight_key")
    return j.set_index("flight_key")[["adep", "ades"]].reindex(truth["flight_key"])


def rows_per_flight(truth: pd.DataFrame, flights: pd.DataFrame) -> pd.Series:
    """How many flight-list rows overlap each truth flight. Zero when none do."""
    return (
        _overlapping(truth, flights)
        .groupby("flight_key")
        .size()
        .reindex(truth["flight_key"])
        .fillna(0)
        .astype(int)
    )


def both_ends_table(
    truth: pd.DataFrame,
    base: pd.DataFrame,
    merged: pd.DataFrame,
    base_rows: pd.Series,
    merged_rows: pd.Series,
) -> pd.DataFrame:
    """Per flow: flights with both ends right on one row, and rows per flight."""
    key = truth["flight_key"].values

    def both(a):
        return (a["adep"].reindex(key).values == truth["gt_adep"].values) & (
            a["ades"].reindex(key).values == truth["gt_ades"].values
        )

    frame = pd.DataFrame(
        {
            "flow": truth["flow"].values,
            "b": both(base),
            "m": both(merged),
            "br": base_rows.reindex(key).values,
            "mr": merged_rows.reindex(key).values,
        }
    )
    rows = []
    for flow, g in frame.groupby("flow"):
        rows.append(
            {
                "flow": flow,
                "n": len(g),
                "base_both_right": int(g["b"].sum()),
                "merged_both_right": int(g["m"].sum()),
                "gained": int((~g["b"] & g["m"]).sum()),
                "lost": int((g["b"] & ~g["m"]).sum()),
                "base_rows_per_flight": float(g["br"].mean()),
                "merged_rows_per_flight": float(g["mr"].mean()),
                "base_one_row": int((g["br"] == 1).sum()),
                "merged_one_row": int((g["mr"] == 1).sum()),
                "fewer_rows": int((g["mr"] < g["br"]).sum()),
                "more_rows": int((g["mr"] > g["br"]).sum()),
            }
        )
    return pd.DataFrame(rows)


def outcome(pred: pd.Series, gt: pd.Series) -> pd.Series:
    """``right``, ``wrong`` or ``none`` for each flight."""
    out = pd.Series("wrong", index=pred.index)
    out[pred.isna().values] = "none"
    out[(pred.values == gt.values) & pred.notna().values] = "right"
    return out


def pair_table(
    truth: pd.DataFrame, base: pd.DataFrame, merged: pd.DataFrame, view: str
) -> pd.DataFrame:
    """One row per (view, flow, side, end): counts and base-to-merged moves.

    ``end`` is ``far`` when that side's truth lies outside the box, which is the
    row the question is about, and ``near`` otherwise.
    """
    rows = []
    key = truth["flight_key"].values
    for side in SIDES:
        gt = truth.set_index("flight_key")[f"gt_{side}"].reindex(key)
        b = outcome(base[side].reindex(key), gt)
        m = outcome(merged[side].reindex(key), gt)
        changed_name = (
            base[side].reindex(key).fillna("").values != merged[side].reindex(key).fillna("").values
        )
        far = _far_mask(truth, side)
        frame = pd.DataFrame(
            {
                "flow": truth["flow"].values,
                "end": ["far" if x else "near" for x in far],
                "b": b.values,
                "m": m.values,
                "changed": changed_name,
            }
        )
        for (flow, end), g in frame.groupby(["flow", "end"]):
            row = {
                "view": view,
                "flow": flow,
                "side": side,
                "end": end,
                "n": len(g),
                "name_changed": int(g["changed"].sum()),
            }
            for arm, c in (("base", "b"), ("merged", "m")):
                for o in ("right", "wrong", "none"):
                    row[f"{arm}_{o}"] = int((g[c] == o).sum())
            for x in ("right", "wrong", "none"):
                for y in ("right", "wrong", "none"):
                    if x != y:
                        row[f"{x}_to_{y}"] = int(((g["b"] == x) & (g["m"] == y)).sum())
            # Two wrong names that differ are a change with no count in the grid.
            row["wrong_to_other_wrong"] = int(
                ((g["b"] == "wrong") & (g["m"] == "wrong") & g["changed"]).sum()
            )
            rows.append(row)
    return pd.DataFrame(rows)


def _far_mask(truth: pd.DataFrame, side: str) -> pd.Series:
    """True where this side of the flight is the end outside the box."""
    if side == "adep":
        return truth["flow"].isin(["outside_to_europe", "neither_in_box"]).values
    return truth["flow"].isin(["europe_to_outside", "neither_in_box"]).values


def flights_with_adsc(truth: pd.DataFrame, adsc_times: pd.DataFrame) -> pd.Series:
    """Number of ADS-C samples of each flight's airframe inside its block window.

    ``adsc_times`` has ``icao24`` and ``event_time``.
    """
    j = truth[["flight_key", "icao24", "t_off", "t_on"]].merge(adsc_times, on="icao24")
    j = j[(j["event_time"] >= j["t_off"]) & (j["event_time"] <= j["t_on"])]
    return j.groupby("flight_key").size().reindex(truth["flight_key"]).fillna(0).astype(int)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", required=True)
    ap.add_argument("--prefix", default=PREFIX)
    ap.add_argument("--truth", default=str(REPO / "reference/flights_202606.parquet"))
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=4)
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv
    from pyspark.sql import functions as F

    from opdi.coverage import EUROPE_BBOX

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(4, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        lists = {}
        for arm in ARMS:
            fl = spark.read.parquet(f"{args.prefix}/{arm}/opdi_flight_list")
            fl = fl.toDF(*[c.lower() for c in fl.columns])
            lists[arm] = fl.select(
                F.lower("icao24").alias("icao24"),
                F.trim("flt_id").alias("callsign"),
                "adep",
                "ades",
                "first_seen",
                "last_seen",
            ).toPandas()
        tracks = spark.read.parquet(f"{args.prefix}/merged/osn_tracks_clean")
        airframes = [r[0] for r in tracks.select(F.lower("icao24")).distinct().collect()]
        adsc = (
            tracks.where(F.col("source") == "ADS-C")
            .select(F.lower("icao24").alias("icao24"), "event_time")
            .toPandas()
        )
        min_lon, min_lat, max_lon, max_lat = EUROPE_BBOX
        apt = spark.read.parquet("s3a://eurocontrol/opdi-world/oa_airports")
        in_box = {
            r[0]
            for r in apt.where(
                (F.col("longitude_deg") >= min_lon)
                & (F.col("longitude_deg") <= max_lon)
                & (F.col("latitude_deg") >= min_lat)
                & (F.col("latitude_deg") <= max_lat)
            )
            .select("ident")
            .collect()
        }
    finally:
        spark.stop()

    truth = label_far_end(load_truth(args.truth, args.day, airframes), in_box)
    truth["adsc_samples"] = flights_with_adsc(truth, adsc).values
    print(f"truth flights: {len(truth):,}")
    print(truth["flow"].value_counts().to_string())
    print("with an ADS-C sample in the block window:", int((truth["adsc_samples"] > 0).sum()))

    for arm in ARMS:
        r = lists[arm]
        blank = r["callsign"].isna() | (r["callsign"] == "")
        print(
            f"{arm}: {len(r):,} rows, adep {r['adep'].notna().sum():,}, "
            f"ades {r['ades'].notna().sum():,}, "
            f"both {(r['adep'].notna() & r['ades'].notna()).sum():,}, "
            f"neither {(r['adep'].isna() & r['ades'].isna()).sum():,}, "
            f"no callsign {blank.sum():,}, of which named "
            f"{(blank & (r['adep'].notna() | r['ades'].notna())).sum():,}"
        )
    tables, both_tables = [], []
    views = (("callsign", match_callsign), ("overlap", match_overlap))
    # Only tracks starting on the day were built, so a flight landing after
    # midnight has probably lost its second piece in both arms. `all` understates
    # the absolute rates and flatters a merge that stretches a track past midnight.
    # `lands_same_day` is the population with neither problem.
    same_day = truth[truth["t_on"].dt.strftime("%Y-%m-%d") == args.day]
    pops = (
        ("all", truth),
        ("adsc_in_window", truth[truth["adsc_samples"] > 0]),
        ("lands_same_day", same_day),
        ("lands_same_day_adsc", same_day[same_day["adsc_samples"] > 0]),
    )
    for view, fn in views:
        a = {arm: fn(truth, lists[arm]) for arm in ARMS}
        for pop, t in pops:
            tb = pair_table(t.reset_index(drop=True), a["base"], a["merged"], view)
            tables.append(tb.assign(population=pop))
    one = {arm: match_one_record(truth, lists[arm]) for arm in ARMS}
    n_rows = {arm: rows_per_flight(truth, lists[arm]) for arm in ARMS}
    for pop, t in pops:
        t = t.reset_index(drop=True)
        both_tables.append(
            both_ends_table(t, one["base"], one["merged"], n_rows["base"], n_rows["merged"]).assign(
                population=pop
            )
        )
    out_table = pd.concat(tables, ignore_index=True)
    both_table = pd.concat(both_tables, ignore_index=True)

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    name = "adsc_flight_list_truth.csv"
    out_table.to_csv(out / name, index=False)
    both_name = "adsc_flight_list_both_ends.csv"
    both_table.to_csv(out / both_name, index=False)
    far = out_table
    show = [
        "view",
        "population",
        "flow",
        "side",
        "end",
        "n",
        "base_right",
        "merged_right",
        "base_wrong",
        "merged_wrong",
        "base_none",
        "merged_none",
        "none_to_right",
        "wrong_to_right",
        "right_to_none",
        "right_to_wrong",
    ]
    print(far[show].to_string(index=False))
    print(both_table.to_string(index=False))

    import provenance
    from regenerate_adsc_v1 import CORE

    for n in (name, both_name):
        provenance.record(
            out,
            n,
            "benchmarks/adsc_flight_list_truth.py",
            sys.argv[1:],
            ["benchmarks/adsc_flight_list_truth.py", "src/opdi/pipeline/flights.py", *CORE],
            inputs={
                "day": args.day,
                "truth_flights": len(truth),
                "truth": args.truth,
                "truth_sha256_16": provenance.file_hash(Path(args.truth)),
            },
            notes="One day, worldwide, airframes with ADS-C only. NM is the truth.",
            input_tables=[f"{args.prefix}/{arm}/opdi_flight_list" for arm in ARMS],
        )
        print(f"wrote {out / n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
