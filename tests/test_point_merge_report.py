"""Point merge usage per airport from the events and the flight list."""

import json

import pandas as pd
import point_merge_report as pr


def _ev(fid, typ, **info):
    return {"flight_id": fid, "type": typ, "info": json.dumps(info)}


def test_usage_table_counts_arrivals_with_a_leg_per_airport():
    ring = lambda f, apt, d="inbound": _ev(f, "xing-40nm", apt_icao=apt, direction=d)  # noqa: E731
    events = pd.DataFrame([
        ring("a", "EIDW"), ring("b", "EIDW"), ring("d", "EIDW"), ring("c", "ENGM"), ring("e", "ENGM"),
        ring("f", "LPPT"), ring("g", "EIDW"),            # g is not in the flight list for EIDW
        _ev("x", "xing-40nm", apt_icao="EIDW", direction="outbound"),
        _ev("a", "pms-leg-exit", apt_icao="EIDW", leg_distance_nm=15.8, leg_level_ft=7000, duration_s=200),
        _ev("a", "pms-merge-point", apt_icao="EIDW", via_leg=True),
        _ev("b", "pms-merge-point", apt_icao="EIDW", via_leg=False),
        _ev("c", "pms-leg-exit", apt_icao="ENGM", leg_distance_nm=18.1, leg_level_ft=9000, duration_s=120),
        _ev("c", "pms-leg-exit", apt_icao="ENGM", leg_distance_nm=18.3, leg_level_ft=9000, duration_s=100),
    ])
    fl = pd.DataFrame({"id": ["a", "b", "d", "c", "e", "f", "h"], "ades": ["EIDW"] * 3 + ["ENGM"] * 2 + ["LPPT", "EIDW"]})
    t = pr.usage_table(events, fl, ["EIDW", "ENGM", "LPPT"]).set_index("apt_icao")
    # h is named as destination but never crossed the ring; g crossed it but is
    # not named: neither is an arrival.
    assert t.loc["EIDW", "named_as_destination"] == 4
    assert t.loc["EIDW", "arrivals"] == 3 and t.loc["EIDW", "arrivals_with_leg"] == 1
    assert t.loc["EIDW", "leg_share_pct"] == 33.3
    assert t.loc["EIDW", "arrivals_at_merge_point"] == 2 and t.loc["EIDW", "merge_point_without_leg"] == 1
    assert t.loc["ENGM", "legs"] == 2 and t.loc["ENGM", "arrivals_with_leg"] == 1
    assert t.loc["ENGM", "median_leg_distance_nm"] == 18.2
    assert t.loc["LPPT", "arrivals"] == 1 and t.loc["LPPT", "arrivals_with_leg"] == 0
    assert pd.isna(t.loc["LPPT", "median_leg_level_ft"])


def test_usage_table_with_no_events():
    fl = pd.DataFrame({"id": ["a"], "ades": ["EIDW"]})
    t = pr.usage_table(pd.DataFrame(columns=["flight_id", "type", "info"]), fl, ["EIDW"])
    assert t.iloc[0]["arrivals"] == 0 and t.iloc[0]["arrivals_with_leg"] == 0
    assert pd.isna(t.iloc[0]["leg_share_pct"])
