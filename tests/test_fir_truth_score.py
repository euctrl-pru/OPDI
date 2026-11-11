"""Scoring xing-fir against NM airspace profiles, on synthetic frames."""

import json

import fir_truth_score as ft
import pandas as pd

T0 = pd.Timestamp("2026-06-14 10:00:00")


def test_truth_crossings_come_from_consecutive_volumes():
    p = pd.DataFrame({
        "ID": [1, 1, 1, 2], "SEQ_ID": [1, 2, 3, 1],
        "AIRSPACE_ID": ["EBBUFIR", "EDGGFIR", "EDGGFIR", "LFFFUIR"],
        "EXIT_TIME": [T0, T0 + pd.Timedelta(minutes=20), T0 + pd.Timedelta(minutes=30), T0],
        "AIRCRAFT_ADDRESS": ["4CA123", "4CA123", "4CA123", "3C6444"], "CALLSIGN": ["BAW1 ", "BAW1 ", "BAW1 ", "DLH2"],
    })
    t = ft.truth_crossings(p)
    assert len(t) == 1
    assert t.iloc[0].to_dict() == {"icao24": "4ca123", "callsign": "BAW1", "fir_from": "EBBUFIR", "fir_to": "EDGGFIR", "t": T0}


def test_score_matches_on_flight_pair_and_time():
    truth = pd.DataFrame({"icao24": ["4ca123", "4ca123"], "callsign": ["BAW1", "BAW1"],
                          "fir_from": ["EBBUFIR", "EDGGFIR"], "fir_to": ["EDGGFIR", "EDMMFIR"],
                          "t": [T0, T0 + pd.Timedelta(minutes=30)]})
    ev = pd.DataFrame({
        "icao24": ["4CA123", "4CA123", "aaaaaa"], "callsign": ["BAW1", "BAW1", "XXX"],
        "event_time": [T0 + pd.Timedelta(seconds=40), T0 + pd.Timedelta(minutes=40), T0],
        "info": [json.dumps({"fir_from": "EBBUFIR", "fir_to": "EDGGFIR"}),
                 json.dumps({"fir_from": "EDGGFIR", "fir_to": "EDMMFIR"}),
                 json.dumps({"fir_from": "A", "fir_to": "B"})],
    })
    s = ft.score_crossings(ft.opdi_crossings(ev), truth, tolerance_s=120.0)
    assert s["matched"] == 1 and s["recall"] == 0.5
    assert s["opdi_crossings_on_truth_flights"] == 2 and s["precision"] == 0.5
    assert s["offset_p50_s"] == 40.0
