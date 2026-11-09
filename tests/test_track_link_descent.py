"""Holes whose new edge is descending: the export the descent question is decided on."""

import csv
import json
from dataclasses import replace

import pytest
from conftest import make_track
from pyspark.sql import functions as F
from test_track_link_controls import CRUISE_KT, _flight, _truth

import track_link_descent as d

from opdi.pipeline.segmentation import SegmentationParams
from opdi.pipeline.segmentation.base import FT_PER_M

SHIPPED = SegmentationParams()
DESCENT_M = 12000.0 / FT_PER_M


def _with_box(truth, crosses=True):
    return truth.withColumn("crosses_box", F.lit(crosses))


def _flight_with_a_hole(spark, hole_from_min, hole_to_min, cruise_min=240, low_m=DESCENT_M,
                        low_fpm=1000.0, callsign_after=None, descent_min=15):
    """One flight whose samples between the two minutes (from take-off) are deleted."""
    samples, truth, _, _ = _flight(0, cruise_min, low_m=low_m, low_fpm=low_fpm,
                                   descent_min=descent_min)
    kept = [s for s in samples if not (hole_from_min * 60 < s["t"] < hole_to_min * 60)]
    if callsign_after is not None:
        for s in kept:
            if s["t"] >= hole_to_min * 60:
                s["callsign"] = callsign_after
    return make_track(spark, kept), _with_box(_truth(spark, [truth]))


def test_a_descending_new_edge_hole_is_exported_with_its_label_and_features(spark):
    # Cruise is minutes 5..245; the descent at 12,000 ft, -1,000 fpm, follows. Cut from
    # minute 60 in cruise to minute 250 on the descent: a 190 min hole.
    tracks, truth = _flight_with_a_hole(spark, 60, 250)
    rows = d.descent_holes(tracks, truth, SHIPPED).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["label"] == "true_link"
    assert row["population"] == "crosses_box"
    assert row["alt_after_ft"] == pytest.approx(12000.0, abs=1.0)
    assert row["vr_after_fpm"] == pytest.approx(-1000.0, abs=1.0)
    assert row["vr_after_samples"] >= 3
    assert row["callsign_state"] == "both_equal"
    assert row["other_checks_pass"] is True
    assert row["implied_kt"] == pytest.approx(CRUISE_KT * (245 - 60) / 190, rel=0.02)
    assert row["gap_min"] == pytest.approx(190.0)


def test_a_hole_whose_new_edge_is_at_cruise_is_not_exported(spark):
    tracks, truth = _flight_with_a_hole(spark, 60, 200)   # both edges in cruise
    assert d.descent_holes(tracks, truth, SHIPPED).count() == 0


def test_a_hole_whose_new_edge_is_below_five_thousand_feet_is_exported_but_marked(spark):
    tracks, truth = _flight_with_a_hole(spark, 60, 250, low_m=4000.0 / FT_PER_M)
    row = d.descent_holes(tracks, truth, SHIPPED).first()
    assert row is not None
    assert row["descent_edge"] is False


@pytest.mark.parametrize("before,after,state", [
    ("DLH1", "DLH1", "both_equal"),
    ("DLH1", "BAW2", "differ"),
    ("DLH1", None, "one_blank"),
    (None, None, "both_blank"),
])
def test_callsign_state_names_the_four_cases(spark, before, after, state):
    frame = spark.createDataFrame([(before, after)], "cs_before string, cs_after string")
    assert frame.select(d.callsign_state().alias("s")).first()["s"] == state


def test_a_hole_with_a_different_callsign_after_fails_the_other_checks(spark):
    tracks, truth = _flight_with_a_hole(spark, 60, 250, callsign_after="BAW2")
    row = d.descent_holes(tracks, truth, SHIPPED).first()
    assert row["callsign_state"] == "differ"
    assert row["other_checks_pass"] is False


@pytest.mark.parametrize("turnaround_min,speed_passes", [(10, True), (60, False)])
def test_a_cruise_descent_control_row_says_whether_the_rule_joined_it(spark, turnaround_min, speed_passes):
    # Two consecutive flights; the second's descent is at 12,000 ft, -1,000 fpm, so the
    # cruise_descent family can cut from the first flight's cruise into it. A long
    # turnaround lowers the straight-line speed across the cut below 400 kt, and the
    # other checks then reject the hidden landing before the edge is even asked.
    s1, t1, _, last_1 = _flight(0, 180, key="F1", low_m=DESCENT_M, low_fpm=1000.0)
    start_2 = last_1 + 5 * 60 + turnaround_min * 60
    s2, t2, _, _ = _flight(start_2, 180, start_nm=180 * CRUISE_KT / 60.0, key="F2",
                           low_m=DESCENT_M, low_fpm=1000.0)
    tracks = make_track(spark, s1 + s2)
    truth = _with_box(_truth(spark, [t1, t2]))
    rows = d.descent_controls(tracks, truth, SHIPPED).collect()
    assert rows, "the pair should give at least one cruise_descent control"
    for row in rows:
        assert row["alt_after_ft"] == pytest.approx(12000.0, abs=1.0)
        assert row["vr_after_fpm"] == pytest.approx(-1000.0, abs=1.0)
        assert row["callsign_state"] == "both_equal"
        assert row["descent_edge"] is True
        # With descent allowed, the rule joins exactly the controls nothing else rejects.
        assert row["joined"] is row["other_checks_pass"]
    shallow = min(rows, key=lambda r: r["depth_min"])
    assert shallow["other_checks_pass"] is speed_passes


def test_the_export_writes_provenance(tmp_path):
    rows = [{"track_id": "a", "label": "true_link"}]
    d.write_rows(tmp_path, "descent_holes_2026-06-14.csv", rows, argv=["--days", "2026-06-14"],
                 input_tables=["s3a://x/osn_tracks/dof=2026-06-14"])
    with open(tmp_path / "descent_holes_2026-06-14.csv") as fh:
        assert list(csv.DictReader(fh)) == rows
    manifest = json.loads((tmp_path / "_manifest.json").read_text())
    entry = manifest["descent_holes_2026-06-14.csv"]
    assert entry["argv"] == ["--days", "2026-06-14"]
    assert "benchmarks/track_link_descent.py" in entry["code_paths"]
    assert "src/opdi/pipeline/segmentation/methods.py" in entry["code_paths"]
