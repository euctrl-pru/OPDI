"""``compare`` measures ids, not partitions, and the tests say why that matters.

The pure-rename case is the whole reason this module exists: ``legacy``
suffixes ``track_id`` with ``_{year}_{month}`` and the other two arms do not,
so a change that leaves every flight boundary exactly where it was still
invalidates every key a consumer holds. A partition comparison scores that as
perfect. This one must score it as total loss.

No Spark: ``compare`` takes ``(track_id, icao24)`` pairs, so the whole contract
is testable in memory.
"""

import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import track_continuity as tc  # noqa: E402


def _rows(*pairs):
    return list(pairs)


def test_identical_partitions_report_full_continuity():
    a = _rows(("t1", "aa1111"), ("t2", "aa1111"), ("t3", "bb2222"))
    r = tc.compare(a, list(a))
    assert r["n_before"] == r["n_after"] == 3
    assert r["identical_ids"] == 3
    assert r["identical_pct"] == 100.0
    assert r["mean_tracks_per_airframe_before"] == 1.5
    assert r["mean_tracks_per_airframe_after"] == 1.5


def test_a_pure_rename_is_a_total_break_at_equal_counts():
    """Same flights, same boundaries, different spelling -- 0%, not 100%.

    This is exactly legacy's ``_2025_6`` suffix against an arm without one.
    Equal counts and equal tracks-per-airframe on both sides are what identify
    it as a rename rather than a merge or a split.
    """
    before = _rows(("t1_2025_6", "aa1111"), ("t2_2025_6", "aa1111"),
                   ("t3_2025_6", "bb2222"))
    after = _rows(("t1", "aa1111"), ("t2", "aa1111"), ("t3", "bb2222"))
    r = tc.compare(before, after)
    assert r["identical_ids"] == 0
    assert r["identical_pct"] == 0.0
    assert r["n_before"] == r["n_after"] == 3
    assert (r["mean_tracks_per_airframe_before"]
            == r["mean_tracks_per_airframe_after"] == 1.5)


def test_merging_two_tracks_halves_tracks_per_airframe():
    """The measurement that tells a merge apart from a rename.

    Both score 0% identical when the ids differ, so tracks-per-airframe is the
    only thing in the row that says which one happened.
    """
    before = _rows(("a1", "aa1111"), ("a2", "aa1111"),
                   ("b1", "bb2222"), ("b2", "bb2222"))
    after = _rows(("m1", "aa1111"), ("m2", "bb2222"))
    r = tc.compare(before, after)
    assert r["mean_tracks_per_airframe_before"] == 2.0
    assert r["mean_tracks_per_airframe_after"] == 1.0
    assert r["n_before"] == 4
    assert r["n_after"] == 2


def test_partial_survival_is_a_share_of_before():
    """Two of before's three ids survive; after gains one of its own.

    The denominator is ``before`` deliberately: the question is what fraction
    of already-published ids still resolve, not what fraction of the new table
    is old. Using ``after`` here would report 2/3 by coincidence and 1/2 as
    soon as the new table grew.
    """
    before = _rows(("t1", "aa1111"), ("t2", "aa1111"), ("t3", "bb2222"))
    after = _rows(("t1", "aa1111"), ("t2", "aa1111"),
                  ("t9", "bb2222"), ("t8", "cc3333"))
    r = tc.compare(before, after)
    assert r["identical_ids"] == 2
    assert r["identical_pct"] == 100.0 * 2 / 3
    assert r["n_before"] == 3
    assert r["n_after"] == 4


def test_empty_before_does_not_divide_by_zero():
    r = tc.compare([], [("t1", "aa1111")])
    assert r["identical_pct"] == 0.0
    assert r["mean_tracks_per_airframe_before"] == 0.0


def test_read_extents_takes_only_the_identity_columns(tmp_path):
    """The extents file carries t_start/t_end/n_points; continuity ignores them."""
    path = tmp_path / tc.extents_name("legacy", "2025")
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(
            fh, fieldnames=["track_id", "icao24", "t_start", "t_end", "n_points"])
        w.writeheader()
        w.writerow({"track_id": "t1", "icao24": "aa1111",
                    "t_start": "2025-06-05 00:00:00",
                    "t_end": "2025-06-05 01:00:00", "n_points": "120"})
    assert tc.read_extents(path) == [("t1", "aa1111")]


def test_comparisons_cover_both_ablation_steps_and_the_end_to_end_pair():
    """legacy->standard is not implied by the two steps and must be measured.

    Continuity does not compose: an id that survives legacy->airframe_only can
    still be renamed by airframe_only->standard, so multiplying the two
    percentages is not the third.
    """
    assert tc.COMPARISONS == [
        ("legacy", "airframe_only"),
        ("airframe_only", "standard"),
        ("legacy", "standard"),
    ]
