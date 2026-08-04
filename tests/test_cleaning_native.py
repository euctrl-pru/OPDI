"""Unit tests for :mod:`opdi.cleaning.native`.

Each test injects one known defect into an otherwise clean synthetic
trajectory and asserts that exactly that defect is removed. Runs against a
local Spark session -- no cluster, no credentials.
"""

import pytest

from opdi.cleaning.native import (
    add_segment_id,
    clean_tracks,
    drop_duplicate_statevectors,
    mask_derivative_spikes,
    mask_isolated_points,
    mask_out_of_range_positions,
    mask_stale_broadcasts,
    null_rate_report,
)
from opdi.config import CleaningConfig

from conftest import column_values, make_track


@pytest.fixture
def cfg() -> CleaningConfig:
    return CleaningConfig()


# ---------------------------------------------------------------------------
# Stage 1 -- dedup
# ---------------------------------------------------------------------------

def test_dedup_keeps_one_row_per_timestamp(spark, cfg):
    df = make_track(
        spark,
        [
            {"t": 0, "lat": 50.0, "last_contact": 0},
            {"t": 1, "lat": 50.1, "last_contact": 1},
            {"t": 1, "lat": 50.2, "last_contact": 5},  # duplicate timestamp
            {"t": 2, "lat": 50.3, "last_contact": 2},
        ],
    )
    out = drop_duplicate_statevectors(df, cfg)

    assert out.count() == 3
    # The freshest last_contact wins, so the tie-break is deterministic.
    assert column_values(out, "lat") == [50.0, 50.2, 50.3]


def test_dedup_can_be_disabled(spark, cfg):
    cfg.dedup_enabled = False
    df = make_track(spark, [{"t": 1}, {"t": 1}])
    assert drop_duplicate_statevectors(df, cfg).count() == 2


# ---------------------------------------------------------------------------
# Stage 2 -- range validity
# ---------------------------------------------------------------------------

def test_out_of_range_coordinates_are_nulled(spark, cfg):
    df = make_track(
        spark,
        [
            {"t": 0, "lat": 50.0, "lon": 4.0},
            {"t": 1, "lat": 91.0, "lon": 4.0},  # impossible latitude
            {"t": 2, "lat": 50.0, "lon": 181.0},  # impossible longitude
            {"t": 3, "lat": -90.0, "lon": -180.0},  # exactly on the bound: valid
        ],
    )
    out = mask_out_of_range_positions(df, cfg)

    assert column_values(out, "lat") == [50.0, None, 50.0, -90.0]
    assert column_values(out, "lon") == [4.0, 4.0, None, -180.0]


# ---------------------------------------------------------------------------
# Stage 3 -- stale broadcasts
# ---------------------------------------------------------------------------

def test_repeated_position_is_nulled(spark, cfg):
    """Identical consecutive lat/lon means repeated, not re-measured."""
    df = make_track(
        spark,
        [
            {"t": 0, "lat": 50.0, "lon": 4.0},
            {"t": 1, "lat": 50.0, "lon": 4.0},  # stale: neither changed
            {"t": 2, "lat": 50.1, "lon": 4.1},  # genuine update
        ],
    )
    out = mask_stale_broadcasts(df, cfg)

    lat = column_values(out, "lat")
    assert lat[1] is None, "repeated position should be masked"
    assert lat[2] == pytest.approx(50.1), "a genuine update must survive"


def test_repeated_speed_fields_are_nulled(spark, cfg):
    df = make_track(
        spark,
        [
            {"t": 0, "velocity": 200.0, "heading": 90.0, "vert_rate": 0.0},
            {"t": 1, "velocity": 200.0, "heading": 90.0, "vert_rate": 0.0},
            {"t": 2, "velocity": 210.0, "heading": 90.0, "vert_rate": 0.0},
        ],
    )
    out = mask_stale_broadcasts(df, cfg)

    assert column_values(out, "velocity")[1] is None
    # Any one of the three changing counts as a genuine update for all three.
    assert column_values(out, "velocity")[2] == pytest.approx(210.0)
    assert column_values(out, "heading")[2] == pytest.approx(90.0)


def test_first_sample_has_no_predecessor_and_is_masked(spark, cfg):
    """Mirrors ``isvar``: a comparison against a missing neighbour is not
    evidence of an update, so the opening sample cannot be confirmed fresh."""
    df = make_track(spark, [{"t": 0, "lat": 50.0}, {"t": 1, "lat": 50.5}])
    out = mask_stale_broadcasts(df, cfg)
    assert column_values(out, "lat")[0] is None


# ---------------------------------------------------------------------------
# Stage 4 -- derivative spikes
# ---------------------------------------------------------------------------

def _ramp(n: int, step: float = 0.001, spike_at: int = None, spike: float = 0.0):
    """A smooth 1 Hz latitude ramp, optionally with one displaced sample."""
    samples = []
    for i in range(n):
        lat = 50.0 + i * step
        if spike_at is not None and i == spike_at:
            lat += spike
        samples.append({"t": i, "lat": lat})
    return samples


def test_isolated_spike_is_removed(spark, cfg):
    """A single displaced sample collects two first-derivative votes."""
    df = make_track(spark, _ramp(11, spike_at=5, spike=0.5))
    cfg.isolated_enabled = False  # isolate stage 4
    cfg.stale_enabled = False
    out = mask_derivative_spikes(df, cfg)

    lat = column_values(out, "lat")
    assert lat[5] is None, "the spike itself must be removed"
    # Points well clear of the spike are untouched.
    assert lat[0] == pytest.approx(50.0)
    assert lat[10] == pytest.approx(50.0 + 10 * 0.001)


def test_moderate_step_change_survives(spark, cfg):
    """The vote rule exists to spare genuine step changes.

    A step larger than the first-derivative threshold (0.01 deg/s) but smaller
    than the second (0.06 deg/s) trips exactly one first-derivative window, so
    it collects a single vote and lives. This is why the second-derivative
    threshold is set six times higher than the first for lat/lon -- the margin
    between them *is* the tolerance for real manoeuvres.
    """
    samples = [{"t": i, "lat": 50.0 + i * 0.001} for i in range(5)]
    step = 0.03  # between latlon_d1_max_deg_s (0.01) and latlon_d2_max_deg_s (0.06)
    samples += [{"t": 5 + i, "lat": 50.004 + step + i * 0.001} for i in range(5)]

    cfg.stale_enabled = False
    cfg.isolated_enabled = False
    out = mask_derivative_spikes(make_track(spark, samples), cfg)

    lat = column_values(out, "lat")
    assert all(v is not None for v in lat), (
        "a step change between the two thresholds must survive intact; "
        f"got {lat}"
    )


def test_heading_wraparound_is_not_a_spike(spark, cfg):
    """359 deg -> 1 deg is a 2 deg turn, not a 358 deg one."""
    samples = [
        {"t": 0, "heading": 355.0},
        {"t": 1, "heading": 357.0},
        {"t": 2, "heading": 359.0},
        {"t": 3, "heading": 1.0},  # wraps
        {"t": 4, "heading": 3.0},
        {"t": 5, "heading": 5.0},
    ]
    cfg.stale_enabled = False
    cfg.isolated_enabled = False
    out = mask_derivative_spikes(make_track(spark, samples), cfg)

    assert all(v is not None for v in column_values(out, "heading")), (
        "the wrap must not be read as a 358 deg/s rate"
    )


@pytest.mark.parametrize(
    "rate_ft_s, expect_masked",
    [(190.0, False), (210.0, True)],
    ids=["just-under-200ft/s", "just-over-200ft/s"],
)
def test_altitude_threshold_is_read_in_feet_not_metres(
    spark, cfg, rate_ft_s, expect_masked
):
    """Pins the unit contract: metres on disk, ft/s thresholds.

    ``baro_altitude`` is stored in metres but ``baro_altitude_d1_max_ft_s`` is
    200 ft/s. A steady climb either side of that boundary must be judged in
    feet. Were the factor dropped, 210 ft/s would read as 64 m/s -- comfortably
    under 200 -- and the filter would silently never fire. That is the 3.28x
    silent bug this test exists to catch.

    A steady ramp has a near-zero second derivative, so only the first
    derivative is in play and the two cases differ by nothing else.
    """
    metres_per_second = rate_ft_s / 3.28084
    samples = [
        {"t": i, "baro_altitude": 3000.0 + i * metres_per_second} for i in range(8)
    ]
    cfg.stale_enabled = False
    cfg.isolated_enabled = False
    out = mask_derivative_spikes(make_track(spark, samples), cfg)

    interior = column_values(out, "baro_altitude")[1:-1]
    if expect_masked:
        assert all(v is None for v in interior), (
            f"{rate_ft_s} ft/s exceeds the 200 ft/s threshold and must be "
            f"masked; got {interior}"
        )
    else:
        assert all(v is not None for v in interior), (
            f"{rate_ft_s} ft/s is below the 200 ft/s threshold and must "
            f"survive; got {interior}"
        )


def test_aviation_factors_cover_every_thresholded_column(cfg):
    """A threshold without its unit factor is a silent 3.28x error."""
    from opdi.cleaning.native import AVIATION_UNIT_FACTOR, _spike_thresholds

    assert set(_spike_thresholds(cfg)) == set(AVIATION_UNIT_FACTOR)


def test_spike_filter_can_be_disabled(spark, cfg):
    cfg.spike_enabled = False
    df = make_track(spark, _ramp(11, spike_at=5, spike=0.5))
    assert all(v is not None for v in column_values(mask_derivative_spikes(df, cfg), "lat"))


# ---------------------------------------------------------------------------
# Stage 5 -- isolated points
# ---------------------------------------------------------------------------

def test_isolated_point_is_removed(spark, cfg):
    """A sample >20 s from any other measurement of that column is unverifiable."""
    df = make_track(
        spark,
        [
            {"t": 0, "lat": 50.0},
            {"t": 1, "lat": 50.001},
            {"t": 2, "lat": 50.002},
            {"t": 200, "lat": 50.5},  # 198 s from the cluster, 300 s from the next
            {"t": 500, "lat": 51.0},
        ],
    )
    out = mask_isolated_points(df, cfg)

    lat = column_values(out, "lat")
    assert lat[3] is None, "the marooned sample must be dropped"
    assert lat[0] is not None and lat[2] is not None, "the dense cluster survives"


def test_track_boundary_sample_is_kept(spark, cfg):
    """Documented deviation from Alligier: a missing neighbour counts as
    infinitely far, so an opening sample is judged on the side that exists
    rather than discarded for sitting at the boundary."""
    df = make_track(spark, [{"t": 0, "lat": 50.0}, {"t": 1, "lat": 50.001}])
    assert column_values(mask_isolated_points(df, cfg), "lat")[0] is not None


# ---------------------------------------------------------------------------
# Stage 6 -- gap segmentation
# ---------------------------------------------------------------------------

def test_segment_id_splits_on_coverage_hole(spark, cfg):
    df = make_track(
        spark,
        [
            {"t": 0},
            {"t": 10},
            {"t": 20},
            {"t": 20 + 400},  # 400 s hole, beyond the 300 s threshold
            {"t": 20 + 410},
        ],
    )
    segments = column_values(add_segment_id(df, cfg), "segment_id")

    assert segments[:3] == ["trk-1_1"] * 3
    assert segments[3:] == ["trk-1_2"] * 2


def test_segment_id_does_not_split_small_gaps(spark, cfg):
    df = make_track(spark, [{"t": 0}, {"t": 100}, {"t": 250}])
    assert len(set(column_values(add_segment_id(df, cfg), "segment_id"))) == 1


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------

def test_clean_tracks_masks_but_does_not_drop_rows(spark, cfg):
    """The 2024 design: mask to NULL, keep the row. Only dedup changes count."""
    df = make_track(spark, _ramp(20, spike_at=9, spike=0.5))
    out = clean_tracks(df, cfg)

    assert out.count() == df.count()
    assert "segment_id" in out.columns


def test_clean_tracks_respects_master_switch(spark, cfg):
    cfg.enabled = False
    df = make_track(spark, _ramp(5))
    out = clean_tracks(df, cfg)
    assert "segment_id" not in out.columns, "disabled cleaning must be a no-op"


def test_clean_tracks_is_stable_on_a_pristine_track(spark, cfg):
    """No defects in, no positions masked out -- beyond the documented
    first-sample rule from the stale-broadcast stage."""
    df = make_track(spark, _ramp(30))
    lat = column_values(clean_tracks(df, cfg), "lat")

    assert lat[0] is None  # opening sample, see test_first_sample_...
    assert all(v is not None for v in lat[1:]), f"clean data was damaged: {lat}"


def test_null_rate_report_counts_masked_values(spark, cfg):
    df = make_track(
        spark,
        [
            {"t": 0, "lat": 50.0},
            {"t": 1, "lat": None},
            {"t": 2, "lat": None},
            {"t": 3, "lat": 50.3},
        ],
    )
    report = null_rate_report(df, ["lat", "lon"])

    assert report["lat"] == pytest.approx(0.5)
    assert report["lon"] == pytest.approx(0.0)
