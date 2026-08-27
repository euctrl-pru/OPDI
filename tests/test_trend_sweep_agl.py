"""The above-field vote cache.

The cache is the v6.1 study's expensive artifact and its *column names* encode
the caps, so a mistake here is not a crash -- it is a sweep that reads the
wrong column and reports a confident number for a cap it never measured. These
tests run the aggregation over a handful of synthetic samples where the right
vote counts are countable by hand.

Local Spark only: no cluster, no credentials.
"""

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest
from pyspark.sql import functions as F

from trend_sweep_agl import FL_CAPS, HEIGHT_CAPS, add_height_votes, predictions

_EPOCH = dt.datetime(2025, 6, 5, 12, 0, 0)

_JOINED = (
    "event_time timestamp, track_id string, icao24 string, flight_id string, "
    "apt_ident string, apt_elev_ft double, baro_altitude double, "
    "flight_level int, dist_nm double, apt_scheduled string"
)

FT_PER_M = 3.28084


def _joined(spark, rows, step_s=30):
    """Rows shaped like the post-join frame the vote aggregation reads.

    Each row is ``(apt_ident, apt_elev_ft, baro_altitude_m)``. ``flight_level``
    is derived exactly as the builder derives it, including the integer cast.
    """
    data = []
    for i, (apt, elev, alt_m) in enumerate(rows):
        data.append((
            _EPOCH + dt.timedelta(seconds=step_s * i),
            "trk-1", "abc123", "KLM1",
            apt,
            None if elev is None else float(elev),
            float(alt_m),
            int(alt_m * FT_PER_M / 100),
            1.0,
            "yes",
        ))
    return spark.createDataFrame(data, _JOINED)


def test_a_climb_above_a_high_field_is_counted_at_the_field_cap(spark):
    """Six samples climbing from 3,960 m to 4,260 m near a 5,763 ft field.

    Those sit about 13,000-14,000 ft above sea level and 7,200-8,200 ft above
    the field. At the 12,000 ft above-field cap every sample votes; at 4,000 ft
    above field none of them do.
    """
    rows = [("LTCE", 5763.0, 3960.0 + 60 * i) for i in range(6)]
    got = add_height_votes(_joined(spark, rows)).first()

    assert got["up_agl_12000"] == 5      # five deltas across six samples
    assert got["dn_agl_12000"] == 0
    assert got["up_agl_4000"] == 0       # all of it is above 4,000 ft AGL


def test_the_field_cap_admits_what_the_flight_level_cap_excludes(spark):
    """The change under test, in the harness rather than the pipeline.

    A climb 2,000-3,000 ft above a 5,763 ft field sits near FL80, so an FL60
    cap sees none of it while a 6,100 ft above-field cap sees all of it.
    """
    alt_ft = [7800.0, 8000.0, 8200.0, 8400.0]
    rows = [("LTCE", 5763.0, ft / FT_PER_M) for ft in alt_ft]
    got = add_height_votes(_joined(spark, rows)).first()

    assert got["up_60"] == 0             # sea-level datum: nothing qualifies
    # Two, not three. The centred five-sample mean over four samples gives the
    # two middle rows the same value, so the delta between them is exactly
    # zero and votes neither way. That is the smoothing behaving correctly on
    # a short series, not a lost vote.
    assert got["up_agl_6100"] == 2       # field datum: the climb is seen


def test_the_cap_columns_exist_for_every_declared_cap(spark):
    """The sweep reads `up_agl_{cap}` by name. A cap in HEIGHT_CAPS with no
    column is an AnalysisException at sweep time, hours after the rebuild.
    """
    got = add_height_votes(_joined(spark, [("EHAM", -11.0, 300.0)]))
    for cap in HEIGHT_CAPS:
        for prefix in ("up_agl", "dn_agl", "dist_agl"):
            assert f"{prefix}_{cap}" in got.columns, f"{prefix}_{cap}"
    for cap in FL_CAPS:
        for prefix in ("up", "dn", "dist"):
            assert f"{prefix}_{cap}" in got.columns, f"{prefix}_{cap}"


def test_an_unknown_elevation_counts_on_the_sea_level_datum(spark):
    """Matching the pipeline: a NULL elevation coalesces to zero rather than
    dropping the aerodrome, so the cache and production agree about which
    samples voted.
    """
    rows = [(None, None, 300.0 + 60 * i) for i in range(4)]
    got = add_height_votes(_joined(spark, rows)).first()
    # 300-480 m is 984-1,575 ft; inside 2,000 ft only on the zero datum. Two
    # votes rather than three for the same reason as above -- the centred mean
    # flattens the middle of a four-sample series.
    assert got["up_agl_2000"] == 2


def test_a_descent_votes_down_not_up(spark):
    """The sign is the whole signal; a flipped comparison would invert every
    arrival and departure while leaving every count plausible."""
    rows = [("EHAM", -11.0, 2000.0 - 100 * i) for i in range(5)]
    got = add_height_votes(_joined(spark, rows)).first()
    assert got["dn_agl_12000"] == 4
    assert got["up_agl_12000"] == 0


def test_predictions_reads_the_family_the_datum_names(spark):
    """`predictions` selects a column family by datum. Reading the sea-level
    family while reporting a field-datum cap is the one failure that would not
    show up as an error anywhere.
    """
    rows = [("LTCE", 5763.0, 7800.0 / FT_PER_M + 60 * i) for i in range(5)]
    votes = add_height_votes(_joined(spark, rows))

    field = predictions(votes, 6100, 0, 30.0, 0.0, datum="field").collect()
    assert len(field) == 1 and field[0]["adep"] == "LTCE"

    # Same flight on the sea-level datum at FL60: no sample qualifies, so the
    # method says nothing rather than saying the same thing.
    assert predictions(votes, 60, 0, 30.0, 0.0, datum="msl").count() == 0


def test_the_height_caps_do_not_widen_the_prefilter():
    """The top of HEIGHT_CAPS silently sets the cost of the whole cache build.

    The pre-filter must admit `max(HEIGHT_CAPS) + highest field`, so raising
    the top cap widens a scan that has nothing to do with the caps anyone would
    ship. At 20,000 ft over a 6,723 ft field that meant FL268 instead of FL200 --
    a third more altitude band, a shuffle that thrashed executors, and no tasks
    completed for the better part of an hour. Nothing failed; it just stopped
    making progress, which is the worst way for a cost regression to present.

    6,723 ft is the highest matchable field measured on the real zone table.
    """
    HIGHEST_FIELD_FT = 6723.0
    implied_fl = int((max(HEIGHT_CAPS) + HIGHEST_FIELD_FT) / 100) + 1
    assert implied_fl <= max(FL_CAPS), (
        f"HEIGHT_CAPS tops out at {max(HEIGHT_CAPS):,} ft, which forces the "
        f"pre-filter to FL{implied_fl} -- above FL{max(FL_CAPS)}, so the cache "
        f"build scans more altitude than it ever has."
    )


def test_an_unknown_datum_is_rejected():
    """Silently defaulting would score one datum and label it the other."""
    with pytest.raises(ValueError, match="datum"):
        predictions(None, 6100, 0, 30.0, 0.0, datum="agl")
