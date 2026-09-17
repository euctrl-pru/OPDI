"""``_add_clean_altitude`` masks; it never substitutes.

It used to replace an implausible altitude with a +/-5 minute centred rolling
mean, which during a climb is roughly the altitude the aircraft reaches
*minutes later*. On 2026-06-10 that rewrote a smooth climb through 8,000 ft to
11,125 ft for two samples, and the FL crossing detector -- which reads
``baro_altitude_c`` -- duly reported a crossing up through FL100 and back down
again. 23,088 such reversals that day, across 6,651 flights.

The threshold moved with it. ``max_vertical_rate_mps`` was 25.4 m/s = 83.3
ft/s = 5,000 ft/min, which jets exceed routinely; of the 119,154 samples it
rejected that day, 64,088 (53.8%) were climbing between 83.3 and 200 ft/s and
were perfectly good. The bound is now
``CleaningConfig.{baro,geo}_altitude_d1_max_ft_s`` -- the Alligier first
derivative the vote-based filter in ``cleaning.native`` already uses.
"""

import datetime as dt

import pytest
from pyspark.sql import Row

from opdi.config import OPDIConfig
from opdi.pipeline.tracks import TrackProcessor

#: m per ft, for stating fixtures in feet and storing them in metres.
FT = 1.0 / 3.28084


def _proc(spark, tmp_path):
    return TrackProcessor(spark, OPDIConfig(),
                          log_file_path=str(tmp_path / "log.parquet"))


def _climb(spark, feet, step_s=5):
    """One track, one sample every *step_s*, at the given altitudes in feet."""
    t0 = dt.datetime(2026, 6, 10, 16, 21, 24)
    return spark.createDataFrame([
        Row(track_id="t1", event_time=t0 + dt.timedelta(seconds=i * step_s),
            baro_altitude=float(ft) * FT)
        for i, ft in enumerate(feet)
    ])


def _clean_ft(proc, df):
    rows = proc._add_clean_altitude(df, "baro_altitude").orderBy("event_time")
    return [None if r["baro_altitude_c"] is None
            else round(r["baro_altitude_c"] * 3.28084)
            for r in rows.collect()]


def test_a_normal_jet_climb_survives(spark, tmp_path):
    # The BEL5LY samples: 85 ft/s, over the old 83.3 ft/s bound and well under
    # the 200 ft/s one. Every value must come through untouched.
    out = _clean_ft(_proc(spark, tmp_path),
                    _climb(spark, [7200, 7600, 8025, 8400, 8750]))
    assert out == [7200, 7600, 8025, 8400, 8750]


def test_a_real_spike_is_masked_not_replaced(spark, tmp_path):
    # 8,000 -> 20,000 ft in 5 s is 2,400 ft/s. The sample must become NULL --
    # not a rolling mean, which is a number nothing downstream can tell from a
    # measurement.
    out = _clean_ft(_proc(spark, tmp_path),
                    _climb(spark, [8000, 8400, 20000, 9200, 9600]))
    assert out[2] is None
    assert out[0] == 8000 and out[4] == 9600


def test_no_value_is_ever_invented(spark, tmp_path):
    # The property that makes the column safe: every non-null output equals
    # its input. Previously 66,130 rows a day failed this by more than 1,000 ft.
    feet = [7200, 7600, 8025, 40000, 8750, 9100, 90000, 9425]
    out = _clean_ft(_proc(spark, tmp_path), _climb(spark, feet))
    for got, want in zip(out, feet):
        assert got is None or got == want


def test_the_first_sample_of_a_track_is_kept(spark, tmp_path):
    # It has nothing to be implausible against; a NULL rate must not mask it.
    out = _clean_ft(_proc(spark, tmp_path), _climb(spark, [7200, 7600]))
    assert out[0] == 7200


def test_duplicate_timestamps_do_not_divide_by_zero(spark, tmp_path):
    t0 = dt.datetime(2026, 6, 10, 16, 21, 24)
    df = spark.createDataFrame([
        Row(track_id="t1", event_time=t0, baro_altitude=7200.0 * FT),
        Row(track_id="t1", event_time=t0, baro_altitude=7300.0 * FT),
    ])
    out = _clean_ft(_proc(spark, tmp_path), df)
    assert out[0] == 7200


def test_geo_altitude_uses_its_own_threshold(spark, tmp_path):
    t0 = dt.datetime(2026, 6, 10, 16, 21, 24)
    df = spark.createDataFrame([
        Row(track_id="t1", event_time=t0 + dt.timedelta(seconds=i * 5),
            geo_altitude=float(ft) * FT)
        for i, ft in enumerate([7350, 7775, 8225, 8625])
    ])
    rows = _proc(spark, tmp_path)._add_clean_altitude(df, "geo_altitude")
    assert all(r["geo_altitude_c"] is not None for r in rows.collect())


def test_the_threshold_comes_from_the_cleaning_config(spark, tmp_path):
    # Not a literal in the function, and not the retired ingestion setting.
    cfg = OPDIConfig()
    cfg.cleaning.baro_altitude_d1_max_ft_s = 10.0      # absurdly strict
    proc = TrackProcessor(spark, cfg, log_file_path=str(tmp_path / "l.parquet"))
    out = _clean_ft(proc, _climb(spark, [7200, 7600, 8025]))
    assert out[1] is None and out[2] is None


def test_the_retired_ingestion_settings_are_gone(spark, tmp_path):
    # Left in place they would read as knobs that still did something.
    cfg = OPDIConfig()
    assert not hasattr(cfg.ingestion, "max_vertical_rate_mps")
    assert not hasattr(cfg.ingestion, "altitude_smoothing_window_minutes")
