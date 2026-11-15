"""The barometric altitude the detectors of step 04 read.

The reports below are reduced from the spot checks of 2026-06-14 (the
``_reports.parquet`` files beside the example sheets). In those files
``alt_m`` is ``baro_altitude_c`` and ``raw_alt_m`` the cleaned
``baro_altitude`` of ``osn_tracks_clean``.
"""

import datetime as dt
import tempfile

from pyspark.sql import functions as F

from opdi.config import EventConfig, OPDIConfig
from opdi.pipeline.detector_altitude import altitude_for_detectors, flight_altitude_only

FT_PER_M = 3.28084
T0 = dt.datetime(2026, 6, 14, 18, 14, 29)
SCHEMA = (
    "track_id string, event_time timestamp, baro_altitude_c double, baro_altitude double, "
    "on_ground boolean"
)


def _frame(spark, rows):
    """``rows``: (seconds after T0, baro_altitude_c ft, cleaned ft, on_ground)."""
    return spark.createDataFrame(
        [
            (
                "trk",
                T0 + dt.timedelta(seconds=s),
                None if c is None else c / FT_PER_M,
                None if r is None else r / FT_PER_M,
                g,
            )
            for s, c, r, g in rows
        ],
        SCHEMA,
    )


def _feet(df):
    return [
        None if r.baro_altitude_c is None else round(r.baro_altitude_c * FT_PER_M)
        for r in df.orderBy("event_time").collect()
    ]


#: Climb of track 85a7635a256b7f3a_1781460849 at 18:14:29 to 18:15:29. OpenSky
#: repeats 7,200 ft and 8,250 ft on reports with no new altitude message; the
#: cleaning removed the repeats, and the step 02 check removed the fresh
#: 8,250 ft and 10,275 ft because it compared them with a repeat 5 s before.
CLIMB_WITH_REPEATS = [
    (0, 7200, 7200, False),
    (5, 7200, None, False),
    (10, 7200, None, False),
    (15, 7200, None, False),
    (20, None, 8250, False),
    (25, 8250, None, False),
    (30, 8250, None, False),
    (60, None, 10275, False),
]


def test_the_detectors_read_the_cleaned_altitude(spark):
    out = altitude_for_detectors(
        _frame(spark, CLIMB_WITH_REPEATS), EventConfig(), cleaned_tracks=True
    )
    assert "baro_altitude" not in out.columns
    assert _feet(out) == [7200, None, None, None, 8250, None, None, 10275]


def test_the_uncleaned_tracks_keep_their_checked_altitude(spark):
    """Without the cleaned tracks ``baro_altitude`` is the received value."""
    out = altitude_for_detectors(
        _frame(spark, CLIMB_WITH_REPEATS), EventConfig(), cleaned_tracks=False
    )
    assert _feet(out) == [7200, 7200, 7200, 7200, None, 8250, 8250, None]


def test_the_release_v002_settings_change_nothing(spark):
    frame = _frame(spark, CLIMB_WITH_REPEATS)
    assert altitude_for_detectors(frame, EventConfig.legacy(), cleaned_tracks=True) is frame


#: Track 6b33e578e1d3c3ef_1781442869 on the ground before take-off: one
#: altitude of 60,800 ft kept by the cleaning (no measured neighbour to judge
#: it by), then the take-off at 200 ft. Below it, an altitude under the
#: plausible range.
GHOST_ON_THE_GROUND = [
    (0, 60800, 60800, True),
    (25, 200, 200, False),
    (35, 175, 175, False),
    (45, -2500, -2500, False),
    (55, 59000, 59000, False),
]


def test_an_altitude_outside_the_plausible_range_is_missing(spark):
    out = altitude_for_detectors(
        _frame(spark, GHOST_ON_THE_GROUND), EventConfig(), cleaned_tracks=True
    )
    assert _feet(out) == [None, 200, 175, None, 59000]


def test_the_range_alone_applies_to_the_uncleaned_tracks(spark):
    config = EventConfig(altitude_from_cleaning=False)
    out = altitude_for_detectors(_frame(spark, GHOST_ON_THE_GROUND), config, cleaned_tracks=True)
    assert "baro_altitude" in out.columns
    assert _feet(out) == [None, 200, 175, None, 59000]


#: Track 0fdae39e1a3b3676_1781469089 at its stand at KBOS: one report reads
#: 125,400 ft; and a report on the ground at the field's altitude.
def test_the_vertical_detectors_ignore_altitudes_on_the_ground(spark):
    frame = _frame(spark, [(0, 125400, 125400, True), (5, 25, 25, True), (60, 900, 900, False)])
    out = flight_altitude_only(frame, EventConfig())
    assert _feet(out) == [None, None, 900]
    assert flight_altitude_only(frame, EventConfig.legacy()) is frame


def test_step_04_reads_the_cleaned_altitude(spark, monkeypatch):
    """``process_day`` puts the cleaned altitude in ``baro_altitude_c``."""
    from opdi.pipeline.events import TRACK_COLUMNS, FlightEventProcessor

    rows = _frame(spark, CLIMB_WITH_REPEATS).withColumn("dof", F.lit(T0.date()))
    for name in TRACK_COLUMNS:
        if name not in rows.columns:
            rows = rows.withColumn(name, F.lit(None).cast("string"))

    class _Storage:
        def table_exists(self, name):
            return name == "osn_tracks_clean"

        def read_table(self, name):
            return rows

    seen = {}

    def capture(self, sdf_input, **kwargs):
        seen["frame"] = sdf_input

    monkeypatch.setattr(FlightEventProcessor, "_etl_flight_events_and_measures", capture)
    with tempfile.TemporaryDirectory() as log_dir:
        proc = FlightEventProcessor(spark, OPDIConfig(), log_dir=log_dir)
        proc.storage = _Storage()
        proc.process_day(T0.date(), skip_if_processed=False)
    assert "baro_altitude" not in seen["frame"].columns
    assert _feet(seen["frame"]) == [7200, None, None, None, 8250, None, None, 10275]


def _departure_with_a_ghost(spark):
    """Shape of xing-fl50#32 (bdc4e056e366eeb8_1781430684): an A321 on the
    ground at KBOS sends one altitude of 9,800 ft, then takes off and climbs
    to 6,000 ft."""
    from conftest import make_track

    samples = [{"t": 0, "baro_altitude": 9800 / FT_PER_M, "on_ground": True}]
    samples += [{"t": t, "baro_altitude": None, "on_ground": True} for t in range(5, 60, 5)]
    samples += [{"t": 60 + 5 * i, "baro_altitude": (100 + 300 * i) / FT_PER_M} for i in range(21)]
    return (
        make_track(spark, samples)
        .withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("cumulative_distance_nm", F.lit(0.0))
        .withColumn("cumulative_time_s", F.lit(0).cast("long"))
    )


def test_a_ghost_altitude_on_the_ground_makes_no_crossing(spark):
    import json

    from opdi.pipeline.events import calculate_threshold_crossing_events

    sdf = _departure_with_a_ghost(spark)

    def fl50(config):
        rows = calculate_threshold_crossing_events(flight_altitude_only(sdf, config), config)
        rows = sorted(
            (r for r in rows.collect() if r.type == "xing-fl50"), key=lambda r: r.event_time
        )
        return [json.loads(r.info)["direction"] for r in rows]

    assert fl50(EventConfig(vertical_ignores_ground_altitude=False)) == ["down", "up"]
    assert fl50(EventConfig()) == ["up"]


def test_a_ghost_altitude_on_the_ground_is_not_the_top_of_climb(spark):
    """Shape of top-of-climb#43 (0fdae39e1a3b3676_1781469089): a report at the
    stand reads 125,400 ft (here 9,800 ft, inside the plausible range)."""
    from opdi.pipeline.vertical_pru import pru_tops

    sdf = _departure_with_a_ghost(spark).withColumn("dist_adep_nm", F.lit(1.0))
    sdf = sdf.withColumn("dist_ades_nm", F.lit(300.0))
    segments = spark.createDataFrame(
        [],
        "track_id string, start_time timestamp, end_time timestamp, "
        "duration_seconds double, level_ft double, distance_nm double",
    )

    def top(config):
        return pru_tops(flight_altitude_only(sdf, config), segments, config).first()

    assert round(top(EventConfig(vertical_ignores_ground_altitude=False)).toc_d200_alt_ft) == 9800
    assert round(top(EventConfig()).toc_d200_alt_ft) == 6100


#: The settings of work package 1 and their value under release v0.0.2.
WP1_SETTINGS = {
    "altitude_from_cleaning": False,
    "altitude_plausible_min_ft": None,
    "altitude_plausible_max_ft": None,
    "vertical_ignores_ground_altitude": False,
    "level_max_sample_gap_seconds": None,
    "level_segment_min_duration_applied": False,
    "level_segment_not_on_ground": False,
    "tops_position_from_nearest_report": False,
    "tops_observed_min_change_ft": None,
    "level_offs_get_position": False,
}


def test_the_corrections_are_on_by_default_and_off_under_release_v002():
    shipped, legacy = EventConfig(), EventConfig.legacy()
    for name, off in WP1_SETTINGS.items():
        assert getattr(legacy, name) == off, name
        assert getattr(shipped, name) not in (None, False), name
