"""Tests for the step-04 processor's wiring.

The detector functions are tested elsewhere. These cover the plumbing that
decides *which* detector runs, what it stamps, and where it reads from -- the
part that turns a configuration into published data, and the part that was
carrying inline literals until now.
"""

import pytest
from pyspark.sql import functions as F

from conftest import make_track

from opdi.config import EventConfig, OPDIConfig
from opdi.pipeline.events import FlightEventProcessor


@pytest.fixture
def processor(spark, tmp_path):
    def build(events: EventConfig) -> FlightEventProcessor:
        config = OPDIConfig()
        config.events = events
        return FlightEventProcessor(spark, config, log_dir=str(tmp_path / "logs"))

    return build


def test_the_processor_reads_its_thresholds_from_the_config(processor):
    """Until this existed, every number in step 04 was an inline literal."""
    events = EventConfig()
    assert processor(events).events is events


def test_a_config_without_events_still_builds(processor, spark, tmp_path):
    """An OPDIConfig from an older pickle or a hand-built stub must not crash
    the step; it falls back to the shipped defaults."""
    config = OPDIConfig()
    del config.events

    proc = FlightEventProcessor(spark, config, log_dir=str(tmp_path / "l2"))

    assert isinstance(proc.events, EventConfig)


def test_the_version_stamped_is_the_configured_one(processor):
    assert processor(EventConfig()).events.events_version == "events_v0.1.0"
    assert processor(EventConfig.legacy()).events.events_version == "events_v0.0.2"


def _ids(spark, proc, rows):
    df = spark.createDataFrame(rows, "track_id string, type string, version string").withColumn(
        "event_time", F.to_timestamp(F.lit("2024-06-01 12:00:00"))
    )
    return [r.id for r in df.withColumn("id", proc._event_id("batch_")).collect()]


def test_event_ids_are_reproducible_across_runs(processor, spark):
    """``monotonically_increasing_id`` encodes the partition index, so the same
    event gets a different id every run and two runs of one month cannot be
    reconciled -- which matters because the write path appends."""
    proc = processor(EventConfig())
    rows = [("trk-1", "take-off", "events_v0.1.0")]

    assert _ids(spark, proc, rows) == _ids(spark, proc, rows)


def test_distinct_events_get_distinct_ids(processor, spark):
    proc = processor(EventConfig())
    ids = _ids(
        spark,
        proc,
        [
            ("trk-1", "take-off", "events_v0.1.0"),
            ("trk-1", "landing", "events_v0.1.0"),
            ("trk-2", "take-off", "events_v0.1.0"),
        ],
    )

    assert len(set(ids)) == 3


def test_the_version_participates_in_the_identity(processor, spark):
    """Two algorithm versions may both describe the same milestone of the same
    track. They must not collide."""
    proc = processor(EventConfig())
    ids = _ids(
        spark,
        proc,
        [("trk-1", "take-off", "events_v0.0.2"), ("trk-1", "take-off", "events_v0.1.0")],
    )

    assert ids[0] != ids[1]


def test_legacy_keeps_the_unreproducible_ids(processor, spark):
    """Not an improvement worth back-porting: legacy must reproduce what was
    published, warts included."""
    proc = processor(EventConfig.legacy())
    rows = [("trk-1", "take-off", "events_v0.0.2")]

    got = _ids(spark, proc, rows)

    assert got[0].startswith("batch_")


def test_measurement_ids_hang_off_their_milestone(processor, spark):
    proc = processor(EventConfig())
    df = (
        spark.createDataFrame([("abc",)], "id_tmp string")
        .withColumn("d", proc._measurement_id("batch_", "_d_"))
        .withColumn("t", proc._measurement_id("batch_", "_t_"))
    )
    row = df.collect()[0]

    assert row.d == "abc_d"
    assert row.t == "abc_t"
    assert row.d != row.t
