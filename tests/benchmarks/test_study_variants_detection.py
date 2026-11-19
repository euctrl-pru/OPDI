"""The flight-list options that moved to benchmarks/study_variants/detection.py."""

import pytest
from pyspark.sql import functions as F
from study_variants.detection import StudyDetectionConfig, StudyFlightListProcessor
from test_flight_detection import _EPOCH, _tracks_for_border_wiring


def _candidates(spark, order):
    rows = [("trk-1", "ades", ident, 3.0) for ident in order]
    return spark.createDataFrame(
        rows, "track_id string, role string, apt_ident string, dist_nm double"
    )


def test_nearest_settles_equal_distance_by_the_identifier(spark):
    got = {
        StudyFlightListProcessor.classify_endpoints(
            _candidates(spark, order), mode="nearest"
        ).first()["apt"]
        for order in (("CYEG", "CA-1291"), ("CA-1291", "CYEG"))
    }
    assert got == {"CA-1291"}


def test_the_study_processor_accepts_nearest_and_the_package_does_not():
    from opdi.pipeline.flights import FlightListProcessor

    assert "nearest" in StudyFlightListProcessor.DETECTION_MODES
    assert "nearest" not in FlightListProcessor.DETECTION_MODES
    with pytest.raises(ValueError, match="unsupported mode"):
        FlightListProcessor.classify_endpoints(None, mode="nearest")


def test_trend_out_of_area_is_implemented_but_not_enabled():
    """Built, measured, and off: on arrivals the label is right 50.35% of the
    time against `endpoint`'s 89.20%, and every label replaces a silence."""
    assert StudyDetectionConfig().trend_ooa is False
    assert StudyDetectionConfig.legacy().trend_ooa is False


def test_border_flags_use_the_configured_box(spark, tmp_path, monkeypatch):
    """`trend`'s out-of-area label reads the box the run was configured with."""
    import study_variants.detection as study_mod

    import opdi.pipeline.flights as flights_mod
    from opdi.config import OPDIConfig
    from opdi.coverage import CoverageConfig

    sentinel_bbox = (1.0, 2.0, 3.0, 4.0)
    recorded = []
    real_at_border = flights_mod.at_border

    def recorder(lat, lon, margin_nm=flights_mod.BORDER_MARGIN_NM, bbox=flights_mod.BBOX):
        recorded.append(bbox)
        return real_at_border(lat, lon, margin_nm=margin_nm, bbox=bbox)

    monkeypatch.setattr(study_mod, "at_border", recorder)

    proc = StudyFlightListProcessor.__new__(StudyFlightListProcessor)
    proc.spark = spark
    proc.config = OPDIConfig(coverage=CoverageConfig(bbox=sentinel_bbox))
    proc._tracks_table_override = "stub_tracks"
    proc._get_data_within_timeframe = lambda table_name, month: _tracks_for_border_wiring(spark)

    flags = proc._track_border_flags(_EPOCH.date()).collect()

    assert recorded == [sentinel_bbox]
    assert len(flags) == 1


def test_trend_sources_follow_the_option(spark):
    t = spark.createDataFrame(
        [("trk-1", "EBBR", None)], "id string, ADEP string, ADES string"
    ).withColumn("ADEP_P", F.lit(None).cast("string"))
    proc = StudyFlightListProcessor.__new__(StudyFlightListProcessor)
    proc.detection = StudyDetectionConfig()
    row = proc._trend_sources(t, _EPOCH.date()).first()
    assert (row["adep_source"], row["ades_source"]) == ("aerodrome", "undetermined")
