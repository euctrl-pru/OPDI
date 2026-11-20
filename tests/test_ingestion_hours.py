"""Which hourly folders of the OpenSky archive step 01 reads."""

from datetime import date, datetime

from opdi.ingestion.osn_statevectors import OSN_STATE_VECTORS, hour_paths


def test_a_day_is_its_24_hourly_folders_in_utc():
    paths = hour_paths(date(2026, 6, 14), date(2026, 6, 15))
    assert len(paths) == 24
    # 2026-06-14 00:00 UTC is 1781395200 s after 1970-01-01 00:00 UTC.
    assert paths[0] == f"{OSN_STATE_VECTORS}/hour=1781395200"
    assert paths[-1] == f"{OSN_STATE_VECTORS}/hour={1781395200 + 23 * 3600}"


def test_a_part_of_a_day_is_given_by_times():
    paths = hour_paths(datetime(2026, 6, 14, 8), datetime(2026, 6, 14, 12), base="s3a://b/sv")
    assert paths == [f"s3a://b/sv/hour={1781395200 + h * 3600}" for h in range(8, 12)]


def test_an_empty_range_reads_nothing():
    assert hour_paths(date(2026, 6, 14), date(2026, 6, 14)) == []


def _hour_folder(spark, base, start, rows):
    """One hourly folder of the archive, with OpenSky's column names."""
    from calendar import timegm

    ddl = "time bigint, icao24 string, lat double, lon double, baroAltitude double"
    stamp = timegm(start.timetuple())
    spark.createDataFrame([(stamp + s, a, 50.0, 4.0, 1000.0) for s, a in rows], ddl).write.parquet(
        f"{base}/hour={stamp}"
    )


def test_ingest_replaces_the_days_read_keeps_the_others_and_counts_the_rows(spark, tmp_path):
    from opdi.config import OPDIConfig
    from opdi.ingestion.osn_statevectors import StateVectorIngestion
    from opdi.utils.storage import StorageManager

    config = OPDIConfig.for_environment("local", worldwide=True, warehouse=str(tmp_path / "wh"))
    config.spark.enable_iceberg = False
    config.spark.enable_hive = False
    storage = StorageManager(spark, config)
    base = str(tmp_path / "archive")
    # Two hours of 2026-06-14, and an hour of the day before.
    _hour_folder(spark, base, datetime(2026, 6, 14, 0), [(0, "a"), (5, "a"), (7, "b")])
    _hour_folder(spark, base, datetime(2026, 6, 14, 1), [(0, "a"), (10, "a")])
    _hour_folder(spark, base, datetime(2026, 6, 13, 23), [(0, "c")])
    ingest = StateVectorIngestion(spark, config, bbox=None)

    assert ingest.ingest_from_s3(datetime(2026, 6, 13, 23), datetime(2026, 6, 14), base) == 1
    # A first, partial copy of the 14th, replaced below.
    assert ingest.ingest_from_s3(datetime(2026, 6, 14, 1), datetime(2026, 6, 14, 2), base) == 2
    assert ingest.ingest_from_s3(date(2026, 6, 14), datetime(2026, 6, 14, 2), base) == 5

    table = storage.read_table("osn_statevectors_v2")
    per_day = {
        str(r["event_time_day"]): r["count"]
        for r in table.groupBy("event_time_day").count().collect()
    }
    # 5-second thinning keeps the last report of each aircraft and 5 s bin:
    # a@0, a@5, b@7 in the first hour; a@0, a@10 in the second.
    assert per_day == {"2026-06-13": 1, "2026-06-14": 5}


def test_a_time_with_a_time_zone_is_read_in_utc():
    from datetime import timedelta, timezone

    paris = timezone(timedelta(hours=2))
    # 10:00 at UTC+2 is 08:00 UTC.
    paths = hour_paths(
        datetime(2026, 6, 14, 10, tzinfo=paris), datetime(2026, 6, 14, 12, tzinfo=paris)
    )
    assert paths == hour_paths(datetime(2026, 6, 14, 8), datetime(2026, 6, 14, 10))
    assert paths[0].endswith(f"hour={1781395200 + 8 * 3600}")


def test_the_adsc_hours_are_utc_whatever_the_machines_time_zone(monkeypatch):
    import time

    from opdi.ingestion.osn_adsc import AdscIngestion

    ingest = AdscIngestion.__new__(AdscIngestion)
    ingest.base = "s3a://b/adsc"
    monkeypatch.setenv("TZ", "America/New_York")
    time.tzset()
    try:
        paths = ingest.hour_paths(date(2026, 6, 14), date(2026, 6, 15))
    finally:
        monkeypatch.delenv("TZ")
        time.tzset()
    assert paths == hour_paths(date(2026, 6, 14), date(2026, 6, 15), base="s3a://b/adsc")
    assert paths[0] == "s3a://b/adsc/hour=1781395200"


def test_a_report_outside_the_hours_read_is_left_out(spark, tmp_path):
    """An hourly folder holding a report of another hour must not put it into a
    day the step did not replace."""
    from opdi.config import OPDIConfig
    from opdi.ingestion.osn_statevectors import StateVectorIngestion
    from opdi.utils.storage import StorageManager

    config = OPDIConfig.for_environment("local", worldwide=True, warehouse=str(tmp_path / "wh"))
    config.spark.enable_iceberg = False
    config.spark.enable_hive = False
    storage = StorageManager(spark, config)
    base = str(tmp_path / "archive")
    _hour_folder(spark, base, datetime(2026, 6, 13, 23), [(0, "c")])
    # The first hour of the 14th holds a stray report of 22:00 on the 13th.
    _hour_folder(spark, base, datetime(2026, 6, 14, 0), [(0, "a"), (5, "a"), (-7200, "s")])
    _hour_folder(spark, base, datetime(2026, 6, 14, 1), [(0, "a"), (10, "a")])
    ingest = StateVectorIngestion(spark, config, bbox=None)

    assert ingest.ingest_from_s3(datetime(2026, 6, 13, 23), datetime(2026, 6, 14), base) == 1
    assert ingest.ingest_from_s3(date(2026, 6, 14), datetime(2026, 6, 14, 2), base) == 4

    table = storage.read_table("osn_statevectors_v2")
    assert sorted((str(r.event_time_day), r.icao24) for r in table.collect()) == [
        ("2026-06-13", "c"),
        ("2026-06-14", "a"),
        ("2026-06-14", "a"),
        ("2026-06-14", "a"),
        ("2026-06-14", "a"),
    ]


def test_the_missing_hours_are_named_in_utc():
    from opdi.ingestion.osn_statevectors import missing_hours

    wanted = hour_paths(date(2026, 1, 1), date(2026, 1, 2), base="s3a://b/sv")
    present = [p for p in wanted if not p.endswith("hour=1767276000")]
    assert missing_hours(present, wanted) == [datetime(2026, 1, 1, 14)]


def test_a_few_missing_hours_a_day_are_accepted_more_are_refused():
    import pytest

    from opdi.ingestion.osn_statevectors import (
        MAX_MISSING_HOURS_PER_DAY,
        ArchiveHoursMissingError,
        check_missing_hours,
    )

    few = [datetime(2026, 1, 1, h) for h in range(MAX_MISSING_HOURS_PER_DAY)]
    check_missing_hours(few)
    # The limit is per day: the same number again on the next day is fine.
    check_missing_hours(few + [datetime(2026, 1, 2, h) for h in range(MAX_MISSING_HOURS_PER_DAY)])
    with pytest.raises(ArchiveHoursMissingError, match="2026-01-01: 4 of 24"):
        check_missing_hours(few + [datetime(2026, 1, 1, 20)])


def test_too_many_missing_hours_is_a_permanent_failure():
    """Running the step again cannot add the hours to the archive."""
    from opdi.ingestion.osn_statevectors import ArchiveHoursMissingError
    from opdi.orchestration import classify_failure

    assert classify_failure(ArchiveHoursMissingError("archive incomplete")) == "permanent"


def _local_ingest(spark, tmp_path):
    from opdi.config import OPDIConfig
    from opdi.ingestion.osn_statevectors import StateVectorIngestion
    from opdi.utils.storage import StorageManager

    config = OPDIConfig.for_environment("local", worldwide=True, warehouse=str(tmp_path / "wh"))
    config.spark.enable_iceberg = False
    config.spark.enable_hive = False
    return StateVectorIngestion(spark, config, bbox=None), StorageManager(spark, config)


def test_a_missing_hour_is_left_out_and_the_others_are_ingested(spark, tmp_path, capsys):
    ingest, storage = _local_ingest(spark, tmp_path)
    base = str(tmp_path / "archive")
    # 00:00 and 02:00 are in the archive, 01:00 is not.
    _hour_folder(spark, base, datetime(2026, 6, 14, 0), [(0, "a")])
    _hour_folder(spark, base, datetime(2026, 6, 14, 2), [(0, "b")])

    assert ingest.ingest_from_s3(datetime(2026, 6, 14, 0), datetime(2026, 6, 14, 3), base) == 2
    assert "left out: 1 hour(s) (UTC): 2026-06-14 01:00" in capsys.readouterr().out
    rows = storage.read_table("osn_statevectors_v2").collect()
    assert sorted(r.icao24 for r in rows) == ["a", "b"]


def test_a_window_with_no_hour_in_the_archive_is_refused(spark, tmp_path):
    import pytest

    from opdi.ingestion.osn_statevectors import ArchiveHoursMissingError

    ingest, _ = _local_ingest(spark, tmp_path)
    with pytest.raises(ArchiveHoursMissingError, match="none of the 2 hourly folders"):
        ingest.ingest_from_s3(
            datetime(2026, 6, 14, 0), datetime(2026, 6, 14, 2), str(tmp_path / "archive")
        )
