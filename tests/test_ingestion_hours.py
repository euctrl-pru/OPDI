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
