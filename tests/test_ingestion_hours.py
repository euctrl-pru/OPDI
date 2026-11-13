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
