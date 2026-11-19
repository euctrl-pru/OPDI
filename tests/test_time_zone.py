"""Every session works in UTC, and publishing refuses any other zone.

The days of OPDI are UTC days. Spark turns times into dates in the session's
time zone, so a session in another zone would move every day boundary.
"""

import datetime as dt
import os
import time

import pytest
from pyspark.sql import functions as F

from opdi.config import OPDIConfig

#: 2026-06-14 23:30 UTC, which is 19:30 in New York and 01:30 the next day
#: in Brussels.
LATE = int(dt.datetime(2026, 6, 14, 23, 30, tzinfo=dt.timezone.utc).timestamp())


@pytest.fixture
def zone(spark):
    """Set the session zone for one test, then put back UTC."""
    old = spark.conf.get("spark.sql.session.timeZone")

    def set_zone(name):
        spark.conf.set("spark.sql.session.timeZone", name)

    yield set_zone
    spark.conf.set("spark.sql.session.timeZone", old)


@pytest.mark.parametrize("env", ["opensky", "local"])
def test_every_environment_sets_the_session_zone_to_utc(env):
    config = OPDIConfig.for_environment(env)
    assert config.spark.to_spark_config(config.project)["spark.sql.session.timeZone"] == "UTC"


def _day_of(spark):
    df = spark.createDataFrame([(LATE,)], "event_time bigint")
    # The same expression as step 01 (ingestion) uses.
    return df.select(F.to_date(F.from_unixtime("event_time")).alias("d")).collect()[0]["d"]


def test_day_boundaries_do_not_move_under_the_pipeline_settings(spark, zone):
    zone("Europe/Brussels")
    assert _day_of(spark) == dt.date(2026, 6, 15)  # what a non-UTC session would do
    config = OPDIConfig.for_environment("opensky")
    for key, value in config.spark.to_spark_config(config.project).items():
        if key == "spark.sql.session.timeZone":
            spark.conf.set(key, value)
    assert _day_of(spark) == dt.date(2026, 6, 14)


def test_publishing_refuses_a_session_not_in_utc(spark, zone, tmp_path):
    from test_published_build import DAY, _Storage, _working_tables

    from opdi.output.published import PublishedLayout, publish_day

    zone("America/New_York")
    with pytest.raises(ValueError, match="UTC"):
        publish_day(
            spark,
            _Storage(_working_tables(spark)),
            DAY,
            PublishedLayout(str(tmp_path / "published")),
            commit="x",
        )


def test_published_at_is_utc_whatever_the_machine_zone(spark, tmp_path, monkeypatch):
    from test_published_build import DAY, _Storage, _working_tables

    from opdi.output.published import PublishedLayout, publish_day

    monkeypatch.setenv("TZ", "America/New_York")
    time.tzset()
    try:
        layout = PublishedLayout(str(tmp_path / "published"))
        before = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)
        publish_day(spark, _Storage(_working_tables(spark)), DAY, layout, commit="x")
        text = (
            spark.read.parquet(layout.catalogue)
            .select(F.date_format("published_at", "yyyy-MM-dd HH:mm:ss").alias("t"))
            .collect()[0]["t"]
        )
    finally:
        monkeypatch.delenv("TZ")
        time.tzset()
    published_at = dt.datetime.fromisoformat(text)
    assert before <= published_at <= before + dt.timedelta(minutes=10)
    assert os.environ.get("TZ") is None
