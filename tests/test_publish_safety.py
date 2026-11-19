"""Publishing a day never replaces good published files with worse ones.

* A day whose flight list has rows but whose working events are gone is
  refused (the events file would be empty or hold only some events).
* A non-empty published file is not replaced by an empty one unless
  ``allow_empty`` is passed.
* Step 04b enriches only the day it is given and never rewrites the flight
  list of other days, so old days keep their milestones after their working
  events have been removed.
"""

import datetime as dt

import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F
from test_published_build import DAY, _Storage, _working_tables

from opdi.output.published import PublishedLayout, publish_day


@pytest.fixture(scope="module")
def tables(spark):
    return _working_tables(spark)


@pytest.fixture()
def layout(tmp_path):
    return PublishedLayout(str(tmp_path / "published"))


def _without_day(tables, name, column, day):
    out = dict(tables)
    out[name] = tables[name].filter(F.col(column) != F.lit(day))
    return out


def test_a_day_whose_working_events_are_gone_is_refused(spark, tables, layout):
    storage = _Storage(_without_day(tables, "opdi_flight_events", "dof", DAY))
    with pytest.raises(ValueError, match="opdi_flight_events"):
        publish_day(spark, storage, DAY, layout, commit="abc")


def test_an_empty_day_does_not_replace_published_files_unless_allowed(spark, tables, layout):
    publish_day(spark, _Storage(tables), DAY, layout, commit="good")
    empty = _without_day(
        _without_day(
            _without_day(tables, "opdi_flight_list", "DOF", DAY), "opdi_flight_events", "dof", DAY
        ),
        "opdi_measurements",
        "dof",
        DAY,
    )
    with pytest.raises(ValueError, match="allow_empty|--allow-empty"):
        publish_day(spark, _Storage(empty), DAY, layout, commit="bad")
    assert pq.read_table(layout.flight_list(DAY)).num_rows == 2
    assert pq.read_table(layout.flight_events(DAY)).num_rows == 5
    row = spark.read.parquet(layout.catalogue).collect()[0]
    assert row["commit"] == "good"

    counts = publish_day(spark, _Storage(empty), DAY, layout, commit="empty", allow_empty=True)
    assert counts["flights"] == 0
    assert pq.read_table(layout.flight_list(DAY)).num_rows == 0


# --- step 04b touches only its own day ----------------------------------------------


def test_milestones_of_one_day_leave_the_other_days_alone(spark, tmp_path):
    from opdi.config import OPDIConfig
    from opdi.pipeline.flight_list_milestones import ADDED_COLUMNS
    from opdi.pipeline.flight_list_step import enrich_day
    from opdi.utils.storage import StorageManager

    config = OPDIConfig.for_environment("opensky", warehouse=str(tmp_path / "wh"))
    storage = StorageManager(spark, config)
    old, day = dt.date(2026, 6, 13), dt.date(2026, 6, 14)
    t_old, t_day = dt.datetime(2026, 6, 13, 8), dt.datetime(2026, 6, 14, 8)
    flights = spark.createDataFrame(
        [
            ("trk_old", "abc", "TST1", old, "EDDF", "EHAM", "v", t_old, t_old),
            ("trk_day", "def", "TST2", day, "EDDF", "EHAM", "v", t_day, t_day),
        ],
        "ID string, ICAO24 string, FLT_ID string, DOF date, ADEP string, ADES string, "
        "VERSION string, FIRST_SEEN timestamp, LAST_SEEN timestamp",
    )
    events = spark.createDataFrame(
        [
            ("trk_old", "ATOT", t_old, '{"osm_airport": "EDDF"}', "events_v0.3.0", old),
            ("trk_day", "ATOT", t_day, '{"osm_airport": "EDDF"}', "events_v0.3.0", day),
        ],
        "flight_id string, type string, event_time timestamp, info string, version string, "
        "dof date",
    )
    storage.write_table(flights, "opdi_flight_list", mode="overwrite", partition_by=["DOF"])
    storage.write_table(events, "opdi_flight_events", mode="overwrite", partition_by=["dof"])
    enrich_day(storage, config.events, old, old)
    enriched_old = storage.read_table("opdi_flight_list").filter(F.col("DOF") == old).collect()
    assert enriched_old[0]["ATOT"] == t_old

    # The working events of the old day are removed, then the new day runs.
    storage.drop_partitions("opdi_flight_events", "dof", [old.isoformat()])
    assert enrich_day(storage, config.events, day, day) == 1

    rows = {r["ID"]: r for r in storage.read_table("opdi_flight_list").collect()}
    assert rows["trk_old"]["ATOT"] == t_old
    assert rows["trk_day"]["ATOT"] == t_day
    assert set(ADDED_COLUMNS) <= set(storage.read_table("opdi_flight_list").columns)


# --- a publish that stops part way ---------------------------------------------------


class _CrashError(Exception):
    pass


def _crash_at(monkeypatch, n):
    """Make the n-th rename or delete of publishing raise."""
    from opdi.output import published

    calls = {"n": 0}
    real = {name: getattr(published._Files, name) for name in ("rename", "delete")}

    def wrap(name):
        def op(self, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] == n:
                raise _CrashError(f"{name} {args}")
            return real[name](self, *args, **kwargs)

        return op

    for name in real:
        monkeypatch.setattr(published._Files, name, wrap(name))
    return calls


def test_a_publish_that_stops_after_any_operation_is_repaired_by_running_it_again(
    spark, tables, tmp_path, monkeypatch
):
    from pathlib import Path

    first = dict(tables)
    first["opdi_flight_events"] = tables["opdi_flight_events"].filter(F.col("id") != "e5")

    # How many renames and deletes one publish over existing files makes.
    probe = PublishedLayout(str(tmp_path / "probe" / "published"))
    publish_day(spark, _Storage(first), DAY, probe, commit="old")
    calls = _crash_at(monkeypatch, 10**9)
    publish_day(spark, _Storage(tables), DAY, probe, commit="new")
    total = calls["n"]
    monkeypatch.undo()
    assert total >= 6

    for n in range(1, total + 1):
        layout = PublishedLayout(str(tmp_path / f"crash{n}" / "published"))
        publish_day(spark, _Storage(first), DAY, layout, commit="old")
        _crash_at(monkeypatch, n)
        with pytest.raises(_CrashError):
            publish_day(spark, _Storage(tables), DAY, layout, commit="new")
        monkeypatch.undo()
        # Nothing is lost: each file is in place or kept aside.
        for path in (layout.flight_list(DAY), layout.flight_events(DAY)):
            aside = f"{layout.tmp}/aside/{path.rsplit('/', 1)[1]}"
            assert Path(path).exists() or Path(aside).exists(), (n, path)

        publish_day(spark, _Storage(tables), DAY, layout, commit="new")
        assert pq.read_table(layout.flight_list(DAY)).num_rows == 2, n
        assert pq.read_table(layout.flight_events(DAY)).num_rows == 5, n
        row = spark.read.parquet(layout.catalogue).collect()
        assert [(r["day"], r["commit"]) for r in row] == [(DAY, "new")], n
        assert not list(Path(layout.tmp).rglob("*.parquet")), n
        assert not [p for p in Path(layout.root).rglob("*") if "tmp" in p.name or "old" in p.name]


# --- one publisher at a time ----------------------------------------------------------


def test_a_second_publish_into_the_same_release_is_refused(spark, tables, layout):
    from opdi.output.published import PublishLock, PublishLockedError

    with PublishLock(spark, layout):
        with pytest.raises(PublishLockedError, match="another publish"):
            publish_day(spark, _Storage(tables), DAY, layout, commit="second")
    # Released afterwards.
    publish_day(spark, _Storage(tables), DAY, layout, commit="after")


def test_a_lock_left_by_a_stopped_publish_is_reported_not_taken(spark, tables, layout):
    import json
    from pathlib import Path

    from opdi.output.published import CLEAR_LOCK_COMMAND, PublishLockedError, clear_lock

    lock = Path(layout.state) / "publish.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text(json.dumps({"host": "pod", "pid": 1, "since": "2026-01-01T00:00:00+00:00"}))
    with pytest.raises(PublishLockedError, match="older than") as info:
        publish_day(spark, _Storage(tables), DAY, layout, commit="x")
    assert CLEAR_LOCK_COMMAND in str(info.value)
    assert lock.exists()
    assert clear_lock(spark, layout) is True
    assert not lock.exists()
    publish_day(spark, _Storage(tables), DAY, layout, commit="x")


def test_temporary_folders_have_names_of_their_own(spark, tmp_path, monkeypatch):
    from opdi.output import published

    seen = []
    real = published.write_temporary_file

    def spy(df, folder, *a, **k):
        seen.append(folder)
        return real(df, folder, *a, **k)

    monkeypatch.setattr(published, "write_temporary_file", spy)
    df = spark.createDataFrame([(1,)], "x int")
    path = str(tmp_path / "pub" / "catalogue.parquet")
    published.write_single_file(df, path, tmp_root=str(tmp_path / "tmp"))
    published.write_single_file(df, path, tmp_root=str(tmp_path / "tmp"))
    assert len(set(seen)) == 2
    assert all(f.startswith(str(tmp_path / "tmp")) for f in seen)


def test_the_clear_lock_command(spark, tmp_path, monkeypatch, capsys):
    from opdi import orchestration as o

    class KeepOpen:
        def __getattr__(self, name):
            return getattr(spark, name)

        def stop(self):
            pass

    monkeypatch.setattr(o, "_session", lambda *a, **k: KeepOpen())
    wh = str(tmp_path / "wh")
    layout = PublishedLayout(o.make_config(wh).project.release_root + "/published")
    from pathlib import Path

    lock = Path(layout.state) / "publish.lock"
    lock.parent.mkdir(parents=True)
    lock.write_text("{}")
    assert o.main(["clear-lock", "--warehouse", wh]) == 0
    assert not lock.exists()
    assert "removed" in capsys.readouterr().out


def test_a_measurement_written_twice_gives_the_largest_value(spark, tables):
    """The choice does not depend on the order Spark reads the rows in."""
    from opdi.output.published import build_flight_events

    extra = spark.createDataFrame(
        [("e1_d2", "e1", "Distance flown (NM)", 7.0, "distance_v0.0.3", DAY)],
        tables["opdi_measurements"].schema,
    )
    for measurements in (
        tables["opdi_measurements"].unionByName(extra),
        extra.unionByName(tables["opdi_measurements"]),
    ):
        rows = build_flight_events(
            tables["opdi_flight_events"], measurements.coalesce(1), tables["opdi_flight_list"]
        ).collect()
        assert {r["event_id"]: r["distance_flown_nm"] for r in rows}["e1"] == 7.0
