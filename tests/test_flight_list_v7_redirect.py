"""``redirect_tracks`` (``benchmarks/flight_list_v7.py``) must not double-wrap
``StorageManager.read_table``, and must leave any table that is not the
redirected tracks table exactly alone.

Both are properties a benchmark that patches a shared class method needs:
``redirect_tracks`` is applied at most once per process, the same pattern
``redirect_candidates``/``guard_writes``/``index_on_read`` in the same module
use, and every one of those guards a *class attribute* flag for exactly this
reason -- a double-apply would either join a track table onto itself twice or
silently stop calling through to a patch installed earlier.

Two of the three tests below run against fakes with no Spark session and no
cluster: whether a second call re-wraps, and whether an unrelated table name
reaches the original ``read_table`` untouched. The third genuinely needs
Spark -- the join is real PySpark code, not something a fake can exercise
honestly -- so it uses the local ``spark`` fixture the rest of the suite
already runs against (``tests/conftest.py``: ``local[1]``, no cluster, no
credentials), building a two-row tracks table and a two-row assignment table
by hand and checking the join actually replaces ``track_id`` and that calling
``redirect_tracks`` twice does not apply that join twice.
"""

import datetime as dt
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

from flight_list_v7 import redirect_tracks  # noqa: E402

from opdi.utils.storage import StorageManager  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_storage_manager_patch():
    """Undo whatever this module patched onto the class after every test.

    ``redirect_tracks`` mutates ``StorageManager.read_table`` and
    ``StorageManager._tc_track_redirect`` in place -- the same pattern the
    other benchmark-side patches in ``flight_list_v7.py`` use. Leaving that in
    place would leak a wrapped ``read_table`` into whatever test runs next in
    this process, which is exactly the kind of bug this file exists to catch.
    """
    orig_read_table = StorageManager.read_table
    yield
    StorageManager.read_table = orig_read_table
    if "_tc_track_redirect" in StorageManager.__dict__:
        del StorageManager._tc_track_redirect


def test_second_call_does_not_install_a_second_wrapper():
    """A second redirect_tracks() call must return immediately: same function
    object stays installed, not a wrapper around the first wrapper."""

    def fake_read_table(self, name, *a, **kw):
        return f"df:{name}"

    StorageManager.read_table = fake_read_table

    redirect_tracks("s3a://fake/assign", "osn_tracks_clean")
    wrapped_once = StorageManager.read_table

    redirect_tracks("s3a://fake/assign", "osn_tracks_clean")
    wrapped_twice = StorageManager.read_table

    assert wrapped_once is wrapped_twice, (
        "a second call replaced the wrapper -- it should have short-circuited "
        "on the _tc_track_redirect guard instead"
    )


def test_unrelated_table_name_passes_through_untouched():
    """A table name that is not the redirected tracks table must reach the
    original read_table exactly, with no join and no argument mangling."""
    seen = []

    def fake_read_table(self, name, *a, **kw):
        seen.append((name, a, kw))
        return f"df:{name}"

    StorageManager.read_table = fake_read_table
    redirect_tracks("s3a://fake/assign", "osn_tracks_clean")

    class FakeSelf:
        spark = None  # never touched: the name mismatch must short-circuit first

    result = StorageManager.read_table(FakeSelf(), "osn_aircraft_db")

    assert result == "df:osn_aircraft_db"
    assert seen == [("osn_aircraft_db", (), {})]


def test_track_table_is_redirected_and_a_repeat_call_does_not_rejoin(spark, tmp_path):
    """The one part of redirect_tracks a fake cannot honestly stand in for:
    the actual join that replaces track_id. Runs against the local Spark
    session the rest of the suite uses -- no cluster, no S3.

    Also checks the idempotency guard end to end: calling redirect_tracks
    twice and then reading the tracks table must still produce exactly the
    joined result, not a double join (which would show up as a duplicate
    track_id/_new_track_id column or as rows multiplied by the join fanout).
    """
    t0 = dt.datetime(2025, 6, 5, 12, 0, 0)
    tracks_df = spark.createDataFrame(
        [
            ("abc123", t0, "orig-track-1", 1.0),
            ("def456", t0, "orig-track-2", 2.0),
        ],
        ["icao24", "event_time", "track_id", "other_col"],
    )

    assign_df = spark.createDataFrame(
        [
            ("abc123", t0, "new-track-1"),
            ("def456", t0, "new-track-2"),
        ],
        ["icao24", "event_time", "track_id"],
    )
    assign_path = str(tmp_path / "assign")
    assign_df.write.mode("overwrite").parquet(assign_path)

    def fake_read_table(self, name, *a, **kw):
        return tracks_df

    StorageManager.read_table = fake_read_table
    redirect_tracks(assign_path, "osn_tracks_clean")
    redirect_tracks(assign_path, "osn_tracks_clean")  # must be a no-op

    class FakeSelf:
        pass

    fake_self = FakeSelf()
    fake_self.spark = spark

    out = StorageManager.read_table(fake_self, "osn_tracks_clean")

    assert out.columns.count("track_id") == 1
    rows = {r.icao24: r.track_id for r in out.collect()}
    assert rows == {"abc123": "new-track-1", "def456": "new-track-2"}
    # other_col survives the join untouched -- the redirect replaces track_id,
    # nothing else about the row.
    assert {r.other_col for r in out.collect()} == {1.0, 2.0}
