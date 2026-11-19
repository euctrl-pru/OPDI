"""A step that fails for a passing reason is run again; a permanent failure stops at once."""

from datetime import date, timedelta

import pytest

from opdi import orchestration as o
from opdi import periodrun

D = date(2026, 6, 14)

THROTTLED = RuntimeError(
    "An error occurred while calling o22628.parquet. : java.nio.file.AccessDeniedException ... "
    "Resource requested is unwritable, please reduce your request rate "
    "(Service: S3, Status Code: 503)"
)


class _KeepOpen:
    """The test session, which a unit must not stop."""

    def __init__(self, spark):
        self._spark = spark

    def __getattr__(self, name):
        return getattr(self._spark, name)

    def stop(self):
        pass


@pytest.fixture
def no_pause(monkeypatch):
    pauses = []
    monkeypatch.setattr(o.time, "sleep", pauses.append)
    return pauses


@pytest.mark.parametrize(
    "error",
    [
        THROTTLED,
        RuntimeError("NoSuchUploadException: The specified multipart upload does not exist."),
        RuntimeError(
            "Job aborted due to stage failure: ExecutorLostFailure (executor 8 exited caused "
            "by one of the running tasks) Reason: The executor with id 8 exited with exit "
            "code 137(SIGKILL, possible container OOM)."
        ),
        RuntimeError("org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output "),
        RuntimeError(
            'pods "x-exec-14" is forbidden: exceeded quota: eurocontrol-quota, requested: '
            "limits.cpu=2"
        ),
        RuntimeError("java.net.SocketTimeoutException: Read timed out"),
        RuntimeError("some other failure of the cluster"),
    ],
)
def test_passing_failures_are_transient(error):
    assert o.classify_failure(error) == "transient"


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("QuotaExceeded: the bucket quota has been exceeded (Status Code: 403)"),
        RuntimeError(
            "the table osn_tracks (s3a://b/osn_tracks) holds no data file: a write that "
            "failed part-way left it empty"
        ),
        RuntimeError("[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column named `dof` cannot be found"),
        ValueError("unknown step '05'"),
        RuntimeError(
            "Job aborted due to stage failure: Task 0 in stage 35.0 failed 8 times ... "
            "org.apache.spark.memory.SparkOutOfMemoryError: [UNABLE_TO_ACQUIRE_MEMORY] Unable "
            "to acquire 65536 bytes of memory"
        ),
        KeyError("h3_res_7"),
    ],
)
def test_lasting_failures_are_permanent(error):
    assert o.classify_failure(error) == "permanent"


def test_the_cause_of_an_error_is_read_too():
    try:
        try:
            raise RuntimeError("NoSuchUploadException: upload aborted")
        except RuntimeError as inner:
            raise RuntimeError("the write failed") from inner
    except RuntimeError as outer:
        assert o.classify_failure(outer) == "transient"


def test_a_step_that_fails_once_runs_again_and_succeeds(monkeypatch, no_pause):
    calls = []

    def attempt(*args):
        calls.append(args)
        if len(calls) == 1:
            raise THROTTLED
        return 0

    monkeypatch.setattr(o, "_attempt_unit", attempt)
    assert o.run_unit("04", D, retries=2, retry_pause=7) == 0
    assert len(calls) == 2
    assert no_pause == [7]


def test_a_step_that_always_fails_stops_after_its_retries(monkeypatch, no_pause):
    calls = []
    monkeypatch.setattr(
        o, "_attempt_unit", lambda *a: calls.append(a) or (_ for _ in ()).throw(THROTTLED)
    )
    assert o.run_unit("02", D, retries=2, retry_pause=0) == 1
    assert len(calls) == 3
    assert len(no_pause) == 2


def test_a_permanent_failure_is_not_retried(monkeypatch, no_pause):
    calls = []

    def attempt(*args):
        calls.append(args)
        raise RuntimeError("[UNRESOLVED_COLUMN.WITH_SUGGESTION] `adep` cannot be found")

    monkeypatch.setattr(o, "_attempt_unit", attempt)
    assert o.run_unit("03", D, retries=2, retry_pause=0) == 1
    assert len(calls) == 1
    assert no_pause == []


def test_a_refusal_is_not_retried(monkeypatch, no_pause):
    # A step that refuses to start (inputs missing, coverage mismatch)
    # returns 1 without raising: running it again would refuse again.
    calls = []
    monkeypatch.setattr(o, "_attempt_unit", lambda *a: calls.append(a) or 1)
    assert o.run_unit("03", D, retries=2, retry_pause=0) == 1
    assert len(calls) == 1


def test_zero_retries_means_one_attempt(monkeypatch, no_pause):
    calls = []
    monkeypatch.setattr(
        o, "_attempt_unit", lambda *a: calls.append(a) or (_ for _ in ()).throw(THROTTLED)
    )
    assert o.run_unit("02", D, retries=0) == 1
    assert len(calls) == 1


def test_a_day_passes_its_retries_to_each_step(monkeypatch):
    seen = []
    monkeypatch.setattr(o, "unfinished_steps", lambda *a, **k: list(o.DAY_STEPS))
    monkeypatch.setattr(o, "_resume_plan", lambda day, first, *a: list(o.DAY_STEPS))
    monkeypatch.setattr(o, "check_space", lambda *a, **k: None)
    monkeypatch.setattr(o, "prune_internal", lambda *a, **k: None)
    monkeypatch.setattr(
        o, "run_unit", lambda step, *a, **k: seen.append((k["retries"], k["retry_pause"])) or 0
    )
    assert o.run_day(D, retries=3, retry_pause=5) == 0
    assert seen == [(3, 5)] * len(o.DAY_STEPS)


def test_the_command_line_sets_the_retries(monkeypatch):
    seen = []
    monkeypatch.setattr(o, "run_day", lambda *a, **k: seen.append(k) or 0)
    assert o.main(["day", "--day", "2026-06-14", "--retries", "1", "--retry-pause", "30"]) == 0
    assert seen == [{"retries": 1, "retry_pause": 30.0}]
    seen.clear()
    assert o.main(["day", "--day", "2026-06-14"]) == 0
    assert seen == [{"retries": o.DEFAULT_RETRIES, "retry_pause": o.DEFAULT_RETRY_PAUSE_S}]


# --- on a local warehouse ---------------------------------------------------


@pytest.fixture
def warehouse(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(o, "_session", lambda *a, **k: _KeepOpen(spark))
    monkeypatch.setattr(o, "missing_inputs", lambda *a, **k: [])
    monkeypatch.setattr(periodrun, "preflight", lambda *a, **k: [])
    return str(tmp_path / "wh")


def _storage(spark, warehouse):
    from opdi.utils.storage import StorageManager

    return StorageManager(spark, o.make_config(warehouse))


def test_a_flaky_step_is_marked_finished_only_after_it_succeeds(
    spark, warehouse, monkeypatch, no_pause
):
    calls = []

    def body(spark_, config, step, day, kwargs):
        calls.append(step)
        assert not _storage(spark, warehouse).is_done(f"03@{D}")
        if len(calls) == 1:
            raise THROTTLED

    monkeypatch.setattr(periodrun, "run_day_step", body)
    assert o.run_unit("03", D, warehouse, retries=2, retry_pause=0) == 0
    assert calls == ["03", "03"]
    assert _storage(spark, warehouse).is_done(f"03@{D}")


def test_a_step_that_always_fails_leaves_no_mark(spark, warehouse, monkeypatch, no_pause):
    def body(*args):
        raise THROTTLED

    monkeypatch.setattr(periodrun, "run_day_step", body)
    assert o.run_unit("03", D, warehouse, retries=1, retry_pause=0) == 1
    assert not _storage(spark, warehouse).is_done(f"03@{D}")


def test_a_permanent_error_on_a_warehouse_stops_at_once(spark, warehouse, monkeypatch, no_pause):
    calls = []

    def body(*args):
        calls.append(1)
        raise ValueError("a setting of the configuration is wrong")

    monkeypatch.setattr(periodrun, "run_day_step", body)
    assert o.run_unit("03", D, warehouse, retries=2, retry_pause=0) == 1
    assert calls == [1]
    assert not _storage(spark, warehouse).is_done(f"03@{D}")


def test_step_01_fetches_again_a_day_whose_fetch_did_not_finish(spark, tmp_path, monkeypatch):
    from opdi.ingestion.osn_statevectors import StateVectorIngestion

    warehouse = str(tmp_path / "wh")
    config = o.make_config(warehouse)
    storage = _storage(spark, warehouse)
    folder = tmp_path / "wh" / config.project.release / "osn_statevectors_v2"
    before, after = D - timedelta(days=1), D + timedelta(days=1)
    for d in (before, D):  # D-1 fetched in full, D left part-way
        (folder / f"event_time_day={d}").mkdir(parents=True)
    storage.mark_done(periodrun.fetched_key(before))

    fetched = []

    def ingest(self, start_date, end_date, **kwargs):
        assert not storage.is_done(periodrun.fetched_key(start_date))
        fetched.append(start_date)

    monkeypatch.setattr(StateVectorIngestion, "ingest_from_s3", ingest)
    monkeypatch.setattr(StateVectorIngestion, "__init__", lambda self, *a, **k: None)
    periodrun.run_day_step(spark, config, "01", D, {})
    assert fetched == [D, after]
    assert all(storage.is_done(periodrun.fetched_key(d)) for d in (before, D, after))


def test_the_fetch_record_is_not_read_as_a_step_of_a_day(spark, tmp_path):
    storage = _storage(spark, str(tmp_path / "wh"))
    storage.mark_done(periodrun.fetched_key(D))
    assert o.days_in_progress(storage) == set()


def test_an_empty_table_folder_is_reported_not_guessed(spark, tmp_path):
    from opdi.utils.storage import EmptyTableError

    warehouse = str(tmp_path / "wh")
    storage = _storage(spark, warehouse)
    # What a write that failed part-way leaves: the folder, no data file.
    (
        tmp_path / "wh" / o.make_config(warehouse).project.release / "osn_tracks" / "_temporary"
    ).mkdir(parents=True)
    with pytest.raises(EmptyTableError, match="holds no data file"):
        storage.read_table("osn_tracks")
    assert not storage.table_exists("osn_tracks")
    try:
        storage.read_table("osn_tracks")
    except EmptyTableError as exc:
        assert o.classify_failure(exc) == "permanent"
