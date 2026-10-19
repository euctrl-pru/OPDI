"""The arm redirect: every write lands in research, every input is shared."""

import re
from pathlib import Path

import pytest

import run_adsc_day as r
from opdi.utils.storage import StorageManager


@pytest.fixture(autouse=True)
def _restore():
    # Every test here patches a class attribute of StorageManager. Leaking one
    # would redirect the rest of the suite's storage calls to a research
    # prefix, and the failures would name the victim rather than the cause.
    yield
    r.restore()


def _manager():
    sm = StorageManager.__new__(StorageManager)
    sm.base_path = "s3a://eurocontrol/opdi-prod"
    return sm


def test_the_three_table_sets_are_disjoint_and_complete():
    # Every table name `src/opdi` resolves has to be in exactly one set. A
    # name in none of them is the failure the A9 study hit twice: the table is
    # written to the research prefix and read back from production, or the
    # reverse, and the run completes with a wrong answer.
    sets = [set(r.SHARED_INPUTS), set(r.DERIVED), set(r.PASSTHROUGH)]
    for a, b in ((0, 1), (0, 2), (1, 2)):
        assert not sets[a] & sets[b]
    assert set(r.ALL_TABLES) == sets[0] | sets[1] | sets[2]


def test_every_table_src_uses_is_classified():
    # Read off `src/opdi` rather than trusted: a new table added to the
    # pipeline must fail this test, not silently default to one side.
    src = Path(r.REPO) / "src" / "opdi"
    found = set()
    pattern = re.compile(
        r"""(?:read_table|table_ref|table_exists)\(\s*["']([a-z_0-9]+)["']"""
        r"""|write_table\([^,]+,\s*["']([a-z_0-9]+)["']""")
    for p in src.rglob("*.py"):
        if ".ipynb_checkpoints" in str(p):
            continue
        for m in pattern.finditer(p.read_text()):
            found.add(m.group(1) or m.group(2))
    assert found <= set(r.ALL_TABLES), sorted(found - set(r.ALL_TABLES))


def test_derived_tables_go_to_the_arm_prefix():
    assert r.arm_path("merged", "osn_tracks") == (
        "s3a://eurocontrol/opdi/research/adsc/merged/osn_tracks")


def test_shared_inputs_are_not_per_arm():
    # The scientific claim is that the arms differ in one setting. Sharing the
    # ADS-B state vectors makes that structurally true rather than asserted,
    # and halves the ingest.
    assert r.arm_path("base", "osn_statevectors_v2") == \
        r.arm_path("merged", "osn_statevectors_v2")
    assert "/input/" in r.arm_path("base", "osn_statevectors_v2")


def test_an_unknown_arm_is_refused():
    with pytest.raises(ValueError, match="unknown arm"):
        r.arm_path("mergd", "osn_tracks")


def test_redirect_sends_derived_writes_to_research():
    r.redirect_arm("merged", "s3a://eurocontrol/opdi-prod")
    sm = _manager()
    assert sm._s3_path("osn_tracks").startswith(
        "s3a://eurocontrol/opdi/research/adsc/merged/")
    assert sm._s3_path("oa_airports") == \
        "s3a://eurocontrol/opdi-prod/oa_airports"


def test_redirect_refuses_an_unknown_table():
    # Loud, not defaulted: an unclassified table is a plan error and the only
    # cheap moment to catch it is the first time it is resolved.
    r.redirect_arm("merged", "s3a://eurocontrol/opdi-prod")
    with pytest.raises(RuntimeError, match="unclassified"):
        _manager()._s3_path("some_new_table")


def test_a_write_outside_research_is_refused():
    with pytest.raises(AssertionError, match="research/adsc"):
        r.guard_write_path("s3a://eurocontrol/opdi-prod/osn_tracks")
    r.guard_write_path(
        "s3a://eurocontrol/opdi/research/adsc/merged/osn_tracks")


def test_writing_a_passthrough_table_is_refused():
    # Reference data is read-only for an arm run. A write reaching it would
    # rewrite production reference tables from a research session.
    r.redirect_arm("merged", "s3a://eurocontrol/opdi-prod")
    sm = _manager()
    with pytest.raises(AssertionError, match="research/adsc"):
        StorageManager.write_table(sm, None, "oa_airports", "overwrite")


def test_restore_is_idempotent():
    r.redirect_arm("base", "s3a://eurocontrol/opdi-prod")
    r.restore()
    r.restore()
    assert _manager()._s3_path("osn_tracks") == \
        "s3a://eurocontrol/opdi-prod/osn_tracks"


def test_the_merged_arm_is_the_only_one_with_adsc_enabled():
    # Read off the source: the two arms differ in exactly one setting, and
    # that setting is this one. A run_arm that enabled it for both would
    # produce two merged arms and a comparison with no baseline.
    import inspect

    src = inspect.getsource(r.run_arm)
    assert 'cfg.adsc.enabled = (arm == "merged")' in src


def test_the_steps_include_adsc_ingestion():
    assert "01b" in r.STEPS
    assert list(r.STEPS).index("01b") < list(r.STEPS).index("02")


def test_each_arm_gets_its_own_etl_log_dir():
    # The ETL progress logs are local files, and one of them gates a remote
    # write: `build_endpoint_candidates` is not keyed to skip_if_processed, so
    # it skips on its own day log and step 03 then reads a table it never
    # wrote. The first attempt at the arm run died exactly there, because a
    # production campaign had logged the same days on this pod.
    import inspect

    src = inspect.getsource(r.run_arm)
    assert '"log_dir"' in src
    assert 'adsc-{arm}' in src


def test_the_log_dir_is_not_the_shared_production_one():
    import inspect

    from opdi.periodrun import DEFAULT_LOG_DIR

    src = inspect.getsource(r.run_arm)
    # The shared directory appears only as the prefix of an arm-specific
    # subdirectory, never on its own.
    assert f'"{DEFAULT_LOG_DIR}"' not in src
