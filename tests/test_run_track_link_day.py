"""The A10 study runner: two arms, one sampled day, an isolated prefix."""

import dataclasses
import re
from pathlib import Path

import pytest

import run_track_link_day as r
from opdi.utils.storage import StorageManager


@pytest.fixture(autouse=True)
def _restore():
    yield
    r.restore()


def _manager():
    sm = StorageManager.__new__(StorageManager)
    sm.base_path = "s3a://eurocontrol/opdi-world"
    return sm


def test_the_arms_differ_only_in_method():
    a = dataclasses.asdict(r.build_config("debounced"))
    b = dataclasses.asdict(r.build_config("cruise_linked"))
    diff = {k for k in a["segmentation"] if a["segmentation"][k] != b["segmentation"][k]}
    assert diff == {"method"}
    a["segmentation"].pop("method")
    b["segmentation"].pop("method")
    assert a == b


def test_the_config_is_worldwide_and_adsc_stays_off():
    cfg = r.build_config("debounced")
    assert cfg.coverage.bbox is None
    assert cfg.adsc.enabled is False


def test_link_params_override_only_link_fields():
    cfg = r.build_config("cruise_linked", {"link_max_gap_minutes": 120.0})
    assert cfg.segmentation.link_max_gap_minutes == 120.0
    with pytest.raises(ValueError, match="link_"):
        r.build_config("cruise_linked", {"gap_minutes": 1.0})
    with pytest.raises(ValueError, match="link_"):
        r.build_config("cruise_linked", {"link_no_such_field": 1.0})


def test_inputs_are_shared_between_arms():
    for t in r.SHARED_INPUTS:
        assert r.arm_path("debounced", t) == r.arm_path("cruise_linked", t)
        assert "/input/" in r.arm_path("debounced", t)
    assert r.arm_path("debounced", "osn_tracks") != \
        r.arm_path("cruise_linked", "osn_tracks")
    assert r.arm_path("cruise_linked", "osn_tracks") == (
        "s3a://eurocontrol/opdi-world/research/a10/cruise_linked/osn_tracks")


def test_an_unknown_arm_is_refused():
    with pytest.raises(ValueError, match="unknown arm"):
        r.arm_path("cruise-linked", "osn_tracks")


def test_a_write_outside_a10_is_refused():
    for bad in ("s3a://eurocontrol/opdi-world/osn_tracks",
                "s3a://eurocontrol/opdi/research/a10/cruise_linked/osn_tracks",
                "s3a://eurocontrol/opdi-world/research/adsc/base/osn_tracks",
                "s3a://eurocontrol/opdi-world/research/a10_x/osn_tracks"):
        with pytest.raises(AssertionError, match="a10"):
            r.guard_write_path(bad)
    r.guard_write_path(
        "s3a://eurocontrol/opdi-world/research/a10/debounced/osn_tracks")


def test_the_three_table_sets_are_disjoint_and_complete():
    sets = [set(r.SHARED_INPUTS), set(r.DERIVED), set(r.PASSTHROUGH)]
    for a, b in ((0, 1), (0, 2), (1, 2)):
        assert not sets[a] & sets[b]
    assert set(r.ALL_TABLES) == sets[0] | sets[1] | sets[2]


def test_every_table_src_uses_is_classified():
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


def test_an_unclassified_table_raises():
    r.redirect_arm("debounced")
    with pytest.raises(RuntimeError, match="unclassified"):
        _manager()._s3_path("some_new_table")


def test_redirect_sends_derived_to_a10_and_reference_through():
    r.redirect_arm("cruise_linked")
    sm = _manager()
    assert sm._s3_path("osn_tracks").startswith(
        "s3a://eurocontrol/opdi-world/research/a10/cruise_linked/")
    assert sm._s3_path("oa_airports") == \
        "s3a://eurocontrol/opdi-world/oa_airports"


def test_writing_a_passthrough_table_is_refused():
    r.redirect_arm("debounced")
    with pytest.raises(AssertionError, match="a10"):
        StorageManager.write_table(_manager(), None, "oa_airports", "overwrite")


def test_the_sample_is_stable_and_about_a_tenth(spark):
    rows = [(f"{i:06x}", t) for i in range(5000) for t in range(3)]
    df = spark.createDataFrame(rows, ["icao24", "t"])
    kept = r.sample_airframes(df)
    again = {x.icao24 for x in r.sample_airframes(df).collect()}
    assert again == {x.icao24 for x in kept.collect()}
    airframes = kept.select("icao24").distinct().count()
    assert 400 <= airframes <= 600
    # whole airframes, never a thinned one
    assert kept.count() == airframes * 3


def test_each_arm_and_prefix_has_its_own_logs():
    assert r.log_dir_name("debounced") != r.log_dir_name("cruise_linked")
    assert r.log_dir_name("debounced") != "adsc-worldwide-base"
    assert "a10" in r.log_dir_name("debounced")


def test_a_low_bucket_stops_the_run():
    r.check_free_space(used_gb=150.0)
    with pytest.raises(RuntimeError, match="free"):
        r.check_free_space(used_gb=198.0)
    r.check_free_space(used_gb=197.0)  # exactly the floor is allowed
    with pytest.raises(RuntimeError, match="free"):
        r.check_free_space(used_gb=197.5)


def test_the_bucket_is_checked_before_spark_starts():
    import inspect

    src = inspect.getsource(r.run_arm)
    assert src.index("check_free_space") < src.index("create_session")
