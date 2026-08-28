import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import track_pipeline_v2 as tp  # noqa: E402


def test_every_table_step_03_writes_is_redirected():
    """The guard catches an unredirected write; the map must make it unnecessary.

    `opdi_endpoint_candidates` is written mode="overwrite". Unredirected it
    resolves to the production cache, so an unguarded run destroys it and every
    later arm reads another arm's candidates. The smoke run proved the guard
    fires; this proves it should not have to.
    """
    assert "opdi_endpoint_candidates" in tp.TABLES
    assert "opdi_flight_list" in tp.TABLES
    resolved = tp.table_for("legacy", "opdi_endpoint_candidates")
    assert resolved.startswith("research/tcv2/legacy/")


def test_the_row_carries_the_extents_export_and_the_null_rates():
    """Both read osn_tracks_clean, and both must sit inside the same window.

    The cleaned table is deleted in the per-method ``finally``, so a call
    placed after it silently measures nothing; and step 03 has to have run
    first, or a failed arm leaves a summary describing a run that did not
    finish. Asserting on the source is crude -- the alternative is a cluster
    run -- and this ordering is exactly what a careless edit would break.
    """
    src = Path(tp.__file__).read_text()
    body = src.split("build_flight_list(spark, cfg, period", 1)[1]
    body = body.split("finally:", 1)[0]
    assert "export_track_extents(" in body
    assert "null_rates(" in body


def test_the_free_space_gate_uses_the_documented_quota():
    """A second copy of the quota is how a 98 GB bucket reported 1.5 GB free."""
    import track_methods

    src = Path(tp.__file__).read_text()
    assert "return BUCKET_QUOTA_GB - total / 1e9" in src
    assert "return 100.0 - total" not in src
    assert track_methods.BUCKET_QUOTA_GB == 200.0


def test_the_default_arm_list_includes_the_ablation_midpoint():
    """Without airframe_only the study reports a total it cannot decompose."""
    src = Path(tp.__file__).read_text()
    assert 'default=["legacy", "airframe_only", "standard"]' in src
