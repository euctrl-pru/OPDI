import csv
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


def test_comment_7_the_flight_list_runs_for_two_arms_only():
    """airframe_only is the ablation's midpoint and has no ADEP/ADES question."""
    assert tp.FLIGHT_LIST_METHODS == ["legacy", "standard"]
    assert "airframe_only" not in tp.FLIGHT_LIST_METHODS


def test_build_flight_list_and_score_adep_ades_are_gated_on_flight_list_methods():
    """Source-level guard: the two step-03 calls must sit behind the same `if`.

    A cluster run is the only way to prove `airframe_only`'s row actually lacks
    ADEP/ADES columns; short of that, asserting the gate exists in the source
    is what catches a regression that un-guards one call but not the other.
    """
    src = Path(tp.__file__).read_text()
    body = src.split("if method in FLIGHT_LIST_METHODS:", 1)[1]
    body = body.split("row.update(export_track_extents(", 1)[0]
    assert "build_flight_list(" in body
    assert "score_adep_ades(spark, method, period, days, args.k)" in body


def test_comment_4_score_segmentation_matches_the_gate_interval_too():
    """The gate join must use `t_off_block`/`t_in_block`, not the airborne pair."""
    src = Path(tp.__file__).read_text()
    body = src.split("def score_segmentation(", 1)[1]
    body = body.split("\n\n\n", 1)[0]
    assert 'bounds=("t_off_block", "t_in_block")' in body
    assert "score_arm_gated(matched, extents, matched_gate)" in body


def test_write_rows_csv_headers_on_the_union_not_the_first_row(tmp_path):
    """The one real trap in comment 7: a subset-first row order must not crash
    or silently drop a later row's extra columns.

    Two orders are exercised because each breaks a different naive
    implementation: fieldnames taken from the first row (`sorted(row)`) either
    crashes on the extra-keys row landing second, or -- taken from a
    flight-list row first -- merely leaves the missing cells blank, which is
    only right by the accident of argument order, not by construction.
    """
    airframe_only_row = {"method": "airframe_only", "clean_match_pct": 90.0}
    legacy_row = {"method": "legacy", "clean_match_pct": 91.0, "coverage": 0.8}

    for order, rows in [
        ("flight-list-method first", [legacy_row, airframe_only_row]),
        ("airframe_only first", [airframe_only_row, legacy_row]),
    ]:
        out = tmp_path / f"{order}.csv"
        tp.write_rows_csv(out, rows)
        with out.open() as fh:
            read_rows = list(csv.DictReader(fh))
        by_method = {r["method"]: r for r in read_rows}
        assert by_method["legacy"]["coverage"] == "0.8", order
        # airframe_only's row never computed `coverage` -- the union header
        # must still declare the column, and DictReader reports the blank
        # cell as an empty string rather than raising or omitting the key.
        assert by_method["airframe_only"]["coverage"] == "", order


def test_every_pipeline_step_disables_the_processed_month_skip():
    """All three steps must pass `skip_if_processed=False`.

    The processed-month log is a local parquet under `OPDI_live/logs/`, outside
    the per-method S3 cleanup, so a marker outlives the table it describes.
    Steps 02 and 02a already opted out; step 03 did not, and the cost was a
    whole `pipeline_2025` run: a marker left by an earlier, long-dead run made
    `process_date_range` skip step 03, and `score_adep_ades` then died on
    PATH_NOT_FOUND reading a flight list nothing had written.

    Source-level because the failure needs a cluster and a stale log file to
    reproduce, and neither belongs in a unit test. Asserting all three call
    sites together is what stops the next step from being added without it.
    """
    src = Path(tp.__file__).read_text()
    for fn in ("def build_tracks", "def clean_tracks", "def build_flight_list"):
        body = src.split(fn, 1)[1].split("\ndef ", 1)[0]
        assert "skip_if_processed=False" in body, (
            f"{fn} does not disable the processed-month skip; a marker from an "
            "earlier method or an earlier run will silently skip its work"
        )


def test_each_method_gets_a_private_emptied_progress_log(tmp_path, monkeypatch):
    """Per-method log dirs, emptied on use — the only fix that reaches the
    endpoint-candidate cache.

    `build_endpoint_candidates` is guarded by its own `rebuild` flag, which
    `skip_if_processed` deliberately does not control: re-running the flight
    list at different thresholds genuinely should reuse the cache. Between
    arms it must not, because the tracks the candidates derive from are what
    changed. Scoping the whole directory is what covers all three markers.
    """
    seen = []
    # Bind the real function BEFORE patching: `tp.shutil` is the same module
    # object this patches, so a lambda closing over the module would call
    # itself.
    real_rmtree = tp.shutil.rmtree

    def spy(path, **kw):
        seen.append(path)
        return real_rmtree(path, **kw)

    monkeypatch.setattr(tp.shutil, "rmtree", spy)

    a = tp.flight_list_log_dir("legacy")
    b = tp.flight_list_log_dir("standard")
    assert a != b, "two arms must not share a progress log"
    assert "legacy" in a and "standard" in b
    # emptied, not merely separated: a surviving marker can only ever lie,
    # because the arm's tables are deleted at the end of its own iteration
    assert seen == [a, b]


def test_build_flight_list_passes_the_method_through():
    """A per-method log dir is worthless if the method never reaches it."""
    src = Path(tp.__file__).read_text()
    sig = src.split("def build_flight_list(", 1)[1].split(")", 1)[0]
    assert "method" in sig
    body = src.split("def build_flight_list(", 1)[1].split("\ndef ", 1)[0]
    assert "log_dir=flight_list_log_dir(method)" in body
