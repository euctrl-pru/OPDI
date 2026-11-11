"""Every study module the tests use still imports.

The list is the set of ``benchmarks/`` modules the test suite imported before
the study variants moved out of the package, the scripts changed when they
moved, and ``study_variants`` itself. A
module that names a removed package name fails here rather than on the
cluster.
"""

import importlib

import pytest

MODULES = [
    "adsc_bridging_diagnosis",
    "adsc_census",
    "adsc_coverage_gain",
    "adsc_events_compare",
    "adsc_flight_list_truth",
    "adsc_subset_breakdown",
    "adsc_time_sync",
    "build_airspace_v1_paper",
    "build_candidates",
    "build_candidates_2024",
    "build_track_v4_paper",
    "build_track_v5_paper",
    "build_zones",
    "callsign_flicker",
    "check_main_parity",
    "check_report_numbers",
    "clean_tracks",
    "compare_layout_sources",
    "debounced_2plus_persist",
    "debounced_vs_recommended_merge",
    "decimation_end_to_end",
    "elevation_arms",
    "elevation_bands",
    "event_bench",
    "events_compare",
    "events_gt",
    "events_score",
    "fir_consistency",
    "fir_truth_score",
    "flight_list_2026",
    "flight_list_v6",
    "flight_list_v62",
    "flight_list_v7",
    "free_adsc_scratch",
    "holding_labels_score",
    "osn_sample",
    "point_merge_report",
    "provenance",
    "rebuild_sample",
    "regenerate_adsc_v1",
    "regenerate_events_v4",
    "regenerate_track_v1",
    "regenerate_track_v2",
    "regenerate_track_v3",
    "regenerate_track_v4",
    "regenerate_track_v5",
    "regenerate_v6",
    "regenerate_v62",
    "regenerate_v7",
    "run_adsc_day",
    "run_airspace_events_day",
    "run_debounced_day",
    "run_track_link_day",
    "study_variants",
    "study_variants.detection",
    "study_variants.segmentation",
    "tools.zone_viewer",
    "track_continuity",
    "track_diagnostics",
    "track_fragmentation",
    "track_link_controls",
    "track_link_descent",
    "track_link_grid",
    "track_link_pipeline_score",
    "track_methods",
    "track_payoff",
    "track_pipeline_v2",
    "track_score",
    "track_sweep",
    "track_truth",
    "trend_sweep_agl",
    "view_zones",
]


@pytest.mark.parametrize("name", MODULES)
def test_the_study_module_imports(name):
    importlib.import_module(name)
