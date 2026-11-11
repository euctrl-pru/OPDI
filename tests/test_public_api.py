r"""Every name the repository imports from ``opdi`` stays importable from the same path.

The lists below were generated from the repository by reading every Python
file under ``src/``, ``tests/``, ``benchmarks/``, ``examples/``, ``tools/`` and
``airflow/``, and the root script ``run_period.py``, and
collecting each absolute ``from opdi... import name`` and ``import opdi...``
statement (with Python's ``ast`` module; the equivalent grep is
``grep -rhoE "^\s*(from opdi[.a-z_0-9]* import|import opdi[.a-z_0-9]*)" ...``).
A structural change (a module turned into a package, code moved between
modules) must keep every one of these working, through re-exports where
needed. Regenerate the lists only when a name is removed on purpose.
"""

import importlib

import pytest

#: Modules imported with ``import opdi...`` or used as ``from ... import``.
MODULES = (
    "opdi",
    "opdi.cleaning",
    "opdi.cleaning.cleaner",
    "opdi.cleaning.native",
    "opdi.cli",
    "opdi.config",
    "opdi.coverage",
    "opdi.ingestion",
    "opdi.ingestion.osn_adsc",
    "opdi.ingestion.osn_aircraft_db",
    "opdi.ingestion.osn_statevectors",
    "opdi.ingestion.ourairports",
    "opdi.output",
    "opdi.output.published",
    "opdi.output.schema",
    "opdi.periodrun",
    "opdi.pipeline",
    "opdi.pipeline.crossings",
    "opdi.pipeline.elevation",
    "opdi.pipeline.events",
    "opdi.pipeline.fir",
    "opdi.pipeline.flight_list_milestones",
    "opdi.pipeline.flight_list_step",
    "opdi.pipeline.flights",
    "opdi.pipeline.ground",
    "opdi.pipeline.holding",
    "opdi.pipeline.holes",
    "opdi.pipeline.layout",
    "opdi.pipeline.level_segments",
    "opdi.pipeline.point_merge",
    "opdi.pipeline.runway_ops",
    "opdi.pipeline.runways",
    "opdi.pipeline.segmentation",
    "opdi.pipeline.segmentation.base",
    "opdi.pipeline.segmentation.methods",
    "opdi.pipeline.tracks",
    "opdi.pipeline.vertical_pru",
    "opdi.reference",
    "opdi.reference.h3_airport_layouts",
    "opdi.reference.h3_airport_zones",
    "opdi.reference.h3_airspaces",
    "opdi.reference.h3_runway_grid",
    "opdi.reference.pbf_filter",
    "opdi.reference.pbf_source",
    "opdi.reference.point_merge",
    "opdi.runner",
    "opdi.sql",
    "opdi.sql.render",
    "opdi.utils",
    "opdi.utils.batching",
    "opdi.utils.datetime_helpers",
    "opdi.utils.geospatial",
    "opdi.utils.h3_helpers",
    "opdi.utils.spark_helpers",
    "opdi.utils.storage",
)

#: Module -> the names imported from it.
NAMES = {
    "opdi": (
        "H3Config",
        "IngestionConfig",
        "OPDIConfig",
        "ProjectConfig",
        "SparkConfig",
        "__version__",
        "orchestration",
        "periodrun",
        "published",
        "runner",
    ),
    "opdi.cleaning": (
        "cleaner",
        "native",
    ),
    "opdi.cleaning.cleaner": ("TrackCleaner",),
    "opdi.cleaning.native": (
        "AVIATION_UNIT_FACTOR",
        "CLEANED_COLUMNS",
        "_T",
        "_next_valid",
        "_spike_thresholds",
        "_track_window",
        "add_segment_id",
        "clean_tracks",
        "drop_duplicate_statevectors",
        "mask_derivative_spikes",
        "mask_isolated_points",
        "mask_out_of_range_positions",
        "mask_stale_broadcasts",
        "null_rate_report",
    ),
    "opdi.cli": ("main",),
    "opdi.config": (
        "AdscConfig",
        "CleaningConfig",
        "DetectionConfig",
        "EventConfig",
        "OPDIConfig",
        "STUDY_SEGMENTATION_METHODS",
        "SegmentationConfig",
    ),
    "opdi.coverage": (
        "CoverageConfig",
        "EUROPE_BBOX",
    ),
    "opdi.ingestion": (
        "AircraftDatabaseIngestion",
        "OurAirportsIngestion",
        "StateVectorIngestion",
        "osn_statevectors",
    ),
    "opdi.ingestion.osn_adsc": (
        "ADSB_SOURCE",
        "ADSC_SOURCE",
        "AdscIngestion",
        "shadow_filter",
        "to_opdi_schema",
    ),
    "opdi.ingestion.osn_aircraft_db": ("AircraftDatabaseIngestion",),
    "opdi.ingestion.osn_statevectors": (
        "DECIMATION_BUCKET",
        "StateVectorIngestion",
    ),
    "opdi.ingestion.ourairports": ("OurAirportsIngestion",),
    "opdi.output": (
        "ParquetExporter",
        "schema",
    ),
    "opdi.output.published": (
        "FLIGHT_EVENTS_BLOOM",
        "PublishedLayout",
        "_exists",
        "build_flight_events",
        "build_flight_list",
        "pipeline_commit",
        "publish_day",
        "publish_range",
        "write_single_file",
    ),
    "opdi.output.schema": (
        "CATALOGUE",
        "CLASSIC_EVENT_COLUMNS",
        "CLASSIC_FLIGHT_LIST",
        "CLASSIC_MEASUREMENT_COLUMNS",
        "Column",
        "EVENT_COLUMNS",
        "FLIGHT_EVENTS",
        "FLIGHT_INDEX",
        "FLIGHT_LIST",
        "GROUPS",
        "MEASUREMENT_COLUMNS",
        "MEASUREMENT_SUFFIX",
        "MEASUREMENT_VERSION",
        "RELEASE",
        "as_csv",
        "as_markdown",
    ),
    "opdi.periodrun": (
        "DAY_STEPS",
        "DEFAULT_LOG_DIR",
        "DEFAULT_WAREHOUSE",
        "INGEST_LOOKAHEAD_DAYS",
        "MIN_DRIVER_HEAP_GB",
        "OPTIONAL_STEPS",
        "PERIOD_STEPS",
        "REFERENCE_OUTPUTS",
        "REQUIRED_REFERENCE_TABLES",
        "RunState",
        "TRACK_TABLES",
        "UNDATED_STEPS",
        "WINDOW_STEPS",
        "_UNSET",
        "_log_dir",
        "_resolve_warehouse",
        "day_state_key",
        "days_in",
        "period_window",
        "plan_steps",
        "preflight",
        "preflight_fresh_build",
        "prune_tracks_for_day",
        "reference_substeps_to_run",
        "resolve_pbf_path",
        "run_day_step",
        "run_period",
        "safe_driver_memory",
    ),
    "opdi.pipeline": (
        "FlightEventProcessor",
        "FlightListProcessor",
        "TrackProcessor",
        "events",
        "flights",
        "level_segments",
    ),
    "opdi.pipeline.crossings": (
        "flight_level_crossings",
        "ring_crossings",
        "threshold_crossings",
    ),
    "opdi.pipeline.elevation": (
        "attach_field_elevation",
        "height_above_aerodrome_ft",
        "height_above_elevation_ft",
        "height_above_field_ft",
    ),
    "opdi.pipeline.events": (
        "DISTANCE_VERSION",
        "EXTRA_MEASURES",
        "FlightEventProcessor",
        "LEGACY_DISTANCE_VERSION",
        "LEGACY_EVENTS_VERSION",
        "TRACK_COLUMNS",
        "_smooth_phase",
        "add_distance_measure",
        "add_time_measure",
        "calculate_airport_events",
        "calculate_block_events",
        "calculate_horizontal_segment_events",
        "calculate_ring_crossing_events",
        "calculate_threshold_crossing_events",
        "distance_version",
        "measurement_suffix",
        "merge_milestone_duplicates",
        "resolve_flight_id",
    ),
    "opdi.pipeline.fir": (
        "FIR_VERSION",
        "attach_fir",
        "calculate_fir_events",
        "h3_parent_expr",
        "load_fir_reference",
    ),
    "opdi.pipeline.flight_list_milestones": (
        "ADDED_COLUMNS",
        "FLIGHT_LIST_RING_RADII_NM",
        "MOVEMENT_COLUMNS",
        "enrich_flight_list",
    ),
    "opdi.pipeline.flight_list_step": ("enrich_day",),
    "opdi.pipeline.flights": (
        "EARTH_R_NM",
        "FLIGHT_LIST_VERSION",
        "FT_PER_M",
        "FlightListProcessor",
        "LEGACY_ENDPOINT_VERSION",
        "LEGACY_TREND_VERSION",
        "OOA",
        "angle_between",
        "at_border",
        "bearing_deg",
        "dominant_flight_id",
        "haversine_nm",
        "height_above_field",
        "resolve_flight_id",
        "trend_altitude_cut",
        "trend_prefilter_ceiling_ft",
    ),
    "opdi.pipeline.ground": (
        "block_times",
        "movement_window",
    ),
    "opdi.pipeline.holding": (
        "FT_PER_M",
        "HOLDING_MEASURES",
        "HOLDING_VERSION",
        "_haversine_nm",
        "_wrap_deg",
        "calculate_holding_events",
    ),
    "opdi.pipeline.holes": (
        "PIECE",
        "add_piece",
    ),
    "opdi.pipeline.layout": (
        "LEGACY_EVENTS_VERSION",
        "calculate_airport_events",
        "flight_aerodrome_sets",
        "height_above_field_ft",
    ),
    "opdi.pipeline.level_segments": (
        "LEVEL_ARMS",
        "classify_level_offs",
        "level_segments",
        "level_segments_pru",
    ),
    "opdi.pipeline.point_merge": (
        "PMS_MEASURES",
        "PMS_VERSION",
        "calculate_point_merge_events",
    ),
    "opdi.pipeline.runway_ops": (
        "MILESTONE_SCHEMA",
        "along_track_nm",
        "classify_traversal",
        "go_arounds",
        "runway_milestones",
        "runway_traversals",
    ),
    "opdi.pipeline.runways": (
        "cross_track_nm",
        "detect_runway_movements",
        "runway_thresholds",
    ),
    "opdi.pipeline.segmentation": (
        "BreakRule",
        "SegmentationParams",
        "assign_track_id",
        "methods",
    ),
    "opdi.pipeline.segmentation.base": (
        "BreakRule",
        "FPM_PER_MPS",
        "FT_PER_M",
        "KT_PER_MPS",
        "SegmentationParams",
        "altitude_ft",
        "assign_track_id",
        "gap_minutes",
        "lookback_minutes",
        "segment_window",
        "speed_kt",
    ),
    "opdi.pipeline.segmentation.methods": (
        "ARMS",
        "LINK_DIRECTION_MIN_RATES",
        "LINK_EDGE_SAMPLES",
        "PLACEHOLDER_ICAO24",
        "STUDY_ARMS",
        "STUDY_HOME",
        "callsign_memory_minutes",
        "cruise_linked",
        "debounced",
        "debounced_callsign_change",
        "legacy",
        "legacy_general_gap",
        "link_checks",
        "link_feature_columns",
        "linkable",
        "select_link_features",
        "stable_callsign",
    ),
    "opdi.pipeline.tracks": (
        "TrackProcessor",
        "_geo_to_h3_cell",
        "_resolve_segmentation_method",
        "keep_tracks_starting_in",
        "required_lookback_hours",
        "tracks_touching_window_end",
    ),
    "opdi.pipeline.vertical_pru": (
        "aerodrome_distances",
        "attach_aerodrome_geometry",
        "pru_top_events",
        "pru_tops",
    ),
    "opdi.reference": (
        "AirportDetectionZoneGenerator",
        "AirportLayoutGenerator",
        "AirspaceH3Generator",
        "h3_airport_layouts",
        "h3_airport_zones",
        "h3_airspaces",
    ),
    "opdi.reference.h3_airport_layouts": (
        "AEROWAY_TAGS",
        "AirportLayoutGenerator",
        "HEXAERO_SCHEMA",
        "clean_str",
        "hexagonify_airport",
        "retrieve_osm_data",
    ),
    "opdi.reference.h3_airport_zones": (
        "AirportDetectionZoneGenerator",
        "generate_circle_polygon",
    ),
    "opdi.reference.h3_airspaces": (
        "AIRSPACE_SCHEMA",
        "AirspaceH3Generator",
        "DEFAULT_FIR_URLS",
        "dissolve_volumes",
        "fill_geometry",
        "fill_geometry_compact",
    ),
    "opdi.reference.h3_runway_grid": (
        "APPROACH_ZONE",
        "DEFAULT_WIDTH_FT",
        "NullGeometryError",
        "RUNWAY_ZONE",
        "RunwayGridGenerator",
        "_strip_cells",
        "runway_cells",
    ),
    "opdi.reference.pbf_filter": ("filter_aeroway_pbf",),
    "opdi.reference.pbf_source": (
        "BOX_MARGIN_KM",
        "PbfLayoutSource",
        "_deg_lon",
        "airport_boxes",
        "pbf_has_aerodrome_outside_bbox",
        "read_aerodromes",
        "read_aeroway_features",
    ),
    "opdi.reference.point_merge": (
        "PMS_CSV",
        "PMS_SCHEMA",
        "load_point_merge_systems",
        "point_merge_systems_for_day",
    ),
    "opdi.runner": (
        "REFERENCE_SUBSTEPS",
        "STEPS",
        "_AEROWAY_EXTRACT_PREFIX",
        "_fmt_size",
        "_prepare_layout_extract",
        "reference_substeps_for_coverage",
        "run_pipeline",
    ),
    "opdi.sql": ("render",),
    "opdi.sql.render": (
        "FLIGHT_EVENTS_FILES",
        "FLIGHT_INDEX_FILES",
        "FLIGHT_INDEX_VIEW_START",
        "FLIGHT_LIST_FILES",
    ),
    "opdi.utils": ("h3_helpers",),
    "opdi.utils.batching": ("filter_batch",),
    "opdi.utils.datetime_helpers": (
        "generate_intervals",
        "generate_months",
        "get_data_within_interval",
        "get_data_within_timeframe",
        "get_start_end_of_month",
    ),
    "opdi.utils.geospatial": (
        "add_cumulative_distance",
        "calculate_bearing",
        "destination_point",
        "flight_level_to_meters",
        "haversine_distance",
        "meters_to_flight_level",
    ),
    "opdi.utils.h3_helpers": (
        "compact_h3_set",
        "get_h3_coords",
        "h3_distance",
        "h3_list_prep",
        "hex_ring",
        "is_valid_h3_index",
        "k_ring",
        "polyfill_geojson",
        "uncompact_h3_set",
    ),
    "opdi.utils.spark_helpers": (
        "SparkSessionManager",
        "get_spark",
    ),
    "opdi.utils.storage": (
        "HIVE_DEFAULT_PARTITION",
        "StorageManager",
        "_partition_dir_value",
    ),
}

#: Names the tests import only to check that they are refused with a message
#: saying where they went (removed from the package on purpose).
REMOVED = {
    ("opdi.output", "ParquetExporter"),
    ("opdi.pipeline.segmentation.methods", "debounced"),
}

#: Optional packages a module may need (the whole list needs PySpark, which
#: the test environment has).
OPTIONAL = {"osmium", "geopandas"}


@pytest.mark.parametrize("module", MODULES)
def test_every_module_imports(module):
    try:
        importlib.import_module(module)
    except ModuleNotFoundError as exc:
        if exc.name in OPTIONAL:
            pytest.skip(f"{module} needs {exc.name}")
        raise


@pytest.mark.parametrize("module,name", [(m, n) for m, names in NAMES.items() for n in names])
def test_every_imported_name_is_still_there(module, name):
    if (module, name) in REMOVED:
        with pytest.raises(ImportError):
            getattr(importlib.import_module(module), name)
        return
    try:
        mod = importlib.import_module(module)
    except ModuleNotFoundError as exc:
        if exc.name in OPTIONAL:
            pytest.skip(f"{module} needs {exc.name}")
        raise
    if not hasattr(mod, name):
        # A submodule imported by name (``from opdi import published``).
        importlib.import_module(f"{module}.{name}")
    else:
        assert getattr(mod, name) is not None or name.startswith("_")
