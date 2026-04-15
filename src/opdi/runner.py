"""
OPDI full pipeline runner.

Executes the complete OPDI data pipeline in order, mirroring the
sequential execution of the ``00_*`` through ``08_*`` scripts in
``OPDI-live/python/v2.0.0/``.

Pipeline stages
---------------
===  ==========================================  ================================
No.  Description                                 Module
===  ==========================================  ================================
00a  Airport H3 detection zones                  ``reference.h3_airport_zones``
00b  Airport ground layouts (OSM -> H3)          ``reference.h3_airport_layouts``
00c  Airspace boundaries (ANSP/FIR -> H3)        ``reference.h3_airspaces``
00d  OurAirports reference data                  ``ingestion.ourairports``
00e  OpenSky aircraft database                   ``ingestion.osn_aircraft_db``
01   State vector ingestion (OpenSky S3)         ``ingestion.osn_statevectors``
02   Track processing                            ``pipeline.tracks``
03   Flight list generation                      ``pipeline.flights``
04   Flight events & measurements                ``pipeline.events``
05   Export to parquet                            ``output.parquet_exporter``
06   Cleanup & CSV export                        ``output.csv_exporter``
07   Basic statistics                            ``monitoring.basic_stats``
08   Advanced statistics & quality report        ``monitoring.advanced_stats``
===  ==========================================  ================================

Usage
-----
Run the full pipeline::

    from datetime import date
    from opdi.runner import run_pipeline

    run_pipeline(
        env="live",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
    )

Run a single step::

    run_pipeline(env="dev", start_date=date(2024, 1, 1),
                 end_date=date(2024, 2, 1), step="02")

Or from the command line::

    opdi run --env live --start 2024-01-01 --end 2024-06-01
    opdi run --step 04 --env dev --start 2024-03-01 --end 2024-04-01
"""

import os
import time
from datetime import date
from typing import Optional

from opdi.config import OPDIConfig
from opdi.utils.spark_helpers import SparkSessionManager


def _print_spark_ui_link(spark):
    """Print a clickable link to the Spark UI.

    On Cloudera (CDSW/CML) the URL is built from environment variables.
    Elsewhere it falls back to the local Spark UI address.
    """
    import os

    engine_id = os.getenv("CDSW_ENGINE_ID")
    domain = os.getenv("CDSW_DOMAIN")

    if engine_id and domain:
        url = f"https://spark-{engine_id}.{domain}"
    else:
        # Local / non-CDSW environment — use the Spark UI port
        ui_port = spark.sparkContext.uiWebUrl or "http://localhost:4040"
        url = str(ui_port)

    print(f"\n  Spark UI: {url}\n")


def _step_00_reference_data(spark, config, **kwargs):
    """Step 00: Generate reference data (airports, airspaces)."""
    print("\n" + "=" * 70)
    print("STEP 00 - Reference data generation")
    print("=" * 70)

    # 00a: Airport detection zones
    print("\n--- 00a: Airport H3 detection zones ---")
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator

    zone_gen = AirportDetectionZoneGenerator(spark, config)
    zones = zone_gen.generate()
    zone_gen.save_to_parquet(
        kwargs.get("airports_hex_raw_path", "data/airport_hex/zones_res7.parquet")
    )
    prepared = zone_gen.prepare_for_flight_list(max_radius_nm=30)
    prepared_path = kwargs.get(
        "airports_hex_path",
        "data/airport_hex/zones_res7_processed.parquet",
    )
    os.makedirs(os.path.dirname(prepared_path) or ".", exist_ok=True)
    prepared.to_parquet(prepared_path)
    print(f"  Generated {len(zones)} zone-ring records, {len(prepared)} hex rows.")

    # 00b: Airport ground layouts
    print("\n--- 00b: Airport ground layouts (OSM -> H3) ---")
    from opdi.reference.h3_airport_layouts import AirportLayoutGenerator

    layout_gen = AirportLayoutGenerator(spark, config)
    layout_gen.create_table_if_not_exists()
    success, failed = layout_gen.process_all()
    print(f"  Processed {len(success)} airports, {len(failed)} failed.")

    # 00c: Airspace boundaries
    print("\n--- 00c: Airspace boundaries (ANSP/FIR/country -> H3) ---")
    from opdi.reference.h3_airspaces import AirspaceH3Generator

    airspace_gen = AirspaceH3Generator(spark, config)
    airspace_gen.create_table_if_not_exists()
    airspace_gen.process_all()

    # 00d: OurAirports reference data
    print("\n--- 00d: OurAirports reference data ---")
    from opdi.ingestion.ourairports import OurAirportsIngestion

    oa = OurAirportsIngestion(spark, config)
    oa.ingest_all()

    # 00e: OpenSky aircraft database
    print("\n--- 00e: OpenSky aircraft database ---")
    from opdi.ingestion.osn_aircraft_db import AircraftDatabaseIngestion

    acdb = AircraftDatabaseIngestion(spark, config)
    acdb.ingest()


def _step_01_ingest_statevectors(spark, config, start_date, end_date, **kwargs):
    """Step 01: Ingest raw state vectors from OpenSky S3."""
    print("\n" + "=" * 70)
    print(f"STEP 01 - State vector ingestion ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.ingestion.osn_statevectors import StateVectorIngestion

    sv = StateVectorIngestion(spark, config)
    sv.ingest(year_filter=start_date.year)


def _step_02_process_tracks(spark, config, start_date, end_date, **kwargs):
    """Step 02: Process raw state vectors into flight tracks."""
    print("\n" + "=" * 70)
    print(f"STEP 02 - Track processing ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.pipeline.tracks import TrackProcessor

    processor = TrackProcessor(spark, config)
    processor.create_table_if_not_exists()
    processor.process_date_range(start_date, end_date)


def _step_03_generate_flight_list(spark, config, start_date, end_date, **kwargs):
    """Step 03: Generate the OPDI flight list."""
    print("\n" + "=" * 70)
    print(f"STEP 03 - Flight list generation ({start_date} to {end_date})")
    print("=" * 70)

    airports_hex_path = kwargs.get(
        "airports_hex_path", "data/airport_hex/zones_res7_processed.parquet"
    )

    from opdi.pipeline.flights import FlightListProcessor

    processor = FlightListProcessor(spark, config)
    processor.create_table_if_not_exists()
    processor.process_date_range(start_date, end_date, airports_hex_path)


def _step_04_extract_events(spark, config, start_date, end_date, **kwargs):
    """Step 04: Extract flight events and measurements."""
    print("\n" + "=" * 70)
    print(f"STEP 04 - Flight events & measurements ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.pipeline.events import FlightEventProcessor

    processor = FlightEventProcessor(spark, config)
    processor.create_tables_if_not_exist()
    processor.process_date_range(start_date, end_date)


def _step_05_export_parquet(spark, config, start_date, end_date, **kwargs):
    """Step 05: Export OPDI tables to parquet files."""
    print("\n" + "=" * 70)
    print(f"STEP 05 - Parquet export ({start_date} to {end_date})")
    print("=" * 70)

    output_dir = kwargs.get("export_dir", "data/OPDI/v002")

    from opdi.output.parquet_exporter import ParquetExporter

    exporter = ParquetExporter(spark, config, output_dir=output_dir)
    exporter.export_all(
        start_date=start_date,
        end_date=end_date,
        last_n_months=kwargs.get("last_n_months", 4),
    )


def _step_06_cleanup(spark, config, start_date, end_date, **kwargs):
    """Step 06: Deduplicate and export to CSV.gz."""
    print("\n" + "=" * 70)
    print("STEP 06 - Data cleanup & CSV export")
    print("=" * 70)

    export_dir = kwargs.get("export_dir", "data/OPDI/v002")

    from opdi.output.csv_exporter import CSVExporter

    for subdir in ["flight_list", "flight_events", "measurements"]:
        import os

        input_dir = os.path.join(export_dir, subdir)
        output_dir = os.path.join(export_dir, f"{subdir}_clean")

        if os.path.isdir(input_dir):
            print(f"\n  Cleaning {subdir}...")
            exporter = CSVExporter(input_dir, output_dir)
            exporter.clean_and_export_all()


def _step_07_basic_stats(spark, config, **kwargs):
    """Step 07: Print basic table statistics."""
    print("\n" + "=" * 70)
    print("STEP 07 - Basic statistics")
    print("=" * 70)

    from opdi.monitoring.basic_stats import BasicStatsCollector

    collector = BasicStatsCollector(spark, config)
    collector.print_summary()


def _step_08_advanced_stats(spark, config, **kwargs):
    """Step 08: Generate advanced data quality report."""
    print("\n" + "=" * 70)
    print("STEP 08 - Advanced statistics & data quality")
    print("=" * 70)

    from datetime import datetime

    today = datetime.now().strftime("%Y-%m-%d")
    output_csv = kwargs.get("quality_csv", f"daily_rows_counts_{today}.csv")
    output_html = kwargs.get("quality_html", f"daily_rows_visualization_{today}.html")

    from opdi.monitoring.advanced_stats import AdvancedStatsCollector

    collector = AdvancedStatsCollector(spark, config)
    collector.generate_quality_report(
        "osn_statevectors_v2",
        output_csv,
        output_html,
    )


# Registry mapping step names to functions
STEPS = {
    "00": _step_00_reference_data,
    "01": _step_01_ingest_statevectors,
    "02": _step_02_process_tracks,
    "03": _step_03_generate_flight_list,
    "04": _step_04_extract_events,
    "05": _step_05_export_parquet,
    "06": _step_06_cleanup,
    "07": _step_07_basic_stats,
    "08": _step_08_advanced_stats,
}


def run_pipeline(
    env: str = "dev",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    step: Optional[str] = None,
    airports_hex_path: str = "data/airport_hex/zones_res7_processed.parquet",
    export_dir: str = "data/OPDI/v002",
    last_n_months: int = 4,
) -> int:
    """
    Run the OPDI pipeline end-to-end or a single step.

    This function mirrors the sequential execution of the ``00_*`` through
    ``08_*`` scripts in ``OPDI-live/python/v2.0.0/``.

    Args:
        env: Environment name (``"dev"``, ``"live"``, or ``"local"``).
        start_date: Pipeline start date (first day of first month to process).
        end_date: Pipeline end date (first day of last month to process).
        step: Run only this step (``"00"`` through ``"08"``).
            If ``None``, all steps run sequentially.
        airports_hex_path: Path to pre-generated airport hex zones parquet
            (produced by step 00, consumed by step 03).
        export_dir: Base directory for parquet/CSV exports (steps 05-06).
        last_n_months: Only export the last N months of data (step 05).

    Returns:
        Exit code: ``0`` on success, ``1`` on failure.

    Example:
        >>> from datetime import date
        >>> from opdi.runner import run_pipeline
        >>> run_pipeline(
        ...     env="live",
        ...     start_date=date(2024, 1, 1),
        ...     end_date=date(2024, 6, 1),
        ... )
    """
    if start_date is None:
        start_date = date(2022, 1, 1)
    if end_date is None:
        end_date = date.today()

    config = OPDIConfig.for_environment(env)

    print("=" * 70)
    print("OPDI Pipeline Runner v2.0.0")
    print("=" * 70)
    print(f"  Environment : {env}")
    print(f"  Project     : {config.project.project_name}")
    print(f"  Date range  : {start_date} to {end_date}")
    print(f"  Step        : {step or 'ALL (00-08)'}")
    print("=" * 70)

    spark = SparkSessionManager.create_session(
        app_name=f"OPDI Pipeline - {env}",
        config=config,
    )

    # Print Spark UI link
    _print_spark_ui_link(spark)

    overall_start = time.time()

    kwargs = dict(
        airports_hex_path=airports_hex_path,
        airports_hex_raw_path=airports_hex_path.replace("_processed", ""),
        export_dir=export_dir,
        last_n_months=last_n_months,
    )

    try:
        if step is not None:
            # Run a single step
            if step not in STEPS:
                print(f"Unknown step '{step}'. Valid steps: {sorted(STEPS.keys())}")
                return 1

            fn = STEPS[step]
            # Steps 00, 07, 08 don't need date range
            if step in ("00", "07", "08"):
                fn(spark, config, **kwargs)
            else:
                fn(spark, config, start_date, end_date, **kwargs)
        else:
            # Run all steps in order
            for step_id in sorted(STEPS.keys()):
                fn = STEPS[step_id]
                if step_id in ("00", "07", "08"):
                    fn(spark, config, **kwargs)
                else:
                    fn(spark, config, start_date, end_date, **kwargs)

    except Exception as e:
        print(f"\nPipeline FAILED at step {step or 'unknown'}: {e}")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        elapsed = time.time() - overall_start
        print(f"\nPipeline completed in {elapsed:.1f}s")
        spark.stop()

    return 0


if __name__ == "__main__":
    import sys

    run_pipeline(
        env=sys.argv[1] if len(sys.argv) > 1 else "dev",
        start_date=date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date(2022, 1, 1),
        end_date=date.fromisoformat(sys.argv[3]) if len(sys.argv) > 3 else date.today(),
    )
