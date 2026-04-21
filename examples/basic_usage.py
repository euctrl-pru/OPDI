"""
Basic usage example for the OPDI package.

Demonstrates how to use the complete OPDI pipeline, from configuration
through data ingestion, track processing, flight list generation,
event detection, data export, and monitoring.

Each pipeline step corresponds to scripts 00_* through 08_* in the
original OPDI-live v2.0.0 pipeline.
"""

from datetime import date
from opdi.config import OPDIConfig
from opdi.utils.spark_helpers import SparkSessionManager
from opdi.utils.datetime_helpers import generate_months, get_start_end_of_month


def main():
    """Walk through the full OPDI pipeline."""

    # ---- Configuration (shared across all steps) ----
    config = OPDIConfig.for_environment("dev")  # "dev", "live", or "local"

    print("OPDI Configuration:")
    print(f"  Project: {config.project.project_name}")
    print(f"  H3 resolutions: detection={config.h3.airport_detection_resolution}, "
          f"layout={config.h3.airport_layout_resolution}")

    spark = SparkSessionManager.create_session(app_name="OPDI Pipeline", config=config)
    print(f"  Spark version: {spark.version}\n")

    # ================================================================
    # STEP 00 - Reference data (run once or when sources update)
    # ================================================================
    # Each generator creates H3-encoded reference data used downstream.

    # 00a: Airport detection zones (H3 hex rings around airports)
    from opdi.reference import AirportDetectionZoneGenerator

    zone_gen = AirportDetectionZoneGenerator(spark, config)
    # zone_gen.generate()
    # zone_gen.save_to_parquet("data/airport_hex/zones_res7.parquet")
    # df_prepared = zone_gen.prepare_for_flight_list(max_radius_nm=30)
    # df_prepared.to_parquet("data/airport_hex/zones_res7_processed.parquet")

    # 00b: Airport ground layouts (runways, taxiways from OSM -> H3)
    from opdi.reference import AirportLayoutGenerator

    layout_gen = AirportLayoutGenerator(spark, config)
    # layout_gen.create_table_if_not_exists()
    # layout_gen.process_all()

    # 00c: Airspace boundaries (ANSP, FIR, countries -> H3)
    from opdi.reference import AirspaceH3Generator

    airspace_gen = AirspaceH3Generator(spark, config)
    # airspace_gen.create_table_if_not_exists()
    # airspace_gen.process_all()

    # 00d: OurAirports reference data (airports, runways, navaids, etc.)
    from opdi.ingestion import OurAirportsIngestion

    oa_ingestion = OurAirportsIngestion(spark, config)
    # oa_ingestion.ingest_all()

    # 00e: OpenSky aircraft database
    from opdi.ingestion import AircraftDatabaseIngestion

    acdb_ingestion = AircraftDatabaseIngestion(spark, config)
    # acdb_ingestion.ingest()

    # ================================================================
    # STEP 01 - Raw data ingestion (state vectors from OpenSky S3)
    # ================================================================
    from opdi.ingestion import StateVectorIngestion

    sv_ingestion = StateVectorIngestion(spark, config)
    # sv_ingestion.ingest(year_filter=2024)

    # ================================================================
    # STEP 02 - Track processing (state vectors -> continuous tracks)
    # ================================================================
    from opdi.pipeline import TrackProcessor

    track_proc = TrackProcessor(spark, config)
    # track_proc.create_table_if_not_exists()
    # track_proc.process_date_range(date(2024, 1, 1), date(2024, 6, 1))

    # ================================================================
    # STEP 03 - Flight list generation (tracks -> flights with airports)
    # ================================================================
    from opdi.pipeline import FlightListProcessor

    flight_proc = FlightListProcessor(spark, config)
    # flight_proc.create_table_if_not_exists()
    # flight_proc.process_date_range(
    #     date(2024, 1, 1), date(2024, 6, 1),
    #     airports_hex_path="data/airport_hex/zones_res7_processed.parquet"
    # )

    # ================================================================
    # STEP 04 - Flight events & measurements (phases, crossings, etc.)
    # ================================================================
    from opdi.pipeline import FlightEventProcessor

    event_proc = FlightEventProcessor(spark, config)
    # event_proc.create_tables_if_not_exist()
    # event_proc.process_date_range(date(2024, 1, 1), date(2024, 6, 1))

    # ================================================================
    # STEP 05 - Export to parquet files
    # ================================================================
    from opdi.output import ParquetExporter

    exporter = ParquetExporter(spark, config, output_dir="data/OPDI/v002")
    # exporter.export_all(
    #     start_date=date(2024, 1, 1),
    #     end_date=date(2024, 6, 1),
    #     last_n_months=4,
    # )

    # ================================================================
    # STEP 06 - Cleanup (deduplicate + export to CSV.gz)
    # ================================================================
    from opdi.output import CSVExporter

    # csv_exporter = CSVExporter("data/OPDI/v002/measurements", "data/OPDI/v002/measurements_clean")
    # csv_exporter.clean_and_export_all()

    # ================================================================
    # STEP 07 - Basic statistics
    # ================================================================
    from opdi.monitoring import BasicStatsCollector

    stats = BasicStatsCollector(spark, config)
    # stats.print_summary()

    # ================================================================
    # STEP 08 - Advanced statistics & data quality
    # ================================================================
    from opdi.monitoring import AdvancedStatsCollector

    advanced = AdvancedStatsCollector(spark, config)
    # advanced.generate_quality_report(
    #     "osn_statevectors_v2",
    #     "daily_counts.csv",
    #     "daily_counts.html",
    # )

    # ---- Date utility examples (always runnable) ----
    months = generate_months(date(2024, 1, 1), date(2024, 3, 1))
    print("Months to process:")
    for m in months:
        start_ts, end_ts = get_start_end_of_month(m)
        print(f"  {m.strftime('%B %Y')}: unix {start_ts:.0f} - {end_ts:.0f}")

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
