"""Run the pipeline steps in order over a range of months.

:func:`run_pipeline` runs every step (or one step) of the pipeline over a
date range, in one Spark session. For day-by-day processing with resuming,
use :mod:`opdi.periodrun` or :mod:`opdi.orchestration` instead.

Pipeline stages
---------------
===  ==========================================  ================================
No.  Description                                 Module
===  ==========================================  ================================
00a  Airport H3 detection zones                  ``reference.h3_airport_zones``
00b  Airport layouts (local OSM extract -> H3)   ``reference.h3_airport_layouts``
00c  Airspace boundaries (ANSP/FIR -> H3)        ``reference.h3_airspaces``
00d  OurAirports reference data                  ``ingestion.ourairports``
00e  OpenSky aircraft database                   ``ingestion.osn_aircraft_db``
00f  Runway + approach H3 grid (oa_runways)      ``reference.h3_runway_grid``
01   State vector ingestion (OpenSky S3)         ``ingestion.osn_statevectors``
02   Track processing                            ``pipeline.tracks``
02a  Trajectory cleaning                         ``cleaning.cleaner``
03   Flight list generation                      ``pipeline.flights``
04   Flight events & measurements                ``pipeline.events``
04b  Flight list milestones                      ``pipeline.flight_list_step``
09   Published daily files                       ``output.published``
===  ==========================================  ================================

Usage
-----
Run the full pipeline::

    from datetime import date
    from opdi.runner import run_pipeline

    run_pipeline(
        env="opensky",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
    )

Run a single step::

    run_pipeline(env="opensky", start_date=date(2024, 1, 1),
                 end_date=date(2024, 2, 1), step="02")

Or from the command line::

    opdi run --start 2024-01-01 --end 2024-06-01
    opdi run --step 04 --start 2024-03-01 --end 2024-04-01
"""

import os
import time
from datetime import date
from pathlib import Path
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


def _step_00a_airport_zones(spark, config, **kwargs):
    """Step 00a: Airport H3 detection zones."""
    print("\n--- 00a: Airport H3 detection zones ---")
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator

    zone_gen = AirportDetectionZoneGenerator(spark, config)

    # Built from the ingested OurAirports table when it exists, so the zones
    # use the same snapshot as the rest of the pipeline (the public CSV is not
    # reachable from the OpenSky cluster).
    from opdi.utils.storage import StorageManager

    storage = StorageManager(spark, config)
    airports_df = storage.read_table("oa_airports") if storage.table_exists("oa_airports") else None
    zone_gen.generate(airports_df=airports_df)
    # No local Parquet copy is written: it would collect the whole result on
    # the driver (hundreds of millions of cells for a network build). Step 03
    # reads the table. `zone_gen.result_df` collects on demand.
    # Generated out to the full ring reach, not clipped to the detection
    # radius. The table carries min_c_radius_nm/max_c_radius_nm, so each
    # consumer narrows it at read time: the flight list keeps its 30 NM, and
    # ASMA ring crossings can use the same table out to 110 NM. Clipping here
    # would mean regenerating the whole reference for every new radius.
    max_radius = kwargs.get("airport_zone_max_radius_nm", max(zone_gen.radii_nm))
    airport_types = kwargs.get("airport_zone_types", None)
    table_name = kwargs.get("airport_zone_table", "h3_airport_detection_zones")

    # Built with Spark and written to the warehouse table, which is S3 in the
    # `opensky` environment.
    zone_gen.save_prepared_to_table(
        max_radius_nm=max_radius,
        airport_types=airport_types,
        table_name=table_name,
    )
    # Optional extra destinations (a local copy, a second bucket). Same batches,
    # no second generation -- see AirportDetectionZoneGenerator.save_prepared.
    extra = kwargs.get("airport_zone_extra_destinations")
    if extra:
        zone_gen.save_prepared(
            destinations=list(extra),
            max_radius_nm=max_radius,
            airport_types=airport_types,
            airports_df=airports_df,
        )
    # Count the rows written, read back from the table, so that an empty
    # table is reported as such.
    written = storage.read_table(table_name).count()
    print(f"  Wrote {written:,} zone rows -> table {table_name} " f"(rings to {max_radius:g} NM).")
    if written == 0:
        raise RuntimeError(
            f"{table_name} was written with 0 rows. A zone table with no cells "
            "silently yields no ADEP or ADES for any flight, so this is a "
            "failure rather than a result."
        )


#: Prefix marking an extract already reduced to aeroway geometry by
#: :func:`opdi.reference.pbf_filter.filter_aeroway_pbf`.
_AEROWAY_EXTRACT_PREFIX = "aeroway-"


def _fmt_size(n: int) -> str:
    """A byte count in the largest unit that keeps it readable (B to GB)."""
    for limit, unit in ((1073741824, "GB"), (1048576, "MB"), (1024, "KB")):
        if n >= limit:
            return f"{n / limit:.1f} {unit}"
    return f"{n} B"


def _prepare_layout_extract(pbf_path: str) -> str:
    """An aeroway-only extract of ``pbf_path``, filtered first if needed.

    Building airport areas from a raw extract of a continent needs the node
    locations of the whole file, more than 16 GB of memory. The aeroway part
    of Europe is about 16 MB against 34.9 GB, and reads in about a second.

    The filtered file is written beside its source as ``aeroway-<name>`` and
    reused unless the source is newer. A path already carrying the prefix is
    returned unchanged.
    """
    src = Path(pbf_path)
    if not src.exists():
        raise FileNotFoundError(
            f"OSM extract not found: {src}. Set h3.airport_layout_pbf_path or "
            f"OPDI_OSM_PBF to a Geofabrik .osm.pbf, or unset both to fall back "
            f"to the Overpass API."
        )
    if src.name.startswith(_AEROWAY_EXTRACT_PREFIX):
        return str(src)

    dst = src.with_name(_AEROWAY_EXTRACT_PREFIX + src.name)
    if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime:
        print(f"  Using cached aeroway extract {dst.name} " f"({_fmt_size(dst.stat().st_size)}).")
        return str(dst)

    from opdi.reference.pbf_filter import filter_aeroway_pbf

    print(
        f"  Filtering {src.name} ({_fmt_size(src.stat().st_size)}) "
        f"to aeroway geometry -> {dst.name} ..."
    )
    stats = filter_aeroway_pbf(str(src), str(dst))
    print(
        f"  Filtered in {stats['seconds']:.0f} s: "
        f"{_fmt_size(stats['bytes_in'])} -> {_fmt_size(stats['bytes_out'])} "
        f"({stats['nodes']:,} nodes, {stats['ways']:,} ways, "
        f"{stats['relations']:,} relations)."
    )
    return str(dst)


def _step_00b_airport_layouts(spark, config, **kwargs):
    """Step 00b: Airport ground layouts (OSM -> H3)."""
    print("\n--- 00b: Airport ground layouts (OSM -> H3) ---")
    from opdi.reference.h3_airport_layouts import AirportLayoutGenerator

    # A local extract when one is configured, else the public Overpass
    # service. The extract is preferred: Overpass finds an airport by name and
    # returns nothing, without an error, for a name it cannot resolve, and it
    # refuses requests for many airports.
    pbf_path = (
        kwargs.get("pbf_path")
        or os.environ.get("OPDI_OSM_PBF")
        or config.h3.airport_layout_pbf_path
    )
    if pbf_path:
        pbf_path = _prepare_layout_extract(pbf_path)
        print(f"  Source: local OSM extract {pbf_path}")
    else:
        print(
            "  Source: public Overpass API (no extract configured). This "
            "cannot build the full network -- see readme.md, step 00b."
        )

    layout_gen = AirportLayoutGenerator(spark, config, pbf_path=pbf_path)
    layout_gen.create_table_if_not_exists()
    success, failed = layout_gen.process_all()
    print(f"  Processed {len(success)} airports, {len(failed)} failed.")
    if failed:
        # Expected: airports outside the extract, or not mapped in
        # OpenStreetMap. Printed so the count is not mistaken for a failure.
        print(f"  Not built (no geometry in this extract): {len(failed)}")


def _step_00c_airspaces(spark, config, **kwargs):
    """Step 00c: Airspace boundaries (ANSP/FIR -> H3)."""
    print("\n--- 00c: Airspace boundaries (ANSP/FIR/country -> H3) ---")
    from opdi.reference.h3_airspaces import AirspaceH3Generator

    airspace_gen = AirspaceH3Generator(spark, config)
    airspace_gen.create_table_if_not_exists()
    airspace_gen.process_all()


def _step_00d_ourairports(spark, config, **kwargs):
    """Step 00d: OurAirports reference data."""
    print("\n--- 00d: OurAirports reference data ---")
    from opdi.ingestion.ourairports import OurAirportsIngestion

    oa = OurAirportsIngestion(spark, config)
    oa.ingest_all()


def _step_00e_aircraft_db(spark, config, **kwargs):
    """Step 00e: OpenSky aircraft database."""
    print("\n--- 00e: OpenSky aircraft database ---")
    from opdi.ingestion.osn_aircraft_db import AircraftDatabaseIngestion

    acdb = AircraftDatabaseIngestion(spark, config)
    # Overwrite: the file is a full snapshot of every aircraft, so appending
    # would double every aircraft in the table.
    acdb.ingest(mode="overwrite")


def _step_00f_runway_grid(spark, config, **kwargs):
    """Step 00f: the H3 grid of runways and approach corridors, from ``oa_runways``.

    Runs after 00d, which writes ``oa_airports`` and ``oa_runways``.
    """
    print("\n--- 00f: Runway + approach H3 grid (oa_runways) ---")
    from opdi.reference.h3_runway_grid import RunwayGridGenerator

    grid_gen = RunwayGridGenerator(spark, config)
    grid_gen.create_table_if_not_exists()
    success, failed = grid_gen.save_prepared_to_table()
    print(f"  Processed {len(success)} airports, {len(failed)} failed.")


#: Reference substeps in dependency order, which is not the order of their
#: ids: 00d writes ``oa_airports`` and ``oa_runways``, which 00a, 00b and 00f
#: read, so it runs first. The ids are kept because they are the names used
#: on the command line (``--no-airport-zones`` and so on) and in the output.
REFERENCE_SUBSTEPS = {
    "00d": ("OurAirports reference data", _step_00d_ourairports),
    "00e": ("OpenSky aircraft database", _step_00e_aircraft_db),
    "00a": ("Airport H3 detection zones", _step_00a_airport_zones),
    "00b": ("Airport ground layouts", _step_00b_airport_layouts),
    "00f": ("Runway + approach H3 grid", _step_00f_runway_grid),
    "00c": ("Airspace boundaries", _step_00c_airspaces),
}


def reference_substeps_for_coverage(config) -> dict:
    """Which reference substeps run under this coverage.

    All of them, under both coverages. The FIR layer of 00c is worldwide and
    is what the FIR crossings read; its ANSP layer is European and is absent
    elsewhere.
    """
    return {substep_id: True for substep_id in REFERENCE_SUBSTEPS}


def _step_00_reference_data(spark, config, **kwargs):
    """Step 00: Generate reference data (airports, airspaces)."""
    print("\n" + "=" * 70)
    print("STEP 00 - Reference data generation")
    print("=" * 70)

    from opdi.utils.storage import StorageManager

    storage = StorageManager(spark, config)
    # Checked before any substep runs, so a warehouse of the other coverage
    # is refused before anything is built.
    problem = storage.check_coverage()
    if problem is not None:
        raise RuntimeError(problem)

    run_ref = kwargs.get("run_reference", {})
    coverage_ref = reference_substeps_for_coverage(config)

    for substep_id, (label, fn) in REFERENCE_SUBSTEPS.items():
        if not coverage_ref.get(substep_id, True):
            print(
                f"\n  Skipping {substep_id} ({label}): sources are "
                "European and nothing reads the table"
            )
            continue
        if not run_ref.get(substep_id, True):
            print(f"\n  Skipping {substep_id} ({label})")
            continue
        fn(spark, config, **kwargs)

    # Stamped only after every substep has finished, so a build that stops
    # half way does not mark the warehouse.
    storage.write_coverage_marker()


def _step_01_ingest_statevectors(spark, config, start_date, end_date, **kwargs):
    """Step 01: Ingest raw state vectors from OpenSky S3."""
    print("\n" + "=" * 70)
    print(f"STEP 01 - State vector ingestion ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.ingestion.osn_statevectors import StateVectorIngestion

    sv = StateVectorIngestion(spark, config)
    # In the opensky environment the archive is read directly over S3A,
    # rather than downloaded with the MinIO client.
    if config.project.project_name == "opensky":
        sv.ingest_from_s3(start_date=start_date, end_date=end_date)
    else:
        sv.ingest(start_date=start_date, end_date=end_date)


def _step_01b_ingest_adsc(spark, config, start_date, end_date, **kwargs):
    """Step 01b: ingest the ADS-C reports.

    Does nothing unless ``config.adsc.enabled``; it is always listed, so a
    full run says that it skipped it.
    """
    if not config.adsc.enabled:
        print("\nSTEP 01b - ADS-C ingestion: disabled " "(config.adsc.enabled is False)")
        return

    print("\n" + "=" * 70)
    print(f"STEP 01b - ADS-C ingestion ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.ingestion.osn_adsc import AdscIngestion

    AdscIngestion(spark, config).ingest_from_s3(start_date=start_date, end_date=end_date)


def _step_02_process_tracks(spark, config, start_date, end_date, **kwargs):
    """Step 02: Process raw state vectors into flight tracks."""
    print("\n" + "=" * 70)
    print(f"STEP 02 - Track processing ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.pipeline.tracks import TrackProcessor

    processor = TrackProcessor(spark, config)
    processor.create_table_if_not_exists()
    processor.process_date_range(start_date, end_date)


def _step_02a_clean_tracks(spark, config, start_date, end_date, **kwargs):
    """Step 02a: Native trajectory cleaning over osn_tracks."""
    print("\n" + "=" * 70)
    print(f"STEP 02a - Trajectory cleaning ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.cleaning.cleaner import TrackCleaner

    cleaner = TrackCleaner(spark, config)
    cleaner.create_table_if_not_exists()
    cleaner.process_date_range(start_date, end_date)


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
    processor.process_date_range(
        start_date,
        end_date,
        airports_hex_path,
        adep_mode=kwargs.get("adep_mode"),
        ades_mode=kwargs.get("ades_mode"),
    )


def _step_04_extract_events(spark, config, start_date, end_date, **kwargs):
    """Step 04: Extract flight events and measurements."""
    print("\n" + "=" * 70)
    print(f"STEP 04 - Flight events & measurements ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.pipeline.events import FlightEventProcessor

    processor = FlightEventProcessor(spark, config)
    processor.create_tables_if_not_exist()
    processor.process_date_range(start_date, end_date)


def _step_04b_enrich_flight_list(spark, config, start_date, end_date, **kwargs):
    """Step 04b: fold the milestones back into the flight list.

    It works on the output of step 04, so it is numbered 04b.
    """
    print("\n" + "=" * 70)
    print(f"STEP 04b - Flight list milestones ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.pipeline.flight_list_step import enrich_day
    from opdi.utils.storage import StorageManager

    storage = StorageManager(spark, config)
    written = enrich_day(storage, config.events, start_date, end_date)
    if written is None:
        return
    print(f"Flight list enriched: {written:,} rows.")


def _step_09_publish(spark, config, start_date, end_date, **kwargs):
    """Step 09: write the published flight list and flight events files of each day."""
    print("\n" + "=" * 70)
    print(f"STEP 09 - Publish the day's files ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.output.published import publish_range

    publish_range(spark, config, start_date, end_date)


# Registry mapping step names to functions
STEPS = {
    "00": _step_00_reference_data,
    "01": _step_01_ingest_statevectors,
    "01b": _step_01b_ingest_adsc,
    "02": _step_02_process_tracks,
    "02a": _step_02a_clean_tracks,
    "03": _step_03_generate_flight_list,
    "04": _step_04_extract_events,
    "04b": _step_04b_enrich_flight_list,
    "09": _step_09_publish,
}


def run_pipeline(
    env: str = "opensky",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    step: Optional[str] = None,
    run_airport_zones: bool = True,
    run_airport_layouts: bool = True,
    run_airspaces: bool = True,
    run_ourairports: bool = True,
    run_aircraft_db: bool = True,
    run_runway_grid: bool = True,
    airports_hex_path: str = "data/airport_hex/zones_res7_processed.parquet",
    adep_mode: Optional[str] = None,
    ades_mode: Optional[str] = None,
    worldwide: bool = True,
) -> int:
    """Run the OPDI pipeline end-to-end or a single step.

    Args:
        env: Environment name (``"opensky"`` or ``"local"``).
        start_date: Pipeline start date (first day of first month to process).
        end_date: Pipeline end date (first day of last month to process).
        step: Run only this step (``"00"`` through ``"09"``).
            If ``None``, all steps run sequentially.
        run_airport_zones: Run step 00a, the airport detection zones.
        run_airport_layouts: Run step 00b, the airport layouts.
        run_airspaces: Run step 00c, the airspaces.
        run_ourairports: Run step 00d, the OurAirports data.
        run_aircraft_db: Run step 00e, the OpenSky aircraft database.
        run_runway_grid: Run step 00f, the runway and approach grid.
        adep_mode: Detection algorithm for departures in step 03
            (``"trend"`` or ``"endpoint"``). ``None`` uses the
            configured default.
        ades_mode: Detection method for arrivals in step 03.
        airports_hex_path: Path to pre-generated airport hex zones parquet
            (produced by step 00, consumed by step 03).
        worldwide: Process the whole world (the default). Pass ``False``
            for the European box. In the ``local`` environment this also
            selects the European warehouse.

    Returns:
        Exit code: ``0`` on success, ``1`` on failure.

    Example:
        >>> from datetime import date
        >>> from opdi.runner import run_pipeline
        >>> run_pipeline(
        ...     env="opensky",
        ...     start_date=date(2024, 1, 1),
        ...     end_date=date(2024, 6, 1),
        ...     run_airport_zones=False,
        ...     run_airport_layouts=False,
        ... )
    """
    if start_date is None:
        start_date = date(2022, 1, 1)
    if end_date is None:
        end_date = date.today()

    config = OPDIConfig.for_environment(env, worldwide=worldwide)

    print("=" * 70)
    print("OPDI Pipeline Runner v2.0.0")
    print("=" * 70)
    print(f"  Environment : {env}")
    print(f"  Project     : {config.project.project_name}")
    print(config.describe_target())
    print(f"  Date range  : {start_date} to {end_date}")
    run_reference = {
        "00a": run_airport_zones,
        "00b": run_airport_layouts,
        "00c": run_airspaces,
        "00d": run_ourairports,
        "00e": run_aircraft_db,
        "00f": run_runway_grid,
    }
    skip_all_ref = not any(run_reference.values())

    step_label = step or (
        "ALL (01-09, incl. 01b/02a/04b)" if skip_all_ref else "ALL (00-09, incl. 01b/02a/04b)"
    )
    print(f"  Step        : {step_label}")
    if step is None:
        skipped = [k for k, v in run_reference.items() if not v]
        if skipped:
            print(f"  Reference   : skipping {', '.join(skipped)}")
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
        run_reference=run_reference,
        # None follows DetectionConfig.
        adep_mode=adep_mode,
        ades_mode=ades_mode,
    )

    try:
        if step is not None:
            # Run a single step
            if step not in STEPS:
                print(f"Unknown step '{step}'. Valid steps: {sorted(STEPS.keys())}")
                return 1

            fn = STEPS[step]
            # Step 00 needs no date range
            if step == "00":
                fn(spark, config, **kwargs)
            else:
                fn(spark, config, start_date, end_date, **kwargs)
        else:
            # Run all steps in order
            for step_id in sorted(STEPS.keys()):
                fn = STEPS[step_id]
                if step_id == "00":
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
        env=sys.argv[1] if len(sys.argv) > 1 else "opensky",
        start_date=date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date(2022, 1, 1),
        end_date=date.fromisoformat(sys.argv[3]) if len(sys.argv) > 3 else date.today(),
    )
