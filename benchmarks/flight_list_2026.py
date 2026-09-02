#!/usr/bin/env python
"""Build the OPDI flight list for the 2026-06 research period.

Every aerodrome-anchored detector in the V4 benchmark needs a flight list to
anchor on, and the *published* one covers only 2025-06-05/06/07 (verified:
360,298 rows, days ``2025-06-05``, ``2025-06-06``, ``2025-06-07`` and a stray
``2025-08-01``). UGKO's APDF ground truth only has 2026 coverage in June, so a
second period means a second flight list -- this builds it, over
``research/tracks_clean_2026`` (built by the A8 ``recommended`` segmentation
arm: its ``track_id`` values are ``{hash}_{offset}``, with no
``_{year}_{month}`` suffix, which is what distinguishes an A8 table from a
legacy one), through the real ``FlightListProcessor.process_dai`` code path --
the one that writes the published flight list -- so the result is what OPDI
would actually produce over this period, not a model of it.

Unlike ``flight_list_v7.py`` this runs exactly one configuration: whatever
``DetectionConfig()`` ships (``adep_mode="endpoint"``, ``ades_mode="trend"``),
because the point here is a flight list to build on, not a comparison of
algorithms. That default ADEP algorithm needs the endpoint-candidate cache,
which ``process_dai`` builds and reads under the fixed name
``opdi_endpoint_candidates`` -- the *production* cache. ``redirect_candidates``
below sends it to a 2026-specific research copy instead, both so a re-run of
this script cannot clobber the cache production reads, and so this run does
not silently inherit some other period's candidates (they are derived from
``track_id`` and are therefore period-specific).

``guard_writes`` (from ``event_bench``) is armed before anything else touches
storage: step 03 writes ``opdi_flight_list`` by name, and an unguarded run
would silently append a month of 2026 data to the *published* table.

    python benchmarks/flight_list_2026.py --executors 10
"""

import argparse
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))
sys.path.insert(0, str(REPO / "src"))

import osn_sample  # noqa: E402
from event_bench import guard_writes  # noqa: E402
from osn_sample import build_spark, load_dotenv  # noqa: E402

#: Input tracks. Carries ``h3_res_12`` and ``baro_altitude_c`` already
#: (verified against the table's parquet schema before spending cluster time),
#: so no ``index_on_read`` patch is needed here the way ``event_bench`` needs
#: one for the 2024 research tracks.
TRACKS_TABLE = "research/tracks_clean_2026"

#: Output. Under ``research/`` so ``guard_writes`` accepts the write and the
#: published flight list is never the target.
OUT_TABLE = "research/flight_list_2026"

#: This run's own endpoint-candidate cache. See module docstring.
CAND_TABLE = "research/cand_2026"

#: The tracks table holds only 2026-06-05..07 (three days, matching UGKO's
#: APDF coverage), so a full-month filter naturally yields a three-day flight
#: list -- there is no need to pass explicit days through to step 03, which
#: only ever filters by month.
MONTH = date(2026, 6, 1)


def redirect_candidates(table: str) -> None:
    """Point the endpoint-candidate cache's *physical path* at `table`.

    Patches ``_s3_path``, not the table name: every caller inside
    ``FlightListProcessor`` asks for the fixed logical name
    ``opdi_endpoint_candidates``, and only the resolved path can be steered
    elsewhere. Copied from ``flight_list_v7.redirect_candidates`` rather than
    imported -- that module carries a plan and a CLI this script does not
    want, and a safety rail should not depend on another benchmark staying
    importable (the same reasoning ``event_bench.guard_writes`` documents for
    itself).
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_fl2026_cand_redirect", False):
        return
    orig_path = StorageManager._s3_path

    def _s3_path(self, name, *a, **kw):
        return orig_path(
            self, table if name == "opdi_endpoint_candidates" else name, *a, **kw
        )

    StorageManager._s3_path = _s3_path
    StorageManager._fl2026_cand_redirect = True
    print(f"  endpoint candidates redirected to {table}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--ui-port", type=int, default=4058)
    ap.add_argument(
        "--log-dir", type=Path,
        default=REPO / "benchmarks" / "logs_flight_list_2026",
        help="processing-progress log directory (own, not shared with "
             "production or another research run)",
    )
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    osn_sample.UI_PORT = args.ui_port
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.sparkContext.setLogLevel("ERROR")

    # Armed before anything else touches storage. Step 03 writes
    # opdi_flight_list, and for the default endpoint-mode ADEP algorithm also
    # opdi_endpoint_candidates, both by name -- an unguarded run appends to or
    # overwrites the published tables silently.
    guard_writes()
    redirect_candidates(CAND_TABLE)

    from opdi.config import OPDIConfig
    from opdi.pipeline.flights import FlightListProcessor

    # The factory, not OPDIConfig() plus an attribute: the environment decides
    # the storage backend, and setting it afterwards leaves StorageManager
    # unwired, so every table looks absent.
    cfg = OPDIConfig.for_environment("opensky")
    args.log_dir.mkdir(parents=True, exist_ok=True)

    proc = FlightListProcessor(
        spark, cfg, log_dir=str(args.log_dir), tracks_table=TRACKS_TABLE
    )
    print(
        f"=== flight_list_2026: ADEP={proc.detection.adep_mode} "
        f"ADES={proc.detection.ades_mode} tracks={proc.tracks_table} ==="
    )
    proc.process_dai(
        month=MONTH,
        skip_if_processed=False,
        table_name=OUT_TABLE,
        write_mode="overwrite",
    )

    n = spark.read.parquet(f"s3a://eurocontrol/opdi/{OUT_TABLE}").count()
    print(f"\nflight list written: {n:,} rows -> {OUT_TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
