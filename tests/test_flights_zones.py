"""Step 03 on one day and on a month: the same endpoint candidates, and the zones let go.

The end reports of the tracks (two per track) are sent to every executor only
when step 03 runs one day; over a month they are about thirty times as many,
and a copy of them on every executor and on the driver would not fit. The
zone table is kept on the executors while a step runs, and let go when the
call that read it returns, on every path.

Run on the golden day (``tests/golden_pipeline.py``) in a local warehouse.
"""

import os

import golden_pipeline
import pytest

from opdi.pipeline.flights import FlightListProcessor

CANDIDATES = "opdi_endpoint_candidates"


@pytest.fixture(scope="module")
def warehouse(spark, tmp_path_factory):
    """The golden inputs and tracks (steps 02 and 02a) in a local warehouse."""
    from opdi.periodrun import run_day_step

    folder = str(tmp_path_factory.mktemp("zones"))
    config = golden_pipeline.configuration("default", folder)
    golden_pipeline.write_inputs(spark, config, "default")
    logs = os.path.join(folder, "_logs")
    for step in ("02", "02a"):
        run_day_step(spark, config, step, golden_pipeline.DAY, {"log_dir": logs})
    spark.catalog.clearCache()
    return folder, config


def _processor(spark, warehouse, tmp_path):
    folder, config = warehouse
    proc = FlightListProcessor(spark, config, log_dir=str(tmp_path / "logs"))
    written = {}
    write = proc.storage.write_table

    def capture(df, name, *args, **kwargs):
        written[name] = df
        return write(df, name, *args, **kwargs)

    proc.storage.write_table = capture
    read = proc._load_airports_hex
    zones = []

    def keep(*args, **kwargs):
        df = read(*args, **kwargs)
        zones.append(df)
        return df

    proc._load_airports_hex = keep
    return proc, written, zones


def _rows(spark, proc):
    df = proc.storage.read_table(CANDIDATES)
    cols = sorted(df.columns)
    return sorted(tuple(r) for r in df.select(*cols).collect())


def _broadcast_hints(df) -> int:
    return df._jdf.queryExecution().analyzed().toString().count("strategy=broadcast")


def test_one_day_and_a_month_give_the_same_candidates(spark, warehouse, tmp_path):
    proc, written, _ = _processor(spark, warehouse, tmp_path)
    proc._batch_day = golden_pipeline.DAY
    try:
        proc.build_endpoint_candidates(golden_pipeline.DAY, rebuild=True)
    finally:
        proc._batch_day = None
    day_rows = _rows(spark, proc)
    day_hints = _broadcast_hints(written[CANDIDATES])

    proc.build_endpoint_candidates(golden_pipeline.DAY, rebuild=True)
    month_rows = _rows(spark, proc)
    month_hints = _broadcast_hints(written[CANDIDATES])

    assert day_rows == month_rows
    assert len(day_rows) > 0
    # The end reports are broadcast for one day only; the airport elevations
    # (a small reference table) are broadcast on both paths.
    assert day_hints == month_hints + 1


def _released(df) -> bool:
    level = df.storageLevel
    return not (level.useMemory or level.useDisk or level.useOffHeap)


def test_building_the_candidates_lets_the_zones_go(spark, warehouse, tmp_path):
    proc, _, zones = _processor(spark, warehouse, tmp_path)
    proc.build_endpoint_candidates(golden_pipeline.DAY, rebuild=True)
    assert zones and all(_released(z) for z in zones)
    assert proc._zones is None


def test_the_month_pass_lets_the_zones_go(spark, warehouse, tmp_path):
    proc, _, zones = _processor(spark, warehouse, tmp_path)
    proc.process_dai(golden_pipeline.DAY, skip_if_processed=False, table_name="_fl_month")
    assert zones and all(_released(z) for z in zones)
    assert proc._zones is None


def test_the_zones_are_let_go_when_the_pass_fails(spark, warehouse, tmp_path):
    proc, _, zones = _processor(spark, warehouse, tmp_path)

    def fail(*args, **kwargs):
        raise RuntimeError("stop")

    proc.classify_endpoints = fail
    with pytest.raises(RuntimeError):
        proc.process_dai(golden_pipeline.DAY, skip_if_processed=False, table_name="_fl_fail")
    assert zones and all(_released(z) for z in zones)
    assert proc._zones is None
