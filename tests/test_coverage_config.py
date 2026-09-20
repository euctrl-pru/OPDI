"""The coverage switch: one box, two modes, and a default that must not move."""
import pytest
from pyspark.sql import functions as F

from opdi.config import OPDIConfig
from opdi.coverage import EUROPE_BBOX, CoverageConfig

#: The published box, written out here on purpose. If someone edits the
#: constant this test fails, which is the entire point: the box reaches
#: published data through track_id and through every study tuned against it.
PUBLISHED = (-25.86653, 26.74617, 49.65699, 70.25976)


def test_the_european_box_is_the_published_one():
    assert EUROPE_BBOX == PUBLISHED


def test_every_environment_still_defaults_to_europe():
    """A coverage that varied by environment would let dev publish rows local
    runs cannot reproduce, with nothing in the data saying why."""
    for env in ("opensky", "local", "dev", "live"):
        assert OPDIConfig.for_environment(env).coverage.bbox == PUBLISHED


def test_worldwide_means_no_box():
    c = OPDIConfig.for_environment("opensky", worldwide=True).coverage
    assert c.bbox is None
    assert c.is_worldwide is True
    assert c.label == "worldwide"


def test_europe_is_not_worldwide():
    c = OPDIConfig.for_environment("opensky").coverage
    assert c.is_worldwide is False
    assert c.label == "europe"


def test_a_custom_box_is_neither():
    """A regional run is a legitimate third case and must not claim to be
    either of the two named ones -- the label ends up in the warehouse marker."""
    assert CoverageConfig(bbox=(0.0, 40.0, 10.0, 50.0)).label == "custom"


def test_worldwide_writes_to_its_own_warehouse():
    """The decision that keeps the published European tables safe. If these two
    are ever equal, a worldwide run appends planet-wide rows to the table the
    European track_id contract lives in."""
    eu = OPDIConfig.for_environment("opensky")
    world = OPDIConfig.for_environment("opensky", worldwide=True)
    assert world.project.warehouse_path != eu.project.warehouse_path
    assert world.project.warehouse_path.endswith("opdi-world")


def test_opdi_warehouse_does_not_leak_into_a_worldwide_run(monkeypatch):
    """OPDI_WAREHOUSE is how production points at the European prefix. If it
    also steered worldwide runs, an operator with it exported -- which is the
    normal state -- would silently write planet-wide rows into opdi-prod."""
    monkeypatch.setenv("OPDI_WAREHOUSE", "s3a://eurocontrol/opdi-prod")
    world = OPDIConfig.for_environment("opensky", worldwide=True)
    assert "opdi-prod" not in world.project.warehouse_path


def test_the_worldwide_warehouse_is_overridable(monkeypatch):
    monkeypatch.setenv("OPDI_WAREHOUSE_WORLD", "s3a://eurocontrol/scratch-world")
    world = OPDIConfig.for_environment("opensky", worldwide=True)
    assert world.project.warehouse_path == "s3a://eurocontrol/scratch-world"


def test_the_spark_filter_is_a_no_op_worldwide(spark):
    """`lit(True)`, not a box of +/-180/90. A literal-true predicate is free for
    the optimiser to remove; a full-globe comparison is four column operations
    Spark must evaluate on every one of ~1e9 rows a day."""
    c = CoverageConfig(bbox=None)
    df = spark.createDataFrame([(0.0, 0.0), (-89.0, 179.9)], "lat double, lon double")
    got = df.filter(c.spark_filter(F.col("lat"), F.col("lon"), offset=False))
    assert got.count() == 2


def test_the_spark_filter_keeps_europe_and_drops_the_rest(spark):
    c = CoverageConfig()
    df = spark.createDataFrame(
        [("EBBR", 50.9, 4.48), ("KJFK", 40.6, -73.8), ("NZAA", -37.0, 174.8)],
        "ident string, lat double, lon double",
    )
    kept = {r.ident for r in
            df.filter(c.spark_filter(F.col("lat"), F.col("lon"), offset=False)).collect()}
    assert kept == {"EBBR"}


def test_the_offset_widens_by_exactly_three_degrees(spark):
    """The reference generators use the box plus an offset so an aerodrome just
    outside it still gets zones. 3 degrees is what all four copies used."""
    c = CoverageConfig()
    assert c.bbox_offset_deg == 3.0
    just_outside = spark.createDataFrame([(72.0, 4.0)], "lat double, lon double")
    assert just_outside.filter(
        c.spark_filter(F.col("lat"), F.col("lon"), offset=False)).count() == 0
    assert just_outside.filter(
        c.spark_filter(F.col("lat"), F.col("lon"), offset=True)).count() == 1


def test_the_pandas_mask_agrees_with_the_spark_filter(spark):
    """Two implementations of one rule. `h3_airport_layouts` filters in pandas
    and `h3_airport_zones` in Spark, so a disagreement means the layout table
    and the zone table cover different airports -- and they join to each other."""
    import pandas as pd
    c = CoverageConfig()
    pdf = pd.DataFrame({
        "lat": [50.9, 40.6, -37.0, 72.0, 26.0],
        "lon": [4.48, -73.8, 174.8, 4.0, 0.0],
    })
    mask = c.pandas_mask(pdf["lat"], pdf["lon"], offset=True)
    sdf = spark.createDataFrame(pdf)
    spark_keep = sdf.filter(
        c.spark_filter(F.col("lat"), F.col("lon"), offset=True)).count()
    assert int(mask.sum()) == spark_keep


def test_the_airport_types_are_the_ones_every_reference_table_uses():
    """Three reference tables keyed to different airport sets would join to
    each other with gaps nothing reports -- the failure h3_airport_zones'
    AIRPORT_TYPES comment records."""
    assert CoverageConfig().airport_types == ("large_airport", "medium_airport")


def test_the_zone_partition_count_scales_with_the_airport_count():
    """2,000 partitions was tuned for 1,357 aerodromes -- ~11 rows per task,
    because 2,400 rows in a task measured 7.7 GB and OOMKilled every executor.
    Worldwide is 3.89x the aerodromes, so a fixed 2,000 restores exactly the
    memory profile that number exists to avoid."""
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator as G
    assert G.zone_build_partitions(1357) == 2000        # Europe, unchanged
    assert G.zone_build_partitions(5280) >= 7000        # worldwide
    assert G.zone_build_partitions(50) == 2000          # floor, never fewer


def test_the_cli_accepts_worldwide(monkeypatch):
    """The parser merely existing is not enough -- `main(["run", "--help"])`
    inside `pytest.raises(SystemExit)` would pass even if `--worldwide` were
    absent entirely, since `--help` always exits. Assert instead on the value
    `run_pipeline` actually receives, which fails if the flag is not wired
    through to dispatch."""
    from opdi.cli import main

    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr("opdi.runner.run_pipeline", fake_run_pipeline)

    main(["run", "--start", "2024-01-01", "--end", "2024-01-02"])
    assert captured["worldwide"] is False

    captured.clear()
    main(["run", "--start", "2024-01-01", "--end", "2024-01-02", "--worldwide"])
    assert captured["worldwide"] is True


def test_run_pipeline_takes_worldwide():
    import inspect
    from opdi.runner import run_pipeline
    sig = inspect.signature(run_pipeline)
    assert sig.parameters["worldwide"].default is False


def test_run_period_takes_worldwide():
    import inspect
    from opdi.periodrun import run_period
    assert inspect.signature(run_period).parameters["worldwide"].default is False


def test_opdi_py_script_accepts_worldwide(monkeypatch):
    """The repo-root `opdi.py` is documented in CLAUDE.md as one of three ways
    to run the pipeline, alongside `opdi run` (covered above) and
    `run_period.py`. Its own argparse `main()` had no `--worldwide` and never
    forwarded `worldwide=` to `run_pipeline` -- found by trying to actually
    run it (progress.md), not by reading the diff.

    Loaded via `importlib` from the file path rather than `import opdi`:
    that name resolves to the *installed package* here (`src/opdi/__init__.py`
    wins over the repo-root script under `python -m pytest`'s sys.path), so a
    plain `import opdi` would silently test the wrong module and this
    regression would pass even with the flag missing.
    """
    import importlib.util
    from pathlib import Path

    script_path = Path(__file__).resolve().parent.parent / "opdi.py"
    spec = importlib.util.spec_from_file_location("opdi_root_script", script_path)
    opdi_script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(opdi_script)

    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr("opdi.runner.run_pipeline", fake_run_pipeline)

    opdi_script.main(["--start", "2024-01-01", "--end", "2024-01-02"])
    assert captured["worldwide"] is False

    captured.clear()
    opdi_script.main(
        ["--start", "2024-01-01", "--end", "2024-01-02", "--worldwide"]
    )
    assert captured["worldwide"] is True

    # opensky is worldwide's only real target -- the S3 warehouse and the OSN
    # data both live there -- so --worldwide is unreachable at its intended
    # use if --env rejects "opensky". This combination used to die on an
    # argparse "invalid choice" error before --env's choices included it.
    captured.clear()
    opdi_script.main(
        ["--env", "opensky", "--worldwide",
         "--start", "2024-01-01", "--end", "2024-01-02"]
    )
    assert captured["env"] == "opensky"
    assert captured["worldwide"] is True
