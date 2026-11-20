"""Reference tables built in batches: memory stays bounded and a stopped build is continued."""

import os
import shutil

import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from opdi.reference.staged import StagedTable

SCHEMA = StructType(
    [StructField("apt", StringType(), True), StructField("cell", IntegerType(), True)]
)


class FolderStorage:
    """Tables as Parquet folders under one directory, like the `opensky` environment."""

    use_s3 = True

    def __init__(self, spark, root):
        self.spark = spark
        self.root = str(root)
        self.writes = []

    def _path(self, name):
        return os.path.join(self.root, name)

    def table_exists(self, name):
        return os.path.isdir(self._path(name))

    def read_table(self, name):
        return self.spark.read.parquet(self._path(name))

    def write_table(self, df, table_name, mode, partition_by=None, partition_values=None):
        self.writes.append((table_name, mode))
        df.write.mode(mode).parquet(self._path(table_name))

    def delete_table(self, name):
        shutil.rmtree(self._path(name), ignore_errors=True)

    def create_table(self, sql):
        pass


def _frame(apt, n):
    return pd.DataFrame({"apt": [apt] * n, "cell": list(range(n))})


def test_rows_are_written_in_batches_and_the_final_table_holds_them_all(spark, tmp_path):
    storage = FolderStorage(spark, tmp_path)
    table = StagedTable(spark, storage, "layouts", SCHEMA, key="apt", batch_rows=5, files=2)
    for apt in ("EBBR", "EHAM", "LSZH", "LFPG"):
        table.add(_frame(apt, 3))
    # Two batches of 6 rows were written while adding; nothing final yet.
    assert storage.writes == [("layouts_building", "overwrite"), ("layouts_building", "append")]
    assert not storage.table_exists("layouts")

    assert table.finish() == 12
    final = storage.read_table("layouts").toPandas()
    assert sorted(final.apt.unique()) == ["EBBR", "EHAM", "LFPG", "LSZH"]
    assert len(final) == 12
    assert not storage.table_exists("layouts_building")


def test_a_stopped_build_leaves_no_final_table_and_is_continued(spark, tmp_path):
    storage = FolderStorage(spark, tmp_path)
    first = StagedTable(spark, storage, "layouts", SCHEMA, key="apt", batch_rows=2)
    first.add(_frame("EBBR", 2))
    first.add(_frame("EHAM", 1))  # still on the driver when the build stops
    assert not storage.table_exists("layouts")

    second = StagedTable(spark, storage, "layouts", SCHEMA, key="apt", batch_rows=2)
    assert second.done_keys() == ["EBBR"]
    second.add(_frame("EHAM", 1))
    assert second.finish() == 3
    assert sorted(storage.read_table("layouts").toPandas().apt.unique()) == ["EBBR", "EHAM"]


def test_a_build_with_no_rows_leaves_the_final_table_alone(spark, tmp_path):
    storage = FolderStorage(spark, tmp_path)
    table = StagedTable(spark, storage, "layouts", SCHEMA, key="apt")
    table.add(_frame("EBBR", 0))
    assert table.finish() == 0
    assert storage.writes == []


def test_the_layout_step_builds_in_batches_and_skips_airports_already_written(
    spark, monkeypatch, tmp_path
):
    import opdi.reference.h3_airport_layouts as mod
    from opdi.config import OPDIConfig
    from tests.test_h3_airport_layouts import _fake_layout

    seen = []

    def layout(apt, resolution=12, source=None):
        seen.append(apt)
        return _fake_layout(apt)

    monkeypatch.setattr(mod, "hexagonify_airport", layout)
    storage = FolderStorage(spark, tmp_path / "warehouse")
    idents = pd.DataFrame({"ident": ["EBBR", "LSZH", "EHAM"]})

    # A stale local progress log that names an airport whose rows were never
    # written: it must not make the step skip that airport.
    logs = tmp_path / "logs"
    logs.mkdir()
    pd.DataFrame({"apt": ["EHAM"]}).to_parquet(logs / "00_hexaero_layout_progress_success.parquet")

    gen = mod.AirportLayoutGenerator(spark, OPDIConfig(), log_dir=str(logs), storage=storage)
    monkeypatch.setattr(gen, "fetch_airport_list", lambda *a, **k: idents)
    success, failed = gen.process_all()

    assert sorted(success) == ["EBBR", "EHAM", "LSZH"] and failed == []
    assert sorted(seen) == ["EBBR", "EHAM", "LSZH"]
    final = storage.read_table("hexaero_airport_layouts").toPandas()
    assert sorted(final.hexaero_apt_icao.unique()) == ["EBBR", "EHAM", "LSZH"]
    assert len(final) == 6
    assert not storage.table_exists("hexaero_airport_layouts_building")
