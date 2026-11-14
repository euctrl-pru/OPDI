"""Step 00a writes the airport zones as at most 8 files, with the rows of a one-file write.

A second write replaces the folder: no file of the first one is left.
"""

import glob
import os

import golden_pipeline

from opdi.config import OPDIConfig
from opdi.reference.h3_airport_zones import ZONE_FILES, AirportDetectionZoneGenerator

ZONES_DDL = (
    "apt_ident string, apt_hex_id string, distance_from_center int, "
    "apt_latitude_deg double, apt_longitude_deg double, apt_min_c_radius_nm double, "
    "apt_max_c_radius_nm double, apt_type string, apt_scheduled string"
)


def _zones(spark, keep=None):
    pdf = golden_pipeline.detection_zones()
    if keep is not None:
        pdf = pdf[pdf["apt_ident"].isin(keep)]
    return spark.createDataFrame(pdf.astype(object).where(pdf.notna(), None), ZONES_DDL)


def _rows(df):
    return sorted(tuple(r) for r in df.select(*sorted(df.columns)).collect())


def _files(folder):
    return sorted(glob.glob(os.path.join(folder, "**", "*.parquet"), recursive=True))


def test_the_zones_are_written_as_few_files_with_the_rows_of_one_file(spark, tmp_path, monkeypatch):
    config = OPDIConfig.for_environment("local", worldwide=True, warehouse=str(tmp_path / "wh"))
    config.spark.enable_iceberg = False
    config.spark.enable_hive = False
    gen = AirportDetectionZoneGenerator(spark, config)
    zones = _zones(spark)
    monkeypatch.setattr(gen, "prepare_for_flight_list_spark", lambda *a, **k: zones)
    gen.save_prepared_to_table(table_name="zones")

    one_file = str(tmp_path / "one")
    zones.coalesce(1).write.parquet(one_file)
    written = gen.storage.read_table("zones")
    assert _rows(written) == _rows(spark.read.parquet(one_file))
    assert len(_rows(written)) > 0

    folder = gen.storage._s3_path("zones")
    first = _files(folder)
    assert 1 <= len(first) <= ZONE_FILES

    # A second, smaller write replaces the whole folder.
    smaller = _zones(
        spark, keep=sorted(r.apt_ident for r in zones.select("apt_ident").collect())[:1]
    )
    monkeypatch.setattr(gen, "prepare_for_flight_list_spark", lambda *a, **k: smaller)
    gen.save_prepared_to_table(table_name="zones")
    assert _rows(gen.storage.read_table("zones")) == _rows(smaller)
    assert not set(first) & set(_files(folder))
