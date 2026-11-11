"""Two defects that a linter found: an undefined name and a missing import."""

import datetime as dt
import typing

import pandas as pd


def test_the_ingest_row_report_counts_what_was_written(spark, tmp_path, monkeypatch, capsys):
    """With ``OPDI_REPORT_INGEST_ROWS`` set, the row count of the write is printed."""
    from opdi.config import OPDIConfig
    from opdi.ingestion.osn_statevectors import StateVectorIngestion

    cfg = OPDIConfig.for_environment("local", warehouse=str(tmp_path / "wh"))
    cfg.spark.enable_iceberg = False
    cfg.spark.enable_hive = False
    download = tmp_path / "download"
    download.mkdir()
    t0 = int(dt.datetime(2024, 6, 1, 12).timestamp())
    pd.DataFrame(
        {
            "time": [t0, t0 + 5, t0 + 10],
            "icao24": ["abc123"] * 3,
            "lat": [50.0] * 3,
            "lon": [4.0] * 3,
        }
    ).to_parquet(download / "part.parquet")
    ingest = StateVectorIngestion(
        spark,
        cfg,
        local_download_path=str(download),
        log_file_path=str(tmp_path / "logs" / "01.log"),
        bbox=None,
    )
    monkeypatch.setenv("OPDI_REPORT_INGEST_ROWS", "1")

    ingest.process_and_write_batch(["part.parquet"])

    assert "Written 3 records to osn_statevectors_v2" in capsys.readouterr().out


def test_the_layout_retriever_annotation_resolves():
    from opdi.reference import h3_airport_layouts

    hints = typing.get_type_hints(h3_airport_layouts.retrieve_osm_data)
    assert hints["return"].__name__ == "GeoDataFrame"
