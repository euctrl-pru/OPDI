"""The published-data modules work in an environment without PySpark.

A researcher who only reads the published files installs pyarrow (and
perhaps duckdb), not Spark. These tests import the modules in a fresh Python
process in which every import of ``pyspark`` fails.
"""

import subprocess
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"

_BLOCK_PYSPARK = (
    "import sys\n" f"sys.path.insert(0, {str(SRC)!r})\n" "sys.modules['pyspark'] = None\n"
)


def _run(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", _BLOCK_PYSPARK + code],
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_pyspark_is_really_blocked():
    out = _run("import pyspark\n")
    assert out.returncode != 0
    assert "pyspark" in out.stderr


def test_schema_imports_without_pyspark():
    out = _run("import opdi.output.schema as s\nassert s.RELEASE\n")
    assert out.returncode == 0, out.stderr


def test_reader_and_sql_import_without_pyspark():
    out = _run(
        "import opdi.published, opdi.output.schema, opdi.sql.render\n"
        "assert opdi.published.PUBLISHED_BASE_URL\n"
        "assert 'CREATE TABLE' in opdi.sql.render.trino_tables()\n"
    )
    assert out.returncode == 0, out.stderr


def test_package_version_needs_no_pyspark():
    out = _run("import opdi\nassert opdi.__version__\n")
    assert out.returncode == 0, out.stderr


def test_public_names_still_resolve():
    import opdi
    import opdi.output
    from opdi import H3Config, IngestionConfig, OPDIConfig, ProjectConfig, SparkConfig

    assert OPDIConfig is opdi.config.OPDIConfig
    assert {ProjectConfig, SparkConfig, H3Config, IngestionConfig}
    assert set(opdi.__all__) >= {"OPDIConfig", "ProjectConfig", "__version__"}


def test_the_removed_exporters_say_what_replaced_them():
    import pytest

    with pytest.raises(ImportError, match="opdi.output.published"):
        from opdi.output import ParquetExporter  # noqa: F401
