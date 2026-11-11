"""The Trino table and view definitions shipped with the package."""

from pathlib import Path

import pytest

from opdi.output.schema import FLIGHT_EVENTS, FLIGHT_INDEX, FLIGHT_LIST, MEASUREMENT_COLUMNS
from opdi.sql import render

SQL = Path(render.__file__).resolve().parent


def test_the_checked_in_files_equal_a_fresh_render():
    assert (SQL / "trino" / "tables.sql").read_text() == render.trino_tables()
    assert (SQL / "trino" / "views.sql").read_text() == render.trino_views()
    assert (SQL / "views.sql").read_text() == render.duckdb_views_template()


@pytest.mark.parametrize(
    "table, columns",
    [
        ("flight_list", FLIGHT_LIST),
        ("flight_events", FLIGHT_EVENTS),
        ("flight_index", FLIGHT_INDEX),
    ],
)
def test_every_column_appears_with_its_comment(table, columns):
    text = render.trino_tables()
    block = text[text.index(f"minio.opdi.{table} (") :]
    block = block[: block.index(";")]
    for c in columns:
        comment = c.description.replace("'", "''")
        assert f"    {c.name} " in block, c.name
        assert f"COMMENT '{comment}'" in block, c.name
    folder = "index" if table == "flight_index" else table
    assert f"external_location = 's3a://eurocontrol/opdi/v2.0.0/published/{folder}'" in block


def test_catalog_schema_and_location_are_arguments():
    text = render.trino_tables("lake", "research", "s3a://b/p/")
    assert "lake.research.flight_list (" in text
    assert "'s3a://b/p/flight_events'" in text
    assert "minio" not in text.split("\n\n", 1)[1]
    views = render.trino_views("lake", "research")
    assert "FROM lake.research.flight_events" in views


def test_the_measurements_view_lists_every_measurement():
    views = render.trino_views()
    for c in MEASUREMENT_COLUMNS:
        assert c.name in views and f"'{c.source}'" in views


def test_the_statements_parse_as_trino():
    sqlglot = pytest.importorskip("sqlglot")
    for text in (render.trino_tables(), render.trino_views()):
        statements = [s for s in sqlglot.parse(text, read="trino") if s is not None]
        assert len(statements) == 3


def test_the_duckdb_template_parses():
    sqlglot = pytest.importorskip("sqlglot")
    text = render.duckdb_views_template()
    text = text.replace(render.FLIGHT_LIST_FILES, "['a.parquet']")
    text = text.replace(render.FLIGHT_EVENTS_FILES, "['b.parquet']")
    text = text.replace(render.FLIGHT_INDEX_FILES, "['c.parquet']")
    statements = [s for s in sqlglot.parse(text, read="duckdb") if s is not None]
    assert len(statements) == 6
