"""Tests for the declared schema of the published tables."""

import inspect
import re

from opdi.output import schema
from opdi.output.schema import (
    CATALOGUE,
    EVENT_COLUMNS,
    FLIGHT_EVENTS,
    FLIGHT_INDEX,
    FLIGHT_LIST,
    GROUPS,
    MEASUREMENT_COLUMNS,
    MEASUREMENT_SUFFIX,
    MEASUREMENT_VERSION,
    RELEASE,
    as_csv,
    as_markdown,
)
from opdi.pipeline import events
from opdi.pipeline.flight_list_milestones import ADDED_COLUMNS
from opdi.pipeline.flights import FlightListProcessor

SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")
SPARK_TYPES = {"string", "date", "timestamp", "double", "boolean", "bigint", "array<string>"}
#: Internal or vague wording that must not reach a published description.
BANNED = (
    "ladder", "rung", "study", "a8", "a9", "a10", "gate", "guard", "arm", "pin",
    "load-bearing", "surface", "robust", "leverage",
)
STANDARD_MEASURES = ("Distance flown (NM)", "Time Passed (s)")


def _flight_list_ddl_columns():
    src = inspect.getsource(FlightListProcessor.create_table_if_not_exists)
    return set(re.findall(r"^\s+([A-Z][A-Z0-9_]*) (?:STRING|DATE|TIMESTAMP)", src, re.M))


def _all_tables():
    return {
        "flight_list": FLIGHT_LIST,
        "flight_events": FLIGHT_EVENTS,
        "flight_index": FLIGHT_INDEX,
        "catalogue": CATALOGUE,
    }


def test_release_name():
    assert RELEASE == "v2.0.0"


def test_names_are_unique_and_snake_case():
    for table, cols in _all_tables().items():
        names = [c.name for c in cols]
        assert len(names) == len(set(names)), table
        assert all(SNAKE.match(n) for n in names), table


def test_types_and_groups_are_known():
    for cols in _all_tables().values():
        for c in cols:
            assert c.dtype in SPARK_TYPES, c
            assert c.group in GROUPS, c


def test_flight_list_order_starts_and_ends_as_declared():
    names = [c.name for c in FLIGHT_LIST]
    assert names[:4] == ["flight_id", "icao24", "flt_id", "dof"]
    assert names[-2:] == ["version", "release"]
    rings = [f"c{nm}_arr" for nm in (40, 50, 60, 100, 110, 120)] + [
        f"c{nm}_dep" for nm in (40, 50, 60, 100, 110, 120)
    ]
    start = names.index("c40_arr")
    assert names[start:start + 12] == rings


def test_every_flight_list_source_is_a_working_table_column():
    working = _flight_list_ddl_columns() | set(ADDED_COLUMNS)
    assert "ID" in working and "ADEP_SOURCE" in working
    for c in FLIGHT_LIST:
        if c.name == "release":
            continue
        assert c.source in working, c
    # And every working column is published.
    assert working == {c.source for c in FLIGHT_LIST if c.name != "release"}


def test_measurement_sources_are_measurement_types():
    extra = {m for _, _, m, _ in events.EXTRA_MEASURES}
    for c in MEASUREMENT_COLUMNS:
        assert c.source in extra or c.source in STANDARD_MEASURES, c
        assert c.dtype == "double"
    assert {c.source for c in MEASUREMENT_COLUMNS} == extra | set(STANDARD_MEASURES)


def test_measurement_suffixes_match_the_event_step():
    assert MEASUREMENT_SUFFIX["Distance flown (NM)"] == "_d"
    assert MEASUREMENT_SUFFIX["Time Passed (s)"] == "_t"
    for _, _, m, _ in events.EXTRA_MEASURES:
        assert MEASUREMENT_SUFFIX[m] == events.measurement_suffix(m)


def test_measurement_versions_match_the_event_step():
    assert MEASUREMENT_VERSION["Distance flown (NM)"] == events.DISTANCE_VERSION
    assert MEASUREMENT_VERSION["Time Passed (s)"] in inspect.getsource(events)
    for _, _, m, v in events.EXTRA_MEASURES:
        assert MEASUREMENT_VERSION[m] == v


def test_flight_events_is_events_measurements_then_flight_columns():
    flight_part = [
        c.name for c in FLIGHT_LIST if c.name not in ("flight_id", "version", "release")
    ]
    expected = (
        [c.name for c in EVENT_COLUMNS]
        + [c.name for c in MEASUREMENT_COLUMNS]
        + flight_part
        + ["flight_version", "release"]
    )
    assert [c.name for c in FLIGHT_EVENTS] == expected
    assert [c.name for c in EVENT_COLUMNS] == [
        "event_id", "flight_id", "type", "event_time", "longitude", "latitude",
        "altitude_ft", "h3_res_5", "source", "version", "info",
    ]


def test_index_columns():
    assert [c.name for c in FLIGHT_INDEX] == [
        "flight_id", "icao24", "flt_id", "adep", "ades", "first_seen", "last_seen",
        "dof", "file",
    ]


def test_descriptions_are_plain_sentences():
    for cols in _all_tables().values():
        for c in cols:
            d = c.description.strip()
            assert d and d[0].isupper() and d.endswith("."), c
            words = set(re.findall(r"[a-z0-9-]+", d.lower()))
            assert not words & set(BANNED), (c.name, words & set(BANNED))


def test_markdown_has_a_header_and_one_line_per_column():
    md = as_markdown(FLIGHT_LIST)
    lines = md.strip("\n").split("\n")
    assert len(lines) == len(FLIGHT_LIST) + 2
    assert lines[0].startswith("| name |")
    assert "| `flight_id` |" in lines[2]


def test_csv_has_a_header_and_one_line_per_column():
    import csv
    import io

    rows = list(csv.reader(io.StringIO(as_csv(FLIGHT_EVENTS))))
    assert rows[0] == ["name", "type", "unit", "description"]
    assert len(rows) == len(FLIGHT_EVENTS) + 1


def test_module_does_not_import_spark():
    src = inspect.getsource(schema)
    assert "pyspark" not in src
