"""Step 00b's writes.

The generator used to write the whole shared `hexaero_airport_layouts` table
from `process_airport`, with `mode="overwrite"`, once per airport. A loop over
twenty airports therefore left the table holding the twentieth, and a single
direct call destroyed it -- which is exactly what happened during the
flight-events-v4 campaign: the published table went from twenty airports to one.
These tests pin the two properties that prevent it.
"""
import pandas as pd
import pytest

from opdi.config import OPDIConfig
from opdi.reference.h3_airport_layouts import AirportLayoutGenerator, clean_str


class StubStorage:
    def __init__(self, airports):
        self._airports = airports
        self.writes = []

    def table_exists(self, name):
        return name == "oa_airports"

    def read_table(self, name):
        return self._airports

    def write_table(self, df, table_name, mode, partition_by=None):
        self.writes.append((table_name, mode, df.count()))

    def create_table(self, sql):
        pass


def _fake_layout(apt):
    """Two rows in HEXAERO_SCHEMA's column order, so `createDataFrame` accepts
    them without the test having to care about the schema's contents."""
    return pd.DataFrame({
        "hexaero_apt_icao": [apt, apt],
        "hexaero_h3_id": [apt + "-a", apt + "-b"],
        "hexaero_latitude": [50.0, 50.1],
        "hexaero_longitude": [4.0, 4.1],
        "hexaero_res": [12, 12],
        "hexaero_aeroway": ["parking_position", "taxiway"],
        "hexaero_length": [1.0, 2.0],
        "hexaero_ref": ["A1", "T1"],
        "hexaero_surface": ["asphalt", "asphalt"],
        "hexaero_width": [20.0, 20.0],
        "hexaero_osm_id": [1, 2],
        "hexaero_type": ["way", "way"],
    })


def test_process_airport_writes_nothing(spark, monkeypatch, tmp_path):
    """The regression that cost a published table. A per-airport builder must
    not decide the fate of a table shared by every other airport."""
    import opdi.reference.h3_airport_layouts as mod
    monkeypatch.setattr(mod, "hexagonify_airport", lambda a, resolution=12, source=None: _fake_layout(a))
    storage = StubStorage(None)
    gen = AirportLayoutGenerator(spark, OPDIConfig(), log_dir=str(tmp_path), storage=storage)

    out = gen.process_airport("EBBR")

    assert out is not None and len(out) == 2
    assert storage.writes == [], "process_airport must not write the shared table"


def test_many_airports_are_written_once_and_together(spark, monkeypatch, tmp_path):
    """Three airports, one write, all three in it -- not three overwrites
    leaving the last."""
    import opdi.reference.h3_airport_layouts as mod
    monkeypatch.setattr(mod, "hexagonify_airport", lambda a, resolution=12, source=None: _fake_layout(a))
    storage = StubStorage(None)
    gen = AirportLayoutGenerator(spark, OPDIConfig(), log_dir=str(tmp_path), storage=storage)
    # `fetch_airport_list` downloads the OurAirports CSV; stub it so the test
    # is about the writes and not about the network.
    monkeypatch.setattr(
        gen, "fetch_airport_list",
        lambda *a, **k: pd.DataFrame({"ident": ["EBBR", "LSZH", "EHAM"]}),
    )

    success, failed = gen.process_all()

    assert sorted(success) == ["EBBR", "EHAM", "LSZH"]
    assert failed == []
    assert len(storage.writes) == 1, f"expected one write, got {len(storage.writes)}"
    table, mode, n_rows = storage.writes[0]
    assert table == "hexaero_airport_layouts"
    assert mode == "overwrite"
    assert n_rows == 6, "every airport's rows must be in the single write"


# --- clean_str -- shared by both the Overpass and PBF paths -----------------
#
# `clean_str` had no test at all before this task, and this task changed it:
# a missing tag arrives as NaN from Overpass (osmnx fills ragged tag columns
# with NaN) and as Python `None` from the PBF path (`obj.tags.get(k)` on a
# tag the feature lacks). Pre-patch, `str(None) == "None"` survived the
# numeric regex as an unparseable string and broke `hexaero_length` on the
# PBF path with `ValueError: could not convert string to float: 'None'`.
# `str(nan) == "nan"`, by contrast, already round-tripped through
# `astype(float)` correctly (numpy parses "nan"), via the pre-patch
# `except Exception: return s` branch. These tests pin both the fixed case
# and the case that must not move, so a future edit to this shared function
# cannot silently reintroduce the bug or regress the Overpass path Task 5's
# baseline comparison depends on.


def test_clean_str_passes_through_a_plain_numeric_string():
    assert clean_str("45") == 45.0


def test_clean_str_converts_feet_to_meters():
    # "50ft" -> 50 * 0.3048 = 15.24, returned as the string of the float
    # (the "ft"/"mi" branches `return str(result)`, unlike the plain-numeric
    # branch above which returns the float itself).
    assert clean_str("50ft") == str(50 * 0.3048)


def test_clean_str_converts_miles_to_meters():
    assert clean_str("2mi") == str(2 * 1609.34)


def test_clean_str_of_nan_matches_the_pre_patch_value():
    """Pre-patch, `clean_str(float("nan"))` fell through to
    `except Exception: return s` with `s` already reassigned to `str(nan)` ==
    "nan" -- `float(re.sub(...))` on the digit-stripped empty string raised,
    and the handler returned the stringified input as-is. This must not move:
    Task 5 compares an Overpass baseline against a PBF build, and any drift
    here would corrupt that comparison by measuring this fix rather than the
    change of source.
    """
    assert clean_str(float("nan")) == "nan"


def test_clean_str_of_none_no_longer_yields_the_unparseable_literal():
    """The bug this task fixed: pre-patch this returned the string "None"
    (from `str(None)`), which `astype(float)` cannot parse. Only the PBF path
    can produce `None` here (`obj.tags.get(k)` on an absent tag); Overpass's
    missing values are always NaN, covered by the test above.
    """
    result = clean_str(None)
    assert result != "None"
    assert result == "nan"


def test_clean_str_of_an_unparseable_string_returns_it_unchanged():
    assert clean_str("not-a-number") == "not-a-number"
