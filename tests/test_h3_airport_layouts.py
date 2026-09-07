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
from opdi.reference.h3_airport_layouts import AirportLayoutGenerator


class StubStorage:
    def __init__(self, airports):
        self._airports = airports
        self.writes = []

    def table_exists(self, name):
        return name == "oa_airports"

    def read_table(self, name):
        return self._airports

    def write_table(self, df, table_name, mode):
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
