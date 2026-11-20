"""Step 00d reads each OurAirports file from the site, else from its mirror."""

import urllib.error

import pandas as pd
import pytest

from opdi.ingestion import ourairports as oa


def test_every_file_has_a_mirror_address():
    assert set(oa.MIRROR_URLS) == set(oa.DEFAULT_URLS)
    assert oa.MIRROR_URLS["airport_frequencies"].endswith("/airport-frequencies.csv")


def test_reads_the_site_when_it_answers(monkeypatch):
    seen = []
    monkeypatch.setattr(oa.pd, "read_csv", lambda url: seen.append(url) or pd.DataFrame({"a": [1]}))
    assert len(oa.read_dataset_csv("airports")) == 1
    assert seen == [oa.DEFAULT_URLS["airports"]]


def test_reads_the_mirror_when_the_site_fails(monkeypatch, capsys):
    seen = []

    def read(url):
        seen.append(url)
        if url == oa.DEFAULT_URLS["runways"]:
            raise urllib.error.URLError("certificate has expired")
        return pd.DataFrame({"a": [1, 2]})

    monkeypatch.setattr(oa.pd, "read_csv", read)
    assert len(oa.read_dataset_csv("runways")) == 2
    assert seen == [oa.DEFAULT_URLS["runways"], oa.MIRROR_URLS["runways"]]
    assert "certificate has expired" in capsys.readouterr().out


def test_an_address_given_by_the_caller_has_no_fallback(monkeypatch):
    def read(url):
        raise urllib.error.URLError("down")

    monkeypatch.setattr(oa.pd, "read_csv", read)
    with pytest.raises(urllib.error.URLError):
        oa.read_dataset_csv("airports", url="https://example.org/airports.csv")


def test_the_whole_step_reads_the_mirror_when_the_site_fails(monkeypatch):
    """ingest_all passes each site address on; that must still fall back."""
    seen = []

    def read(url):
        seen.append(url)
        if url in oa.DEFAULT_URLS.values():
            raise urllib.error.URLError("certificate has expired")
        return pd.DataFrame({"a": [1]})

    class Step(oa.OurAirportsIngestion):
        def __init__(self):
            pass

        def create_tables(self):
            pass

        def ingest_dataset(self, name, url=None):
            return len(oa.read_dataset_csv(name, url))

    monkeypatch.setattr(oa.pd, "read_csv", read)
    assert set(Step().ingest_all().values()) == {1}
    assert seen.count(oa.MIRROR_URLS["airports"]) == 1
