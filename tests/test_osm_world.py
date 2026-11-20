"""Building the worldwide aeroway extract from Geofabrik regions.

The download, filter and merge functions are replaced by fakes for the
order and clean-up tests, so nothing is fetched. The osmium tests run the
real command-line tool on a small synthetic file and are skipped when it is
not installed.
"""

import hashlib
import io
import os
import threading

import pytest

from opdi.reference import osm_world
from opdi.reference.osm_world import (
    WORLD_EXTRACT_NAME,
    WORLD_REGIONS,
    build_world_extract,
    download,
    merge,
    osmium_tool,
    region_url,
    tags_filter,
)


def test_the_regions_cover_the_world_once():
    assert set(WORLD_REGIONS) == {
        "africa",
        "antarctica",
        "asia",
        "australia-oceania",
        "central-america",
        "europe",
        "north-america",
        "russia",
        "south-america",
    }
    assert len(WORLD_REGIONS) == len(set(WORLD_REGIONS))


def test_a_large_region_is_never_followed_by_another_large_one():
    """Two raw regions are on the disk at once: the one being filtered and the next."""
    large = {"europe", "north-america", "asia"}
    for a, b in zip(WORLD_REGIONS, WORLD_REGIONS[1:]):
        assert not (a in large and b in large), (a, b)


class _Recorder:
    """Fake download, filter and merge that note what was on the disk."""

    def __init__(self, work_dir, fail_filter=None, fail_fetch=None):
        self.work_dir = work_dir
        self.fail_filter = fail_filter
        self.fail_fetch = fail_fetch
        self.events = []
        self.most_raw_on_disk = 0

    def _raw_on_disk(self):
        return [n for n in os.listdir(self.work_dir) if n.endswith("-latest.osm.pbf")]

    def fetch(self, url, dest, stop=None):
        if self.fail_fetch and self.fail_fetch in url:
            raise RuntimeError(f"could not download {url}")
        with open(dest, "wb") as f:
            f.write(b"raw")
        self.events.append(("fetch", os.path.basename(dest)))
        self.most_raw_on_disk = max(self.most_raw_on_disk, len(self._raw_on_disk()))
        return dest

    def filter_file(self, src, dst):
        if self.fail_filter and self.fail_filter in src:
            raise RuntimeError("filter failed")
        with open(dst, "wb") as f:
            f.write(b"filtered")
        self.events.append(("filter", os.path.basename(src)))
        return dst

    def combine(self, sources, dst):
        self.events.append(("merge", [os.path.basename(s) for s in sources]))
        with open(dst, "wb") as f:
            f.write(b"world")
        return dst


def test_every_region_is_filtered_and_merged_and_only_the_world_file_is_left(tmp_path):
    rec = _Recorder(str(tmp_path))
    out = build_world_extract(
        str(tmp_path),
        regions=("europe", "russia", "antarctica"),
        fetch=rec.fetch,
        filter_file=rec.filter_file,
        combine=rec.combine,
        log=lambda _: None,
    )
    assert out == str(tmp_path / WORLD_EXTRACT_NAME)
    assert os.listdir(tmp_path) == [WORLD_EXTRACT_NAME]
    assert [e for e in rec.events if e[0] == "filter"] == [
        ("filter", "europe-latest.osm.pbf"),
        ("filter", "russia-latest.osm.pbf"),
        ("filter", "antarctica-latest.osm.pbf"),
    ]
    assert rec.events[-1] == (
        "merge",
        ["aeroway-europe.osm.pbf", "aeroway-russia.osm.pbf", "aeroway-antarctica.osm.pbf"],
    )
    # At most the region being filtered and the next one.
    assert rec.most_raw_on_disk <= 2


@pytest.mark.parametrize("failure", ["filter", "fetch"])
def test_a_failed_region_stops_the_build_and_leaves_nothing_behind(tmp_path, failure):
    """A world without a region would give no layouts for all its airports."""
    rec = _Recorder(
        str(tmp_path),
        fail_filter="russia" if failure == "filter" else None,
        fail_fetch="russia" if failure == "fetch" else None,
    )
    with pytest.raises(RuntimeError):
        build_world_extract(
            str(tmp_path),
            regions=("europe", "russia", "antarctica"),
            fetch=rec.fetch,
            filter_file=rec.filter_file,
            combine=rec.combine,
            log=lambda _: None,
        )
    assert os.listdir(tmp_path) == []
    assert not any(e[0] == "merge" for e in rec.events)


def test_a_failure_stops_the_download_that_is_running(tmp_path):
    started = threading.Event()
    stopped = threading.Event()

    def fetch(url, dest, stop=None):
        if "europe" in url:
            with open(dest, "wb") as f:
                f.write(b"raw")
            return dest
        started.set()
        assert stop.wait(10), "the build did not ask the download to stop"
        stopped.set()
        raise RuntimeError("stopped")

    def filter_file(src, dst):
        assert started.wait(10)
        raise RuntimeError("filter failed")

    with pytest.raises(RuntimeError, match="filter failed"):
        build_world_extract(
            str(tmp_path),
            regions=("europe", "asia"),
            fetch=fetch,
            filter_file=filter_file,
            combine=lambda s, d: d,
            log=lambda _: None,
        )
    assert stopped.is_set()


def test_region_addresses():
    assert region_url("europe") == "https://download.geofabrik.de/europe-latest.osm.pbf"


class _Response(io.BytesIO):
    def __init__(self, body, url, length=True):
        super().__init__(body)
        self._url = url
        self.headers = {"Content-Length": str(len(body))} if length else {}

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _opener(body, md5, calls=None, final="https://mirror.example/x-latest.osm.pbf"):
    def opener(url, timeout=None):
        if calls is not None:
            calls.append(url)
        if url.endswith(".md5"):
            assert url == final + ".md5"
            return _Response(f"{md5}  x-latest.osm.pbf\n".encode(), url)
        return _Response(body, final)

    return opener


def test_a_download_is_checked_against_the_md5_of_the_server_it_came_from(tmp_path):
    body = b"pbf bytes"
    dest = tmp_path / "x-latest.osm.pbf"
    download(
        "https://download.geofabrik.de/x-latest.osm.pbf",
        str(dest),
        opener=_opener(body, hashlib.md5(body).hexdigest()),
        log=lambda _: None,
    )
    assert dest.read_bytes() == body
    assert not (tmp_path / "x-latest.osm.pbf.part").exists()


def test_a_mismatching_download_is_tried_again_then_refused(tmp_path):
    calls = []
    dest = tmp_path / "x-latest.osm.pbf"
    with pytest.raises(RuntimeError, match="could not download"):
        download(
            "https://download.geofabrik.de/x-latest.osm.pbf",
            str(dest),
            attempts=2,
            opener=_opener(b"pbf bytes", "0" * 32, calls),
            log=lambda _: None,
        )
    assert len([c for c in calls if not c.endswith(".md5")]) == 2
    assert os.listdir(tmp_path) == []


def test_a_download_larger_than_the_free_space_is_refused_at_once(tmp_path, monkeypatch):
    calls = []

    class _Usage:
        free = 10

    monkeypatch.setattr(osm_world.shutil, "disk_usage", lambda _: _Usage())
    with pytest.raises(RuntimeError, match="not enough free space"):
        download(
            "https://download.geofabrik.de/x-latest.osm.pbf",
            str(tmp_path / "x-latest.osm.pbf"),
            opener=_opener(b"pbf bytes", "unused", calls),
            log=lambda _: None,
        )
    assert len(calls) == 1
    assert os.listdir(tmp_path) == []


def test_a_stop_ends_the_download(tmp_path):
    stop = threading.Event()
    stop.set()
    with pytest.raises(RuntimeError, match="stopped"):
        download(
            "https://download.geofabrik.de/x-latest.osm.pbf",
            str(tmp_path / "x-latest.osm.pbf"),
            stop=stop,
            opener=_opener(b"pbf bytes", "unused"),
            log=lambda _: None,
        )
    assert os.listdir(tmp_path) == []


needs_osmium = pytest.mark.skipif(osmium_tool() is None, reason="osmium-tool not installed")

#: An apron mapped as a multipolygon relation whose member ways have no
#: aeroway tag, a stand mapped as a bare node, and a road.
_SYNTHETIC = """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="test">
  <node id="1" version="1" lat="0.0" lon="0.0"/>
  <node id="2" version="1" lat="0.0" lon="0.01"/>
  <node id="3" version="1" lat="0.01" lon="0.01"/>
  <node id="4" version="1" lat="0.01" lon="0.0"/>
  <node id="5" version="1" lat="1" lon="1"/>
  <node id="6" version="1" lat="1" lon="1.1"><tag k="aeroway" v="parking_position"/></node>
  <node id="7" version="1" lat="2" lon="2"/>
  <node id="8" version="1" lat="2" lon="2.1"/>
  <way id="10" version="1"><nd ref="1"/><nd ref="2"/><nd ref="3"/></way>
  <way id="11" version="1"><nd ref="3"/><nd ref="4"/><nd ref="1"/></way>
  <way id="12" version="1"><nd ref="7"/><nd ref="8"/><tag k="highway" v="road"/></way>
  <relation id="20" version="1">
    <member type="way" ref="10" role="outer"/>
    <member type="way" ref="11" role="outer"/>
    <tag k="type" v="multipolygon"/>
    <tag k="aeroway" v="apron"/>
  </relation>
</osm>
"""


def _ids(path):
    import re

    text = open(path, encoding="utf-8").read()
    return re.findall(r'<(node|way|relation) id="(\d+)"', text)


@needs_osmium
def test_the_filter_keeps_the_untagged_member_ways_of_an_apron(tmp_path):
    src = tmp_path / "syn.osm"
    src.write_text(_SYNTHETIC)
    dst = tmp_path / "out.osm"
    tags_filter(str(src), str(dst))
    assert _ids(dst) == [
        ("node", "1"),
        ("node", "2"),
        ("node", "3"),
        ("node", "4"),
        ("node", "6"),
        ("way", "10"),
        ("way", "11"),
        ("relation", "20"),
    ]


@needs_osmium
def test_the_merge_writes_an_object_found_in_two_regions_once(tmp_path):
    src = tmp_path / "syn.osm"
    src.write_text(_SYNTHETIC)
    a, b = tmp_path / "a.osm.pbf", tmp_path / "b.osm.pbf"
    tags_filter(str(src), str(a))
    tags_filter(str(src), str(b))
    out = tmp_path / "world.osm"
    merge([str(a), str(b)], str(out))
    assert len(_ids(out)) == 8
