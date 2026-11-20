"""Step 00b's extract preparation.

These tests deliberately use dummy files rather than real ``.osm.pbf`` data:
every path exercised here returns *before* anything is parsed, so a real
extract would only make the suite slower. The parsing itself is covered by
``test_pbf_filter.py`` against the Luxembourg extract.
"""

import pytest

from opdi.runner import _AEROWAY_EXTRACT_PREFIX, _fmt_size, _prepare_layout_extract


def test_an_already_filtered_extract_is_returned_untouched(tmp_path):
    """The prefix is the marker that filtering has happened.

    Re-filtering would be merely wasteful on a small file, but on a
    continental extract it is twenty minutes, so the check has to be cheap and
    it has to happen before the file is opened.
    """
    src = tmp_path / f"{_AEROWAY_EXTRACT_PREFIX}europe-latest.osm.pbf"
    src.write_bytes(b"not really a pbf")
    assert _prepare_layout_extract(str(src)) == str(src)


def test_a_cached_filtered_extract_is_reused(tmp_path):
    """Only the first run of step 00b should pay for filtering."""
    src = tmp_path / "europe-latest.osm.pbf"
    src.write_bytes(b"raw")
    cached = tmp_path / f"{_AEROWAY_EXTRACT_PREFIX}europe-latest.osm.pbf"
    cached.write_bytes(b"filtered")

    # The cache is only valid while it is no older than its source; touching
    # it here states that relationship explicitly rather than relying on the
    # order the two happened to be written in.
    import os

    os.utime(cached, (src.stat().st_atime + 10, src.stat().st_mtime + 10))

    assert _prepare_layout_extract(str(src)) == str(cached)


def test_a_stale_cache_is_not_reused(tmp_path):
    """A newer source means the extract was replaced, so the cache is wrong.

    Reusing it would silently build the layout grid from last month's OSM data
    while the operator believes they refreshed it -- and nothing downstream
    records which vintage produced a cell.
    """
    cached = tmp_path / f"{_AEROWAY_EXTRACT_PREFIX}small.osm.pbf"
    cached.write_bytes(b"filtered")
    src = tmp_path / "small.osm.pbf"
    src.write_bytes(b"raw")

    import os

    os.utime(cached, (src.stat().st_atime - 10, src.stat().st_mtime - 10))

    # Reuse is refused, so it tries to filter -- and fails on the dummy bytes.
    # The failure is the evidence: a reused cache would have returned happily.
    with pytest.raises(Exception) as exc:
        _prepare_layout_extract(str(src))
    assert not isinstance(exc.value, FileNotFoundError)


def test_a_missing_extract_fails_loudly(tmp_path):
    """Not silently falling back to Overpass.

    A typo in the configured path would otherwise reach the public API, which
    cannot build the network and fails per-airport without raising -- so the
    run would produce a mostly-empty table and report success.
    """
    with pytest.raises(FileNotFoundError, match="OSM extract not found"):
        _prepare_layout_extract(str(tmp_path / "absent.osm.pbf"))


@pytest.mark.parametrize(
    "n, expected",
    [
        (34940824103, "32.5 GB"),  # the raw Europe extract
        (16125133, "15.4 MB"),  # the filtered Europe extract
        (42496, "41.5 KB"),  # the filtered Luxembourg extract
        (12, "12 B"),
    ],
)
def test_sizes_print_in_a_unit_that_reads(n, expected):
    """These files span five orders of magnitude; a fixed unit prints either
    ``0.0 MB`` for the small ones or an unreadable row of digits for the
    large."""
    assert _fmt_size(n) == expected


class _FakeStorage:
    """Stands in for StorageManager: a dict of warehouse paths to bytes."""

    files: dict = {}

    def __init__(self, spark, config):
        self.state_path = "s3a://bucket/opdi/v2.0.0/_state"

    def fetch_file(self, path, local_path):
        if path not in self.files:
            return False
        with open(local_path, "wb") as f:
            f.write(self.files[path])
        return True

    def store_file(self, local_path, path):
        with open(local_path, "rb") as f:
            self.files[path] = f.read()


_KEPT = "s3a://bucket/opdi/v2.0.0/_state/osm/aeroway-world.osm.pbf"


def _patch_world(monkeypatch, tmp_path, kept):
    from opdi.reference import osm_world
    from opdi.utils import storage

    _FakeStorage.files = dict(kept)
    built = []

    def fake_build(work_dir, **kwargs):
        built.append(work_dir)
        out = tmp_path / "work" / "aeroway-world.osm.pbf"
        out.write_bytes(b"new world")
        return str(out)

    monkeypatch.setattr(storage, "StorageManager", _FakeStorage)
    monkeypatch.setattr(osm_world, "build_world_extract", fake_build)
    monkeypatch.setenv("OPDI_OSM_WORK_DIR", str(tmp_path / "work"))
    (tmp_path / "work").mkdir()
    return built


def test_the_world_extract_kept_in_the_warehouse_is_reused(monkeypatch, tmp_path):
    from opdi.runner import _world_layout_extract

    built = _patch_world(monkeypatch, tmp_path, {_KEPT: b"kept world"})
    path = _world_layout_extract(None, None)
    assert open(path, "rb").read() == b"kept world"
    assert built == []


def test_without_a_kept_extract_it_is_built_and_kept(monkeypatch, tmp_path):
    from opdi.runner import _world_layout_extract

    built = _patch_world(monkeypatch, tmp_path, {})
    path = _world_layout_extract(None, None)
    assert built == [str(tmp_path / "work")]
    assert open(path, "rb").read() == b"new world"
    assert _FakeStorage.files[_KEPT] == b"new world"
