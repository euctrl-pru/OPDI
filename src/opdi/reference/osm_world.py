"""A worldwide OpenStreetMap extract of airport ground geometry, built from Geofabrik.

Step 00b reads the runways, taxiways, aprons and stands of every airport from
an OpenStreetMap extract that keeps only ``aeroway`` objects. This module
builds that extract when none is given:

1. it downloads the Geofabrik extract of each region of the world in turn
   (:data:`WORLD_REGIONS`, about 90 GB in all, Europe alone about 35 GB) and
   checks it against the MD5 sum Geofabrik publishes beside it;
2. it filters each one with the ``osmium`` command-line tool
   (``osmium tags-filter <file> nwr/aeroway``), which keeps the ``aeroway``
   nodes, ways and relations together with the nodes and member ways they
   refer to, and deletes the downloaded file;
3. it merges the filtered files into one (``osmium merge``), which writes an
   object found in two neighbouring regions only once.

The next region downloads while the last one is filtered, so the disk holds
at most two raw regions at a time. The regions are ordered so that a large
one is always followed by a small one, which keeps that to about 37 GB.

The ``osmium`` tool (Ubuntu package ``osmium-tool``) is used rather than
:func:`opdi.reference.pbf_filter.filter_aeroway_pbf` because it decodes the
file on several threads in C++, and it keeps the same objects.
"""

import hashlib
import os
import shutil
import subprocess
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional, Sequence

#: Where the regional extracts are downloaded from.
GEOFABRIK = "https://download.geofabrik.de"

#: The Geofabrik regions that together cover the world. Russia is a region of
#: its own, in neither Europe nor Asia. Large and small regions alternate, so
#: that the region being filtered and the one downloading are never both large.
WORLD_REGIONS = (
    "europe",
    "australia-oceania",
    "north-america",
    "central-america",
    "asia",
    "south-america",
    "africa",
    "russia",
    "antarctica",
)

#: File name of the merged worldwide extract. The ``aeroway-`` prefix tells
#: step 00b the file is already filtered.
WORLD_EXTRACT_NAME = "aeroway-world.osm.pbf"

#: Free space, in bytes, left over after a download, for the filtered files
#: and anything else on the disk.
DISK_MARGIN_BYTES = 2 * 1024**3

_CHUNK_BYTES = 8 * 1024**2


def osmium_tool() -> Optional[str]:
    """Path of the ``osmium`` command-line tool, or ``None`` when it is not installed."""
    return shutil.which("osmium")


def region_url(region: str) -> str:
    """Address of the latest Geofabrik extract of ``region``."""
    return f"{GEOFABRIK}/{region}-latest.osm.pbf"


def _read_md5(url: str, opener) -> str:
    with opener(url, timeout=60) as resp:
        return resp.read().decode("ascii").split()[0].lower()


def download(
    url: str,
    dest: str,
    stop: Optional[threading.Event] = None,
    attempts: int = 3,
    opener: Callable = urllib.request.urlopen,
    log: Callable[[str], None] = print,
) -> str:
    """Download ``url`` to ``dest`` and check it against its MD5 sum.

    The file is written as ``dest + ".part"`` and renamed when it is complete
    and its sum matches the ``.md5`` file next to it (on the server the
    download was redirected to). A failed or mismatching download is tried
    again, up to ``attempts`` times in all; a mismatch can happen when
    Geofabrik replaces the file during the download. Setting ``stop`` ends
    the download at its next chunk.

    Raises:
        RuntimeError: The disk has too little free space for the file, or
            every attempt failed.
    """
    part = dest + ".part"
    folder = os.path.dirname(os.path.abspath(dest))
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        started = time.monotonic()
        try:
            with opener(url, timeout=120) as resp:
                size = int(resp.headers.get("Content-Length") or 0)
                free = shutil.disk_usage(folder).free
                if size and size + DISK_MARGIN_BYTES > free:
                    raise RuntimeError(
                        f"not enough free space in {folder} for {url}: "
                        f"{size / 1e9:.1f} GB needed, {free / 1e9:.1f} GB free"
                    )
                final_url = resp.geturl()
                digest = hashlib.md5()
                with open(part, "wb") as out:
                    while True:
                        chunk = resp.read(_CHUNK_BYTES)
                        if not chunk:
                            break
                        if stop is not None and stop.is_set():
                            raise RuntimeError(f"download of {url} stopped")
                        digest.update(chunk)
                        out.write(chunk)
            expected = _read_md5(final_url + ".md5", opener)
            if digest.hexdigest() != expected:
                raise ValueError(f"MD5 sum of {url} does not match {final_url}.md5")
            os.replace(part, dest)
            seconds = time.monotonic() - started
            got = os.path.getsize(dest)
            log(
                f"    downloaded {os.path.basename(dest)}: {got / 1e9:.1f} GB in "
                f"{seconds:.0f} s ({got / 1e6 / max(seconds, 1e-3):.0f} MB/s)"
            )
            return dest
        except RuntimeError:
            if os.path.exists(part):
                os.remove(part)
            raise
        except Exception as error:  # network, HTTP or checksum errors
            last_error = error
            if os.path.exists(part):
                os.remove(part)
            log(f"    attempt {attempt} of {attempts} to download {url} failed: {error}")
    raise RuntimeError(f"could not download {url}: {last_error}") from last_error


def _osmium(*args: str) -> None:
    tool = osmium_tool()
    if tool is None:
        raise RuntimeError(
            "the osmium command-line tool is not installed (Ubuntu package osmium-tool)"
        )
    subprocess.run([tool, *args, "--overwrite", "--no-progress"], check=True)


def tags_filter(src: str, dst: str) -> str:
    """Write the ``aeroway`` objects of ``src``, and what they refer to, to ``dst``."""
    _osmium("tags-filter", src, "nwr/aeroway", "-o", dst)
    return dst


def merge(sources: Sequence[str], dst: str) -> str:
    """Merge sorted extracts into ``dst``, writing an object found in several only once."""
    _osmium("merge", *sources, "-o", dst)
    return dst


def build_world_extract(
    work_dir: str,
    regions: Sequence[str] = WORLD_REGIONS,
    fetch: Callable[..., str] = download,
    filter_file: Callable[[str, str], str] = tags_filter,
    combine: Callable[[Sequence[str], str], str] = merge,
    log: Callable[[str], None] = print,
) -> str:
    """Build the worldwide ``aeroway`` extract in ``work_dir`` and return its path.

    Downloads and filters every region of ``regions``, downloading the next
    region while the last one is filtered, then merges the results into
    ``work_dir/`` :data:`WORLD_EXTRACT_NAME`. Every downloaded and filtered
    regional file is deleted, also when a region fails; a failure stops the
    build, because a world missing a region would give no ground layouts for
    all its airports.

    Args:
        work_dir: A folder on a disk with room for about 60 GB.
        regions: Geofabrik region names.
        fetch: Download function, called as ``fetch(url, dest, stop=event)``;
            replaceable for tests.
        filter_file: Filter function, called as ``filter_file(src, dst)``.
        combine: Merge function, called as ``combine(sources, dst)``.
        log: Where progress lines are written.
    """
    os.makedirs(work_dir, exist_ok=True)
    started = time.monotonic()

    def raw_path(region: str) -> str:
        return os.path.join(work_dir, f"{region}-latest.osm.pbf")

    def fetch_region(region: str) -> str:
        log(f"  downloading {region_url(region)}")
        return fetch(region_url(region), raw_path(region), stop=stop)

    stop = threading.Event()
    filtered: list = []
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        pending = pool.submit(fetch_region, regions[0]) if regions else None
        for i, region in enumerate(regions):
            raw = pending.result()
            pending = pool.submit(fetch_region, regions[i + 1]) if i + 1 < len(regions) else None
            out = os.path.join(work_dir, f"aeroway-{region}.osm.pbf")
            t0 = time.monotonic()
            filter_file(raw, out)
            os.remove(raw)
            filtered.append(out)
            log(
                f"  filtered {region} in {time.monotonic() - t0:.0f} s: "
                f"{os.path.getsize(out) / 1e6:.1f} MB"
            )
        world = os.path.join(work_dir, WORLD_EXTRACT_NAME)
        combine(filtered, world)
        log(
            f"  built {WORLD_EXTRACT_NAME} ({os.path.getsize(world) / 1e6:.1f} MB) from "
            f"{len(filtered)} regions in {time.monotonic() - started:.0f} s"
        )
        return world
    finally:
        # A download still running after a failure stops at its next chunk.
        stop.set()
        pool.shutdown(wait=True, cancel_futures=True)
        for region in regions:
            for path in (raw_path(region), raw_path(region) + ".part"):
                if os.path.exists(path):
                    os.remove(path)
        for path in filtered:
            if os.path.exists(path):
                os.remove(path)
