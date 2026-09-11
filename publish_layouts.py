"""Publish the gated PBF layout grid to `hexaero_airport_layouts`.

Run from /home/jupyter/work/opdi-workspace/opdi:

    .venv310/bin/python publish_layouts.py           # dry run, writes nothing
    .venv310/bin/python publish_layouts.py --commit  # performs the overwrite

What it does
------------
Copies `research/hexaero_airport_layouts_pbf` (1,036 aerodromes, 2,900,230
rows) over the published `hexaero_airport_layouts` (currently degraded to 15
aerodromes, 140,593 rows), then reads the result back and verifies it.

It copies the *already-gated* table rather than re-running the build, so what
lands in production is byte-for-byte what passed the acceptance gate.

Safety
------
* The current published state is already backed up and verified at
  `research/hexaero_airport_layouts_backup_20260907` (140,593 rows, 15
  aerodromes). To roll back, copy that path over the published one.
* Refuses to run unless the source has the expected row count and all 40 key
  aerodromes carry `parking_position` cells.
* Verifies by reading the published table back afterwards; a failed check is
  reported loudly rather than swallowed.
"""
import argparse
import os
import sys
from pathlib import Path

SRC = "eurocontrol/opdi/research/hexaero_airport_layouts_pbf"
DST = "eurocontrol/opdi/hexaero_airport_layouts"
BACKUP = "eurocontrol/opdi/research/hexaero_airport_layouts_backup_20260907"

EXPECTED_ROWS = 2900230
EXPECTED_APTS = 1036

STUDY = ("EBBR LSZH EICK EFHK LEIB EDDS LFLL LHBP LFPO LPFR "
         "ESSA ENVA LOWW LKPR EDDP LPPT EGGD EGNT EGCC UGKO").split()
ORIGINAL = ("EBBR EDDF EDDM EGKK EGLL EHAM EIDW EKCH ENGM EPWA "
            "ESSA LEBL LEMD LFPG LGAV LIRF LOWW LPPT LSZH LTFM").split()
FAMILIES = ["taxiway", "runway", "apron", "hangar", "threshold",
            "parking_position", "deicing_pad"]


def _fs():
    for line in Path(".env").read_text().splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
    import pyarrow.fs as pafs
    return pafs.S3FileSystem(
        endpoint_override="https://s3.opensky-network.org",
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        scheme="https",
    )


def _check(table, label):
    """Every condition the acceptance gate asserts, re-checked on real data."""
    import pandas as pd
    df = table.select(["hexaero_apt_icao", "hexaero_aeroway"]).to_pandas()
    apts = set(df.hexaero_apt_icao)
    stands = set(df.loc[df.hexaero_aeroway == "parking_position", "hexaero_apt_icao"])
    fams = df.hexaero_aeroway.value_counts().to_dict()
    ok = True
    print(f"  {label}: {len(apts)} aerodromes, {len(df)} rows")
    for name, group in (("study-20", STUDY), ("original-20", ORIGINAL)):
        missing = sorted(set(group) - apts)
        no_stands = sorted(set(group) - stands)
        status = "OK" if not missing and not no_stands else "FAIL"
        ok &= not missing and not no_stands
        print(f"    {name}: present {len(group) - len(missing)}/{len(group)}, "
              f"with stands {len(group) - len(no_stands)}/{len(group)}  {status}")
        if missing:
            print(f"      missing: {missing}")
        if no_stands:
            print(f"      without stands: {no_stands}")
    for fam in FAMILIES:
        n = fams.get(fam, 0)
        ok &= n > 0
        print(f"    {fam:20s} {n:>9,}  {'OK' if n else 'FAIL - family absent'}")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true",
                    help="actually overwrite the published table")
    args = ap.parse_args()

    import pyarrow.dataset as pds
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq
    fs = _fs()

    print("SOURCE (already passed the acceptance gate)")
    src = pds.dataset(SRC, filesystem=fs, format="parquet").to_table()
    if not _check(src, SRC):
        print("\nREFUSING: the source does not satisfy the gate.")
        return 1
    if src.num_rows != EXPECTED_ROWS or len(set(
            src.column("hexaero_apt_icao").to_pandas())) != EXPECTED_APTS:
        print(f"\nREFUSING: expected {EXPECTED_ROWS} rows / {EXPECTED_APTS} "
              f"aerodromes, found {src.num_rows}. The source changed since it "
              f"was gated -- re-run the gate before publishing.")
        return 1

    print(f"\nBackup of the current published state: {BACKUP}")
    back = pds.dataset(BACKUP, filesystem=fs, format="parquet").to_table()
    print(f"  {back.num_rows} rows -- roll back by copying this over {DST}")

    if not args.commit:
        print("\nDRY RUN. Nothing written. Re-run with --commit to publish.")
        return 0

    old = [f for f in fs.get_file_info(pafs.FileSelector(DST, recursive=True))
           if f.type == pafs.FileType.File]
    print(f"\nDeleting {len(old)} existing published files...")
    for f in old:
        fs.delete_file(f.path)

    n, chunks = src.num_rows, 4
    step = (n + chunks - 1) // chunks
    for i in range(chunks):
        part = src.slice(i * step, min(step, n - i * step))
        if part.num_rows == 0:
            continue
        pq.write_table(part, f"{DST}/part-{i:05d}-pbf-c000.snappy.parquet",
                       filesystem=fs, compression="snappy")
        print(f"  wrote part-{i:05d}  {part.num_rows:>9,} rows")
    with fs.open_output_stream(f"{DST}/_SUCCESS") as s:
        s.write(b"")

    print("\nREAD-BACK VERIFICATION (not trusting the write)")
    got = pds.dataset(DST, filesystem=fs, format="parquet").to_table()
    if not _check(got, DST) or got.num_rows != EXPECTED_ROWS:
        print("\nPUBLISHED TABLE FAILED VERIFICATION. "
              f"Roll back from {BACKUP}.")
        return 1
    print("\nPUBLISHED AND VERIFIED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
