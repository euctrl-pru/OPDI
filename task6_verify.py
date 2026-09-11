import os
from pathlib import Path
for l in Path(".env").read_text().splitlines():
    if l.strip() and not l.startswith("#") and "=" in l:
        k, v = l.split("=", 1); os.environ.setdefault(k.strip(), v.strip())
import pyarrow.dataset as pds, pyarrow.fs as pafs
fs = pafs.S3FileSystem(endpoint_override="https://s3.opensky-network.org",
    access_key=os.environ["AWS_ACCESS_KEY_ID"],
    secret_key=os.environ["AWS_SECRET_ACCESS_KEY"], scheme="https")
t = pds.dataset("eurocontrol/opdi/research/hexaero_airport_layouts_pbf",
                filesystem=fs, format="parquet").to_table(
    columns=["hexaero_apt_icao", "hexaero_aeroway"]).to_pandas()
print("airports:", t["hexaero_apt_icao"].nunique(), "rows:", len(t))
print(t["hexaero_aeroway"].value_counts().to_string())

STUDY = "EBBR LSZH EICK EFHK LEIB EDDS LFLL LHBP LFPO LPFR ESSA ENVA LOWW LKPR EDDP LPPT EGGD EGNT EGCC UGKO".split()
have_stands = set(t.loc[t["hexaero_aeroway"] == "parking_position", "hexaero_apt_icao"])
print("study airports with stands: %d/20" % len(set(STUDY) & have_stands))
print("missing (gate 1):", sorted(set(STUDY) - have_stands))

ORIGINAL20 = "EBBR EDDF EDDM EGKK EGLL EHAM EIDW EKCH ENGM EPWA ESSA LEBL LEMD LFPG LGAV LIRF LOWW LPPT LSZH LTFM".split()
present = set(t["hexaero_apt_icao"].unique())
print("original20 present: %d/20" % len(set(ORIGINAL20) & present))
print("missing (gate 2):", sorted(set(ORIGINAL20) - present))

print("\n--- cells per aerodrome ---")
counts = t.groupby("hexaero_apt_icao").size()
print("min:", counts.min(), "median:", counts.median(), "max:", counts.max())
print("max aerodrome:", counts.idxmax(), counts.max())
print("zero-cell aerodromes (should be none, by construction):", (counts == 0).sum())
print("smallest 10:")
print(counts.sort_values().head(10).to_string())
print("largest 10:")
print(counts.sort_values(ascending=False).head(10).to_string())

print("\n--- family census (gate 3) ---")
fam_expected = ["taxiway", "runway", "apron", "hangar", "threshold", "parking_position", "deicing_pad"]
vc = t["hexaero_aeroway"].value_counts()
for fam in fam_expected:
    print(fam, vc.get(fam, 0))
missing_fam = [f for f in fam_expected if vc.get(f, 0) == 0]
print("families with zero cells (gate 3 failure if non-empty):", missing_fam)
