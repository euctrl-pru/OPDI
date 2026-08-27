# `trend` on the Field-Elevation Datum — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the `trend` detection method's altitude cut from flight level (a sea-level pressure datum) onto height above each aerodrome's own field elevation, measure the effect in a new v6.1 paper, and ship it only if the gain is concentrated at elevated aerodromes.

**Architecture:** The cut currently runs *before* the aerodrome join, so it cannot see an elevation to subtract. It moves to *after* the join, guarded by a provably-lossless wide pre-filter that keeps the join affordable. A `coalesce(elevation, 0)` makes both the unknown-elevation and no-aerodrome cases degrade to exactly today's behaviour. Measurement forks V6's harness rather than editing it, following the same fork convention that produced `flight_list_v7.py` from `flight_list_v6.py`.

**Tech Stack:** PySpark 4.1.1 (native column expressions only — no pandas UDFs), pytest 9.1.1 against a local Spark session, Quarto (HTML + PDF via xelatex), mermaid for HTML diagrams.

**Spec:** `docs/superpowers/specs/2026-08-21-trend-field-elevation-datum-design.md`

**Branch:** `v61-field-elevation`, worktree `opdi/.claude/worktrees/v61-field-elevation`.

## Global Constraints

- **Units are aviation, carried in the field name.** New thresholds are `*_ft`, never a converted SI constant. Storage stays SI (`baro_altitude` is metres).
- **Reuse the existing conversion constants.** `3.28084` m→ft is already used in `flights.py` and `events.py`; name it once and reuse, so the two cannot drift.
- **Never mutate a published `version` string.** `LEGACY_TREND_VERSION = "v2.0.0"` and `LEGACY_ENDPOINT_VERSION = "v3.0.0"` are frozen.
- **`tracks.py:_add_track_id` is frozen** — marked `CRITICAL - DO NOT MODIFY`. Nothing in this plan touches it.
- **Everything is native Spark** — column expressions and window functions. No `traffic`, no `applyInPandas`, no `pandas_udf`.
- **Do not edit `benchmarks/flight_list_v6.py`, `benchmarks/trend_sweep.py`, `benchmarks/regenerate_v6.py`, or anything under `papers/adep-ades-detection-v6/` or `-v7/`.** Fork instead. Editing them changes V6's job fingerprints and forces a re-render of a published paper.
- **Commit messages describe the change only** — no `Co-Authored-By`, no "generated with" trailer.
- **Datasets live in S3** at `s3a://eurocontrol/opdi/`, never local disk.
- Run tests with `.venv310/bin/python -m pytest`, from the repo root.

## Two findings that shape this plan

Both were discovered while reading the code and both are load-bearing.

**1. `FL <= 60` is not `alt_ft <= 6000`.** `flight_level` is an *int cast*: `(baro_altitude * 3.28084 / 100).cast("int")`. The cast truncates, so `flight_level <= 60` admits everything below **6,100 ft**, not below 6,000. Rewriting the MSL branch as a comparison in feet would silently move the cut by up to 99 ft — the exact class of unit bug the repo warns about. **The MSL branch must keep the original expression verbatim.** The datum arm therefore compares FL60 against **6,100 ft** above field, so the ceiling is identical and the datum is the only variable.

**2. V6's manifest will show its pipeline jobs stale after Task 4, and that is correct.** `regenerate_v6.py:PIPE` fingerprints `src/opdi/pipeline/flights.py` and `src/opdi/config.py`. Changing production code genuinely changes what V6's `recommended` run would produce, because that run is built from `DetectionConfig()` defaults. V6's committed CSVs and PDF stay exactly as published; only a *re-render* would recompute them. Do not attempt to freeze V6 by editing `flight_list_v6.py` — that edit is itself a fingerprint change. Record the consequence in `V61_RUN_NOTES.md` (Task 5) instead.

---

## File Structure

**Production (`src/`)**
- `src/opdi/config.py` — three new `DetectionConfig` fields; `legacy()` pins the old datum.
- `src/opdi/pipeline/flights.py` — two new module-level pure functions, one shared elevation helper, one cached max-elevation lookup, and the reordered cut inside `_fetch_and_label_sv`.

**Tests (`tests/`)**
- `tests/test_detection_config.py` — extended: the new fields must be inert at legacy values.
- `tests/test_flight_detection.py` — extended: the cut expression, both datums, all four null cases.
- `tests/test_elevation_bands.py` — new: the banding function's boundaries.

**Benchmarks (`benchmarks/`) — all forks, no edits to V6's files**
- `benchmarks/elevation_bands.py` — new; `airport_elevations()` + `elevation_band()`, shared by the census and the v6.1 arms.
- `benchmarks/elevation_census.py` — new; the pre-flight feasibility gate.
- `benchmarks/trend_sweep_agl.py` — fork of `trend_sweep.py` carrying an above-field vote family.
- `benchmarks/flight_list_v61.py` — fork of `flight_list_v6.py` with datum-aware runs.
- `benchmarks/regenerate_v61.py` — fork of `regenerate_v6.py`, cut to the four arms.
- `benchmarks/V61_RUN_NOTES.md` — extended with this study's notes.

**Paper (`../opdi-portal/papers/adep-ades-detection-v6.1/`)**
- `index.qmd`, `data/` — new. V6 and V7 untouched.

---

## Task 1: Elevation banding, and the pre-flight feasibility gate

The entire study rests on Arm C showing the gain concentrated at high aerodromes. If the sample carries too few ground-truthed movements at elevated fields, Arm C cannot show anything either way — and that must be discovered for the price of one small query, not after a full cache rebuild.

**Files:**
- Create: `benchmarks/elevation_bands.py`
- Create: `benchmarks/elevation_census.py`
- Create: `tests/test_elevation_bands.py`

**Interfaces:**
- Consumes: `benchmarks/adep_ades.py` — `AIRPORTS` (`"s3a://eurocontrol/opdi/oa_airports"`), `load_ground_truth(spark, months, days)`, `airport_locations(spark)`.
- Produces:
  - `elevation_bands.airport_elevations(spark) -> DataFrame` with columns `_apt: str`, `_elev_ft: double`.
  - `elevation_bands.elevation_band(elev_col: Column) -> Column` returning a `string` band label.
  - `elevation_bands.BANDS: tuple[tuple[str, float, float], ...]` — `(label, lo_ft, hi_ft)`, `hi` exclusive.

- [ ] **Step 1: Write the failing test**

Create `tests/test_elevation_bands.py`:

```python
"""Boundary tests for the field-elevation banding.

The banding is the axis Arm C is read along: if a boundary is off by one, an
aerodrome lands in the wrong band and the study's central claim is measured
against the wrong population. Cheap to get right, expensive to get wrong
silently, so the boundaries are asserted rather than eyeballed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest
from pyspark.sql import functions as F

from elevation_bands import BANDS, elevation_band


def _band_of(spark, elev):
    df = spark.createDataFrame([(elev,)], "elev double")
    return df.select(elevation_band(F.col("elev")).alias("b")).first()["b"]


def test_bands_are_contiguous_and_ordered():
    """No gap and no overlap: every elevation falls in exactly one band."""
    for (_, _, hi), (_, lo_next, _) in zip(BANDS, BANDS[1:]):
        assert hi == lo_next
    assert BANDS[0][1] == float("-inf")
    assert BANDS[-1][2] == float("inf")


@pytest.mark.parametrize(
    "elev,expected",
    [
        (-11.0, "<500"),        # Schiphol, below sea level
        (0.0, "<500"),
        (499.9, "<500"),
        (500.0, "500-1500"),    # boundary belongs to the upper band
        (1499.9, "500-1500"),
        (1500.0, "1500-3000"),
        (1998.0, "1500-3000"),  # Madrid
        (2999.9, "1500-3000"),
        (3000.0, ">3000"),
        (5763.0, ">3000"),      # Erzurum
    ],
)
def test_band_boundaries(spark, elev, expected):
    assert _band_of(spark, elev) == expected


def test_a_null_elevation_bands_as_unknown(spark):
    """An aerodrome OurAirports has no elevation for must be visible as such.

    Folding it into `<500` would inflate the very band the study uses as its
    control, and hide how much of the sample the reference data cannot place.
    """
    assert _band_of(spark, None) == "unknown"
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `.venv310/bin/python -m pytest tests/test_elevation_bands.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'elevation_bands'`

- [ ] **Step 3: Write the minimal implementation**

Create `benchmarks/elevation_bands.py`:

```python
"""Field elevation, and the bands Arm C of the v6.1 study is read along.

`trend`'s altitude cut was measured against sea level, which costs an
aerodrome its detection in proportion to how high it sits. The claim is
therefore not "the method improves" but "the method improves *at elevated
aerodromes and not elsewhere*", and that is only checkable against a banding
fixed in advance.

The bands are chosen before any result is seen, and are not to be re-cut
afterwards to make a boundary fall more favourably.
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from adep_ades import AIRPORTS

#: (label, lo_ft inclusive, hi_ft exclusive). Contiguous and total.
#:
#: `<500` is the control: at these fields the datum change is a no-op to
#: within 500 ft, so any movement there is noise or a second-order effect and
#: bounds how much of the headline gain can be attributed to the datum.
BANDS = (
    ("<500", float("-inf"), 500.0),
    ("500-1500", 500.0, 1500.0),
    ("1500-3000", 1500.0, 3000.0),
    (">3000", 3000.0, float("inf")),
)


def elevation_band(elev_col: Column) -> Column:
    """Label an elevation in feet with its band, or ``unknown`` if NULL."""
    out = F.when(elev_col.isNull(), F.lit("unknown"))
    for label, lo, hi in BANDS:
        cond = elev_col < F.lit(hi) if lo == float("-inf") else (
            elev_col >= F.lit(lo) if hi == float("inf")
            else (elev_col >= F.lit(lo)) & (elev_col < F.lit(hi))
        )
        out = out.when(cond, F.lit(label))
    return out.otherwise(F.lit("unknown"))


def airport_elevations(spark: SparkSession) -> DataFrame:
    """Every aerodrome's field elevation in feet, from OurAirports.

    The same table `flights.py` reads elevations from, so the study and the
    pipeline band aerodromes by the same number.
    """
    return (
        spark.read.parquet(AIRPORTS)
        .select(
            F.col("ident").alias("_apt"),
            F.col("elevation_ft").cast("double").alias("_elev_ft"),
        )
        .filter(F.col("_apt").isNotNull())
    )
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `.venv310/bin/python -m pytest tests/test_elevation_bands.py -v`
Expected: PASS, 12 tests.

- [ ] **Step 5: Write the census script**

Create `benchmarks/elevation_census.py`:

```python
"""How much ground-truthed traffic the sample carries per elevation band.

The feasibility gate for the v6.1 study. Arm C asks whether the datum change
helps *at elevated aerodromes specifically*; if the sample holds only a
handful of movements above 3,000 ft, that question has no answer and the
study needs re-scoping before anything expensive is rebuilt.

Reads ground truth and the aerodrome reference only. No vote cache, no track
scan -- it is meant to be run first and to cost almost nothing.

    python benchmarks/elevation_census.py --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import functions as F

import osn_sample
from adep_ades import airport_locations, label_ground_truth, load_ground_truth
from elevation_bands import BANDS, airport_elevations, elevation_band
from osn_sample import build_spark, load_dotenv

DAYS_2025 = ["2025-06-05", "2025-06-06", "2025-06-07"]
DAYS_2024 = ["2024-06-05", "2024-06-06", "2024-06-07"]


def census(spark, months, days, period):
    gt = label_ground_truth(
        load_ground_truth(spark, months, days), airport_locations(spark)
    )
    elev = airport_elevations(spark)

    # One row per (flight, role): a departure from a high field and an arrival
    # at one are both movements the datum change could affect, and counting
    # flights instead would hide whichever role is scarcer.
    sides = [
        gt.select(F.col("ADEP").alias("apt"), F.lit("departure").alias("role")),
        gt.select(F.col("ADES").alias("apt"), F.lit("arrival").alias("role")),
    ]
    moves = sides[0].unionByName(sides[1]).filter(F.col("apt").isNotNull())

    return (
        moves.join(elev, moves.apt == elev._apt, "left")
        .withColumn("band", elevation_band(F.col("_elev_ft")))
        .groupBy("band", "role")
        .agg(
            F.count("*").alias("movements"),
            F.countDistinct("apt").alias("aerodromes"),
        )
        .withColumn("period", F.lit(period))
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=4)
    ap.add_argument("--ui-port", type=int, default=4046)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(4, "4g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    both = census(spark, ["202506"], DAYS_2025, "2025-06").unionByName(
        census(spark, ["202406"], DAYS_2024, "2024-06")
    )
    pdf = both.toPandas()
    pdf.to_csv(out / "elevation_census.csv", index=False)

    order = [label for label, _, _ in BANDS] + ["unknown"]
    print("\n=== ground-truthed movements per field-elevation band ===")
    print(
        pdf.pivot_table(
            index="band", columns=["period", "role"],
            values="movements", aggfunc="sum", fill_value=0,
        ).reindex(order).to_string()
    )
    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run the census (needs cluster credentials)**

Run: `.venv310/bin/python benchmarks/elevation_census.py --results-dir /tmp/v61_census`

**This is a gate. Stop and report the table before continuing.**

Read it against this bar: Arm C needs at least **~200 ground-truthed movements in the `>3000` band per period, spread over at least 5 aerodromes**, or the band's coverage figure will swing on single flights and no conclusion is supportable. If `>3000` is thin but `1500-3000` is healthy, the study is still viable with `1500-3000` as the treatment band — say so and adjust Arm C rather than proceeding silently. If both are thin, stop: the design's stated mitigation is to re-scope here, not to push on.

- [ ] **Step 7: Commit**

```bash
git add benchmarks/elevation_bands.py benchmarks/elevation_census.py tests/test_elevation_bands.py
git commit -m "Count the sample's traffic by field elevation before spending on it

Arm C of the v6.1 study asks whether moving trend's altitude cut onto the
field-elevation datum helps at elevated aerodromes specifically. If the
sample carries too little ground-truthed traffic up there, that question
has no answer -- and this finds out for the price of one small query
rather than after a vote-cache rebuild.

Bands are fixed here, before any result is seen, so a boundary cannot be
re-cut afterwards to suit an outcome."
```

---

## Task 2: The datum switch in `DetectionConfig`

**Files:**
- Modify: `src/opdi/config.py:463-479` (the `trend` block) and `src/opdi/config.py:674-690` (`legacy()`)
- Test: `tests/test_detection_config.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `DetectionConfig.trend_max_height_ft: float = 6000.0`, `DetectionConfig.trend_max_datum: str = "field"`, `DetectionConfig.trend_max_fl: int = 60` (retained). `DetectionConfig.legacy()` returns a config with `trend_max_datum == "msl"`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_detection_config.py`:

```python
def test_legacy_stays_on_the_sea_level_datum():
    """Released data was built with a flight-level cut; the preset must keep it.

    If the preset silently moved onto the field-elevation datum, every
    reprocessed month would differ from what was published at exactly the
    aerodromes the change is meant to help -- and `legacy()` exists precisely
    so that cannot happen.
    """
    legacy = DetectionConfig.legacy()
    assert legacy.trend_max_datum == "msl"
    assert legacy.trend_max_fl == 40


def test_the_shipped_default_is_the_field_datum():
    d = DetectionConfig()
    assert d.trend_max_datum == "field"
    assert d.trend_max_height_ft == 6000.0
    # Retained, not removed: the msl branch still reads it.
    assert d.trend_max_fl == 60


def test_an_unknown_datum_is_rejected_at_construction():
    """A typo must fail loudly. Falling through to one of the two branches
    would apply a cut nobody asked for and report it as the other one.
    """
    with pytest.raises(ValueError, match="trend_max_datum"):
        DetectionConfig(trend_max_datum="agl")
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `.venv310/bin/python -m pytest tests/test_detection_config.py -v -k "datum or shipped_default"`
Expected: FAIL — `AttributeError: 'DetectionConfig' object has no attribute 'trend_max_datum'`

- [ ] **Step 3: Write the minimal implementation**

In `src/opdi/config.py`, immediately after the `trend_max_fl` field and its docstring (which ends at line 479), add:

```python
    trend_max_height_ft: float = 6000.0
    """Ceiling for the trend vote, as a height **above field elevation**.

    Used when ``trend_max_datum == "field"``, which is the shipped setting.

    **New.** `trend` was the last of the three altitude tests still measured
    against sea level. ``endpoint`` has used ``endpoint_height_ft`` above field
    elevation since V6, and step 04's ground membership moved to the same datum
    under ``phase_ground_above_field`` -- for the reason recorded there, that a
    fixed cut-off measured from sea level is not the same test at every
    aerodrome.

    The failure is silence, not error. At FL60 an aerodrome at sea level gets
    6,000 ft of climb and descent to vote on; one at 5,763 ft gets about 240 ft,
    so the trace never clearly rises or falls and the method abstains. The loss
    is biased against high-elevation aerodromes in exactly the way
    ``phase_ground_above_field`` documents for step 04.

    Note this is a *height*, not a flight level: "FL60 above field elevation"
    would be a contradiction, since a flight level is by definition referenced
    to the standard pressure datum."""

    trend_max_datum: str = "field"
    """Which datum ``trend``'s altitude cut is measured against.

    ``"field"``
        Height above the candidate aerodrome's own elevation, capped by
        ``trend_max_height_ft``. The shipped setting.
    ``"msl"``
        Flight level above the standard pressure datum, capped by
        ``trend_max_fl``. What every published flight list was built with, and
        what :meth:`legacy` pins.

    A switch rather than a replacement because the two cuts are not
    interchangeable: ``flight_level`` is an integer cast, so ``<= 60`` admits
    everything below 6,100 ft rather than below 6,000. The ``msl`` branch keeps
    the original expression verbatim for that reason."""
```

`DetectionConfig` is a `@dataclass` (`config.py:426`) and currently has **no** `__post_init__`, so add one. It is the only validator on the class; keep it to this one check rather than opening a general validation pass:

```python
    def __post_init__(self) -> None:
        if self.trend_max_datum not in ("field", "msl"):
            raise ValueError(
                f"trend_max_datum must be 'field' or 'msl', got "
                f"{self.trend_max_datum!r}. Falling through to a default would "
                f"apply one cut while reporting the other."
            )
```

In `legacy()`, add `trend_max_datum="msl"` alongside the existing `trend_max_fl=40`:

```python
        return cls(
            trend_max_fl=40,
            trend_max_datum="msl",
            trend_radius_nm=30.0,
            ...
        )
```

Finally, update the class docstring at `config.py:442-445`, which currently says `trend` votes "across every sample below ``trend_max_fl``":

```
    ``trend``
        Votes on the sign of a smoothed altitude change across every sample
        below ``trend_max_height_ft`` **above field elevation** inside
        ``trend_radius_nm`` -- or below ``trend_max_fl`` when
        ``trend_max_datum`` is ``"msl"``. Its abstention is evidence-based --
        "the altitude trace does not clearly rise or fall".
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `.venv310/bin/python -m pytest tests/test_detection_config.py -v`
Expected: PASS — including the pre-existing `test_legacy_matches_the_shipped_constants`, which must not regress.

- [ ] **Step 5: Commit**

```bash
git add src/opdi/config.py tests/test_detection_config.py
git commit -m "Give trend's altitude cut a datum, and default it to the field

endpoint and step 04 both measure height above field elevation; trend was
the last test still referenced to sea level, where a fixed ceiling is not
the same test at every aerodrome.

A switch rather than a replacement: flight_level is an integer cast, so
FL60 admits everything below 6,100 ft, not 6,000. The two cuts are not
interchangeable and legacy() must keep the original."
```

---

## Task 3: The cut expression, as pure functions

Extracted as module-level functions before being wired in, because `_fetch_and_label_sv` reads from storage and cannot be unit-tested directly. `test_flight_detection.py` already tests `angle_between`, `bearing_deg` and `at_border` this way — follow that pattern.

**Files:**
- Modify: `src/opdi/pipeline/flights.py` — add near `haversine_nm`, `at_border` and the other module-level helpers (around lines 95-110)
- Test: `tests/test_flight_detection.py`

**Interfaces:**
- Consumes: `DetectionConfig.trend_max_datum`, `.trend_max_height_ft`, `.trend_max_fl` from Task 2.
- Produces:
  - `flights.FT_PER_M: float = 3.28084`
  - `flights.height_above_field(baro_altitude_m: Column, elevation_ft: Column) -> Column`
  - `flights.trend_altitude_cut(detection) -> Column` — a boolean `Column` referring to `baro_altitude`, `apt_elevation_ft` and `flight_level` by name.
  - `flights.trend_prefilter_ceiling_ft(detection, max_field_elevation_ft: float) -> float`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_flight_detection.py`:

```python
from opdi.config import DetectionConfig
from opdi.pipeline.flights import (
    FT_PER_M,
    height_above_field,
    trend_altitude_cut,
    trend_prefilter_ceiling_ft,
)

#: Shaped like the trend path just after its left join to the zone table:
#: altitude in metres (SI storage), elevation in feet, and both nullable.
_CUT_SCHEMA = StructType([
    StructField("baro_altitude", DoubleType()),
    StructField("apt_elevation_ft", DoubleType()),
    StructField("flight_level", IntegerType()),
])


def _cut_rows(spark, rows, detection):
    """Which rows survive the cut, as a list of booleans in input order."""
    df = spark.createDataFrame(rows, schema=_CUT_SCHEMA)
    df = df.withColumn("_i", F.monotonically_increasing_id())
    kept = df.filter(trend_altitude_cut(detection)).select("_i").collect()
    keep = {r["_i"] for r in kept}
    return [r["_i"] in keep for r in df.select("_i").collect()]


def _row(alt_ft, elev_ft):
    """A row at *alt_ft* pressure altitude near a field at *elev_ft*."""
    alt_m = alt_ft / FT_PER_M
    return (alt_m, elev_ft, int(alt_ft / 100))


def test_height_above_field_subtracts_the_elevation(spark):
    df = spark.createDataFrame([(1828.8, 1000.0)], "alt double, elev double")
    got = df.select(
        height_above_field(F.col("alt"), F.col("elev")).alias("h")
    ).first()["h"]
    # 1828.8 m is 6,000 ft; above a 1,000 ft field that is 5,000 ft.
    assert got == pytest.approx(5000.0, abs=0.5)


def test_a_null_elevation_falls_back_to_the_sea_level_height(spark):
    """Unknown elevation must degrade to the published behaviour, not vanish.

    Dropping the aerodrome instead would remove detection from exactly the
    fields the reference data is weakest about.
    """
    df = spark.createDataFrame([(1828.8, None)], "alt double, elev double")
    got = df.select(
        height_above_field(F.col("alt"), F.col("elev")).alias("h")
    ).first()["h"]
    assert got == pytest.approx(6000.0, abs=0.5)


def test_the_field_datum_admits_a_high_aerodrome_that_msl_excludes(spark):
    """The whole point of the change, as one assertion.

    A flight 3,000 ft above Erzurum (field 5,763 ft) sits at 8,763 ft
    pressure altitude. On the sea-level datum it is far above FL60 and never
    votes; on the field datum it is well inside a 6,000 ft ceiling.
    """
    rows = [_row(8763.0, 5763.0)]
    assert _cut_rows(spark, rows, DetectionConfig(trend_max_datum="msl")) == [False]
    assert _cut_rows(spark, rows, DetectionConfig(trend_max_datum="field")) == [True]


def test_the_field_datum_changes_nothing_at_a_sea_level_aerodrome(spark):
    """The control. At elevation 0 the two datums are the same test, so any
    difference here would mean the change is doing something else as well.
    """
    rows = [_row(3000.0, 0.0), _row(5900.0, 0.0), _row(9000.0, 0.0)]
    msl = _cut_rows(spark, rows, DetectionConfig(trend_max_datum="msl"))
    field = _cut_rows(spark, rows, DetectionConfig(trend_max_datum="field"))
    assert msl == [True, True, False]
    assert field == [True, True, False]


def test_the_msl_branch_keeps_the_integer_flight_level_cut(spark):
    """FL60 admits everything below 6,100 ft, because flight_level is an int
    cast. Rewriting it as `alt_ft <= 6000` would move the cut by up to 99 ft
    and nothing downstream would notice.
    """
    rows = [_row(6050.0, 0.0)]
    assert _cut_rows(spark, rows, DetectionConfig(trend_max_datum="msl")) == [True]
    # The field datum, being an honest height, cuts at 6,000 exactly.
    assert _cut_rows(spark, rows, DetectionConfig(trend_max_datum="field")) == [False]


def test_a_sample_matching_no_aerodrome_is_cut_on_sea_level(spark):
    """The trend path left-joins the zone table, and unmatched rows must
    survive to keep otherwise-unnamed tracks in the flight list. A NULL
    elevation is what those rows carry, so they must behave exactly as before.
    """
    rows = [_row(3000.0, None), _row(9000.0, None)]
    assert _cut_rows(spark, rows, DetectionConfig(trend_max_datum="field")) == [True, False]


def test_the_prefilter_ceiling_covers_the_highest_field():
    """The pre-filter is a performance guard and must drop nothing the exact
    cut would have kept. Its ceiling is therefore the union bound over every
    aerodrome: the cap plus the highest field elevation in the reference set.
    """
    d = DetectionConfig(trend_max_datum="field", trend_max_height_ft=6000.0)
    assert trend_prefilter_ceiling_ft(d, 5763.0) == pytest.approx(11763.0)


def test_the_prefilter_is_the_plain_cut_on_the_msl_datum():
    """With no elevation term there is nothing to widen for."""
    d = DetectionConfig(trend_max_datum="msl", trend_max_fl=60)
    assert trend_prefilter_ceiling_ft(d, 5763.0) == pytest.approx(6100.0)
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `.venv310/bin/python -m pytest tests/test_flight_detection.py -v -k "datum or height_above_field or prefilter or flight_level_cut or no_aerodrome"`
Expected: FAIL — `ImportError: cannot import name 'height_above_field'`

- [ ] **Step 3: Write the minimal implementation**

In `src/opdi/pipeline/flights.py`, near the other module-level helpers (`NM_PER_DEG`, `EARTH_R_NM`, around line 95):

```python
#: Metres to feet. The same factor `events.py` publishes `altitude_ft` with,
#: named here so the two conversions cannot drift apart.
FT_PER_M = 3.28084


def height_above_field(baro_altitude_m: Column, elevation_ft: Column) -> Column:
    """Height in feet above an aerodrome's own field elevation.

    A NULL elevation coalesces to zero, which leaves height above the standard
    pressure datum -- that is, exactly the published behaviour. Two cases rely
    on it and both are deliberate:

    * an aerodrome whose elevation OurAirports does not carry degrades to the
      old cut rather than dropping out of detection, the same convention
      ``phase_ground_above_field`` set in step 04;
    * a sample matching *no* aerodrome carries a NULL elevation out of the
      trend path's left join, and those rows must pass through untouched --
      dropping them would remove tracks from the flight list entirely rather
      than leaving them unnamed.
    """
    return baro_altitude_m * FT_PER_M - F.coalesce(elevation_ft, F.lit(0.0))


def trend_altitude_cut(detection) -> Column:
    """The trend vote's altitude condition, on whichever datum is configured.

    Expects ``baro_altitude`` and ``apt_elevation_ft`` on the field datum, and
    ``flight_level`` on the sea-level one.
    """
    if getattr(detection, "trend_max_datum", "msl") == "field":
        return height_above_field(
            F.col("baro_altitude"), F.col("apt_elevation_ft")
        ) <= F.lit(float(detection.trend_max_height_ft))

    # Verbatim, not an equivalent. `flight_level` is an integer cast, so this
    # admits everything below 6,100 ft at FL60 -- rewriting it as a comparison
    # in feet would move the published cut by up to 99 ft silently.
    return F.col("flight_level") <= F.lit(int(detection.trend_max_fl))


def trend_prefilter_ceiling_ft(detection, max_field_elevation_ft: float) -> float:
    """Ceiling in feet for the cheap pre-filter ahead of the aerodrome join.

    The exact cut needs an aerodrome and so can only run after the join, but
    the join is only affordable because most samples are dropped before it.
    This is the union bound that keeps both: a sample survives the exact cut at
    *some* aerodrome only if it is within the cap of the highest field in the
    reference set, so cutting there provably discards nothing the exact test
    would have kept.

    Bounded by the aerodromes actually in the zone table rather than a constant.
    A hardcoded ceiling would fail the way every threshold bug in this codebase
    fails -- by quietly never firing once the reference set moved.
    """
    if getattr(detection, "trend_max_datum", "msl") == "field":
        return float(detection.trend_max_height_ft) + float(max_field_elevation_ft)
    # The integer cast again: FL60 reaches 6,100 ft.
    return (float(detection.trend_max_fl) + 1.0) * 100.0
```

`Column` is **not** currently imported in `flights.py` — line 19 reads `from pyspark.sql import SparkSession, DataFrame`. Extend it:

```python
from pyspark.sql import SparkSession, DataFrame, Column
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `.venv310/bin/python -m pytest tests/test_flight_detection.py -v`
Expected: PASS — all pre-existing tests included.

- [ ] **Step 5: Commit**

```bash
git add src/opdi/pipeline/flights.py tests/test_flight_detection.py
git commit -m "Express trend's altitude cut as a datum-aware column expression

Pulled out as module-level functions so the behaviour can be tested without
storage: _fetch_and_label_sv reads from S3 and cannot be exercised directly.

The coalesce to zero is load-bearing twice over. An aerodrome with no known
elevation degrades to the old cut instead of dropping out of detection, and
a sample matching no aerodrome at all -- which the left join emits and which
must survive to keep its track in the list -- passes through unchanged."
```

---

## Task 4: Wire the cut into the trend path

**Files:**
- Modify: `src/opdi/pipeline/flights.py:552-576` — extract `_attach_field_elevation`
- Modify: `src/opdi/pipeline/flights.py:594-716` — `_fetch_and_label_sv`, the reordering
- Test: `tests/test_flight_detection.py`

**Interfaces:**
- Consumes: `height_above_field`, `trend_altitude_cut`, `trend_prefilter_ceiling_ft` from Task 3.
- Produces:
  - `FlightListProcessor._attach_field_elevation(df: DataFrame, ident_col: str = "apt_ident") -> DataFrame` — adds `apt_elevation_ft: double`, left-joined, NULL when unmatched.
  - `FlightListProcessor._max_field_elevation_ft(sdf_apt: DataFrame) -> float` — cached on the instance.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_flight_detection.py`:

```python
def test_attach_field_elevation_keeps_unmatched_rows(spark, monkeypatch):
    """The helper is shared with the endpoint path, and the trend path feeds it
    left-joined rows whose apt_ident is NULL. Those must come back with a NULL
    elevation, not be dropped -- an inner join here would silently delete every
    sample that matched no aerodrome.
    """
    proc = _processor_with_airports(
        spark, [("EHAM", -11.0), ("LTCE", 5763.0)]
    )
    cand = spark.createDataFrame(
        [("EHAM",), ("LTCE",), (None,), ("ZZZZ",)], "apt_ident string"
    )
    got = {
        r["apt_ident"]: r["apt_elevation_ft"]
        for r in proc._attach_field_elevation(cand).collect()
    }
    assert got["EHAM"] == pytest.approx(-11.0)
    assert got["LTCE"] == pytest.approx(5763.0)
    assert got[None] is None          # matched no aerodrome
    assert got["ZZZZ"] is None        # aerodrome not in the reference
    assert len(got) == 4              # nothing dropped


def test_max_field_elevation_comes_from_the_zone_table_not_the_world(spark):
    """The pre-filter's width is set by the highest aerodrome that can actually
    match. Taking the max over all of OurAirports would drag in fields above
    14,000 ft that the bounding box excludes, widening the pre-filter by two
    thirds for aerodromes no sample can ever be joined to.
    """
    proc = _processor_with_airports(
        spark, [("EHAM", -11.0), ("LTCE", 5763.0), ("ZUDC", 14472.0)]
    )
    zones = spark.createDataFrame([("EHAM",), ("LTCE",)], "apt_ident string")
    assert proc._max_field_elevation_ft(zones) == pytest.approx(5763.0)
```

Add the helper `_processor_with_airports` near the top of the test file. It builds a `FlightListProcessor` whose `storage` is a stub returning an in-memory `oa_airports`:

```python
def _processor_with_airports(spark, rows):
    """A FlightListProcessor whose only storage table is `oa_airports`.

    The processor's real StorageManager needs S3 and Kerberos; the two helpers
    under test read exactly one table, so a stub is honest here rather than a
    shortcut.
    """
    class _Storage:
        def table_exists(self, name):
            return name == "oa_airports"

        def read_table(self, name):
            assert name == "oa_airports"
            return spark.createDataFrame(
                rows, "ident string, elevation_ft double"
            )

    proc = FlightListProcessor.__new__(FlightListProcessor)
    proc.storage = _Storage()
    proc.spark = spark
    proc.detection = DetectionConfig()
    proc._max_elev_ft = None
    return proc
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `.venv310/bin/python -m pytest tests/test_flight_detection.py -v -k "attach_field_elevation or max_field_elevation"`
Expected: FAIL — `AttributeError: 'FlightListProcessor' object has no attribute '_attach_field_elevation'`

- [ ] **Step 3: Extract the shared elevation helper**

Add these two methods to `FlightListProcessor`, placed just above `_build_endpoint_candidates`:

```python
    def _attach_field_elevation(
        self, df: DataFrame, ident_col: str = "apt_ident"
    ) -> DataFrame:
        """Attach ``apt_elevation_ft`` from OurAirports, keyed on *ident_col*.

        Shared by both detection paths so they read the same elevation for the
        same aerodrome by construction. The endpoint path has done this since
        V6 and the trend path needs it now; two copies of the same broadcast
        join is precisely how the two would come to disagree.

        Always a left join. The trend path passes rows whose ``apt_ident`` is
        NULL -- samples near no aerodrome -- and those must survive with a NULL
        elevation.
        """
        if "apt_elevation_ft" in df.columns:
            return df
        if not self.storage.table_exists("oa_airports"):
            # Without the reference the height test can only be satisfied by
            # on_ground, and the field datum collapses to the sea-level one.
            # Said with a column rather than an exception so a cluster without
            # step 00 still runs, exactly as the endpoint path already does.
            return df.withColumn("apt_elevation_ft", lit(None).cast("double"))

        elev_ref = self.storage.read_table("oa_airports").select(
            col("ident").alias("_elev_ident"),
            col("elevation_ft").cast("double").alias("apt_elevation_ft"),
        )
        return df.join(
            broadcast(elev_ref), col(ident_col) == col("_elev_ident"), "left"
        ).drop("_elev_ident")

    def _max_field_elevation_ft(self, sdf_apt: DataFrame) -> float:
        """Highest field elevation among aerodromes that can actually match.

        Bounded by the zone table, not by OurAirports as a whole: the reference
        carries fields above 14,000 ft that the ingestion bounding box excludes,
        and widening the pre-filter for aerodromes no sample can join to would
        cost the scan for nothing.

        Cached on the instance -- it is one small aggregate, but
        ``_fetch_and_label_sv`` is called once per month.
        """
        if getattr(self, "_max_elev_ft", None) is not None:
            return self._max_elev_ft

        idents = sdf_apt.select(col("apt_ident")).distinct()
        with_elev = self._attach_field_elevation(idents)
        row = with_elev.select(f_max("apt_elevation_ft")).first()
        self._max_elev_ft = float(row[0]) if row and row[0] is not None else 0.0
        return self._max_elev_ft
```

Initialise `self._max_elev_ft = None` in `FlightListProcessor.__init__`.

Then replace the inline elevation block in `_build_endpoint_candidates` (lines 552-563) with a call to the helper, leaving the `agl_ft` / `elev_known` derivation below it untouched:

```python
        # The detection zones carry geometry, not aerodrome metadata, so
        # elevation comes from OurAirports. Without it the height test can only
        # be satisfied by on_ground and the endpoint mode collapses into a
        # surface-samples-only rule.
        cand = self._attach_field_elevation(cand)
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `.venv310/bin/python -m pytest tests/test_flight_detection.py -v`
Expected: PASS.

- [ ] **Step 5: Reorder the cut in `_fetch_and_label_sv`**

Replace the block at `flights.py:664-668` — currently:

```python
        # Filter to low altitude and join with airport zones
        sv_low_alt = sv_f.filter(col("flight_level") <= self.detection.trend_max_fl)
```

with:

```python
        # The cut needs an aerodrome, so it cannot run here -- but the join is
        # only affordable because most samples never reach it. So: a wide,
        # provably lossless pre-filter now, and the exact per-aerodrome cut
        # after the join. On the sea-level datum the two coincide and the
        # behaviour is unchanged.
        ceiling_ft = trend_prefilter_ceiling_ft(
            self.detection, self._max_field_elevation_ft(sdf_apt)
        )
        sv_low_alt = sv_f.filter(col("baro_altitude") * FT_PER_M <= lit(ceiling_ft))
```

Then, after the exact-radius filter block that ends at line 716 (`return sv_nearby_apt`), attach elevation and apply the exact cut. Replace the `return` with:

```python
        # Elevation, then the exact cut. Both after the join because both need
        # to know which aerodrome the sample is a candidate for.
        #
        # `apt_ident` is NULL for samples that matched no aerodrome. Those get
        # a NULL elevation, which `height_above_field` coalesces to zero -- so
        # they are cut on sea level exactly as before, and the population of
        # unnamed-but-present tracks does not move.
        sv_nearby_apt = self._attach_field_elevation(sv_nearby_apt)
        sv_nearby_apt = sv_nearby_apt.filter(trend_altitude_cut(self.detection))
        return sv_nearby_apt
```

Ensure `FT_PER_M`, `trend_altitude_cut` and `trend_prefilter_ceiling_ft` are in scope (same module), and that `f_max` is imported — it already is, used for `last_seen`.

Update `_fetch_and_label_sv`'s docstring, whose step 4 currently reads "Filter to below FL40":

```
        4. Pre-filter wide, join the aerodrome zones, then cut on height above
           field elevation (or on flight level, when trend_max_datum is "msl")
```

- [ ] **Step 6: Run the whole suite**

Run: `.venv310/bin/python -m pytest tests/ -v`
Expected: PASS, no regressions. Pay attention to `test_detection_config.py` and any test touching `process_dai`.

- [ ] **Step 7: Measure the pre-filter's cost**

> **Deferred to Task 7 during execution.** This step needs `flight_list_v61.py`,
> which Task 7 creates. Writing a scratch script to run it earlier would mean
> paying for a track scan twice and measuring a code path that is not the one
> that ships. The abort threshold below still applies, unchanged, and the
> measurement is a required part of Task 7's verification rather than optional.

The spec flags this as the secondary risk and requires it be measured rather than assumed. The pre-filter widens from FL60 to roughly FL60 + the highest field in the zone table — plausibly around FL150 — so about 2× the altitude band entering the join.

Run one month of `process_dai` both ways on the cluster and record wall time and the row count entering the zone join:

```bash
.venv310/bin/python benchmarks/flight_list_v61.py --months 202506 \
    --days 2025-06-05 --runs recommended --trend-datum msl   --executors 10
.venv310/bin/python benchmarks/flight_list_v61.py --months 202506 \
    --days 2025-06-05 --runs recommended --trend-datum field --executors 10
```

(Task 6 creates `flight_list_v61.py`; if running this step first, use a scratch script.) Record both numbers in `benchmarks/V61_RUN_NOTES.md`. If the field-datum run is more than ~2.5× slower, stop and report — the fallback is to narrow the pre-filter per H3 band, which is a design change and needs discussion, not a silent workaround.

- [ ] **Step 8: Commit**

```bash
git add src/opdi/pipeline/flights.py tests/test_flight_detection.py
git commit -m "Cut trend's altitude after the aerodrome join, not before it

The cut could not subtract a field elevation because it ran before the
join and so had no aerodrome to subtract one for. It now runs after,
behind a wide pre-filter that keeps the join affordable and provably
drops nothing the exact test would keep -- the ceiling is the cap plus
the highest field in the zone table.

Both paths now read elevation through one helper, so they cannot come to
disagree about the same aerodrome."
```

---

## Task 5: Version string, and the record of what V6 now means

**Files:**
- Modify: `src/opdi/pipeline/flights.py:88` — `FLIGHT_LIST_VERSION`
- Modify: `benchmarks/V61_RUN_NOTES.md`
- Test: `tests/test_flight_detection.py` (extend `test_version_is_new_unless_the_run_is_a_legacy_one`)

**Interfaces:**
- Consumes: `DetectionConfig.legacy()` from Task 2.
- Produces: `flights.FLIGHT_LIST_VERSION == "v5.0.0"`.

- [ ] **Step 1: Update the existing version test, then add the datum case**

`tests/test_flight_detection.py:229` already has `test_version_is_new_unless_the_run_is_a_legacy_one`, and it asserts the literal `"v4.0.0"` **three times**. Those literals are the test — update them to `"v5.0.0"`. Its `"v2.0.0"` and `"v3.0.0"` assertions are the frozen legacy stamps and must **not** change.

It uses a local `Stub` class rather than a fixture; match it exactly rather than introducing a second helper:

```python
    class Stub:
        _version_for = FlightListProcessor._version_for

        def __init__(self, detection, tracks):
            self.detection = detection
            self.tracks_table = tracks
```

Then append the datum-specific case:

```python
def test_the_field_datum_does_not_stamp_a_legacy_version():
    """A row cut on the field datum must never carry a version string that
    promises the sea-level one. The stamp is derived from the configuration
    for exactly this reason -- it cannot disagree with the algorithm that
    produced the row.
    """
    from opdi.config import DetectionConfig

    class Stub:
        _version_for = FlightListProcessor._version_for

        def __init__(self, detection, tracks):
            self.detection = detection
            self.tracks_table = tracks

    # The shipped default is the field datum, so this is not a legacy run
    # even over uncleaned tracks.
    assert Stub(DetectionConfig(), "osn_tracks")._version_for("trend") == "v5.0.0"

    # Legacy thresholds on the sea-level datum still reproduce the release.
    legacy = DetectionConfig.legacy()
    assert legacy.trend_max_datum == "msl"
    assert Stub(legacy, "osn_tracks")._version_for("trend") == "v2.0.0"

    # And the datum alone is enough to make a run non-legacy: everything else
    # here is the legacy preset.
    import dataclasses
    field_legacy = dataclasses.replace(legacy, trend_max_datum="field")
    assert Stub(field_legacy, "osn_tracks")._version_for("trend") == "v5.0.0"
```

That last assertion is the one worth having. `_version_for` decides by comparing the whole config against `DetectionConfig.legacy()`, so it only works if `trend_max_datum` participates in dataclass equality — which it does, but silently, and a future refactor could break it without any other test noticing.

- [ ] **Step 2: Run it to make sure it fails**

Run: `.venv310/bin/python -m pytest tests/test_flight_detection.py -v -k version`
Expected: FAIL — `assert 'v4.0.0' == 'v5.0.0'`

- [ ] **Step 3: Bump the version**

In `src/opdi/pipeline/flights.py:88`:

```python
#: Bumped once for three unreleased algorithm changes together: the bucket
#: sampler, exact-haversine candidate ranking, and trend's move onto the
#: field-elevation datum. None of them has been published under v4.0.0, so one
#: bump covers all three -- and a fourth unversioned change accumulating behind
#: the same string is what this avoids.
FLIGHT_LIST_VERSION = "v5.0.0"
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `.venv310/bin/python -m pytest tests/ -v`
Expected: PASS.

- [ ] **Step 5: Record what this does to V6**

Append to `benchmarks/V61_RUN_NOTES.md`:

```markdown
## The field-elevation datum (v6.1)

`trend`'s altitude cut moved from flight level onto height above field
elevation. `endpoint` and step 04 were already on that datum; `trend` was the
last one measured against sea level.

**V6's pipeline jobs are now stale, and that is correct rather than a
regression.** `regenerate_v6.py:PIPE` fingerprints `flights.py` and
`config.py`, and V6's `recommended` run is built from `DetectionConfig()`
defaults -- so under the new default that run genuinely would produce
different numbers. V6's committed CSVs and its PDF are untouched and remain
what V6 published; only a re-render would recompute them. **Do not try to
freeze V6 by editing `flight_list_v6.py`** -- that edit is itself a
fingerprint change, and would mark the same jobs stale for a different reason.

`DetectionConfig.legacy()` pins `trend_max_datum="msl"`, so released data
stays reproducible.

**The version string is now `v5.0.0`**, bumped once to cover the sampler, the
ranking change and the datum together. This closes the item listed as
outstanding above.
```

- [ ] **Step 6: Commit**

```bash
git add src/opdi/pipeline/flights.py tests/test_flight_detection.py benchmarks/V61_RUN_NOTES.md
git commit -m "Bump the flight list version once, for all three pending changes

The bucket sampler, exact-haversine ranking and the field-elevation datum
are all unreleased under v4.0.0. One bump covers them; the alternative was
a fourth unversioned change accumulating behind the same string.

Also records why V6's jobs now read stale, and why editing flight_list_v6.py
to prevent that would not help."
```

---

## Task 6: The above-field vote cache

Forks `trend_sweep.py`. The fork is deliberate and follows the convention that produced `flight_list_v7.py` and `regenerate_v7.py`: editing `trend_sweep.py` would mark V6's `trend_bearing` job and both `03_trend_votes_*` stages stale.

The cache is already the right shape — it keys on `(track_id, apt_ident)` and applies each cap as a conditional sum *after* the aerodrome join, so an above-field family costs an elevation join and more aggregate columns, not a second pass.

**Files:**
- Create: `benchmarks/trend_sweep_agl.py` (fork of `benchmarks/trend_sweep.py`)
- Test: `tests/test_trend_sweep_agl.py`

**Interfaces:**
- Consumes: `elevation_bands.airport_elevations` (Task 1); `adep_ades.score`, `.load_ground_truth`, `.label_ground_truth`, `.airport_locations`.
- Produces:
  - `trend_sweep_agl.HEIGHT_CAPS: tuple[int, ...]` — ceilings in feet.
  - `trend_sweep_agl.build_cache(spark, days, tracks=..., add_h3=False) -> DataFrame` — adds `up_agl_{cap}`, `dn_agl_{cap}`, `dist_agl_{cap}` columns alongside the existing `up_{cap}` family.
  - `trend_sweep_agl.predictions(votes, cap, margin, radius, penalty_nm, datum="field") -> DataFrame`
  - Cache location: `s3a://eurocontrol/opdi/research/trend_votes_agl` (2025) and `.../trend_votes_agl_2024` (2024).

- [ ] **Step 1: Copy the file and set the constants**

```bash
cp benchmarks/trend_sweep.py benchmarks/trend_sweep_agl.py
```

Then in `benchmarks/trend_sweep_agl.py` change:

```python
CACHE = table("research/trend_votes_agl")

#: Ceilings in **feet above field elevation**, the analogue of FL_CAPS.
#:
#: 6100 rather than 6000 is the datum arm's control: `flight_level` is an
#: integer cast, so FL60 admits everything below 6,100 ft. Comparing 6,000 ft
#: above field against FL60 would move the ceiling and the datum at once, and
#: the arm is meant to move one thing.
HEIGHT_CAPS = (2000, 3000, 4000, 6100, 8000, 10000, 12000, 15000, 20000)
```

Update the module docstring to say what this fork is for and that `trend_sweep.py` remains V6's.

- [ ] **Step 2: Write the failing test**

Create `tests/test_trend_sweep_agl.py`:

```python
"""The above-field vote cache.

The cache is the study's expensive artifact and its columns encode the caps,
so a mistake here is not a crash -- it is a sweep that reads the wrong column
and reports a confident number for a cap it never measured. These tests run
the aggregation over a handful of synthetic samples where the right vote
counts are countable by hand.
"""

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest
from pyspark.sql import functions as F

from trend_sweep_agl import HEIGHT_CAPS, add_height_votes

_EPOCH = dt.datetime(2025, 6, 5, 12, 0, 0)


def _joined(spark, rows):
    """Rows shaped like the post-join frame the vote aggregation reads."""
    data = [
        (
            _EPOCH + dt.timedelta(seconds=30 * i),
            "trk-1", "abc123", "KLM1",
            apt, float(elev), float(alt_m), 1.0,
        )
        for i, (apt, elev, alt_m) in enumerate(rows)
    ]
    return spark.createDataFrame(
        data,
        "event_time timestamp, track_id string, icao24 string, flight_id string, "
        "apt_ident string, apt_elev_ft double, baro_altitude double, dist_nm double",
    )


def test_a_climb_above_a_high_field_is_counted_at_the_field_cap(spark):
    """Six samples climbing from 5,800 m to 6,100 m near a 5,763 ft field.

    Those sit 13,300-14,300 ft above sea level and roughly 7,500-8,500 ft above
    the field. At the 10,000 ft above-field cap every sample votes; at FL60
    none of them would.
    """
    rows = [("LTCE", 5763.0, 5800.0 + 60 * i) for i in range(6)]
    got = add_height_votes(_joined(spark, rows)).first()
    assert got["up_agl_10000"] == 5      # five deltas across six samples
    assert got["dn_agl_10000"] == 0
    assert got["up_agl_4000"] == 0       # all of it is above 4,000 ft AGL


def test_the_cap_columns_exist_for_every_declared_cap(spark):
    """The sweep reads `up_agl_{cap}` by name. A cap in HEIGHT_CAPS with no
    column is an AnalysisException at sweep time, hours after the rebuild.
    """
    got = add_height_votes(_joined(spark, [("EHAM", -11.0, 300.0)]))
    for cap in HEIGHT_CAPS:
        for prefix in ("up_agl", "dn_agl", "dist_agl"):
            assert f"{prefix}_{cap}" in got.columns


def test_an_unknown_elevation_counts_on_the_sea_level_datum(spark):
    """Matching the pipeline: a NULL elevation coalesces to zero rather than
    dropping the aerodrome, so the cache and production agree about which
    samples voted.
    """
    rows = [(None, None, 300.0 + 60 * i) for i in range(4)]
    got = add_height_votes(_joined(spark, rows)).first()
    # 300-480 m is 984-1,575 ft; inside 2,000 ft only on the zero datum.
    assert got["up_agl_2000"] == 3
```

- [ ] **Step 3: Run it to make sure it fails**

Run: `.venv310/bin/python -m pytest tests/test_trend_sweep_agl.py -v`
Expected: FAIL — `ImportError: cannot import name 'add_height_votes'`

- [ ] **Step 4: Implement the above-field vote family**

In `benchmarks/trend_sweep_agl.py`, extract the aggregation into a testable function and add the height family. Replace the tail of `build_cache` (the `aggs` block) with a call to `add_height_votes`, and add:

```python
def add_height_votes(j: DataFrame) -> DataFrame:
    """Vote counts per (track, aerodrome) at every cap, on both datums.

    Both families come out of one pass. `_sm` is a centred rolling mean of
    barometric altitude and `_d` its first difference, so a vote is the sign of
    `_d`; the caps differ only in which samples are admitted to vote, which is
    the whole content of the datum change and is why one pass can serve both.
    """
    part = ["icao24", "flight_id", "track_id", "apt_ident"]
    w_avg = Window.partitionBy(part).orderBy("event_time").rowsBetween(-2, 2)
    w_lag = Window.partitionBy(part).orderBy("event_time")

    j = (j.withColumn("_sm", F.avg("baro_altitude").over(w_avg))
         .withColumn("_d", F.col("_sm") - F.lag("_sm").over(w_lag))
         # Matches `flights.height_above_field` exactly, including the
         # coalesce: the sweep must admit the same samples production does.
         .withColumn("_agl_ft",
                     F.col("baro_altitude") * F.lit(3.28084)
                     - F.coalesce(F.col("apt_elev_ft"), F.lit(0.0))))

    aggs = []
    for cap in FL_CAPS:
        inc = F.col("flight_level") <= cap
        aggs += [
            F.sum(F.when(inc & (F.col("_d") > 0), 1).otherwise(0)).alias(f"up_{cap}"),
            F.sum(F.when(inc & (F.col("_d") < 0), 1).otherwise(0)).alias(f"dn_{cap}"),
            F.min(F.when(inc, F.col("dist_nm"))).alias(f"dist_{cap}"),
        ]
    for cap in HEIGHT_CAPS:
        inc = F.col("_agl_ft") <= F.lit(float(cap))
        aggs += [
            F.sum(F.when(inc & (F.col("_d") > 0), 1).otherwise(0)).alias(f"up_agl_{cap}"),
            F.sum(F.when(inc & (F.col("_d") < 0), 1).otherwise(0)).alias(f"dn_agl_{cap}"),
            F.min(F.when(inc, F.col("dist_nm"))).alias(f"dist_agl_{cap}"),
        ]
    aggs += [F.first("apt_scheduled", ignorenulls=True).alias("apt_scheduled"),
             F.first("apt_elev_ft", ignorenulls=True).alias("apt_elev_ft"),
             F.min("event_time").alias("t_first"), F.max("event_time").alias("t_last")]
    return j.groupBy(*part).agg(*aggs)
```

In `build_cache`, join elevations onto the zone table before the sv join, and drop the `FL_CAPS`-only pre-filter in favour of one wide enough for both families:

```python
    z = (z.filter(F.col(rc) <= CACHE_RADIUS_NM)
         .select(F.col(hexc).alias("_hex"), F.col(idc).alias("apt_ident"),
                 F.col(latc).alias("apt_lat"), F.col(lonc).alias("apt_lon"),
                 F.col(schc).alias("apt_scheduled")))
    elev = airport_elevations(spark)
    z = z.join(F.broadcast(elev), z.apt_ident == elev._apt, "left") \
         .drop("_apt").withColumnRenamed("_elev_ft", "apt_elev_ft")
```

and widen the sv pre-filter, since the above-field caps reach higher in pressure altitude than the FL caps do:

```python
    # Wide enough for both families: the highest above-field cap plus the
    # highest field in the reference. Same union bound the pipeline's
    # pre-filter uses, and for the same reason.
    max_elev = elev.select(F.max("_elev_ft")).first()[0] or 0.0
    ceiling_fl = int((max(HEIGHT_CAPS) + max_elev) / 100) + 1
    ... .filter(F.col("flight_level") <= max(max(FL_CAPS), ceiling_fl))
```

Add a `datum` parameter to `predictions`, selecting the column family:

```python
def predictions(votes: DataFrame, cap: int, margin: int, radius: float,
                penalty_nm: float, datum: str = "field") -> DataFrame:
    """Apply the trend rule at one parameter setting, on one datum."""
    suffix = f"agl_{cap}" if datum == "field" else f"{cap}"
    up, dn, dist = (F.col(f"up_{suffix}"), F.col(f"dn_{suffix}"),
                    F.col(f"dist_{suffix}"))
    ...
```

Add `--datum {field,msl}` and `--height-caps` to `main()`, mirroring the existing `--fl-caps` handling — including the module-global assignment, since the cache's column names encode the caps and builder and reader must agree by construction.

- [ ] **Step 5: Run the tests and make sure they pass**

Run: `.venv310/bin/python -m pytest tests/test_trend_sweep_agl.py -v`
Expected: PASS.

- [ ] **Step 6: Confirm V6's harness is untouched**

Run: `git status --porcelain benchmarks/trend_sweep.py benchmarks/flight_list_v6.py benchmarks/regenerate_v6.py`
Expected: **empty output.** Any modification here means V6 has been changed and the fork was pointless.

- [ ] **Step 7: Commit**

```bash
git add benchmarks/trend_sweep_agl.py tests/test_trend_sweep_agl.py
git commit -m "Cache trend votes on both datums in one pass

The cache already keyed on (track, aerodrome) and applied each cap as a
conditional sum after the join, so the above-field family costs an
elevation join and more aggregate columns rather than a second pass over
the tracks.

Forked rather than edited: trend_sweep.py is fingerprinted by V6's vote
stages and its bearing job, and editing it would mark a published paper's
figures stale.

6,100 ft is the datum arm's control, not a typo -- flight_level is an
integer cast, so FL60 admits everything below 6,100."
```

---

## Task 7: The v6.1 regeneration harness

**Files:**
- Create: `benchmarks/flight_list_v61.py` (fork of `benchmarks/flight_list_v6.py`)
- Create: `benchmarks/regenerate_v61.py` (fork of `benchmarks/regenerate_v6.py`)
- Create: `benchmarks/elevation_arms.py` — Arm C's per-band scorer
- Test: `tests/test_regenerate_v61.py`

**Interfaces:**
- Consumes: `trend_sweep_agl.predictions`, `elevation_bands.elevation_band`, `elevation_bands.airport_elevations`, `provenance.record`.
- Produces: `regenerate_v61.jobs() -> list[Job]`, `regenerate_v61.stages() -> list[Stage]`, writing into `../opdi-portal/papers/adep-ades-detection-v6.1/data/`.

- [ ] **Step 1: Fork the two scripts**

```bash
cp benchmarks/flight_list_v6.py benchmarks/flight_list_v61.py
cp benchmarks/regenerate_v6.py benchmarks/regenerate_v61.py
```

In `flight_list_v61.py`, add `--trend-datum {field,msl}` and `--trend-max-height-ft`, and thread both into every `DetectionConfig(...)` construction, including `tuned()`, `single()`, `path_cfg()` and the grid.

In `regenerate_v61.py` set:

```python
PAPER = REPO.parent / "opdi-portal" / "papers" / "adep-ades-detection-v6.1"
T_VOTES_AGL   = "s3a://eurocontrol/opdi/research/trend_votes_agl"
T_VOTES_AGL24 = "s3a://eurocontrol/opdi/research/trend_votes_agl_2024"
PIPE = CORE + ["src/opdi/pipeline/flights.py", "src/opdi/config.py",
               "benchmarks/flight_list_v61.py"]
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_regenerate_v61.py`:

```python
"""Guards on the v6.1 job registry.

Needs no cluster: these check the registry's shape, which is where this study
has historically gone wrong -- a job whose declared dependencies are
incomplete looks current while serving numbers from code that has since
changed, and nothing in the file says so.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest

import regenerate_v61


def test_every_job_declares_the_pipeline_sources_it_depends_on():
    """A job that runs process_dai must fingerprint flights.py and config.py.
    Without them the datum change would not mark its own results stale.
    """
    for job in regenerate_v61.jobs():
        if "flight_list_v61" in job.script:
            assert "src/opdi/pipeline/flights.py" in job.code_paths, job.name
            assert "src/opdi/config.py" in job.code_paths, job.name


def test_no_job_writes_into_the_v6_or_v7_paper():
    """V6 and V7 are frozen. A stray output path is the one way this study
    could damage a published paper, and it would not be visible in a diff of
    the paper's own directory.
    """
    for job in regenerate_v61.jobs():
        for staged in job.outputs.values():
            assert "v6.1" in str(regenerate_v61.DATA), regenerate_v61.DATA
    assert regenerate_v61.PAPER.name == "adep-ades-detection-v6.1"


def test_both_periods_are_covered():
    """The study runs on both samples as standard, so a period missing from
    the registry is a silently half-measured result.
    """
    names = {j.name for j in regenerate_v61.jobs()}
    assert any(n.endswith("_2024") for n in names)
    assert any(n.endswith("_2025") for n in names)


def test_the_arms_the_paper_argues_from_all_exist():
    names = {j.name for j in regenerate_v61.jobs()}
    for arm in ("datum_swap", "height_sweep", "elevation_bands", "pipeline_path"):
        assert any(n.startswith(arm) for n in names), arm
```

- [ ] **Step 3: Run it to make sure it fails**

Run: `.venv310/bin/python -m pytest tests/test_regenerate_v61.py -v`
Expected: FAIL — the arms do not exist yet.

- [ ] **Step 4: Write Arm C's scorer**

> **Amended after Task 1's census.** Each treatment band is dominated by one
> aerodrome — LEMD is 40% of `1500-3000`, LTAC is 54% of `>3000`. So `per_band`
> alone cannot tell an elevation effect from a Madrid effect. Add two things
> alongside it: `per_aerodrome()`, giving the delta for each elevated aerodrome
> individually, and a leave-one-out column on `per_band()` that re-scores each
> band with its largest contributor removed. Both write into
> `elevation_bands.csv` so the paper can show them beside the band means.

Create `benchmarks/elevation_arms.py`:

```python
"""Arm C: the datum's effect, banded by field elevation.

The study's discriminating measurement. A headline gain is consistent with
several explanations; a gain concentrated at elevated aerodromes and absent at
sea-level ones is consistent with almost none but the datum.

Scores both datums per band, so the paper can show the control band alongside
the treatment rather than asserting the difference.
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import functions as F

import osn_sample
from adep_ades import airport_locations, label_ground_truth, load_ground_truth, score
from elevation_bands import BANDS, airport_elevations, elevation_band
from osn_sample import build_spark, load_dotenv
from trend_sweep_agl import identities, predictions


def per_band(spark, votes, ident, gt, cap, margin, radius, penalty, datum):
    """Score one datum, restricted to each elevation band's truth in turn."""
    elev = airport_elevations(spark)
    gt_banded = (
        gt.join(elev, gt.ADES == elev._apt, "left")
        .withColumn("band", elevation_band(F.col("_elev_ft")))
        .drop("_apt", "_elev_ft")
    )
    pred = predictions(votes, cap, margin, radius, penalty, datum=datum)

    out = []
    for label, _, _ in BANDS:
        sub = gt_banded.filter(F.col("band") == label)
        n = sub.count()
        if n == 0:
            # An empty band is reported, not skipped. A band silently absent
            # from the table reads as "no effect" rather than "no data".
            out.append({"band": label, "datum": datum, "n": 0})
            continue
        m = score(pred, ident, sub)
        m.update(band=label, datum=datum, n=n)
        out.append(m)
    return out
```

Add a `main()` following the pattern in `trend_sweep_agl.py`: parse `--months`, `--days`, `--cache`, `--results-dir`, `--executors`; build Spark; load votes, identities and ground truth; call `per_band` for both `"msl"` and `"field"`; write `elevation_bands.csv`.

- [ ] **Step 5: Write the job registry**

Replace `regenerate_v61.jobs()` with the four arms. Every job appears twice, once per period. Example for the first:

```python
def jobs() -> list:
    """The four arms of the v6.1 study, in dependency order."""
    return [
        Job("datum_swap_2025", "benchmarks/flight_list_v61.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--runs", "datum_msl", "datum_field",
             "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "datum_swap_2025.csv"}, PIPE,
            "Arm A: FL60 against 6,100 ft above field. The ceiling is "
            "identical -- flight_level is an int cast, so FL60 reaches 6,100 "
            "-- which leaves the datum as the only difference",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),

        Job("datum_swap_2024", "benchmarks/flight_list_v61.py",
            ["--months", "202406", "--days", *DAYS_2024,
             "--tracks", T_TRACKS24, "--runs", "datum_msl", "datum_field",
             "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "datum_swap_2024.csv"}, PIPE,
            "Arm A on the second period",
            inputs=[T_TRACKS24, T_ZONES, T_CAND24, T_REF]),

        Job("height_sweep_2025", "benchmarks/trend_sweep_agl.py",
            ["--months", "202506", "--days", *DAYS_2025, "--datum", "field",
             "--executors", "10"],
            {"trend_sweep.csv": "height_sweep_2025.csv"},
            CORE + ["benchmarks/trend_sweep_agl.py"],
            "Arm B: the above-field cap swept on its own terms",
            inputs=[T_VOTES_AGL, T_ZONES, T_REF]),

        # ... height_sweep_2024, elevation_bands_2025, elevation_bands_2024,
        #     pipeline_path_2025, pipeline_path_2024
    ]
```

Add matching `stages()` entries building `T_VOTES_AGL` and `T_VOTES_AGL24` via `trend_sweep_agl.py --build --build-only`, modelled on V6's `03_trend_votes_*` stages.

- [ ] **Step 6: Run the tests and make sure they pass**

Run: `.venv310/bin/python -m pytest tests/test_regenerate_v61.py -v`
Expected: PASS.

- [ ] **Step 7: Check staleness without running anything**

Run: `.venv310/bin/python benchmarks/regenerate_v61.py --check`
Expected: exits non-zero listing every job as stale — nothing has been produced yet. This needs no credentials and confirms the registry parses and fingerprints resolve.

Also run `.venv310/bin/python benchmarks/regenerate_v6.py --check` and record its output. V6's pipeline jobs are expected to be stale for the reason recorded in Task 5; confirm that the *non*-pipeline jobs (`sampler_comparison`, `merge_diagnosis`) are **not**, which shows the fingerprinting is discriminating rather than blanket.

- [ ] **Step 8: Commit**

```bash
git add benchmarks/flight_list_v61.py benchmarks/regenerate_v61.py \
        benchmarks/elevation_arms.py tests/test_regenerate_v61.py
git commit -m "Define the v6.1 study as four arms over both periods

Arm C is the one that can fail: it scores each datum per field-elevation
band, so a gain concentrated where the datum bites can be told apart from
a gain that appears everywhere and therefore came from something else.

An empty band is reported rather than skipped -- a band missing from the
table reads as no effect when it means no data."
```

---

## Task 8: The methodology diagrams

Four flows — `endpoint` and `trend`, each for departures and arrivals. V7's idiom is mandatory here: mermaid does not render reliably to PDF on this toolchain, so each diagram ships as a `{mermaid}` block inside `::: {.content-visible when-format="html"}` **plus** an equivalent table inside a `when-format="pdf"` block. See `papers/adep-ades-detection-v7/index.qmd:222-265`.

**Files:**
- Create: `../opdi-portal/papers/adep-ades-detection-v6.1/index.qmd` (the methodology section; the results sections come in Task 9)

- [ ] **Step 1: Audit what V6 already explains**

Read `papers/adep-ades-detection-v6/index.qmd:320-470`, which covers both methods, and `:1195-1210`, which states the datum asymmetry. Write down which steps are already explained well and which are not.

The brief is to explain each step of the final pipeline early **if it is not already explained** — so carry across what still holds, and write only the genuinely missing steps. Do not paraphrase V6's existing prose into near-duplicates.

- [ ] **Step 2: Write the section with both diagram forms**

In `index.qmd`, after the introduction, add a `## How a flight gets its aerodromes {#sec-method}` section. Each diagram follows this shape:

````markdown
:::: {.content-visible when-format="html"}
```{mermaid}
%%| label: fig-trend-arrival
%%| fig-cap: "The `trend` method naming an arrival aerodrome. The altitude cut is the step this version changes."
flowchart TD
    T[(osn_tracks<br/>one track)] --> SM["smooth baro_altitude<br/><i>centred 5-sample mean</i>"]
    SM --> PRE["pre-filter<br/><i>cap + highest field elevation</i>"]
    PRE --> J{{"join H3 aerodrome zones<br/><i>res 7, within radius</i>"}}
    J --> EL["attach field elevation<br/><i>OurAirports</i>"]
    EL --> CUT["<b>altitude cut</b><br/><i>height above field &le; cap</i>"]
    CUT --> V["vote per sample<br/><i>sign of smoothed change</i>"]
    V --> M{"descending votes<br/>exceed climbing<br/>by the margin?"}
    M -->|yes| R["rank candidates<br/><i>haversine + scheduled penalty</i>"]
    M -->|no| U(["undetermined"])
    R --> A(["ADES = nearest surviving aerodrome"])
```
::::

:::: {.content-visible when-format="pdf"}
The same method, step by step.

| Step | What it does | Reads |
|---|---|---|
| 1 | Smooth `baro_altitude` over a centred 5-sample window | the whole track, before any cut |
| 2 | Pre-filter on pressure altitude at the cap plus the highest field elevation | — |
| 3 | Join the H3 aerodrome zones at resolution 7, then cut on exact distance | `h3_airport_detection_zones` |
| 4 | Attach the candidate aerodrome's field elevation | `oa_airports` |
| 5 | **Cut on height above field elevation** — the step this version changes | |
| 6 | Vote per sample on the sign of the smoothed change | |
| 7 | If descending votes exceed climbing by the margin, the track is arriving | |
| 8 | Rank surviving candidates on haversine distance plus the scheduled-service penalty; the nearest becomes `ADES` | |

::::
````

Repeat for `fig-trend-departure` (climbing votes, `ADEP`), `fig-endpoint-arrival` (last fix, radius **and** height-above-field **or** `on_ground`, falling back to `OOA` at the border and `undetermined` otherwise) and `fig-endpoint-departure` (first fix; note the precedence — aerodrome first, border second, because Ponta Delgada sits ~8 NM inside the western edge and the other order labelled its departures out-of-area with the aircraft still on the runway).

**Name the datum explicitly on every altitude box.** The asymmetry between the methods is the paper's subject; a diagram that says only "altitude test" hides it.

- [ ] **Step 3: Render both formats**

```bash
cd ../opdi-portal/papers
OPDI_RENDER=skip quarto render adep-ades-detection-v6.1
```

`OPDI_RENDER=skip` renders the prose without running the analysis, which is what this task needs — the numbers arrive in Task 9.

Expected: both `_site/adep-ades-detection-v6.1/index.html` and the PDF build. **Open the PDF and confirm the four tables appear and no mermaid block leaks into it**; that failure mode is the entire reason V7 carries two forms.

- [ ] **Step 4: Commit (in `opdi-portal`, then the pointer)**

```bash
cd ../opdi-portal
git add papers/adep-ades-detection-v6.1/index.qmd
git commit -m "Draw how endpoint and trend name an aerodrome, per role

Four flows: each method for departures and arrivals. The altitude box on
each names its datum, because the difference between them is what this
version is about and a diagram saying only 'altitude test' would hide it.

Two forms per diagram, following v7: mermaid for HTML, a table for PDF,
where mermaid does not render on this toolchain."
```

---

## Task 9: Run the chain, and write what it found

**Files:**
- Modify: `../opdi-portal/papers/adep-ades-detection-v6.1/index.qmd` — results
- Modify: `src/opdi/config.py` — `trend_max_height_ft` set to the swept optimum
- Modify: `benchmarks/V61_RUN_NOTES.md`

- [ ] **Step 1: Build the vote caches**

```bash
.venv310/bin/python benchmarks/regenerate_v61.py --with-stages --check
.venv310/bin/python benchmarks/regenerate_v61.py --with-stages
```

Run `--check` first and read what it intends to do. The cache build is the expensive step and writes to `research/trend_votes_agl*`; confirm those prefixes, **not** V6's `research/trend_votes`, before letting it run.

- [ ] **Step 2: Verify the caches are the size they should be**

The failure mode this study keeps producing is a silent wrong-size table, not a crash. Before reading any result:

```bash
.venv310/bin/python - <<'PY'
# row count and distinct (track_id, apt_ident) pairs per cache
PY
```

Check that `research/trend_votes_agl` has the same order of magnitude of `(track, aerodrome)` pairs as V6's `research/trend_votes`, and that `apt_elev_ft` is non-NULL for the large majority of rows. A cache where most elevations are NULL means the join key is wrong, and every above-field cap would then silently equal its sea-level counterpart.

- [ ] **Step 3: Read Arm C first**

Open `data/elevation_bands_2025.csv` and `data/elevation_bands_2024.csv`.

**Apply the success criteria from the spec, in this order:**

1. Is the gain concentrated in the elevated bands, with the `<500` control band flat? If **no**, the change does not ship regardless of the headline — say so plainly and write the paper as a null result. That is a real finding: it closes a question the study has carried since V6.
2. Only if yes: is pooled arrival coverage up by ≥ 0.5 pp, with accuracy not falling?

Do not reorder these. Reading the headline first and the bands afterwards is how a confirmatory reading happens.

- [ ] **Step 4: Set the shipped threshold from Arm B**

If and only if the criteria pass, set `trend_max_height_ft` in `src/opdi/config.py` to the swept optimum from `height_sweep_*.csv`, and add to its docstring the same style of justification the other tuned fields carry — the value, the range swept, whether it is an interior optimum, and on which periods.

Then re-run the pipeline arm so the shipped figure is the pipeline's own:

```bash
.venv310/bin/python benchmarks/regenerate_v61.py
```

- [ ] **Step 5: Write the results sections**

Follow V6's structure: one section per arm, each reading its committed CSV, with the provenance table at the foot.

State the two bounds the spec calls for, so the result is not over-claimed:

> The vote is unchanged. `trend` votes on the *sign* of a smoothed altitude
> change, and adding a constant offset to every altitude in a track cannot flip
> a sign. Field elevation changes only which samples are admitted to vote, never
> how a vote is cast — so this is a coverage result, and any accuracy movement
> is a consequence of coverage rather than of better discrimination.

Explain the both-periods design once, in the method section, and report pooled figures — the second period as confirmation in prose, not as a duplicate set of tables.

- [ ] **Step 6: Verify every figure has provenance**

Run: `.venv310/bin/python benchmarks/regenerate_v61.py --check`
Expected: exit 0, nothing stale.

Then check `data/_manifest.json` has an entry for every CSV the `.qmd` reads. A figure with no manifest entry must be reported as unverified rather than shown as fact.

- [ ] **Step 7: Full render**

```bash
cd ../opdi-portal/papers && quarto render adep-ades-detection-v6.1
```

Expected: HTML and PDF both build, with no stale-figure warnings.

- [ ] **Step 8: Run the whole test suite once more**

Run: `.venv310/bin/python -m pytest tests/ -v`
Expected: PASS. `trend_max_height_ft` moved in Step 4, so `test_the_shipped_default_is_the_field_datum` will need its expected value updated to match — update the test to the measured value, and only for that reason.

- [ ] **Step 9: Commit both repos**

```bash
git add src/opdi/config.py benchmarks/V61_RUN_NOTES.md tests/test_detection_config.py
git commit -m "Ship the swept above-field ceiling, and record what it bought"

cd ../opdi-portal
git add papers/adep-ades-detection-v6.1/
git commit -m "Report v6.1: trend's altitude cut on the field-elevation datum"
```

Then commit the submodule pointers in the meta-repo, submodule first — the order matters, per the workspace convention.

---

## Self-Review

**Spec coverage.** Every section of the spec maps to a task: production change → 3, 4; config surface → 2; version string → 5; measurement Arms A–D → 6, 7, 9; paper and diagrams → 8, 9; success criteria → 9 Step 3; the pre-flight risk mitigation → 1. The two "out of scope" items (`nearest`, retuning radius/margin/penalty) appear in no task, which is correct.

**Deviations from the spec, both deliberate:**
- The spec's Arm A said "6,000 ft"; the plan uses **6,100 ft**, because `flight_level` is an integer cast and FL60 reaches 6,100. Using 6,000 would move the ceiling and the datum together and spoil the single-variable arm.
- The spec left `max_field_elevation_ft` as "a runtime max over the reference table"; the plan narrows it to **the zone table**, since OurAirports carries fields above 14,000 ft that the bounding box excludes and that no sample can ever join to.

**Placeholder scan.** Two steps intentionally cannot carry final content: Task 9 Step 4 sets a threshold that does not exist until Arm B is run, and Task 9 Step 5 writes prose about results not yet obtained. Both state their decision rule and their inputs rather than deferring the thinking. Task 7 Step 5 shows three of eight registry entries in full and names the remaining five — repeat the shown pattern.

**Type consistency.** `apt_elevation_ft` (double) is the production column name throughout Tasks 3–5; the sweep fork uses `apt_elev_ft` in `trend_sweep_agl.py` because that is a separate frame, and `add_height_votes` is the only consumer. `elevation_band` returns a string in both. `trend_max_datum` takes exactly `"field"` or `"msl"` everywhere, validated in Task 2.

**One gap found and closed:** the plan originally had no check that the fork actually protected V6. Task 6 Step 6 now asserts `git status` on V6's three files is empty, and Task 7 Step 7 confirms V6's non-pipeline jobs stay current — which shows the fingerprinting is discriminating rather than marking everything stale.

**Three assumptions checked against the code and corrected before hand-off:**

- `DetectionConfig` is a `@dataclass` with **no** existing `__post_init__` (`config.py:426`), so Task 2 adds one rather than extending one.
- `Column` is **not** imported in `flights.py` (line 19 imports only `SparkSession, DataFrame`), so Task 3's type hints need the import added.
- Task 5 originally invented a `_config_with` helper that does not exist. The real test at `test_flight_detection.py:229` uses a local `Stub` class and hardcodes `"v4.0.0"` three times — those literals must be updated by the version bump, which the task now says explicitly. Missing this would have left the suite red with no explanation in the plan.
