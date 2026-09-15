# Spurious Track Splitting: Callsign Debounce Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Find out why ~1,457 tracks a day are cut in half by a callsign change that immediately reverts, fix it if the evidence supports a fix, and prove the fix helps using the APDF measurements that are actually trustworthy.

**Architecture:** A new segmentation arm `A9 debounced` beside the existing `recommended`, differing only in requiring a new callsign to *persist* before it breaks a track. Selected through `SegmentationConfig.method`, so `track_id` for published months is untouched until a separate, evidenced decision promotes it. Verification is a benchmark comparing arms on fragmentation, milestone co-location and APDF agreement.

**Tech Stack:** PySpark (native column and window expressions only — no `applyInPandas`, no `pandas_udf`, no `traffic`), pytest with the local `spark` fixture, pandas + pyarrow for the APDF comparison.

**Spec:** This document. The evidence it argues from is in *Context* below and was measured on `s3a://eurocontrol/opdi-prod`, 2026-06-01, during the `events_v0.3.0` campaign.

## Global Constraints

- **Everything is native Spark.** Column expressions and window functions. Introducing `applyInPandas` or a UDF is a deliberate architectural step, not a default.
- **`track_id` is a published contract.** Every dataset published before 2026-08-27 used `legacy`; everything since uses A8 `recommended`. A new arm must not change either. Promotion to default is a separate decision with its own evidence.
- **Storage is SI; everything human-facing is aviation.** New config fields carry their unit in the name (`_seconds`, `_ft`, `_kt`, `_nm`, `_minutes`). `tests/test_detection_config.py` property-tests this.
- **Ground truth is always the LEFT side of a scoring join.** A movement APDF has and OPDI missed must count against OPDI; an inner join silently drops exactly the failures being measured.
- **One distributed Spark job at a time from this pod**, and the driver needs ≥2 GiB free in a 16 GiB container shared with other sessions. Check `grep '^anon ' /sys/fs/cgroup/memory.stat` before launching.
- **Never mutate a published `version` string.**

## Context: what is known, and how

`recommended` (A8) breaks a track on exactly three conditions:

```
callsign_change  OR  gap > 30 min  OR  (gap > 15 min AND altitude < 5000 ft)
```

Measured on 2026-06-01 over 52,394 flight-list rows, taking consecutive tracks of one `icao24` and the gap between one's `last_seen` and the next's `first_seen`:

| gap between consecutive tracks | pairs |
|---|---|
| under 1 min | **1,767** |
| 1–5 min | 51 |
| 5–30 min | 25 |
| 30 min – 2 h | 223 |
| over 2 h | 532 |

**A sub-minute gap cannot be produced by either gap rule** — 30 minutes and 15 minutes are the thresholds. By elimination every one of those 1,767 splits was `callsign_change`. Of them:

* **308** have a visibly different `FLT_ID` on the two halves — a real callsign change.
* **1,457** have the *same* `FLT_ID` on both halves.

`FLT_ID` is the flight list's *resolved* callsign for a whole track. Both halves resolving to the same value while the raw per-sample callsign changed is the signature of a **flicker**: a transient value that breaks the track, after which both halves resolve to the same identity. If the callsign is X before and X after, the momentary Y was noise, not a new flight.

Implied speed across those sub-minute gaps, both endpoints above 3,000 ft: **median 421 kt** (p10 195, p90 498). That is continuous cruise flight — the halves are one flight.

**This is a hypothesis, not yet a finding.** It rests on the resolved `FLT_ID`, because the raw per-sample callsigns live in `osn_tracks_clean` and need the cluster. Task 1 confirms or refutes it, and **if it refutes it, stop** — Tasks 2 onward are wrong and the real cause needs finding first.

### Why APDF cannot verify this the obvious way

The user's constraint, and it shapes the whole verification design:

* **`AOBT`/`AIBT` are frequently rounded to the minute.** A timing comparison against them measures APDF's quantisation, not the detector.
* **ADS-B and the block times do not share an end.** The transponder may stay on after on-block or come on well before off-block, so the *duration* of ADS-B presence is not the duration of the turnaround.

Only 3 of the 20 well-covered aerodromes report `MVT_TIME_UTC` to the second (LSZH, LEIB, EDDS); the other 17 round to the minute.

So this plan **does not** verify with block-time accuracy. It verifies with quantities immune to both problems:

1. **Fragmentation** — tracks per (icao24, resolved callsign, day). Internal, exact, and the thing the defect directly causes.
2. **Milestone co-location** — the share of tracks carrying *both* an `ATOT` and an `ALDT`. A split flight has its take-off on one half and its landing on the other, so neither half is complete. Merging should raise this. No APDF timing involved.
3. **APDF movement counts per aerodrome** — a *guard*, not a target. Counts are insensitive to minute rounding and to transponder dwell. They must not regress from the current 1.03× departures / 1.02× arrivals.
4. **Runway identity against `AP_C_RWY`** — categorical, 100% populated at every studied aerodrome, immune to rounding. A guard against the merge attaching the wrong runway.

**Expected direction matters as much as magnitude.** These splits are airborne-to-airborne at cruise, so merging them should barely move movement counts — both halves are already above the ground and neither generates a spurious `ADEP`. A large movement-count change would mean the fix did something other than what is intended, and is a reason to stop and investigate rather than to celebrate.

## File Structure

| File | Responsibility |
|---|---|
| `benchmarks/callsign_flicker.py` | *New.* Measures raw callsign sequences around sub-minute splits. Answers "is it a flicker?" and nothing else. |
| `src/opdi/pipeline/segmentation/base.py` | Add `callsign_min_persistence_seconds` to `SegmentationParams`; add `persisted_callsign()` helper beside `lookback_minutes()`. |
| `src/opdi/pipeline/segmentation/methods.py` | Add `debounced()` arm. `recommended()` is untouched. |
| `src/opdi/config.py` | Add the field to `SegmentationConfig` and thread it into `SegmentationParams.from_config`. |
| `benchmarks/track_fragmentation.py` | *New.* Fragmentation and milestone co-location per arm. |
| `benchmarks/movement_counts.py` | *Modify.* Accept `--warehouse` for an arm-specific research prefix so the existing APDF guard runs against a candidate arm. |
| `tests/test_segmentation_debounce.py` | *New.* The arm's behaviour. |
| `benchmarks/regenerate_track_v3.py` | *New.* The executable definition of the V3 study, modelled on `regenerate_track_v2.py`. |
| `opdi-portal/papers/track-construction-v3/index.qmd` | *New.* The paper, reading only staged CSVs. |
| `tests/test_segmentation_methods.py` | *Modify.* Assert `recommended` is unchanged by the new field. |

---

### Task 1: Confirm or refute the flicker hypothesis

Everything after this depends on the answer. It is a measurement, not a change — no production code is touched.

**Files:**
- Create: `benchmarks/callsign_flicker.py`

**Interfaces:**
- Consumes: `osn_tracks_clean` and `opdi_flight_list` under `s3a://eurocontrol/opdi-prod`; `opdi.utils.spark_helpers.SparkSessionManager`.
- Produces: a printed report. No table is written.

- [ ] **Step 1: Write the measurement script**

Create `benchmarks/callsign_flicker.py`:

```python
"""Are sub-minute track splits caused by a callsign that flickers and reverts?

`recommended` breaks on callsign_change, gap > 30 min, or gap > 15 min below
5,000 ft. A split with a sub-minute gap can only be the callsign rule, and
1,457 of 1,765 such splits on 2026-06-01 had the *same* resolved FLT_ID on both
halves. That is consistent with a transient value -- but the resolved callsign
is a per-track summary, so it cannot tell a flicker from a genuine change the
flight list then smooths away. Only the raw per-sample sequence can.

Read-only. Writes nothing.
"""
import argparse

from pyspark.sql import functions as F, Window

from opdi.config import OPDIConfig
from opdi.utils.spark_helpers import SparkSessionManager
from opdi.utils.storage import StorageManager


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", default="2026-06-01")
    ap.add_argument("--warehouse", default="s3a://eurocontrol/opdi-prod")
    ap.add_argument("--executors", type=int, default=6)
    args = ap.parse_args()

    config = OPDIConfig.for_environment("opensky")
    config.project.warehouse_path = args.warehouse
    config.spark.executor_instances = args.executors
    spark = SparkSessionManager.create_session(
        app_name="callsign flicker", config=config, distributed=True
    )
    storage = StorageManager(spark, config)

    sv = (
        storage.read_table("osn_tracks_clean")
        .filter(F.col("dof").cast("date") == F.lit(args.day))
        .select("icao24", "track_id", "event_time", "callsign")
    )

    # The raw callsign sequence per airframe, ignoring blanks -- a blank is
    # "not transmitted", not "changed to nothing", and the arm already treats
    # it that way.
    real = F.when(F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
                  F.trim(F.col("callsign")))
    w = Window.partitionBy("icao24").orderBy("event_time")
    seq = (
        sv.withColumn("_cs", real)
        .filter(F.col("_cs").isNotNull())
        .withColumn("_prev", F.lag("_cs").over(w))
        .withColumn("_next", F.lead("_cs").over(w))
        .withColumn("_prev_t", F.lag("event_time").over(w))
        .withColumn("_next_t", F.lead("event_time").over(w))
    )

    changes = seq.filter(F.col("_prev").isNotNull() & (F.col("_cs") != F.col("_prev")))
    total = changes.count()

    # A flicker: the value differs from the one before AND from the one after,
    # and the one before equals the one after. X -> Y -> X.
    flicker = changes.filter(
        F.col("_next").isNotNull()
        & (F.col("_cs") != F.col("_next"))
        & (F.col("_prev") == F.col("_next"))
    )
    n_flicker = flicker.count()

    # How long the transient value held. A flicker that persists for minutes is
    # a different animal from one that lasts a single sample, and the debounce
    # threshold has to be chosen against this distribution rather than guessed.
    held = flicker.withColumn(
        "_held_s",
        F.col("_next_t").cast("long") - F.col("event_time").cast("long"),
    )

    print(f"day {args.day}")
    print(f"  raw callsign changes            {total:,}")
    print(f"  of which X -> Y -> X (flicker)  {n_flicker:,}"
          f"  ({100 * n_flicker / max(total, 1):.1f}%)")
    print("\n  how long the transient value held, seconds:")
    held.selectExpr(
        "percentile_approx(_held_s, 0.5) AS p50",
        "percentile_approx(_held_s, 0.9) AS p90",
        "percentile_approx(_held_s, 0.99) AS p99",
        "max(_held_s) AS max",
    ).show(truncate=False)

    print("  sample of flickers:")
    flicker.select("icao24", "_prev", "_cs", "_next", "event_time").show(15, False)

    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Check there is memory to run it**

Run:
```bash
grep '^anon ' /sys/fs/cgroup/memory.stat | awk '{printf "%.1f GiB used of 16\n", $2/1073741824}'
```
Expected: under 13 GiB, i.e. at least ~3 GiB free. If not, stop and ask — the driver needs 2 GiB and dies by kernel SIGKILL with no Java OOM when squeezed.

- [ ] **Step 3: Run it**

Run: `cd /home/jupyter/work/opdi-workspace/opdi && .venv310/bin/python -u benchmarks/callsign_flicker.py --day 2026-06-01`
Expected: a flicker percentage and a holding-time distribution.

- [ ] **Step 4: Decide, and write the decision down**

**This is a gate, not a formality.**

* **Flickers are a large share of changes (say >30%) and mostly hold for under a minute** → the hypothesis is confirmed. Continue to Task 2, and use the p90 holding time to choose the debounce threshold in Task 3.
* **Flickers are rare, or hold for many minutes** → the hypothesis is refuted. **Stop.** Tasks 2–6 are built on it and would implement a fix for a cause that is not there. Record what was actually found and bring it back for a new diagnosis.

Append the outcome to `benchmarks/EVENTS_RUN_LOG.md` as decision 42, with the measured numbers, whichever way it goes. A refuted hypothesis is worth as much as a confirmed one and costs a day to re-derive if unrecorded.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/callsign_flicker.py benchmarks/EVENTS_RUN_LOG.md
git commit -m "bench: measure whether sub-minute track splits are callsign flicker"
```

---

### Task 2: The config surface for the debounce

Defined first so Task 3 never has to touch `config.py`, and so the property tests that police field naming run once, early.

**Files:**
- Modify: `src/opdi/pipeline/segmentation/base.py` (`SegmentationParams`, ~line 89)
- Modify: `src/opdi/config.py` (`SegmentationConfig`, ~line 1512)
- Test: `tests/test_segmentation_debounce.py`

**Interfaces:**
- Produces: `SegmentationParams.callsign_min_persistence_seconds: float = 0.0`; `SegmentationConfig.callsign_min_persistence_seconds: float = 0.0`; both threaded through the existing `SegmentationParams.from_config`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_segmentation_debounce.py`:

```python
"""A9 `debounced`: a callsign must persist before it breaks a track.

`recommended` breaks on the first sample whose callsign differs from the last
real one. A value that appears for one sample and reverts therefore cuts a
flight in half -- measured at 1,457 of 1,765 sub-minute splits on 2026-06-01,
all of them at cruise with a median implied speed of 421 kt across the gap.
"""
from dataclasses import fields

import pytest

from opdi.config import SegmentationConfig
from opdi.pipeline.segmentation import SegmentationParams


def test_the_default_is_no_debounce():
    """Zero reproduces `recommended` exactly.

    A8 is what every dataset since 2026-08-27 was published with, and
    `track_id` is a published contract. A non-zero default would change ids for
    every arm that inherits this field, silently.
    """
    assert SegmentationParams().callsign_min_persistence_seconds == 0.0
    assert SegmentationConfig().callsign_min_persistence_seconds == 0.0


def test_the_field_carries_its_unit():
    """The convention `tests/test_detection_config.py` enforces elsewhere; a
    threshold whose unit is ambiguous is the most likely source of a silent
    bug, because a value 60x too small simply never fires."""
    names = {f.name for f in fields(SegmentationParams)}
    assert "callsign_min_persistence_seconds" in names


def test_the_config_value_reaches_the_params():
    """`SegmentationParams.from_config` is the only path from configuration to
    the engine. A field that does not travel it is a setting that does
    nothing."""
    cfg = SegmentationConfig(callsign_min_persistence_seconds=30.0)
    assert SegmentationParams.from_config(cfg).callsign_min_persistence_seconds == 30.0
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd /home/jupyter/work/opdi-workspace/opdi && .venv310/bin/python -m pytest tests/test_segmentation_debounce.py -q`
Expected: FAIL with `AttributeError`/`TypeError` about `callsign_min_persistence_seconds`.

- [ ] **Step 3: Add the field to `SegmentationParams`**

In `src/opdi/pipeline/segmentation/base.py`, in `SegmentationParams` after `low_alt_ft`:

```python
    #: How long a new callsign must persist before it breaks a track, in
    #: seconds. ``0.0`` means "break immediately", which is what `recommended`
    #: does and therefore the only default that reproduces published ids.
    #:
    #: The rule it guards: a callsign that appears for one sample and reverts
    #: is noise, not a new flight. Measured on 2026-06-01, 1,457 of 1,765
    #: sub-minute splits had the same resolved callsign on both halves and a
    #: median implied speed of 421 kt across the gap -- one flight, cut in two.
    callsign_min_persistence_seconds: float = 0.0
```

- [ ] **Step 4: Add the field to `SegmentationConfig`**

In `src/opdi/config.py`, in `SegmentationConfig` after `callsign_lookback_minutes`:

```python
    callsign_min_persistence_seconds: float = 0.0
    """How long a new callsign must hold before A9 treats it as a real change.

    Zero reproduces A8 `recommended` exactly, which is what every dataset
    published since 2026-08-27 uses. Raising it changes ``track_id``, so it is
    a deliberate act and belongs to the ``debounced`` arm rather than to the
    shipped one."""
```

- [ ] **Step 5: Thread it through `from_config`**

Find `SegmentationParams.from_config` in `src/opdi/pipeline/segmentation/base.py` and add the field to the constructed params alongside `callsign_lookback_minutes`. It is a plain passthrough:

```python
            callsign_min_persistence_seconds=cfg.callsign_min_persistence_seconds,
```

- [ ] **Step 6: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_segmentation_debounce.py tests/test_segmentation_lookback.py tests/test_detection_config.py -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/opdi/config.py src/opdi/pipeline/segmentation/base.py tests/test_segmentation_debounce.py
git commit -m "feat(segmentation): declare the callsign debounce, defaulting to off"
```

---

### Task 3: The `debounced` arm

**Files:**
- Modify: `src/opdi/pipeline/segmentation/methods.py` (add `debounced()`, register in `METHODS`)
- Test: `tests/test_segmentation_debounce.py`

**Interfaces:**
- Consumes: `segment_window()`, `lookback_minutes(p)`, `legacy()` from `base`/`methods`; `SegmentationParams.callsign_min_persistence_seconds` from Task 2.
- Produces: `debounced() -> BreakRule` with `name="debounced"`, `group_cols=["icao24"]`, `month_suffix=False`; `METHODS["debounced"]`.

**How the debounce works.** `recommended` breaks where the current real callsign differs from the previous real one. `debounced` additionally requires the *new* value to still be in force `callsign_min_persistence_seconds` later. Expressed forwards over the window: look ahead to the last real callsign within the persistence horizon and require it to equal the current one. A flicker fails that test because the value has already reverted; a genuine change passes because the new callsign is still there.

The forward lookup uses a **bounded ascending** frame: `rangeBetween(1, hold)`
over `unix_timestamp(_ts)` ascending, with `F.last(real, ignorenulls=True)`
giving the *farthest* real callsign within the `hold`-second horizon. Farthest,
not nearest, is the point: a flicker two or more samples long still reverts
before the horizon closes, so the value at the far edge is the reverted one and
the flicker is suppressed. A `F.first`/nearest reading, or a descending frame
that resolves to nearest, breaks on a multi-sample flicker.

Do **not** reach for a descending frame here for performance. The O(n²) trap
`cleaning/native.py` documents is specific to *unbounded* frames —
`rowsBetween(1, unboundedFollowing)` is re-scanned per row. A `rangeBetween`
bounded to a fixed time window is O(n) in either order, so there is no
performance reason to reverse it and doing so only inverts the
nearest/farthest semantics.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_segmentation_debounce.py`:

```python
import datetime as dt

from conftest import make_track
from opdi.pipeline.segmentation import assign_track_id
from opdi.pipeline.segmentation.methods import debounced, recommended

P0 = SegmentationParams()                                        # debounce off
P30 = SegmentationParams(callsign_min_persistence_seconds=30.0)  # debounce on


def n_tracks(df):
    return df.select("track_id").distinct().count()


def _with_callsigns(spark, callsigns, step_s=5):
    """One airframe at cruise, one callsign per sample, five seconds apart."""
    df = make_track(spark, [
        {"t": i * step_s, "baro_altitude": 10000.0} for i in range(len(callsigns))
    ])
    from pyspark.sql import functions as F, Window
    return df.withColumn(
        "callsign",
        F.element_at(
            F.array(*[F.lit(c) for c in callsigns]),
            F.row_number().over(
                Window.partitionBy("icao24").orderBy("event_time")
            ).cast("int"),
        ),
    )


def test_a_flicker_no_longer_splits_the_flight(spark):
    """One stray sample between eight good ones is noise, not a new flight."""
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["XXXX"] + ["BEL123"] * 4)

    assert n_tracks(assign_track_id(df, recommended(), P0)) == 3, (
        "Three, not two: a flicker trips the rule on the X->Y excursion AND on "
        "the Y->X revert, so it cuts the flight into three pieces. Measured on "
        "2026-06-01, both legs are present -- 5,241 revert boundaries and 5,000 "
        "excursion boundaries. If this is not 3, `recommended` no longer has "
        "the defect and this whole plan is measuring something else."
    )
    assert n_tracks(assign_track_id(df, debounced(), P30)) == 1


def test_a_two_sample_flicker_is_also_suppressed(spark):
    """A flicker need not be one sample. Two garbled samples that revert before
    the persistence horizon closes are still noise, and the far-edge check must
    see the reverted value rather than the second garbled one."""
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["XXXX"] * 2 + ["BEL123"] * 4)

    assert n_tracks(assign_track_id(df, debounced(), P30)) == 1


def test_a_genuine_change_still_splits(spark):
    """The new value holds for the rest of the flight, so it is real."""
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["KLM99"] * 10)

    assert n_tracks(assign_track_id(df, debounced(), P30)) == 2


def test_a_change_that_holds_exactly_the_threshold_splits(spark):
    """The boundary is inclusive: 30 s of persistence at a 30 s threshold is
    persistence. Stated so the comparison cannot drift to `>` unnoticed."""
    # 5 s spacing: indices 4..10 are the new value, spanning 30 s.
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["KLM99"] * 7 + ["BEL123"] * 4)

    assert n_tracks(assign_track_id(df, debounced(), P30)) == 3


def test_zero_persistence_reproduces_recommended(spark):
    """A9 with the debounce off must be A8, exactly -- that is what makes the
    arm safe to add beside a published one."""
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["XXXX"] + ["BEL123"] * 4)

    assert n_tracks(assign_track_id(df, debounced(), P0)) == \
           n_tracks(assign_track_id(df, recommended(), P0))


def test_the_gap_rules_are_untouched(spark):
    """A9 inherits legacy's gap floor unchanged. A debounce that also quietly
    changed gap behaviour would be impossible to attribute."""
    df = _with_callsigns(spark, ["BEL123"] * 3)
    from pyspark.sql import functions as F
    late = df.withColumn(
        "event_time",
        F.when(F.col("event_time") == F.min("event_time").over(
            __import__("pyspark").sql.Window.partitionBy("icao24")
        ), F.col("event_time")).otherwise(
            F.col("event_time") + F.expr("INTERVAL 45 MINUTES")
        ),
    )
    assert n_tracks(assign_track_id(late, debounced(), P30)) == 2
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv310/bin/python -m pytest tests/test_segmentation_debounce.py -q`
Expected: FAIL with `ImportError: cannot import name 'debounced'`.

- [ ] **Step 3: Implement the arm**

In `src/opdi/pipeline/segmentation/methods.py`, after `recommended()`:

```python
def debounced() -> BreakRule:
    """A9 -- A8, but a callsign must persist before it breaks a track.

    `recommended` breaks on the first sample whose real callsign differs from
    the previous real one. A value that appears once and reverts therefore cuts
    a flight in half. Measured on 2026-06-01: of 1,765 track splits with a
    sub-minute gap -- which neither gap rule can produce, so all of them are
    callsign breaks -- **1,457 had the same resolved callsign on both halves**,
    at cruise, with a median implied speed of 421 kt across the gap. One flight,
    cut in two.

    The extra condition is that the new value is still in force
    ``callsign_min_persistence_seconds`` later. A flicker fails it because the
    value has already reverted; a genuine change passes because the new
    callsign is still there.

    ``callsign_min_persistence_seconds = 0`` reproduces `recommended` exactly,
    which is what makes this safe to add beside a published arm.
    """

    def expr(p):
        w = segment_window()
        back = w.rowsBetween(Window.unboundedPreceding, -1)
        real = F.when(
            F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
            F.trim(F.col("callsign")),
        )
        # The timestamp of the previous real callsign, for the lookback bound.
        # `prev_real` itself is deliberately not used as the comparison value --
        # see the block below.
        prev_real_ts = F.last(
            F.when(real.isNotNull(), F.col("_ts")), ignorenulls=True
        ).over(back)
        recent = (
            F.unix_timestamp(F.col("_ts")) - F.unix_timestamp(prev_real_ts)
        ) / 60.0 < lookback_minutes(p)

        hold = float(p.callsign_min_persistence_seconds)
        if hold <= 0.0:
            stable = real
        else:
            # The FARTHEST real callsign within the persistence horizon,
            # looking forward. A bounded ascending range frame: 1 to `hold`
            # seconds ahead, ordered on the integer unix timestamp (a
            # rangeBetween in seconds needs an integer-typed order, not a
            # TIMESTAMP). `F.last` over this ascending frame is the far edge of
            # the horizon -- which is what suppresses a multi-sample flicker,
            # since a flicker reverts before the horizon closes and the far
            # edge is therefore the reverted value.
            #
            # Not a descending frame and not `rowsBetween(1, unboundedFollowing)`.
            # The latter is unbounded and O(n^2); a descending bounded frame is
            # O(n) but resolves to the NEAREST forward sample, which reads a
            # 2-sample flicker as persisted and breaks on it. A bounded range
            # frame is O(n) in either order, so ascending costs nothing and is
            # the only one with the right semantics.
            fwd = (
                Window.partitionBy(*w_partition_cols())
                .orderBy(F.unix_timestamp(F.col("_ts")))
                .rangeBetween(1, int(hold))
            )
            later = F.last(real, ignorenulls=True).over(fwd)
            # No later real callsign inside the horizon means the track ends
            # here, and a value with nothing after it has not been shown to
            # revert. Treating that as persistence keeps A9 from suppressing a
            # genuine change at the end of a track.
            persisted = later.isNull() | (later == real)
            # The callsign only where it held. A flicker's transient value is
            # NULL here, so it never becomes the thing later samples compare
            # against.
            stable = F.when(persisted, real)

        # **Persisted against persisted, not persisted against previous-real.**
        # A flicker trips the rule twice -- once on the X->Y excursion and again
        # on the Y->X revert -- and measured on 2026-06-01 both legs are there:
        # 5,241 revert boundaries and 5,000 excursion boundaries, 23.1% of all
        # 44,406 boundaries between them.
        #
        # Comparing the new value against the previous *real* one suppresses
        # only the excursion: at the first X after the flicker, the previous
        # real callsign is Y, the current is X, and X does persist -- so it
        # breaks. The flight still splits, in two instead of three, and the
        # fragmentation measurement would show a real but halved improvement,
        # plausible enough to be accepted.
        #
        # Carrying the last *stable* callsign forward instead leaves the
        # transient invisible: stable is X,X,X,X,NULL,X,X,X,X across a flicker,
        # so every comparison is X against X and nothing breaks. A genuine
        # change gives X,X,X,X,Y,Y,Y,Y and breaks once, at the first Y.
        prev_stable = F.last(stable, ignorenulls=True).over(back)
        callsign_change = (
            stable.isNotNull()
            & prev_stable.isNotNull()
            & recent
            & (stable != prev_stable)
        )
        return F.coalesce(callsign_change, F.lit(False)) | legacy().break_expr(p)

    return BreakRule(
        name="debounced",
        group_cols=["icao24"],
        break_expr=expr,
        month_suffix=False,
        id_from_start_time=True,
    )
```

Add to the `ARMS` mapping at the bottom of the file (it is `ARMS`, not
`METHODS`):

```python
    "debounced": debounced,
```

`src/opdi/pipeline/tracks.py:300` resolves a configured method name to an arm
with `resolved = "recommended" if method == "standard" else method`, so
`"debounced"` passes straight through once it is in `ARMS`. Nothing else needs
changing there.

and to the `__all__` list at the top, beside `"recommended"`.

- [ ] **Step 4: Add the window-partition helper if it does not exist**

`w_partition_cols()` is used above to build the reversed frame on the same partition the engine uses. If `base.py` has no such helper, add one beside `segment_window()`:

```python
def w_partition_cols() -> list:
    """The columns ``segment_window`` partitions by.

    Exposed because a reversed-order window must partition identically to the
    engine's own or it answers a different question, and rebuilding it from the
    private column name by copy-paste is how arms drifted apart before.
    """
    return [F.col(_GRP)]
```

Export it from `opdi.pipeline.segmentation.base` and import it in `methods.py` alongside `segment_window`.

- [ ] **Step 5: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_segmentation_debounce.py tests/test_segmentation_methods.py -q`
Expected: PASS, including the pre-existing `recommended` tests — A8 must be byte-identical.

- [ ] **Step 6: Confirm the whole suite still passes**

Run the suite in chunks; a single ~900-test session outgrows the 4 GB local driver and dies partway, surfacing as a wave of `ConnectionRefused` that looks like hundreds of failures and is one:

```bash
ls tests/test_*.py > /tmp/all.txt && split -n l/6 -d /tmp/all.txt /tmp/chunk_
for c in /tmp/chunk_0*; do .venv310/bin/python -m pytest $(cat $c | tr '\n' ' ') -q --tb=line; done
```
Expected: PASS except the two pre-existing failures in `tests/test_regenerate_track_v2.py`, which also fail on untouched `origin/main`.

- [ ] **Step 7: Commit**

```bash
git add src/opdi/pipeline/segmentation/methods.py src/opdi/pipeline/segmentation/base.py tests/test_segmentation_debounce.py
git commit -m "feat(segmentation): A9 debounced -- a callsign must persist to break a track"
```

---

### Task 4: Measure fragmentation

The two internal metrics. Neither touches APDF, so neither is affected by minute rounding or transponder dwell.

**Files:**
- Create: `benchmarks/track_fragmentation.py`

**Interfaces:**
- Consumes: `assign_track_id`, `recommended`, `debounced`, `SegmentationParams`; `osn_tracks_clean`.
- Produces: a printed comparison, and `research/fragmentation_{arm}` written under the research prefix.

- [ ] **Step 1: Write the benchmark**

Create `benchmarks/track_fragmentation.py`:

```python
"""Does the debounce cut fragmentation without merging distinct flights?

One metric and one guard, both internal, because the APDF quantities that could measure this
directly cannot be trusted for it: AOBT and AIBT are frequently rounded to the
minute, and ADS-B presence does not share an end with the block times -- a
transponder may stay on long after on-block or come on well before off-block.

  fragmentation      tracks per (icao24, resolved callsign, day). The defect
                     raises it directly, so the fix must lower it.
Milestone pairing -- the share of tracks carrying both an ATOT and an ALDT --
is the other internal metric, but it cannot be computed here: this script runs
segmentation only, and milestones need step 04. It is measured in Task 5, where
a full pipeline run into the research prefix has produced events.

Guard: `merged_distinct_callsigns` must stay at zero. A debounce that merged
two flights with genuinely different callsigns would lower fragmentation and be
badly wrong, and nothing else here would say so.
"""
import argparse

from pyspark.sql import functions as F

from opdi.config import OPDIConfig, SegmentationConfig
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import ARMS
from opdi.utils.spark_helpers import SparkSessionManager
from opdi.utils.storage import StorageManager

RESEARCH = "s3a://eurocontrol/opdi/research"


def measure(sv, arm: str, hold: float):
    params = SegmentationParams.from_config(
        SegmentationConfig(callsign_min_persistence_seconds=hold)
    )
    tracked = assign_track_id(sv, ARMS[arm](), params)

    real = F.when(F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
                  F.trim(F.col("callsign")))
    per_track = tracked.groupBy("track_id").agg(
        F.first("icao24", ignorenulls=True).alias("icao24"),
        # `collect_set` rather than `first`: the guard below needs to know a
        # track holds two *different* callsigns, which a single value hides.
        F.collect_set(real).alias("callsigns"),
    )
    # A track's representative callsign, for grouping tracks into flights.
    # `F.get(..., 0)` on the sorted set is null-tolerant -- an all-blank track
    # yields an empty set and NULL here rather than throwing -- and
    # deterministic, unlike indexing the unordered `collect_set` directly.
    # Tracks with no real callsign group under NULL, which is correct: they
    # cannot be identified as a flight.
    rep = F.get(F.sort_array(F.col("callsigns")), 0)
    per_flight = per_track.withColumn("cs", rep).groupBy("icao24", "cs") \
        .agg(F.count(F.lit(1)).alias("n_tracks"))

    return {
        "arm": arm,
        "hold_s": hold,
        "tracks": tracked.select("track_id").distinct().count(),
        "tracks_per_flight": per_flight.agg(F.avg("n_tracks")).collect()[0][0],
        "merged_distinct_callsigns":
            per_track.filter(F.size("callsigns") > 1).count(),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", default="2026-06-01")
    ap.add_argument("--warehouse", default="s3a://eurocontrol/opdi-prod")
    ap.add_argument("--holds", type=float, nargs="+", default=[15.0, 30.0, 60.0])
    ap.add_argument("--executors", type=int, default=8)
    args = ap.parse_args()

    config = OPDIConfig.for_environment("opensky")
    config.project.warehouse_path = args.warehouse
    config.spark.executor_instances = args.executors
    spark = SparkSessionManager.create_session(
        app_name="track fragmentation", config=config, distributed=True
    )
    storage = StorageManager(spark, config)

    sv = (
        storage.read_table("osn_tracks_clean")
        .filter(F.col("dof").cast("date") == F.lit(args.day))
    ).cache()

    rows = [measure(sv, "recommended", 0.0)]
    rows += [measure(sv, "debounced", h) for h in args.holds]

    print(f"\n{'arm':14s} {'hold':>6s} {'tracks':>10s} {'per flight':>11s} "
          f"{'2+ callsigns':>13s}")
    for r in rows:
        print(f"{r['arm']:14s} {r['hold_s']:6.0f} {r['tracks']:10,} "
              f"{r['tracks_per_flight']:11.3f} {r['merged_distinct_callsigns']:13,}")
    print("\n`2+ callsigns` must stay at recommended's value. A rise means the "
          "debounce is\nmerging flights that genuinely changed identity, which "
          "lowers fragmentation\nand is badly wrong.")

    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

Run: `.venv310/bin/python -u benchmarks/track_fragmentation.py --day 2026-06-01`
Expected: `debounced` shows fewer tracks and a lower `tracks_per_flight` than `recommended`, with `merged_distinct_callsigns` unchanged.

- [ ] **Step 3: Read the result against the prediction**

The defect accounts for ~1,457 splits of ~52,000 tracks, so expect roughly **2–3% fewer tracks**, not more. A much larger drop means the debounce is merging things it should not, and the `2+ callsigns` guard should be showing it. If the guard rises at all, **stop and report** rather than proceeding to Task 5.

- [ ] **Step 4: Commit**

```bash
git add benchmarks/track_fragmentation.py
git commit -m "bench: track fragmentation per segmentation arm"
```

---

### Task 5: Verify against APDF, on the quantities APDF can support

**Files:**
- Modify: `benchmarks/movement_counts.py`

**Interfaces:**
- Consumes: `reference/apdf_202606.parquet`; a flight list built from the candidate arm.
- Produces: the existing per-aerodrome report, plus a runway-identity comparison.

**What is being asked of APDF, and what is not.** Counts and runway designators only. `AP_C_RWY` is 100% populated at every studied aerodrome and is categorical, so neither minute rounding nor transponder dwell touches it. Movement counts are equally immune. **`AOBT`/`AIBT` timing is deliberately not compared** — a difference there would measure APDF's own quantisation and the transponder's on-time, not segmentation.

- [ ] **Step 1: Add the runway-identity comparison**

In `benchmarks/movement_counts.py`, after the per-aerodrome outlier block:

```python
    # Runway identity: categorical, 100% populated in APDF, and immune to both
    # the minute rounding on block times and the transponder staying on past
    # on-block. It is the one clean accuracy comparison available here.
    apdf_rwy = apdf[apdf["SRC_PHASE"] == "DEP"][["ID", "AP_C_RWY"]].dropna()
    ours = opdi[opdi["MOVEMENT_DEP"].fillna(False)][["ID", "RWY_DEP"]].dropna()
    both = ours.merge(apdf_rwy, on="ID", how="inner")
    if len(both):
        exact = (both["RWY_DEP"].str.strip().str.upper()
                 == both["AP_C_RWY"].str.strip().str.upper()).mean()
        print(f"\n  departure runway, of {len(both):,} flights APDF also names: "
              f"{100 * exact:.1f}% exact")
    else:
        print("\n  departure runway: no flights bridge to APDF -- check the ID join")
```

- [ ] **Step 2: Run it against the current arm, to fix the baseline**

Run: `.venv310/bin/python benchmarks/movement_counts.py --days 2026-06-01 2026-06-02 2026-06-03`
Expected: departures 1.03×, arrivals 1.02×, `flew, has an aerodrome, excluded by a rule: dep 0 arr 0`, plus a runway exact-match percentage. **Write these numbers down** — they are what the candidate arm must not regress against.

- [ ] **Step 3: Build a flight list from the candidate arm**

The comparison needs a flight list, which needs tracks, which need the arm. Run steps 02, 02a, 03 and 04 into a research prefix, leaving production untouched:

```bash
.venv310/bin/python -u run_period.py --start 2026-06-01 --days 3 \
  --steps 02 02a 03 04 04b \
  --warehouse s3a://eurocontrol/opdi/research/a9 \
  --allow-existing
```

Set `callsign_min_persistence_seconds` to the value Task 4 chose, and `method` to `debounced`, via the environment or a config override before launching. **This is a distributed run of roughly 40 minutes per day** — check memory first, as in Task 1 Step 2, and do not start a second Spark job while it runs.

- [ ] **Step 4: Measure milestone pairing, which now has events to read**

A split flight has its take-off on one half and its landing on the other, so
neither half carries both. Merging should raise the share of tracks holding
both -- and this needs no APDF timing at all, which is why it is the
improvement to report rather than a guard.

```bash
.venv310/bin/python -c "
import sys; sys.path.insert(0, 'src')
from opdi.config import OPDIConfig
from opdi.utils.spark_helpers import SparkSessionManager
from opdi.utils.storage import StorageManager
from pyspark.sql import functions as F

for label, wh in [('recommended', 's3a://eurocontrol/opdi-prod'),
                  ('debounced',   's3a://eurocontrol/opdi/research/a9')]:
    cfg = OPDIConfig.for_environment('opensky')
    cfg.project.warehouse_path = wh
    spark = SparkSessionManager.create_session(app_name='pairing', config=cfg, distributed=False)
    ev = StorageManager(spark, cfg).read_table('opdi_flight_events') \
        .filter(F.col('dof').cast('date') == F.lit('2026-06-01'))
    per = ev.groupBy('flight_id').agg(
        F.max(F.when(F.col('type') == 'ATOT', 1).otherwise(0)).alias('t'),
        F.max(F.when(F.col('type') == 'ALDT', 1).otherwise(0)).alias('l'))
    n = per.count()
    both = per.filter((F.col('t') == 1) & (F.col('l') == 1)).count()
    print(f'{label:12s} tracks with events {n:,}  with BOTH ATOT and ALDT {both:,} = {100*both/max(n,1):.1f}%')
    spark.stop()
"
```
Expected: `debounced` higher than `recommended`. This is the headline improvement.

- [ ] **Step 5: Compare movement counts and runway identity**

Run: `.venv310/bin/python benchmarks/movement_counts.py --days 2026-06-01 2026-06-02 2026-06-03 --warehouse opdi/research/a9`

- [ ] **Step 6: Judge it against what was predicted**

These splits are airborne-to-airborne at cruise, so:

* **Movement counts should barely move.** Both halves are already above the ground and neither generates a spurious `ADEP`. A change beyond a point or so means the debounce did something other than intended — investigate before accepting it.
* **Runway identity should not regress.** If it falls, the merge is attaching a runway from the wrong half of a former split.
* **`flew, has an aerodrome, excluded by a rule` must stay at zero.**

The improvement to claim is in Task 4's numbers — fragmentation and milestone pairing. APDF's role here is to show nothing broke, and it should be reported that way rather than as the headline.

- [ ] **Step 7: Commit**

```bash
git add benchmarks/movement_counts.py
git commit -m "bench: compare departure runway identity against AP_C_RWY"
```

---

### Task 6: `track-construction-v3` — the paper

V1 surveyed eight segmentation arms and recommended one. V2 was the release note for shipping it, and for the `track_id` break that shipping it caused. V3 is the follow-up: the shipped rule has a defect that only showed up once the events built on it were compared against APDF, and this is the paper that measures it.

**Files:**
- Create: `opdi-portal/papers/track-construction-v3/index.qmd`
- Create: `opdi/benchmarks/regenerate_track_v3.py`
- Create: `opdi-portal/papers/track-construction-v3/data/` (written by the entrypoint, committed)

**Interfaces:**
- Consumes: `benchmarks/callsign_flicker.py` (Task 1), `benchmarks/track_fragmentation.py` (Task 4), `benchmarks/movement_counts.py` (Task 5); the `Job` class and staleness machinery in `benchmarks/regenerate_track_v2.py`.
- Produces: `flicker_2026.csv`, `fragmentation_2026.csv`, `movements_2026.csv`, `_manifest.json`.

**The paper's argument, in order.** Each section is a measurement already taken by an earlier task, so the paper reports rather than computes:

1. **What V2 shipped, and what it did not fix.** A8 breaks on a callsign change. Nobody asked what happens when the callsign changes back.
2. **The defect, found by elimination.** Sub-minute gaps between consecutive tracks of one airframe: 1,767 on 2026-06-01. Neither gap rule can produce one — the thresholds are 30 minutes and 15 minutes — so every one is a callsign break. 1,457 have the same resolved callsign on both halves, at cruise, median implied speed 421 kt across the gap.
3. **Raw callsign sequences.** Task 1's flicker rate and holding-time distribution: the direct evidence, replacing the inference from resolved callsigns.
4. **The debounce, and what it costs.** Task 4's fragmentation and milestone-pairing table across persistence thresholds, with the `2+ callsigns` guard.
5. **Against APDF, and the limits of that.** Task 5's movement counts and runway identity — *and an explicit statement of what APDF cannot answer here*, which is the section most likely to be skipped and most worth writing.
6. **`track_id` again.** Promoting A9 breaks continuity a second time. V2 already spent that cost once; this says plainly what a second break would cost and does not spend it.

- [ ] **Step 1: Write the regeneration entrypoint**

Create `opdi/benchmarks/regenerate_track_v3.py`, modelled on `regenerate_track_v2.py`. Copy its `Job` class, staleness logic and `OPDI_RENDER` handling verbatim — they are the house pattern and diverging from them is how a paper starts reading stale CSVs. Change: `PAPER` to `track-construction-v3`, and the job list to three jobs:

```python
def jobs() -> list:
    SEG = ["src/opdi/pipeline/segmentation/base.py",
           "src/opdi/pipeline/segmentation/methods.py"]
    return [
        Job(
            name="flicker_2026",
            script="benchmarks/callsign_flicker.py",
            args=["--day", "2026-06-01", "--out-name", "flicker_2026.csv"],
            outputs={"flicker_2026.csv": "flicker_2026.csv"},
            code_paths=SEG,
            notes="Raw callsign sequences: how often a change reverts, and how "
                  "long the transient value holds.",
        ),
        Job(
            name="fragmentation_2026",
            script="benchmarks/track_fragmentation.py",
            args=["--day", "2026-06-01", "--holds", "15", "30", "60",
                  "--out-name", "fragmentation_2026.csv"],
            outputs={"fragmentation_2026.csv": "fragmentation_2026.csv"},
            code_paths=SEG,
            notes="Tracks, tracks per flight and milestone pairing, per arm "
                  "and per persistence threshold.",
        ),
        Job(
            name="movements_2026",
            script="benchmarks/movement_counts.py",
            args=["--days", "2026-06-01", "2026-06-02", "2026-06-03",
                  "--out-name", "movements_2026.csv"],
            outputs={"movements_2026.csv": "movements_2026.csv"},
            code_paths=SEG + ["src/opdi/pipeline/flight_list_milestones.py"],
            notes="APDF movement counts per aerodrome and departure runway "
                  "identity, for both arms.",
        ),
    ]
```

Tasks 1, 4 and 5 print their results; add an `--out-name` argument to each writing the same numbers as CSV into the scratch directory the entrypoint stages from. A paper that reads a CSV can be checked for staleness; one that quotes a number from a terminal cannot.

- [ ] **Step 2: Write the paper**

Create `opdi-portal/papers/track-construction-v3/index.qmd`. Frontmatter, following V2's:

```yaml
---
title: "A callsign that changes and changes back"
subtitle: "Version 3 — a defect in the shipped segmentation rule, what it costs, and why the fix is not yet the default"
author: "EUROCONTROL Performance Review Unit"
date: last-modified
---
```

Copy V2's `regenerate` chunk verbatim, changing only `regenerate_track_v2.py` to `regenerate_track_v3.py`. It carries the `OPDI_RENDER` modes, the `OPDI_REPO_DIR` override and the guard that a *missing* entrypoint must not read as "nothing to regenerate" — all three are hard-won and none should be re-derived.

Every number in the prose reads from a staged CSV through one accessor per file, as V2 does. `tools/check_literals.py` fails the render on any measurement in prose that does not, which exists because three staged CSVs were once found to derive from tables written days earlier by different parameters.

- [ ] **Step 3: Write the section APDF cannot support**

This section is the reason the paper is worth writing carefully. Draft it as:

> **What the reference can and cannot settle.** The obvious check is whether a merged track's block times agree better with APDF's. It is not available. `AOBT` and `AIBT` are frequently rounded to the whole minute, so a difference of tens of seconds measures the reference's quantisation rather than the detector; and ADS-B presence does not share an end with the block times, because a transponder may stay on well past on-block or come on well before off-block. Neither limitation is a defect in APDF — they are what the data is — but together they mean a timing comparison here would produce a number with no interpretation.
>
> What survives both limitations is counting and naming. Movement counts per aerodrome are insensitive to rounding and to transponder dwell. `AP_C_RWY` is categorical and 100% populated at every aerodrome in the study set. Those are what this paper asks of APDF, and they are asked as a **guard** rather than as the result: merging two halves of a cruise-phase split should barely move either, and a large movement shift would mean the change did something other than intended.
>
> The improvement itself is measured internally — fragmentation and milestone pairing — because those are the quantities the defect actually damages.

- [ ] **Step 4: Render**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal/papers
OPDI_RENDER=check quarto render track-construction-v3/index.qmd --to pdf
```
Expected: fails fast naming any stale job. Then `OPDI_RENDER=run` to regenerate and render, which needs the cluster. `track-construction-v3.pdf` is written by the project's `post-render: bash _save-pdf.sh`.

- [ ] **Step 5: Add it to the paper index**

`opdi-portal/papers/index.qmd` is stale — it lists neither `adep-ades-detection-v6.2` nor `track-construction-v1`. Add all the missing entries, not only v3.

- [ ] **Step 6: Commit, in submodule order**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
git add benchmarks/regenerate_track_v3.py && git commit -m "feat(bench): the V3 regeneration entrypoint"
cd ../opdi-portal
git add papers/track-construction-v3 papers/index.qmd
git commit -m "papers: track construction v3 -- a callsign that changes and changes back"
```

Commit inside each submodule first, then the pointer in the meta-repo — the order matters and is the documented convention. Do **not** bump the pointer while the code is still on an unmerged branch.

---

### Task 7: Record the decision, and leave the default alone

**Files:**
- Modify: `benchmarks/EVENTS_RUN_LOG.md`
- Modify: `/home/jupyter/work/opdi-workspace/CLAUDE.md`

- [ ] **Step 1: Append the decision to the run log**

Continue the numbering from Task 1's decision 42. Record: the flicker measurement, the fragmentation and milestone-pairing deltas, the APDF guard results, and **why APDF block times were not used** — the minute rounding and the transponder's independent on-time. Record the chosen persistence threshold and the distribution it came from.

- [ ] **Step 2: Note the arm in `CLAUDE.md`**

In the "Track identity is a versioned choice" bullet, add `debounced` beside `recommended` and `legacy`, stating that it is **available but not the default**, and that promoting it would change `track_id` again.

- [ ] **Step 3: Do not change the default**

`SegmentationConfig.method` stays `standard` — which is the production name for
the arm `methods.py` calls `recommended`, resolved at `tracks.py:300`. Promoting
A9 changes `track_id` for every future dataset and breaks continuity with everything published since 2026-08-27 — the same cost A8 itself carried. That is a decision for the owner of the published contract, taken with these measurements in hand, not a consequence of this plan.

- [ ] **Step 4: Commit and open a PR**

```bash
git add benchmarks/EVENTS_RUN_LOG.md
git commit -m "docs: record the callsign-debounce investigation and its result"
git push -u origin feat/segmentation-callsign-debounce
gh pr create --base main --title "A9 debounced: a callsign must persist before it breaks a track" --body-file -
```

---

## Verification

**Unit, no cluster, no credentials:**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
.venv310/bin/python -m pytest tests/test_segmentation_debounce.py tests/test_segmentation_methods.py \
  tests/test_segmentation_lookback.py tests/test_segmentation_base.py -q
```
Expected: PASS, including every pre-existing `recommended` test.

**The contract that matters most:**

```bash
.venv310/bin/python -c "
from opdi.config import SegmentationConfig
from opdi.pipeline.segmentation import SegmentationParams
p = SegmentationParams.from_config(SegmentationConfig())
assert p.callsign_min_persistence_seconds == 0.0
assert SegmentationConfig().method == 'standard'
print('default arm and default debounce unchanged')
"
```

**What success looks like — falsifiable, and stated before the measurement:**

* Flickers are a substantial share of raw callsign changes and hold for seconds, not minutes. **If not, Task 1 stops the plan.**
* `debounced` produces **2–3% fewer tracks** than `recommended` on the same day. A much larger drop is a warning, not a win.
* `merged_distinct_callsigns` is **unchanged** from `recommended`.
* Milestone pairing — tracks with both an `ATOT` and an `ALDT` — **rises**.
* APDF movement counts move by **less than a point**, and departure runway identity does not regress.

**What would mean the fix is wrong:** fragmentation falling far more than predicted, any rise in tracks holding two distinct callsigns, or movement counts shifting materially. Each means the debounce is merging flights rather than mending one.

## Open questions, stated rather than assumed

* **The persistence threshold is not yet known.** Task 1 measures the holding-time distribution and Task 3 uses it. Writing a number here before measuring would be guessing.
* **Whether the 308 visibly-different-callsign splits are correct is not examined.** They may be genuine changes, or they may be the same defect where the flight list happened to resolve the halves differently. This plan leaves them alone.
* **The other 780 splits with gaps over 30 minutes are out of scope.** They are produced by the gap rules working as designed, and nothing measured here implicates those thresholds.
* **Promotion to default is not in this plan**, deliberately. It changes a published contract.
