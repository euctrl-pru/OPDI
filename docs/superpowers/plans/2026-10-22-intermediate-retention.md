# Intermediate-table retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A period run deletes `osn_statevectors_v2`, `osn_tracks` and `osn_tracks_clean` partitions as soon as nothing still needs them — by default, and never before the day's outputs are verifiably on S3 — and the same rules clean up what earlier runs left behind in `opdi-world`.

**Architecture:** One pure planner, `opdi/retention.py::plan_retention`, takes partition listings (sets of ISO dates) and returns exactly which partitions may go. Nothing else decides. `periodrun` calls it after each day and once at the end of the run; a small CLI (`python -m opdi.retention`) calls it against an existing warehouse, dry-run unless `--apply`. Deletion goes through the existing `StorageManager.drop_partitions` (Hadoop FS delete, so no boto3 `Content-MD5` problem).

**Tech Stack:** Python 3.10 (`.venv310`), PySpark 4.1.1, pytest. Branch from `worldwide-default` (it contains `periodrun`'s `prune_tracks`, commit `4c5387d`, which `main` does not).

**Spec:** this conversation, 2026-10-20..22, summarised in "Who reads what" below.

## Who reads what (the facts the rules rest on)

Verified in code on `worldwide-default`:

| Table | Partition column | Read by | Which days |
|---|---|---|---|
| `osn_statevectors_v2` | `event_time_day` | step 02 only | day D's step 02 reads D-1, D, D+1 (`periodrun.run_day_step` "01", sliding window) |
| `osn_tracks` | `dof` | step 02a (cleaning); step 03/04 only if cleaning is off; **opdi-benchmarker build** (`opdi-benchmarker/src/opdi_benchmarker/build/cli.py:219`) | day D's steps read only `dof=D` (`filter_batch(df, day=day)`) |
| `osn_tracks_clean` | `dof` | steps 03 and 04 (`flights.tracks_table`, `events.feeds_from_clean_tracks`) | `dof=D` only |
| `opdi_flight_list` | `DOF` | — output | |
| `opdi_flight_events` | `dof` | — output | |
| `opdi_measurements` | `dof` | — output | |

The benchmarker **app** reads only its own cache (`<warehouse>/research/benchmarker/v1/trajectory`); only its **build** reads raw `osn_tracks`. A day that should be viewable but is not cached yet must keep raw `osn_tracks` — that is what `keep_tracks_on` names.

## Global Constraints

- Only these three tables may ever be dropped: `osn_statevectors_v2`, `osn_tracks`, `osn_tracks_clean`. Any other name passed to the deleter raises `ValueError`.
- A day's track partitions are dropped only if **all three** outputs have a partition for that day: `opdi_flight_list/DOF=D`, `opdi_flight_events/dof=D`, `opdi_measurements/dof=D`. Being in the state file is not enough; the listing is the proof.
- Raw `osn_tracks` for a day in `keep_tracks_on` is never dropped. `osn_tracks_clean` for it is (the benchmarker does not read it).
- A statevector day S is dropped only if every day D in {S-1, S, S+1} that lies inside the window has step 02 done, where "done" = `osn_tracks` has `dof=D` **or** D is output-complete. Statevector days outside [window start - 1, window end + 1] are never touched.
- Partition values that are not ISO dates (e.g. `__HIVE_DEFAULT_PARTITION__`) are ignored, never dropped.
- Retention is **on by default** in `run_period`; `--keep-intermediates` turns it off. `--prune-tracks` stays accepted as a no-op so existing commands parse.
- The CLI is dry-run unless `--apply` is given.

## Review Focus

1. **Run limited to early steps** (`--steps 01 02`): today `prune_tracks` drops the tracks right after 02 because 02 is the "last requested step" — data loss. Must keep them: outputs are missing. Test in Task 2.
2. **A day whose step 04 failed or never ran** (the real 2025-06-14 / 2026-06-14 state: flight list present, no events): its tracks must survive. Test in Task 1.
3. **Window edge statevectors**: the D+1 lookahead day after the last window day has no in-window owner except the last day; it must go once the last day's step 02 is done, and must not drop days outside the window. Test in Task 1.
4. **Prune failure** (S3 error mid-delete) must not fail a run whose outputs are already written — keep the existing behaviour of `prune_tracks_for_day`. Test in Task 2.
5. **Cleaning disabled** (`config.cleaning.enabled = False`): 03/04 read raw `osn_tracks`; the output-complete check still gates the drop, so nothing changes — test that keep days still keep raw. Covered by Task 1's keep test, which doesn't depend on cleaning.

---

### Task 1: The pure planner

**Files:**
- Create: `src/opdi/retention.py`
- Test: `tests/test_retention.py`

**Interfaces:**
- Produces:
  - `INTERMEDIATE_TABLES: Dict[str, str] = {"osn_statevectors_v2": "event_time_day", "osn_tracks": "dof", "osn_tracks_clean": "dof"}`
  - `OUTPUT_TABLES: Dict[str, str] = {"opdi_flight_list": "DOF", "opdi_flight_events": "dof", "opdi_measurements": "dof"}`
  - `@dataclass(frozen=True) class RetentionPlan: drops: Dict[str, List[str]]; kept: Dict[str, List[Tuple[str, str]]]` — `drops[table]` = sorted ISO dates to delete; `kept[table]` = `(date, reason)` for every present in-window partition not dropped. `def total(self) -> int`.
  - `def plan_retention(present: Mapping[str, Iterable[str]], window: Sequence[date], keep_tracks_on: Iterable[date] = (), *, statevectors: bool = True) -> RetentionPlan` — `present` maps every table in both dicts above to its partition values (missing key = empty). `statevectors=False` plans tracks only (used per day, Task 2).

- [ ] **Step 1: Write the failing tests** — in `tests/test_retention.py`, using a helper `_present(**tables)` that fills absent tables with `[]`:

```python
W = [date(2025, 6, d) for d in range(1, 15)]          # 2025-06-01 .. 06-14
done = [f"2025-06-{d:02d}" for d in range(1, 14)]     # outputs for 01..13

def test_complete_day_drops_both_track_tables():
    p = plan_retention(_present(osn_tracks=["2025-06-02"], osn_tracks_clean=["2025-06-02"],
        opdi_flight_list=done, opdi_flight_events=done, opdi_measurements=done), W)
    assert p.drops["osn_tracks"] == ["2025-06-02"]
    assert p.drops["osn_tracks_clean"] == ["2025-06-02"]

def test_day_without_events_keeps_its_tracks():          # Review Focus 2
    p = plan_retention(_present(osn_tracks=["2025-06-14"], osn_tracks_clean=["2025-06-14"],
        opdi_flight_list=done + ["2025-06-14"], opdi_flight_events=done, opdi_measurements=done), W)
    assert p.drops.get("osn_tracks", []) == [] and p.drops.get("osn_tracks_clean", []) == []
    assert ("2025-06-14", "outputs incomplete") in p.kept["osn_tracks"]

def test_keep_day_keeps_raw_but_drops_clean():
    p = plan_retention(_present(osn_tracks=["2025-06-01"], osn_tracks_clean=["2025-06-01"],
        opdi_flight_list=done, opdi_flight_events=done, opdi_measurements=done), W,
        keep_tracks_on=[date(2025, 6, 1)])
    assert p.drops.get("osn_tracks", []) == []
    assert p.drops["osn_tracks_clean"] == ["2025-06-01"]
    assert ("2025-06-01", "keep_tracks_on") in p.kept["osn_tracks"]

def test_statevectors_go_once_every_reader_has_tracks():  # Review Focus 3
    # sv 06-13..15 left by the sliding window; 13 complete, 14 has tracks.
    p = plan_retention(_present(osn_statevectors_v2=["2025-06-13", "2025-06-14", "2025-06-15"],
        osn_tracks=["2025-06-14"], opdi_flight_list=done, opdi_flight_events=done,
        opdi_measurements=done), W)
    assert p.drops["osn_statevectors_v2"] == ["2025-06-13", "2025-06-14", "2025-06-15"]

def test_statevectors_stay_while_a_reader_still_needs_step_02():
    p = plan_retention(_present(osn_statevectors_v2=["2025-06-13", "2025-06-14", "2025-06-15"],
        opdi_flight_list=done, opdi_flight_events=done, opdi_measurements=done), W)  # no tracks for 14
    # 06-14's step 02 reads sv 13, 14 and 15, and it has not run.
    assert p.drops.get("osn_statevectors_v2", []) == []
    assert ("2025-06-15", "step 02 pending for 2025-06-14") in p.kept["osn_statevectors_v2"]

def test_out_of_window_and_non_dates_are_never_touched():
    p = plan_retention(_present(osn_statevectors_v2=["2025-05-01"], osn_tracks=["2026-06-01"],
        opdi_flight_list=done + ["__HIVE_DEFAULT_PARTITION__"], opdi_flight_events=done,
        opdi_measurements=done), W)
    assert p.total() == 0

def test_statevectors_false_plans_tracks_only():
    p = plan_retention(_present(osn_statevectors_v2=["2025-06-01"], opdi_flight_list=done,
        opdi_flight_events=done, opdi_measurements=done), W, statevectors=False)
    assert p.drops.get("osn_statevectors_v2", []) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv310/bin/python -m pytest tests/test_retention.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'opdi.retention'`

- [ ] **Step 3: Implement `plan_retention` in `src/opdi/retention.py`**

Parse values with `date.fromisoformat` inside `try` and discard failures. `complete(D)` = D in all three `OUTPUT_TABLES` listings. `step02_done(D)` = D in `osn_tracks` or `complete(D)`. Apply the Global Constraints rules literally; `kept` reasons are exactly `"outputs incomplete"`, `"keep_tracks_on"`, `"step 02 pending for <ISO date>"` (the first in-window reader, in date order, whose step 02 is not done).

- [ ] **Step 4: Run to verify pass**

Run: `.venv310/bin/python -m pytest tests/test_retention.py -q`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/opdi/retention.py tests/test_retention.py
git commit -m "feat(retention): plan which intermediate partitions are no longer read"
```

### Task 2: Apply the plan from `run_period`, on by default

**Files:**
- Modify: `src/opdi/retention.py` (add listing + apply)
- Modify: `src/opdi/periodrun.py:545-578` (replace `TRACK_TABLES` / `prune_tracks_for_day`), `:786-800` (signature), `:955-962` (banner), `:1134-1155` (per-day hook) and the end of `run_period` (end-of-run hook)
- Modify: `run_period.py:103-115,150` (flags)
- Test: `tests/test_retention.py`, `tests/test_periodrun.py:979-1032`

**Interfaces:**
- Consumes: `plan_retention`, `RetentionPlan`, `INTERMEDIATE_TABLES`, `OUTPUT_TABLES` (Task 1).
- Produces:
  - `def list_present(storage) -> Dict[str, List[str]]` — `storage.list_partitions(t, col)` for every table in both dicts.
  - `def apply_retention(storage, plan: RetentionPlan) -> int` — calls `storage.drop_partitions(table, INTERMEDIATE_TABLES[table], values)` per table; raises `ValueError` for any table not in `INTERMEDIATE_TABLES` **before** deleting anything; catches and prints per-table exceptions (a failed drop must not fail the run); returns partitions removed.
  - `run_period(..., retain_intermediates: bool = False, keep_tracks_on: Sequence[date] = (), ...)` — `prune_tracks` parameter removed.

- [ ] **Step 1: Write the failing tests**

In `tests/test_retention.py`:

```python
def test_apply_refuses_an_output_table_before_deleting_anything():
    calls = []
    class S:  # noqa
        def drop_partitions(self, t, c, v): calls.append(t); return len(v)
    bad = RetentionPlan(drops={"osn_tracks": ["2025-06-02"], "opdi_flight_list": ["2025-06-02"]}, kept={})
    with pytest.raises(ValueError):
        apply_retention(S(), bad)
    assert calls == []

def test_apply_survives_a_failed_drop():                    # Review Focus 4
    class S:  # noqa
        def drop_partitions(self, t, c, v):
            if t == "osn_tracks": raise RuntimeError("s3 down")
            return len(v)
    plan = RetentionPlan(drops={"osn_tracks": ["2025-06-02"], "osn_tracks_clean": ["2025-06-02"]}, kept={})
    assert apply_retention(S(), plan) == 1
```

In `tests/test_periodrun.py`, replace `test_prune_tracks_drops_only_the_track_tables`, `test_prune_tracks_for_day_drops_that_day_in_both_tables` and `test_prune_failure_does_not_propagate` (now covered above) and change the signature test at `:1031` to:

```python
    assert sig.parameters["retain_intermediates"].default is False
    assert "prune_tracks" not in sig.parameters
```

and add, mirroring `test_the_wrapper_script_passes_worldwide_through`:

```python
def test_the_wrapper_script_retains_only_when_asked(monkeypatch):
    import run_period as script
    captured = _fake_run_period(monkeypatch, script)
    script.main(["--start", "2026-06-01"])
    assert captured["retain_intermediates"] is False
    captured.clear(); script.main(["--start", "2026-06-01", "--keep-intermediates"])
    assert captured["retain_intermediates"] is True
    captured.clear(); script.main(["--start", "2026-06-01", "--prune-tracks"])   # old flag still parses
    assert captured["retain_intermediates"] is False

def test_early_steps_only_never_drop_tracks():                # Review Focus 1
    from opdi.retention import plan_retention
    p = plan_retention({"osn_tracks": ["2026-06-01"], "osn_tracks_clean": ["2026-06-01"]},
                       [date(2026, 6, 1)], statevectors=False)
    assert p.total() == 0
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv310/bin/python -m pytest tests/test_retention.py tests/test_periodrun.py -q`
Expected: FAIL on the new names (`apply_retention`, `retain_intermediates`, `--keep-intermediates`).

- [ ] **Step 3: Implement**

- `retention.py`: `list_present`, `apply_retention` as specified.
- `periodrun.py`: delete `TRACK_TABLES` and `prune_tracks_for_day`. Per-day hook (where `prune_tracks_for_day` was called, same `step == last_day_step` condition, same local session pattern): unless `retain_intermediates`, `plan = plan_retention(list_present(storage), [day], keep_set, statevectors=False)`, print each `kept` reason, `apply_retention`. End-of-run hook, after the loop and before `"All requested steps complete"`: unless `retain_intermediates`, plan over `all_days` with `statevectors=True` and apply, printing the same summary. Banner line becomes `retention : drop intermediates once outputs exist` / `retention : off (--keep-intermediates)`, plus the keep list.
- `run_period.py`: add `--keep-intermediates` (`store_true`, help: "Keep osn_statevectors_v2, osn_tracks and osn_tracks_clean. By default each day's tracks are dropped once its flight list, events and measurements are on S3, and leftover state vectors at the end of the run."). `--prune-tracks` stays with `help=argparse.SUPPRESS` and is ignored. `--keep-tracks-on` help: "Days whose raw osn_tracks are kept for the opdi-benchmarker trajectory build."; pass `retain_intermediates=args.keep_intermediates`.

- [ ] **Step 4: Run to verify pass**

Run: `.venv310/bin/python -m pytest tests/test_retention.py tests/test_periodrun.py tests/test_coverage_config.py -q`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add src/opdi/retention.py src/opdi/periodrun.py run_period.py tests/test_retention.py tests/test_periodrun.py
git commit -m "feat(periodrun): drop intermediates by default, only once outputs are on S3"
```

### Task 3: CLI for an existing warehouse, and docs

**Files:**
- Modify: `src/opdi/retention.py` (add `main(argv=None) -> int` and `if __name__ == "__main__"`)
- Modify: `readme.md` (retention paragraph next to "Worldwide coverage"), `benchmarks/DATASETS.md` (one line under "The bucket has a quota": intermediates are now dropped automatically; `python -m opdi.retention` cleans older runs)
- Test: `tests/test_retention.py`

**Interfaces:**
- Consumes: `list_present`, `plan_retention`, `apply_retention`.
- Produces: `python -m opdi.retention --start YYYY-MM-DD --days N [--keep-tracks-on D ...] [--warehouse PREFIX] [--europe] [--apply]`. Without `--warehouse` it uses `OPDIConfig.for_environment("opensky", worldwide=not europe).project.warehouse_path`. Prints, per table, every drop and every kept `(date, reason)`, then `would remove N partition(s)` (dry run) or `removed N partition(s)`. Spark session local (`distributed=False`): listing and FS deletes need no executors.

- [ ] **Step 1: Write the failing test**

```python
def test_cli_is_a_dry_run_unless_apply(monkeypatch, capsys):
    import opdi.retention as r
    applied = []
    monkeypatch.setattr(r, "_storage_for", lambda warehouse, europe: object())
    monkeypatch.setattr(r, "list_present", lambda s: {"osn_tracks_clean": ["2026-06-01"],
        "opdi_flight_list": ["2026-06-01"], "opdi_flight_events": ["2026-06-01"],
        "opdi_measurements": ["2026-06-01"]})
    monkeypatch.setattr(r, "apply_retention", lambda s, p: applied.append(p) or p.total())
    assert r.main(["--start", "2026-06-01", "--days", "1"]) == 0
    assert applied == [] and "would remove 1 partition(s)" in capsys.readouterr().out
    assert r.main(["--start", "2026-06-01", "--days", "1", "--apply"]) == 0
    assert len(applied) == 1
```

`_storage_for(warehouse: Optional[str], europe: bool) -> StorageManager` is the one seam that builds the session; it is private to the module.

- [ ] **Step 2: Run to verify failure**

Run: `.venv310/bin/python -m pytest tests/test_retention.py::test_cli_is_a_dry_run_unless_apply -q`
Expected: FAIL, `AttributeError: ... has no attribute 'main'`

- [ ] **Step 3: Implement `main` and `_storage_for`; write the two doc paragraphs**

- [ ] **Step 4: Run the whole suite**

Run: `OPDI_PORTAL=../opdi-portal .venv310/bin/python -m pytest tests/ -q -p no:cacheprovider`
Expected: the only failures are the 7 that fail on the base branch too (`test_events_labelling::test_events_imports_the_helper_rather_than_owning_a_copy`, `test_flight_detection::test_border_wiring_threads_the_configured_box_through_both_call_sites`, `test_flight_list_v7_redirect::test_track_table_is_redirected_and_a_repeat_call_does_not_rejoin`, `test_h3_airport_layouts::test_process_airport_writes_nothing`, `test_h3_airport_layouts::test_many_airports_are_written_once_and_together`, and the two in `test_regenerate_track_v2`).

- [ ] **Step 5: Commit**

```bash
git add src/opdi/retention.py tests/test_retention.py readme.md benchmarks/DATASETS.md
git commit -m "feat(retention): python -m opdi.retention cleans an existing warehouse, dry-run by default"
```

### Task 4: Clean up `opdi-world` (operational, no code)

State on 2026-10-22: `osn_statevectors_v2` 2025-06-13..15 (25.2 GB); `osn_tracks` and `osn_tracks_clean` for 2025-06-01, 2025-06-14, 2026-06-01, 2026-06-14, 2026-09-15 (93.8 GB). Events/measurements are missing for 2025-06-14 and 2026-06-14. The benchmarker trajectory cache already holds 2026-06-01 and 2026-09-15.

- [ ] **Step 1: Dry-run all three windows**

```bash
for w in "2025-06-01 14" "2026-06-01 14" "2026-09-15 1"; do set -- $w
  .venv310/bin/python -m opdi.retention --start $1 --days $2 --keep-tracks-on 2025-06-01
done
```

Expected, and stop if it differs: drops `osn_statevectors_v2` 2025-06-13/14/15; `osn_tracks_clean` 2025-06-01, 2026-06-01, 2026-09-15; `osn_tracks` 2026-06-01, 2026-09-15. Kept: `osn_tracks` 2025-06-01 (`keep_tracks_on`); both track tables for 2025-06-14 and 2026-06-14 (`outputs incomplete`). About **73.9 GB** freed (state vectors 25.2, clean tracks 28.2, raw tracks 20.5).

- [ ] **Step 2: Show the dry-run output to the user and get an explicit yes**

- [ ] **Step 3: Same three commands with `--apply`** (if Claude Code's auto-mode blocks it, give the user the command to run with `!`)

- [ ] **Step 4: Re-inventory the bucket**, confirm the kept partitions are still there and the outputs (`opdi_flight_list`, `opdi_flight_events`, `opdi_measurements`) are byte-for-byte the same size as before.
