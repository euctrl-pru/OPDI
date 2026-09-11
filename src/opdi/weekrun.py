"""Run the full OPDI pipeline over a short window, resumably.

``run_pipeline`` builds one Spark session, walks every step, and returns 1 on
the first exception. That is the right shape for a single month run by hand. It
is the wrong shape for a week of network-wide data, for three reasons this
module exists to fix.

**A failure must not cost the steps that already succeeded.** Steps 01 to 04
over seven days are hours of cluster time. ``run_pipeline`` keeps no record of
what finished, so a failure in step 04 means re-ingesting and re-tracking from
scratch. Here every step's completion is written to a state file as it happens,
and a re-invocation skips what is already done. Resuming is the default; the
only way to redo finished work is to ask for it.

**The driver heap must fit the container, not the preset.** ``opensky`` asks
for a 10 GB heap (``config.py``, ``SparkConfig.driver_memory``). The JupyterLab
pod this driver runs in is capped at 16 GiB *shared with everything else in it*,
and a 15-hour events run was SIGKILLed on its final rung for exactly this
reason -- with no crash dump, because the kernel kills the process rather than
the JVM throwing ``OutOfMemoryError``. ``safe_driver_memory`` sizes the heap
from the cgroup's real headroom at launch.

**The session must actually be distributed.** ``run_pipeline`` calls
``create_session`` without ``distributed=True``, so even under ``--env
opensky`` it never attaches to the Kubernetes master and silently runs
local-only. That is survivable for a day of one aerodrome and hopeless for a
week of the network.

A fourth choice is deliberate rather than corrective: **one Spark session per
step**. A long-lived ``SparkContext`` accumulates retained UI state -- stages,
jobs, tasks, SQL executions -- and that growth is what put the driver within
reach of the container cap in the first place. Recreating the session between
steps bounds it, and costs a session start (tens of seconds) against steps that
run for hours.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

#: Steps a data run performs, in execution order.
#:
#: Step 00 is **included**, because this runner's contract is a build that
#: reuses nothing. Reference tables are not carried over from an existing
#: warehouse; they are rebuilt into the target prefix alongside the data, so
#: every table a run reads is one that run produced. That is what makes the
#: output self-contained and what makes "which geometry produced this event"
#: answerable without a separate vintage record.
#:
#: The substeps inside 00 run in dependency order, not id order -- see
#: ``runner.REFERENCE_SUBSTEPS``. Skipping 00 is still possible
#: (``--steps 01 02 02a 03 04``), and then the reference tables must already
#: exist, which ``preflight`` checks.
WEEK_STEPS: Tuple[str, ...] = ("00", "01", "02", "02a", "03", "04")

#: Steps that run once for the whole window, not per day.
#:
#: 00 builds reference tables, which have no notion of a day. 01 ingests state
#: vectors by calendar date and has no track semantics at all -- and it must
#: cover the whole window *plus* the lookahead before any day is segmented,
#: because day D reads into D+1 to finish the flights that start on D. Running
#: ingestion per day would have each day depend on the next day's ingest, which
#: is an ordering knot with nothing to gain.
WINDOW_STEPS: Tuple[str, ...] = ("00", "01")

#: Steps that run once per day, on the tracks that day owns.
DAY_STEPS: Tuple[str, ...] = ("02", "02a", "03", "04")

#: What each reference substep produces.
#:
#: Step 00 is all-or-nothing without this: once the set is incomplete it runs,
#: and every substep runs with it -- so a build that died on the runway grid
#: re-downloads and rewrites OurAirports on the way back to it. Harmless, since
#: those writes replace rather than append, but it is minutes of work for an
#: unchanged result and it obscures which table the run is actually there to
#: build.
#:
#: 00c is listed for completeness. Its output is read by nothing today, so
#: skipping it changes no result either way.
REFERENCE_OUTPUTS = {
    "00a": ("h3_airport_detection_zones",),
    "00b": ("hexaero_airport_layouts",),
    "00c": ("opdi_h3_airspace_ref",),
    "00d": ("oa_airports", "oa_runways"),
    "00e": ("osn_aircraft_db",),
    "00f": ("h3_runway_zones",),
}


def reference_substeps_to_run(storage, force: bool = False) -> Dict[str, bool]:
    """Which reference substeps still have work to do.

    A substep is skipped when everything it produces is present *and
    non-empty*; see :func:`preflight` for why emptiness counts as absent. The
    check is per substep rather than per step, so a build that died partway
    resumes at the table it died on instead of starting again from OurAirports.
    """
    if force:
        return {k: True for k in REFERENCE_OUTPUTS}
    return {
        substep: bool(preflight(storage, tables))
        for substep, tables in REFERENCE_OUTPUTS.items()
    }


#: Extra days of state vectors to ingest past the window.
#:
#: The last day of the window is segmented over data running into the day
#: after it, so that a flight departing at 22:50 is complete. Without this the
#: final day would be segmented against an empty tail and every late departure
#: in it would be truncated -- which the truncation check would report, loudly,
#: but only after the run had spent the time.
INGEST_LOOKAHEAD_DAYS = 2

#: Where a run writes when the caller names nothing else.
#:
#: A separate prefix from the published ``s3a://eurocontrol/opdi``. Everything
#: -- reference tables included -- is written here, so a run cannot read a
#: table an earlier campaign left behind and cannot damage one either.
#: ``StorageManager`` resolves every table name against this single value, so
#: one knob redirects the entire pipeline.
DEFAULT_WAREHOUSE = "s3a://eurocontrol/opdi-prod"

#: Steps available but not run by default. Export and statistics are
#: publication actions; a test week should be inspected before anything is
#: published from it.
OPTIONAL_STEPS: Tuple[str, ...] = ("05", "06", "07", "08")

#: Steps that take no date range. Mirrors the same split in ``runner.py``; if
#: that list changes this one must change with it, which ``test_weekrun``
#: pins.
UNDATED_STEPS: frozenset = frozenset({"00", "07", "08"})

#: Reference tables the data steps read. A run that starts without these does
#: not fail fast -- it produces empty joins and reports them as coverage
#: failures, which is the most expensive kind of wrong answer because it looks
#: like a result. Sources, in order: 00d, 00f, 00e, 00b, 00f, 00a.
REQUIRED_REFERENCE_TABLES: Tuple[str, ...] = (
    "oa_airports",
    "oa_runways",
    "osn_aircraft_db",
    "hexaero_airport_layouts",
    "h3_runway_zones",
    "h3_airport_detection_zones",
)

#: Fraction of measured container headroom the JVM heap may claim.
#:
#: A JVM's resident size is its heap plus metaspace, thread stacks, code cache,
#: GC structures and direct byte buffers; Spark's driver adds Python for
#: PySpark and the off-heap buffers used by the S3A committer. Those are not
#: small and they are not counted in ``-Xmx``. Claiming much more than half of
#: what is free leaves the kernel to make up the difference, and the kernel
#: makes it up with SIGKILL.
HEAP_FRACTION_OF_HEADROOM = 0.55

#: Never propose a heap below this. Under it Spark spends its time in GC and a
#: run fails slowly instead of quickly, which is worse: a fast failure is a
#: message, a slow one is a wasted afternoon.
MIN_DRIVER_HEAP_GB = 2


def _cgroup_v2_limit() -> Optional[Tuple[int, int]]:
    """``(limit_bytes, anon_bytes)`` from cgroup v2, or ``None``.

    ``memory.max`` is the literal string ``max`` when no limit is set, which is
    the normal case on an unconstrained host and must not be read as zero.
    ``anon`` from ``memory.stat`` is used rather than ``memory.current``
    because the latter counts page cache, which is reclaimable and would make
    headroom look far smaller than it is.
    """
    try:
        raw = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        if raw == "max":
            return None
        limit = int(raw)
        anon = 0
        for line in Path("/sys/fs/cgroup/memory.stat").read_text().splitlines():
            if line.startswith("anon "):
                anon = int(line.split()[1])
                break
        return limit, anon
    except (OSError, ValueError):
        return None


def safe_driver_memory(
    requested: str,
    limit_bytes: Optional[int] = None,
    used_bytes: int = 0,
    fraction: float = HEAP_FRACTION_OF_HEADROOM,
) -> str:
    """The largest driver heap that fits, expressed as a Spark size string.

    Returns ``requested`` unchanged when there is no cgroup limit to respect,
    or when what was asked for already fits. Otherwise returns the reduced
    value. Never returns more than ``requested``: this exists to stop a driver
    being killed, not to hand it memory the operator did not ask for.

    Args:
        requested: Spark size string, e.g. ``"10G"``. Case-insensitive.
        limit_bytes: Container memory cap. ``None`` means unconstrained, and
            ``requested`` is returned as-is.
        used_bytes: Anonymous memory already resident in the container. The
            driver has to fit in what is left, not in the whole cap -- this
            pod also hosts the notebook kernel that launched it.
        fraction: Share of headroom the heap may claim. See
            ``HEAP_FRACTION_OF_HEADROOM``.

    Raises:
        ValueError: if ``requested`` is not a parseable size string, or if the
            headroom cannot fit ``MIN_DRIVER_HEAP_GB``. The second is a refusal
            rather than a silent shrink: a run that cannot be given a workable
            heap should say so at launch, not die four hours in.
    """
    want_gb = _parse_size_gb(requested)
    if limit_bytes is None:
        return requested

    headroom = limit_bytes - used_bytes
    allowed_gb = int((headroom * fraction) // (1024 ** 3))

    if allowed_gb < MIN_DRIVER_HEAP_GB:
        raise ValueError(
            f"only {headroom / 1024 ** 3:.1f} GiB free in a "
            f"{limit_bytes / 1024 ** 3:.1f} GiB container, which allows a heap "
            f"of {allowed_gb} GiB -- below the {MIN_DRIVER_HEAP_GB} GiB floor. "
            "Free memory in this container, or run the driver somewhere else."
        )
    return requested if want_gb <= allowed_gb else f"{allowed_gb}g"


def _parse_size_gb(size: str) -> int:
    """Spark size string to whole GiB. Accepts ``g``/``G``, ``m``/``M``."""
    s = size.strip().lower().rstrip("b")
    if s.endswith("g"):
        return int(float(s[:-1]))
    if s.endswith("m"):
        return int(float(s[:-1]) // 1024)
    raise ValueError(f"cannot parse memory size {size!r}; use e.g. '6g'")


def week_window(start: date, days: int = 7) -> Tuple[date, date]:
    """The half-open window ``[start, start + days)`` as inclusive dates.

    The pipeline's steps take ``start_date``/``end_date`` and treat them as an
    inclusive range, so seven days is ``start`` to ``start + 6``. Stating this
    once here keeps every caller from doing its own off-by-one, which is the
    kind of error that costs a day of cluster time and is invisible in the
    output.
    """
    if days < 1:
        raise ValueError(f"days must be >= 1, got {days}")
    return start, start + timedelta(days=days - 1)


@dataclass
class RunState:
    """What a run has finished, persisted so a re-invocation can skip it.

    Held as a plain JSON file rather than a table because it must be readable
    and editable when a run has gone wrong at three in the morning, and because
    it has to survive the cluster being unreachable -- which is exactly when it
    matters.
    """

    path: Path
    run_id: str = ""
    start_date: str = ""
    end_date: str = ""
    env: str = ""
    completed: Dict[str, dict] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "RunState":
        path = Path(path)
        if not path.exists():
            return cls(path=path)
        data = json.loads(path.read_text())
        return cls(
            path=path,
            run_id=data.get("run_id", ""),
            start_date=data.get("start_date", ""),
            end_date=data.get("end_date", ""),
            env=data.get("env", ""),
            completed=data.get("completed", {}),
        )

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_id": self.run_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "env": self.env,
            "completed": self.completed,
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        tmp.replace(self.path)

    def mark(self, step: str, seconds: float) -> None:
        """Record a step as done. Written immediately, not at the end.

        Batching these to the end would lose the whole record to the crash
        they exist to protect against.
        """
        self.completed[step] = {
            "finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seconds": round(seconds, 1),
        }
        self.save()

    def matches(self, env: str, start: date, end: date) -> bool:
        """Whether this state describes the window now being asked for.

        A state file from a different window must never be used to skip steps:
        the step names are identical, so nothing else would catch it, and the
        result would be a run that silently reports someone else's data.
        """
        if not self.completed:
            return True
        return (
            self.env == env
            and self.start_date == start.isoformat()
            and self.end_date == end.isoformat()
        )


def plan_steps(
    requested: Sequence[str],
    state: RunState,
    force: bool = False,
) -> List[str]:
    """Which of ``requested`` still need to run, in order.

    Order is ``requested``'s, not sorted: ``"02a"`` must follow ``"02"`` and
    sorting strings puts it there by luck rather than by rule. The luck holds
    today and would stop holding the moment a step named ``"02b"`` or ``"10"``
    appeared.
    """
    if force:
        return list(requested)
    return [s for s in requested if s not in state.completed]


def resolve_pbf_path(config) -> Optional[str]:
    """The OSM extract step 00b will read, by the same precedence runner uses.

    ``OPDI_OSM_PBF`` beats the configured value, matching
    ``runner._step_00b_airport_layouts`` so this check cannot disagree with
    what the step actually does. ``None`` means no extract is configured, and
    00b would fall back to the public Overpass API -- which resolves airport
    *names* through Nominatim, returns no data rather than an error when a name
    does not resolve, and refuses outright at this scale. Five of twenty
    aerodromes came back silently empty the last time that path built anything.
    """
    return os.environ.get("OPDI_OSM_PBF") or getattr(
        config.h3, "airport_layout_pbf_path", None
    )


def preflight_fresh_build(config) -> List[str]:
    """Problems that would make a from-scratch reference build fail or lie.

    Returns human-readable problems, empty when there are none. Checked before
    a session is created, because every one of these is knowable without the
    cluster and each costs hours if discovered later.
    """
    problems: List[str] = []
    pbf = resolve_pbf_path(config)
    if not pbf:
        problems.append(
            "no OSM extract configured for step 00b: set OPDI_OSM_PBF or "
            "h3.airport_layout_pbf_path. Without it the build falls back to "
            "the public Overpass API, which returns empty results silently."
        )
    elif not Path(pbf).exists():
        problems.append(f"the configured OSM extract does not exist: {pbf}")
    return problems


def preflight(storage, tables: Iterable[str] = REQUIRED_REFERENCE_TABLES) -> List[str]:
    """Names of required reference tables that are missing or unreadable.

    Returns a list rather than raising so the caller can report every problem
    at once. Being told about one missing table, fixing it, and then being told
    about the next is the behaviour this avoids.
    """
    missing: List[str] = []
    for name in tables:
        try:
            if storage.read_table(name).limit(1).count() == 0:
                # Present but empty counts as missing, and the distinction is
                # not academic. A Spark stage whose executors all die can still
                # commit: it writes a zero-row part file and a _SUCCESS marker,
                # so the table exists, reads cleanly, and contains nothing.
                # Observed exactly this on a from-scratch build -- step 00a
                # lost eight executors to OOM and left
                # h3_airport_detection_zones at 0 rows with _SUCCESS beside it.
                #
                # Testing readability alone would then call the reference set
                # complete, skip the rebuild, and run the whole pipeline
                # against an empty detection grid -- producing no ADEP or ADES
                # for any flight, with nothing anywhere reporting why.
                missing.append(name)
        except Exception:
            missing.append(name)
    return missing


def run_day_step(spark, config, step: str, day: date, kwargs: dict) -> None:
    """Run one pipeline step for the tracks that started on ``day``.

    Each processor has a ``process_day`` that reads the day's own tracks --
    whole, including the part of a late departure that runs past midnight --
    rather than a slice of a month. They are called directly rather than
    through ``runner.STEPS`` because those entry points take a date *range*
    and would quietly widen a day back into a month.
    """
    if step == "02":
        from opdi.pipeline.tracks import TrackProcessor

        TrackProcessor(spark, config).process_day(day, skip_if_processed=False)
    elif step == "02a":
        from opdi.cleaning.cleaner import TrackCleaner

        TrackCleaner(spark, config).process_day(day, skip_if_processed=False)
    elif step == "03":
        from opdi.pipeline.flights import FlightListProcessor

        FlightListProcessor(spark, config).process_day(
            day,
            airports_hex_path=kwargs.get("airports_hex_path"),
            skip_if_processed=False,
        )
    elif step == "04":
        from opdi.pipeline.events import FlightEventProcessor

        FlightEventProcessor(spark, config).process_day(day, skip_if_processed=False)
    else:
        raise ValueError(
            f"{step!r} is not a per-day step; expected one of {DAY_STEPS}."
        )


def day_state_key(step: str, day: date) -> str:
    """How a per-day step records completion.

    Namespaced by day, because the same step runs once per day and a bare step
    name would mark all seven done after the first.
    """
    return f"{step}@{day.isoformat()}"


def days_in(start: date, end: date) -> List[date]:
    """Every day from ``start`` to ``end`` inclusive."""
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


def _describe_memory(limit_bytes: Optional[int], used_bytes: int) -> str:
    if limit_bytes is None:
        return "container memory: unconstrained"
    return (
        f"container memory: {used_bytes / 1024 ** 3:.1f} GiB used of "
        f"{limit_bytes / 1024 ** 3:.1f} GiB "
        f"({(limit_bytes - used_bytes) / 1024 ** 3:.1f} GiB free)"
    )


def run_week(
    env: str = "opensky",
    start: Optional[date] = None,
    days: int = 7,
    steps: Sequence[str] = WEEK_STEPS,
    state_path: Optional[Path] = None,
    force: bool = False,
    driver_memory: Optional[str] = None,
    executors: Optional[int] = None,
    skip_preflight: bool = False,
    dry_run: bool = False,
    warehouse: str = DEFAULT_WAREHOUSE,
    allow_existing: bool = False,
) -> int:
    """Run the pipeline over a window of ``days``, resuming where it stopped.

    Returns a process exit code: 0 if every requested step is complete, 1
    otherwise. A step that fails stops the run -- steps are a dependency chain,
    and continuing past a failure would build step 04 on a step 03 that is not
    there.

    Args:
        env: Environment name. ``"opensky"`` additionally turns on distributed
            mode, which ``run_pipeline`` does not do for any environment.
        start: First day of the window, inclusive.
        days: Window length. Seven by default; the name of this module is not
            a constraint on it.
        steps: Step ids to run, in dependency order.
        state_path: Where completion is recorded. Defaults to
            ``logs/weekrun_{start}_{days}d.json``.
        force: Re-run steps already marked complete.
        driver_memory: Override the heap. Still capped to what the container
            can hold -- an override raises the ask, it does not waive physics.
        executors: Override ``spark.executor.instances``.
        skip_preflight: Do not check the reference tables. For when a
            reference table is known-missing and the run is deliberate.
        dry_run: Report the plan and exit without creating a session.
        warehouse: Prefix every table is resolved against. Defaults to a
            prefix separate from the published one, so a run neither reads
            nor damages published data.
        allow_existing: Proceed even though the warehouse prefix already holds
            objects. Off by default: a rebuild that silently merged with a
            previous attempt's output would produce a table nobody could date.
    """
    from opdi.config import OPDIConfig
    from opdi.runner import STEPS
    from opdi.utils.spark_helpers import SparkSessionManager
    from opdi.utils.storage import StorageManager

    if start is None:
        raise ValueError("start is required; a week run must name its window")

    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise ValueError(f"unknown step(s) {unknown}; valid: {sorted(STEPS)}")

    start_date, end_date = week_window(start, days)
    if state_path is None:
        state_path = Path("logs") / f"weekrun_{start_date.isoformat()}_{days}d.json"
    state = RunState.load(Path(state_path))

    if not state.matches(env, start_date, end_date):
        raise SystemExit(
            f"state file {state_path} describes {state.env} "
            f"{state.start_date}..{state.end_date}, but this run is {env} "
            f"{start_date}..{end_date}. Refusing to reuse it -- pass a "
            "different --state-path, or delete that file if it is stale."
        )

    state.env = env
    state.start_date = start_date.isoformat()
    state.end_date = end_date.isoformat()
    if not state.run_id:
        state.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    all_days = days_in(start_date, end_date)

    # A unit of work is a (step, day) pair for the per-day steps and a bare
    # step for the window-wide ones, so resuming lands on the day that failed
    # rather than restarting the week.
    units: List[Tuple[str, Optional[date]]] = []
    for step in steps:
        if step in DAY_STEPS:
            units.extend((step, d) for d in all_days)
        else:
            units.append((step, None))

    def _key(step, day):
        return day_state_key(step, day) if day is not None else step

    todo = [u for u in units if force or _key(*u) not in state.completed]
    done = [u for u in units if u not in todo]

    config = OPDIConfig.for_environment(env)
    # Before anything reads it. StorageManager resolves every table name
    # against this, so setting it here redirects reference tables, intermediate
    # tables and outputs together -- there is no second place a stale prefix
    # could survive.
    config.project.warehouse_path = warehouse
    mem = _cgroup_v2_limit()
    limit_bytes, used_bytes = mem if mem else (None, 0)
    heap = safe_driver_memory(
        driver_memory or config.spark.driver_memory, limit_bytes, used_bytes
    )

    print("=" * 72)
    print("OPDI week run")
    print("=" * 72)
    print(f"  environment : {env}")
    print(f"  window      : {start_date} .. {end_date}  ({days} days)")
    print(f"  steps       : {' '.join(steps)}")
    if done:
        print(f"  already done: {len(done)} unit(s)  (from {state_path})")
    print(f"  to run      : {len(todo)} unit(s) over {len(all_days)} day(s)")
    print(f"  day steps   : {' '.join(s for s in steps if s in DAY_STEPS)}")
    print(f"  {_describe_memory(limit_bytes, used_bytes)}")
    if heap != (driver_memory or config.spark.driver_memory):
        print(
            f"  driver heap : {heap}  "
            f"(reduced from {driver_memory or config.spark.driver_memory} to fit)"
        )
    else:
        print(f"  driver heap : {heap}")
    print(f"  warehouse   : {warehouse}")
    print(f"  state file  : {state_path}")
    print("=" * 72)

    building_reference = ("00", None) in todo
    if building_reference and not skip_preflight:
        problems = preflight_fresh_build(config)
        if problems:
            print("Preflight failed before any session was created:")
            for prob in problems:
                print(f"  - {prob}")
            return 1
        print(f"  preflight   : OSM extract {resolve_pbf_path(config)}")

    if not todo:
        print("Nothing to do. Pass --force to re-run completed steps.")
        return 0
    if dry_run:
        print("Dry run: no session created, nothing executed.")
        return 0

    config.spark.driver_memory = heap
    if executors is not None:
        config.spark.executor_instances = str(executors)

    distributed = bool(config.spark.k8s_master)

    kwargs = dict(
        airports_hex_path="data/airport_hex/zones_res7_processed.parquet",
        airports_hex_raw_path="data/airport_hex/zones_res7.parquet",
        export_dir="data/OPDI/v002",
        last_n_months=4,
        # Filled in below, once a session exists to inspect the warehouse
        # with. Substeps whose output is already built are skipped.
        run_reference={k: True for k in REFERENCE_OUTPUTS},
        adep_mode=None,
        ades_mode=None,
    )

    overall = time.time()
    for step, day in todo:
        scope = f"day {day}" if day is not None else f"{start_date} .. {end_date}"
        print(f"\n{'-' * 72}\n>>> step {step}  ({scope})\n{'-' * 72}")
        step_start = time.time()
        # A session per step. The driver's retained UI state grows with every
        # stage and SQL execution it has ever seen, and this run may span
        # hours; a fresh context per step keeps that growth inside one step
        # rather than accumulating across all of them.
        spark = SparkSessionManager.create_session(
            app_name=f"OPDI week {start_date}..{end_date} step {step}",
            config=config,
            distributed=distributed,
            extra_configs={
                "spark.ui.retainedStages": "50",
                "spark.ui.retainedJobs": "50",
                "spark.ui.retainedTasks": "1000",
                "spark.sql.ui.retainedExecutions": "50",
            },
        )
        try:
            # `(step, day)`, not `step`: todo holds pairs since the run
            # became day-by-day, and comparing the bare name to a pair is
            # always False -- which made this whole block dead code.
            if not skip_preflight and (step, day) == todo[0]:
                storage = StorageManager(spark, config)
                if building_reference:
                    # Reference tables already in the warehouse are reused, not
                    # rebuilt.
                    #
                    # A campaign runs one day at a time, and the state file is
                    # per-window -- so day 2 starts with no record that day 1
                    # built the reference data. Rebuilding it each day would
                    # repeat hours of geometry for an unchanged result, and
                    # before this the aircraft database was appended rather
                    # than replaced, so seven days left seven stacked copies.
                    #
                    # Completeness is the test, not emptiness: a *partial* set
                    # means an earlier build died midway, and finishing it is
                    # exactly what should happen.
                    missing = preflight(storage)
                    if not missing and not force:
                        print(
                            f"  reference   : already built in {warehouse}; "
                            "reusing it (pass --force to rebuild)"
                        )
                        state.mark(step, 0.0)
                        continue
                    if missing:
                        print(
                            f"  reference   : building {len(missing)} missing "
                            f"table(s): {', '.join(missing)}"
                        )
                        # Narrow step 00 to the substeps with work left. The
                        # set is incomplete, but that is no reason to
                        # re-download OurAirports on the way to the runway
                        # grid.
                        todo_subs = reference_substeps_to_run(storage, force=force)
                        kwargs["run_reference"] = todo_subs
                        skipped = [k for k, v in sorted(todo_subs.items()) if not v]
                        if skipped:
                            print(
                                f"  reference   : skipping {', '.join(skipped)} "
                                "(already built)"
                            )
                else:
                    missing = preflight(storage)
                    if missing:
                        print(
                            "Preflight failed. Missing or unreadable reference "
                            f"tables: {', '.join(missing)}.\n"
                            "Run step 00 to build them, or pass "
                            "--skip-preflight if their absence is intended."
                        )
                        return 1
                    print("  preflight   : all reference tables readable")

            if day is not None:
                run_day_step(spark, config, step, day, kwargs)
            else:
                fn = STEPS[step]
                if step in UNDATED_STEPS:
                    fn(spark, config, **kwargs)
                else:
                    # Ingestion has to cover the lookahead as well as the
                    # window: day D is segmented over data running into D+1,
                    # so the last day's tail must already be on disk.
                    ingest_end = end_date + timedelta(days=INGEST_LOOKAHEAD_DAYS)
                    fn(spark, config, start_date, ingest_end, **kwargs)
        except Exception as exc:  # noqa: BLE001 - reported, then surfaced
            elapsed = time.time() - step_start
            print(f"\nStep {step} ({scope}) FAILED after {elapsed:.1f}s: {exc}")
            import traceback

            traceback.print_exc()
            print(
                f"\nCompleted steps are recorded in {state_path}. "
                "Re-run the same command to resume from here."
            )
            return 1
        finally:
            spark.stop()

        elapsed = time.time() - step_start
        state.mark(_key(step, day), elapsed)
        print(f"<<< step {step} {scope} done in {elapsed:.1f}s")

    print(f"\nAll requested steps complete in {time.time() - overall:.1f}s.")
    print(f"State: {state_path}")
    return 0
