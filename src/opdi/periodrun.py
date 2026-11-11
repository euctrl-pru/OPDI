"""Run the pipeline over a period of days, resuming where it stopped.

``python -m opdi.periodrun --start 2026-06-14 --days 7`` processes seven days,
one day at a time: each day goes through all its steps before the next day
starts. The steps of release v2.0.0 are 00 (reference tables, once), 01
(ingestion of the position reports), 02 (tracks), 02a (cleaning), 03 (flight
list), 04 (events) and 04b (milestones on the flight list); 09 (publishing)
is added on request.

Three properties make long runs practical:

* **Resumable.** Every finished unit (a step, or a step for one day) is
  recorded in a state file at once, and running the same command again skips
  it. ``--force`` runs finished units again.
* **Driver memory that fits.** The Spark driver's memory is set from the
  memory actually free in the container when the run starts
  (:func:`safe_driver_memory`), because a container that runs out of memory
  stops the process without an error message.
* **One Spark session per step,** distributed on Kubernetes in the
  ``opensky`` environment. A long-lived session accumulates history and
  memory; a new one per step costs seconds against steps that run for hours.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence

#: Steps a run performs, in order.
#:
#: Step 00 is included: the reference tables are built in the warehouse the
#: run writes to (and reused when they are complete), so every table a run
#: reads was produced by it. Its substeps run in dependency order (see
#: ``runner.REFERENCE_SUBSTEPS``). Leaving out 00 (``--steps 01 02 02a 03
#: 04``) requires the reference tables to exist already, which ``preflight``
#: checks.
PERIOD_STEPS: tuple[str, ...] = ("00", "01", "01b", "02", "02a", "03", "04", "04b")

#: Steps that run once for the whole period, not per day: 00 builds the
#: reference tables, which have no day, and 01 fetches the position reports
#: by calendar day and is run per day inside :func:`run_day_step`.
WINDOW_STEPS: tuple[str, ...] = ("00",)

#: Steps that run once per day, on the tracks that day owns.
DAY_STEPS: tuple[str, ...] = ("01", "01b", "02", "02a", "03", "04", "04b")

#: The tables each reference substep produces. A substep whose tables all
#: exist and are not empty is skipped, so a build that stopped part way
#: resumes at the table it stopped on. 00c (airspaces) is read only by the
#: FIR crossings.
REFERENCE_OUTPUTS = {
    "00a": ("h3_airport_detection_zones",),
    "00b": ("hexaero_airport_layouts",),
    "00c": ("opdi_h3_airspace_ref",),
    "00d": ("oa_airports", "oa_runways"),
    "00e": ("osn_aircraft_db",),
    "00f": ("h3_runway_zones",),
}


def reference_substeps_to_run(storage, force: bool = False) -> dict[str, bool]:
    """The reference substeps that still have work to do.

    A substep is skipped when every table it produces is present and not
    empty (see :func:`preflight` for why an empty table counts as missing).
    """
    if force:
        return {k: True for k in REFERENCE_OUTPUTS}
    return {
        substep: bool(preflight(storage, tables)) for substep, tables in REFERENCE_OUTPUTS.items()
    }


#: Extra days of position reports fetched after the period, so that the last
#: day's late departures are complete.
INGEST_LOOKAHEAD_DAYS = 2

#: The warehouse a run uses when neither the caller nor the
#: ``OPDI_WAREHOUSE`` environment variable names one. The tables of a release
#: go into its own folder inside it, ``<warehouse>/v2.0.0``, so a run never
#: touches the tables of another release or anything else in the bucket.
DEFAULT_WAREHOUSE = "s3a://eurocontrol/opdi"

#: Marks a ``warehouse`` argument the caller did not give, as distinct from
#: any string the caller did give.
_UNSET = object()


def _resolve_warehouse(warehouse, configured: str) -> str:
    """The warehouse a period run uses.

    Args:
        warehouse: What the caller passed, or ``_UNSET`` when nothing was
            passed.
        configured: The warehouse of the environment's configuration: the
            ``OPDI_WAREHOUSE`` environment variable if set, else
            :data:`DEFAULT_WAREHOUSE`.

    Returns:
        ``warehouse`` when the caller gave one, else ``configured``. The
        coverage never changes the result.
    """
    if warehouse is not _UNSET and warehouse:
        return warehouse
    return configured


#: Steps available but not run by default. Publishing is a release action; a
#: test period should be inspected before anything is published from it.
OPTIONAL_STEPS: tuple[str, ...] = ("09",)

#: Optional steps that run once per day, like :data:`DAY_STEPS`. Step 09
#: publishes the day's flight list and flight events files; put it last.
OPTIONAL_DAY_STEPS: tuple[str, ...] = ("09",)


def is_day_step(step: str) -> bool:
    """Whether ``step`` runs once per day rather than once for the whole window."""
    return step in DAY_STEPS or step in OPTIONAL_DAY_STEPS


#: Steps that take no date range. The same list as in ``runner.py``;
#: ``tests/test_periodrun.py`` checks that the two agree.
UNDATED_STEPS: frozenset = frozenset({"00"})

#: Reference tables the data steps read. A run without them does not fail;
#: it produces empty joins that look like missing coverage, so they are
#: checked before the run. Produced by 00d, 00f, 00e, 00b, 00f and 00a.
REQUIRED_REFERENCE_TABLES: tuple[str, ...] = (
    "oa_airports",
    "oa_runways",
    "osn_aircraft_db",
    "hexaero_airport_layouts",
    "h3_runway_zones",
    "h3_airport_detection_zones",
)

#: Share of the container's free memory the driver's Java heap may take.
#: The process also needs memory outside the heap (metaspace, thread stacks,
#: Python, buffers), and a container that runs out of memory stops it without
#: warning.
HEAP_FRACTION_OF_HEADROOM = 0.55

#: Smallest driver heap, in GB, worth starting with; below it Spark spends
#: its time collecting garbage and fails slowly.
MIN_DRIVER_HEAP_GB = 2


def _cgroup_v2_limit() -> tuple[int, int] | None:
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
    limit_bytes: int | None = None,
    used_bytes: int = 0,
    fraction: float = HEAP_FRACTION_OF_HEADROOM,
) -> str:
    """The largest driver heap that fits, as a Spark size string.

    Returns ``requested`` unchanged when there is no container limit or when
    it already fits; otherwise a smaller value. Never more than
    ``requested``.

    Args:
        requested: Spark size string, for example ``"10G"``; case does not
            matter.
        limit_bytes: Memory limit of the container. ``None`` means no limit,
            and ``requested`` is returned as is.
        used_bytes: Memory already in use in the container (the driver must
            fit in what is left).
        fraction: Share of the free memory the heap may take; see
            :data:`HEAP_FRACTION_OF_HEADROOM`.

    Raises:
        ValueError: If ``requested`` cannot be read, or if the free memory
            cannot hold :data:`MIN_DRIVER_HEAP_GB`; the run is then refused
            at the start rather than failing hours later.
    """
    want_gb = _parse_size_gb(requested)
    if limit_bytes is None:
        return requested

    headroom = limit_bytes - used_bytes
    allowed_gb = int((headroom * fraction) // (1024**3))

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


def period_window(start: date, days: int = 7) -> tuple[date, date]:
    """The dates ``start`` to ``start + days - 1``, both included.

    The steps take ``start_date`` and ``end_date`` as an inclusive range, so
    seven days are ``start`` to ``start + 6``.
    """
    if days < 1:
        raise ValueError(f"days must be >= 1, got {days}")
    return start, start + timedelta(days=days - 1)


@dataclass
class RunState:
    """What a run has finished, kept so that running it again skips it.

    A plain JSON file, so it can be read and edited by hand, and read without
    the cluster.
    """

    path: Path
    run_id: str = ""
    start_date: str = ""
    end_date: str = ""
    env: str = ""
    coverage: str = ""
    """``config.coverage.label`` when this state was written. An empty value
    reads as ``"europe"``, the coverage of state files that do not record
    one."""
    completed: dict[str, dict] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> RunState:
        """Read the state file at ``path``; an empty state when it is absent."""
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
            coverage=data.get("coverage", ""),
            completed=data.get("completed", {}),
        )

    def save(self) -> None:
        """Write the state file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_id": self.run_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "env": self.env,
            "coverage": self.coverage,
            "completed": self.completed,
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        tmp.replace(self.path)

    def mark(self, step: str, seconds: float) -> None:
        """Record a step as done, and write the file at once."""
        self.completed[step] = {
            "finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seconds": round(seconds, 1),
        }
        self.save()

    def matches(self, env: str, start: date, end: date, coverage: str = "europe") -> bool:
        """Whether this state belongs to the period and coverage now asked for.

        A state file of another period or coverage must never be used to skip
        steps: the step names are the same, so nothing else would notice. An
        empty ``coverage`` (a file that does not record one) reads as
        ``"europe"``.
        """
        if not self.completed:
            return True
        return (
            self.env == env
            and self.start_date == start.isoformat()
            and self.end_date == end.isoformat()
            and (self.coverage or "europe") == coverage
        )


def plan_steps(
    requested: Sequence[str],
    state: RunState,
    force: bool = False,
) -> list[str]:
    """The units of ``requested`` that still need to run, in their order.

    The order of ``requested`` is kept rather than sorted, because "02a" must
    follow "02" by rule, not by alphabetical luck.
    """
    if force:
        return list(requested)
    return [s for s in requested if s not in state.completed]


def resolve_pbf_path(config) -> str | None:
    """The OpenStreetMap extract step 00b will read.

    ``OPDI_OSM_PBF`` takes precedence over the configured value, as in
    ``runner._step_00b_airport_layouts``. ``None`` means no extract: step 00b
    would then query the public Overpass service, which cannot serve many
    airports and returns nothing, without an error, for an airport whose
    name it cannot resolve.
    """
    return os.environ.get("OPDI_OSM_PBF") or getattr(config.h3, "airport_layout_pbf_path", None)


#: Degrees added around ``EUROPE_BBOX`` before deciding that an extract holds
#: airports outside Europe (see ``preflight_fresh_build``). A European extract
#: also holds airports somewhat outside the box (Svalbard, up to 8.7 degrees
#: north of it; the Azores), which must not count as worldwide coverage.
_GREATER_EUROPE_MARGIN_DEG = 10.0


def preflight_fresh_build(config) -> list[str]:
    """Problems that would make a reference build from scratch fail or lack data.

    Checked before a Spark session starts, because each can be known without
    the cluster and would otherwise cost hours.

    Returns:
        Readable descriptions of the problems; empty when there are none.
    """
    problems: list[str] = []
    pbf = resolve_pbf_path(config)
    if not pbf:
        problems.append(
            "no OSM extract configured for step 00b: set OPDI_OSM_PBF or "
            "h3.airport_layout_pbf_path. Without it the build falls back to "
            "the public Overpass API, which returns empty results silently."
        )
    elif not Path(pbf).exists():
        problems.append(f"the configured OSM extract does not exist: {pbf}")
    elif getattr(getattr(config, "coverage", None), "is_worldwide", False):
        # A worldwide run with a European extract would build worldwide
        # detection zones and runway grid (from OurAirports) but European
        # layouts only, and every airport outside Europe would look as if
        # OpenStreetMap had no map of it. One airport outside the widened box
        # is enough to show the extract is worldwide, so the scan stops at the
        # first. The box is widened by _GREATER_EUROPE_MARGIN_DEG because a
        # European extract holds a few airports just outside the strict box;
        # a worldwide extract's nearest airport outside Europe is much
        # further away.
        from opdi.coverage import EUROPE_BBOX
        from opdi.reference.pbf_source import pbf_has_aerodrome_outside_bbox

        min_lon, min_lat, max_lon, max_lat = EUROPE_BBOX
        d = _GREATER_EUROPE_MARGIN_DEG
        greater_europe = (min_lon - d, min_lat - d, max_lon + d, max_lat + d)
        if not pbf_has_aerodrome_outside_bbox(pbf, greater_europe):
            problems.append(
                f"worldwide coverage is configured, but the OSM extract at "
                f"{pbf} has no aerodrome outside the European box -- it "
                "looks like a European-only extract. A worldwide run built "
                "from it would silently have no ground layouts (stands, "
                "taxiways, aprons) for every aerodrome outside Europe."
            )
    return problems


def preflight(storage, tables: Iterable[str] = REQUIRED_REFERENCE_TABLES) -> list[str]:
    """Names of the required reference tables that are missing, empty or unreadable.

    Returns every problem at once rather than raising at the first. A
    warehouse built with a different coverage is reported first. ``storage``
    may be a test stand-in without ``check_coverage``.
    """
    problems: list[str] = []
    check_coverage = getattr(storage, "check_coverage", None)
    if check_coverage is not None:
        coverage_problem = check_coverage()
        if coverage_problem is not None:
            problems.append(coverage_problem)

    missing: list[str] = []
    for name in tables:
        try:
            if storage.read_table(name).limit(1).count() == 0:
                # Present but empty counts as missing: a Spark job whose
                # executors all fail can still write an empty file and a
                # _SUCCESS marker. An empty detection zone table would give no
                # flight an airport, with nothing reporting why.
                missing.append(name)
        except Exception:
            missing.append(name)
    return problems + missing


#: Tables that only feed later steps and can be removed once a day's outputs
#: are written. The tracks are most of a day's working data (about 10 GB
#: each for ``osn_tracks`` and ``osn_tracks_clean`` worldwide), and steps 03
#: and 04 read them, so they are removed after those steps, not skipped.
TRACK_TABLES: tuple[str, ...] = ("osn_tracks", "osn_tracks_clean")


def prune_tracks_for_day(storage, day: date) -> int:
    """Remove the track day folders of ``day``.

    Only once every step that reads the tracks (03 and 04) has finished for
    the day; the caller makes sure of that. A removed day can no longer have
    its events computed again without fetching its reports again.

    Returns:
        The number of day folders removed.
    """
    removed = 0
    for table in TRACK_TABLES:
        try:
            removed += storage.drop_partitions(table, "dof", [day.isoformat()])
        except Exception as exc:  # noqa: BLE001 - pruning must not fail a run
            print(f"  prune: {table} dof={day} not dropped ({exc})")
    return removed


#: Default directory for the per-step ETL progress logs.
DEFAULT_LOG_DIR = "OPDI_live/logs"


def _log_dir(kwargs: dict) -> str:
    """The local folder of this run's progress logs.

    A run that writes its tables elsewhere must keep its logs elsewhere as
    well: the logs are on the local disk, and one of them decides whether
    the endpoint candidates are built. With shared logs, a run in another
    warehouse would skip the build and then fail to read a table it never
    wrote. The default is the shared folder.
    """
    return kwargs.get("log_dir") or DEFAULT_LOG_DIR


#: Steps whose processors keep progress logs.
STEPS_WITH_LOGS = ("02", "02a", "03", "04")


def _shared_log_dir(storage) -> str:
    """A local folder that mirrors the progress logs kept in the warehouse.

    One folder per warehouse, so two warehouses never share logs on one
    machine.
    """
    import hashlib

    digest = hashlib.sha256(storage.state_path.encode("utf-8")).hexdigest()[:12]
    return os.path.join("OPDI_live", "state", digest, "logs")


def run_day_step(spark, config, step: str, day: date, kwargs: dict) -> None:
    """Run one pipeline step for the tracks that started on ``day``.

    Each processor's ``process_day`` reads the day's own tracks, whole,
    including the part of a late departure after midnight. They are called
    directly, not through ``runner.STEPS``, whose entry points take a date
    range.

    In the ``opensky`` environment the steps' progress logs are kept in the
    warehouse, under ``<release root>/_state/logs``: they are copied to a
    local folder before the step and back after it succeeds, so the next
    step can run on another machine. A ``log_dir`` in ``kwargs`` turns this
    off and uses that local folder alone.

    Args:
        spark: Active Spark session.
        config: OPDI configuration.
        step: Step id, for example ``"02"``.
        day: The day.
        kwargs: Extra arguments for the step (``log_dir``).
    """
    if step in STEPS_WITH_LOGS and not kwargs.get("log_dir"):
        from opdi.utils.storage import StorageManager

        storage = StorageManager(spark, config)
        if storage.use_s3:
            local = _shared_log_dir(storage)
            storage.pull_logs(local)
            run_day_step(spark, config, step, day, dict(kwargs, log_dir=local))
            storage.push_logs(local)
            return

    if step == "01":
        from opdi.ingestion.osn_statevectors import StateVectorIngestion
        from opdi.utils.storage import StorageManager

        # A sliding window of three days, not a new fetch. Day D is built from
        # the reports of D-1 to D+1, so consecutive days share two of their
        # three days. Only the days not already in the table are fetched, and
        # only the days the window has passed are removed, so each calendar
        # day is fetched once and the table holds three days.
        needed = [day - timedelta(days=1), day, day + timedelta(days=1)]
        storage = StorageManager(spark, config)
        present = set(storage.list_partitions("osn_statevectors_v2", "event_time_day"))

        missing = [d for d in needed if d.isoformat() not in present]
        stale = sorted(present - {d.isoformat() for d in needed})

        print(
            f"  window {needed[0]} .. {needed[-1]}: "
            f"{len(needed) - len(missing)} already present, "
            f"{len(missing)} to fetch, {len(stale)} to drop"
        )

        sv = StateVectorIngestion(spark, config)
        for d in missing:
            print(f"    fetching {d}")
            if config.project.project_name == "opensky":
                sv.ingest_from_s3(start_date=d, end_date=d + timedelta(days=1))
            else:
                sv.ingest(start_date=d, end_date=d + timedelta(days=1))

        if stale:
            # After fetching, never before: after a failure in between, a
            # short window could not be told from a day the archive lacks.
            n = storage.drop_partitions("osn_statevectors_v2", "event_time_day", stale)
            print(f"    dropped {n} day(s) the window has passed: {', '.join(stale)}")
    elif step == "01b":
        if not config.adsc.enabled:
            print("  ADS-C ingestion: disabled (config.adsc.enabled is False)")
            return

        from opdi.ingestion.osn_adsc import AdscIngestion

        # The same three-day window as step 01, so the tracks at the window's
        # edges see the same sources on both sides of midnight. The ADS-C table
        # is a few MB per day, so the three days are simply fetched again.
        adsc = AdscIngestion(spark, config)
        adsc.ingest_from_s3(
            start_date=day - timedelta(days=1),
            end_date=day + timedelta(days=2),
        )
    elif step == "02":
        from opdi.pipeline.tracks import TrackProcessor

        TrackProcessor(
            spark,
            config,
            log_file_path=os.path.join(_log_dir(kwargs), "02_osn-tracks-etl-log.parquet"),
        ).process_day(day, skip_if_processed=False)
    elif step == "02a":
        from opdi.cleaning.cleaner import TrackCleaner

        TrackCleaner(
            spark,
            config,
            log_file_path=os.path.join(_log_dir(kwargs), "02a_track_cleaning.log"),
        ).process_day(day, skip_if_processed=False)
    elif step == "03":
        from opdi.pipeline.flights import FlightListProcessor

        FlightListProcessor(
            spark,
            config,
            log_dir=_log_dir(kwargs),
        ).process_day(
            day,
            airports_hex_path=kwargs.get("airports_hex_path"),
            skip_if_processed=False,
        )
    elif step == "04":
        from opdi.pipeline.events import FlightEventProcessor

        FlightEventProcessor(
            spark,
            config,
            log_dir=_log_dir(kwargs),
        ).process_day(day, skip_if_processed=False)
    elif step == "04b":
        from opdi.pipeline.flight_list_step import enrich_day
        from opdi.utils.storage import StorageManager

        # Only the day this unit owns: the step rewrites the flight list day
        # folder it reads.
        written = enrich_day(StorageManager(spark, config), config.events, day, day)
        if written is not None:
            print(f"  flight list enriched: {written:,} rows")
    elif step == "09":
        from opdi.output.published import PublishedLayout, pipeline_commit, publish_day
        from opdi.utils.storage import StorageManager

        # Writes the day's flight list and flight events files into the
        # published folder, then updates the month's index and the catalogue.
        storage = StorageManager(spark, config)
        counts = publish_day(
            spark, storage, day, PublishedLayout.for_storage(storage), pipeline_commit()
        )
        print(f"  published: {counts['flights']:,} flights, {counts['events']:,} events")
    else:
        raise ValueError(
            f"{step!r} is not a per-day step; expected one of " f"{DAY_STEPS + OPTIONAL_DAY_STEPS}."
        )


def day_state_key(step: str, day: date) -> str:
    """The state key of a step for one day: ``"<step>@<YYYY-MM-DD>"``."""
    return f"{step}@{day.isoformat()}"


def days_in(start: date, end: date) -> list[date]:
    """Every day from ``start`` to ``end`` inclusive."""
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


def _session_extras() -> dict[str, str]:
    """Spark settings added to every step's session.

    Caps on the history the driver keeps (stages, jobs, tasks, SQL runs),
    which otherwise grows for the life of the session. Spark event logging is
    turned on when ``OPDI_EVENTLOG_DIR`` names a folder, to measure where a
    step spends its time.
    """
    extras = {
        "spark.ui.retainedStages": "50",
        "spark.ui.retainedJobs": "50",
        "spark.ui.retainedTasks": "1000",
        "spark.sql.ui.retainedExecutions": "50",
    }
    eventlog = os.environ.get("OPDI_EVENTLOG_DIR")
    if eventlog:
        # Spark refuses to start if the event log folder does not exist. A
        # local folder is created; a remote one is left to the operator.
        if eventlog.startswith("file://"):
            Path(eventlog[len("file://") :]).mkdir(parents=True, exist_ok=True)
        elif "://" not in eventlog:
            Path(eventlog).mkdir(parents=True, exist_ok=True)
        extras.update(
            {
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": eventlog,
                # Record what each stage cost, not only that it ran.
                "spark.eventLog.logStageExecutorMetrics": "true",
            }
        )
    return extras


def _describe_memory(limit_bytes: int | None, used_bytes: int) -> str:
    if limit_bytes is None:
        return "container memory: unconstrained"
    return (
        f"container memory: {used_bytes / 1024 ** 3:.1f} GiB used of "
        f"{limit_bytes / 1024 ** 3:.1f} GiB "
        f"({(limit_bytes - used_bytes) / 1024 ** 3:.1f} GiB free)"
    )


def run_period(
    env: str = "opensky",
    start: date | None = None,
    days: int = 7,
    steps: Sequence[str] = PERIOD_STEPS,
    state_path: Path | None = None,
    force: bool = False,
    driver_memory: str | None = None,
    executors: int | None = None,
    skip_preflight: bool = False,
    dry_run: bool = False,
    warehouse: str = _UNSET,
    prune_tracks: bool = False,
    keep_tracks_on: Sequence[date] = (),
    worldwide: bool = True,
) -> int:
    """Run the pipeline over ``days`` days, resuming where it stopped.

    Each day goes through all requested steps before the next day starts. A
    failed step stops the run, because later steps read its output.

    Args:
        env: Environment name, ``"opensky"`` (distributed on Kubernetes) or
            ``"local"``.
        start: First day of the period.
        days: Number of days (default 7).
        steps: Step ids to run, in order.
        state_path: File that records the finished units. Default:
            ``logs/periodrun_{start}_{days}d.json``.
        force: Run units again that are recorded as finished.
        driver_memory: Driver heap to ask for; still reduced to what the
            container can hold.
        executors: Number of Spark executors (``spark.executor.instances``).
        skip_preflight: Do not check the reference tables first.
        dry_run: Print the plan and stop, without starting Spark.
        warehouse: The warehouse. Default: the ``OPDI_WAREHOUSE``
            environment variable, else :data:`DEFAULT_WAREHOUSE`. The tables
            go to the release root inside it (``<warehouse>/v2.0.0`` in the
            ``opensky`` environment).
        prune_tracks: Remove each day's track folders once steps 03 and 04
            have finished for it (see :func:`prune_tracks_for_day`).
        keep_tracks_on: Days whose tracks are kept even with
            ``prune_tracks``.
        worldwide: Process the whole world (default). ``False`` limits the
            run to the European box. A warehouse built with the other
            coverage is refused.

    Returns:
        0 when every requested unit is finished, 1 otherwise.
    """
    from opdi.config import OPDIConfig
    from opdi.runner import STEPS, reference_substeps_for_coverage
    from opdi.utils.spark_helpers import SparkSessionManager
    from opdi.utils.storage import StorageManager

    if start is None:
        raise ValueError("start is required; a period run must name its window")

    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise ValueError(f"unknown step(s) {unknown}; valid: {sorted(STEPS)}")

    # Built first: the state file is tied to the coverage as well as to the
    # period.
    config = OPDIConfig.for_environment(env, worldwide=worldwide)

    start_date, end_date = period_window(start, days)
    if state_path is None:
        state_path = Path("logs") / f"periodrun_{start_date.isoformat()}_{days}d.json"
        if not state_path.exists():
            # Adopt a state file written under the module's former name, so a
            # running campaign does not start again from its first day.
            legacy = state_path.with_name(f"weekrun_{start_date.isoformat()}_{days}d.json")
            if legacy.exists():
                print(f"  resuming from pre-rename state file {legacy}")
                state_path = legacy
    state = RunState.load(Path(state_path))

    if not state.matches(env, start_date, end_date, config.coverage.label):
        raise SystemExit(
            f"state file {state_path} describes {state.env} "
            f"{state.start_date}..{state.end_date} "
            f"({state.coverage or 'europe'}), but this run is {env} "
            f"{start_date}..{end_date} ({config.coverage.label}). Refusing "
            "to reuse it -- pass a different --state-path, or delete that "
            "file if it is stale. This has bitten before: a stale state file "
            "once made a run report units complete and skip steps it had "
            "never actually run."
        )

    state.env = env
    state.start_date = start_date.isoformat()
    state.end_date = end_date.isoformat()
    state.coverage = config.coverage.label
    if not state.run_id:
        state.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    all_days = days_in(start_date, end_date)

    # A unit is a (step, day) pair for the per-day steps and a bare step for
    # the others, so a resumed run starts at the day that failed.
    #
    # Day by day, not step by step: a day goes through every step before the
    # next day starts. Step 01 keeps only a three-day window of reports, so
    # running every day's step 01 first would replace day 1's reports before
    # step 02 reached day 1. A failure then also costs one day, and a stopped
    # run holds whole days.
    units: list[tuple[str, date | None]] = []
    units.extend((step, None) for step in steps if not is_day_step(step))
    for d in all_days:
        units.extend((step, d) for step in steps if is_day_step(step))

    def _key(step, day):
        return day_state_key(step, day) if day is not None else step

    todo = [u for u in units if force or _key(*u) not in state.completed]
    done = [u for u in units if u not in todo]

    # Set before anything reads it: every table, reference or working, and the
    # published files are resolved inside this warehouse's release root.
    warehouse = _resolve_warehouse(warehouse, config.project.warehouse_path)
    config.project.warehouse_path = warehouse
    mem = _cgroup_v2_limit()
    limit_bytes, used_bytes = mem if mem else (None, 0)
    heap = safe_driver_memory(driver_memory or config.spark.driver_memory, limit_bytes, used_bytes)

    print("=" * 72)
    print("OPDI period run")
    print("=" * 72)
    print(f"  environment : {env}")
    print(config.describe_target())
    print(f"  window      : {start_date} .. {end_date}  ({days} days)")
    print(f"  steps       : {' '.join(steps)}")
    if done:
        print(f"  already done: {len(done)} unit(s)  (from {state_path})")
    print(f"  to run      : {len(todo)} unit(s) over {len(all_days)} day(s)")
    print(f"  day steps   : {' '.join(s for s in steps if is_day_step(s))}")
    # The last day step in the requested order, which is where the tracks can
    # be removed (a run of only 01 and 02 removes them after 02).
    last_day_step = next((s for s in reversed(list(steps)) if is_day_step(s)), None)
    keep_set = set(keep_tracks_on)
    if prune_tracks:
        print(
            f"  retention   : tracks pruned after {last_day_step}"
            + (f", kept for {', '.join(str(d) for d in sorted(keep_set))}" if keep_set else "")
        )
    print(f"  {_describe_memory(limit_bytes, used_bytes)}")
    if heap != (driver_memory or config.spark.driver_memory):
        print(
            f"  driver heap : {heap}  "
            f"(reduced from {driver_memory or config.spark.driver_memory} to fit)"
        )
    else:
        print(f"  driver heap : {heap}")
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
        # Filled in below, once a session exists; substeps whose tables are
        # already built are skipped.
        run_reference={k: True for k in REFERENCE_OUTPUTS},
        adep_mode=None,
        ades_mode=None,
    )

    overall = time.time()
    for step, day in todo:
        scope = f"day {day}" if day is not None else f"{start_date} .. {end_date}"
        print(f"\n{'-' * 72}\n>>> step {step}  ({scope})\n{'-' * 72}")
        step_start = time.time()
        # A new Spark session per step keeps the driver's growing history
        # within one step.
        spark = SparkSessionManager.create_session(
            app_name=f"OPDI period {start_date}..{end_date} step {step}",
            config=config,
            distributed=distributed,
            extra_configs=_session_extras(),
        )
        try:
            # The units are (step, day) pairs, so the pair is compared.
            if not skip_preflight and (step, day) == todo[0]:
                storage = StorageManager(spark, config)

                # The coverage is checked first and on its own: the branch
                # below treats any problem `preflight()` returns as "tables to
                # build", and would otherwise try to build reference tables in
                # a warehouse of the other coverage instead of refusing.
                coverage_problem = storage.check_coverage()
                if coverage_problem is not None:
                    print(f"Preflight failed: {coverage_problem}")
                    return 1

                if building_reference:
                    # Reference tables already in the warehouse are reused. A
                    # period is run one day at a time with one state file per
                    # period, so day 2 does not know that day 1 built them.
                    # An incomplete set means an earlier build stopped part
                    # way, and it is completed.
                    missing = preflight(storage)
                    if not missing and not force:
                        print(
                            f"  reference   : already built in "
                            f"{config.project.release_root}; "
                            "reusing it (pass --force to rebuild)"
                        )
                        state.mark(step, 0.0)
                        continue
                    if missing:
                        print(
                            f"  reference   : building {len(missing)} missing "
                            f"table(s): {', '.join(missing)}"
                        )
                        # Run only the substeps with work left.
                        todo_subs = reference_substeps_to_run(storage, force=force)
                        # 00c is left out of a worldwide run, which has no
                        # airspace table by design (the airspace sources are
                        # European), so it does not count as work left.
                        for substep, run in reference_substeps_for_coverage(config).items():
                            if not run:
                                todo_subs[substep] = False
                        kwargs["run_reference"] = todo_subs
                        skipped = [k for k, v in sorted(todo_subs.items()) if not v]
                        if skipped:
                            print(
                                f"  reference   : skipping {', '.join(skipped)} " "(already built)"
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
                    # Fetch the period plus the lookahead: the last day is
                    # built from reports of the day after it.
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

        # Removing the tracks, once the last day step of THIS day has
        # finished (steps 03 and 04 read them). Days in `keep_tracks_on` keep
        # their tracks.
        if prune_tracks and day is not None and step == last_day_step:
            if day in keep_set:
                print(f"  prune: keeping tracks for {day} (exempt)")
            else:
                storage = StorageManager(
                    SparkSessionManager.create_session(
                        app_name=f"opdi prune {day}", config=config, distributed=False
                    ),
                    config,
                )
                try:
                    n = prune_tracks_for_day(storage, day)
                    print(f"  prune: dropped {n} track partition(s) for {day}")
                finally:
                    storage.spark.stop()

    print(f"\nAll requested steps complete in {time.time() - overall:.1f}s.")
    print(f"State: {state_path}")
    return 0
