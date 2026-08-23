"""
Build flight lists with the actual pipeline, and score them. Version 7.

Every number in the V7 report that describes what OPDI *produces* comes from
here: this calls ``FlightListProcessor.process_dai`` -- the code path that
writes the published flight list -- and scores what it wrote against EUROCONTROL
Network Manager movement data.

Three things distinguish it from the V6 version it replaces.

**Configurations are named in full, never inherited.** V6 built its
"recommended" run by starting from ``legacy()`` and overriding the fields the
author remembered, and forgot the ranking rule -- so the row labelled
*recommended* silently kept the old behaviour and scored below the baseline it
was meant to beat. Here every run states every switch it turns on, and
``verify_plan`` asserts that the run named ``shipped`` is field-for-field
``DetectionConfig()``. A configuration assembled by hand can disagree with the
one that ships; this one cannot.

**Both periods run through the pipeline, not just the sweep.** V6 measured its
second period in the research harness only. ``--period 2024`` points the same
detection code at the second period's tracks through ``tracks_table``, so
"does it hold on another sample" is answered by the thing that ships rather
than by a model of it.

**The ladder is cumulative and says so.** Each ``L*`` run adds one change to
the one above and keeps it, so the marginal worth of each change is the
difference between adjacent rows. That is a different kind of table from the
mode comparison, where each row is a whole alternative, and V7 never draws them
the same way.

    python benchmarks/flight_list_v7.py --period 2025 --runs ladder \\
        --results-dir <dir>
"""

import argparse
import dataclasses
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))
sys.path.insert(0, str(REPO / "src"))

import osn_sample  # noqa: E402
from adep_ades import (  # noqa: E402
    airport_locations,
    airport_types,
    label_ground_truth,
    load_ground_truth,
    per_airport_counts,
    per_type_counts,
    score,
)
from osn_sample import build_spark, load_dotenv  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

#: Where flight lists land. Under ``research/`` and suffixed, so the published
#: flight list is never the target -- and ``guard_writes`` enforces it rather
#: than trusting the template.
OUT_TMPL = "research/flight_list_v7_{run}"

#: The two samples. Each names its own tracks, so a run cannot silently mix a
#: period's ground truth with another period's data -- which is how the V6
#: decimation comparison came to report a sampler collapsing coverage by fifty
#: points when the two arms had drifted onto different periods.
PERIODS = {
    "2025": {
        "months": ["202506"],
        "days": ["2025-06-05", "2025-06-06", "2025-06-07"],
        # None means "resolve from the config" -- which is `osn_tracks_clean`
        # when cleaning feeds the flight list, and the point of V7.
        "tracks": None,
        "raw_tracks": "osn_tracks",
    },
    "2024": {
        "months": ["202406"],
        "days": ["2024-06-05", "2024-06-06", "2024-06-07"],
        "tracks": "research/tracks_clean",
        "raw_tracks": "research/tracks",
        # The raw 2024 tracks pre-date step 02's H3 indexing, and the ladder's
        # early rungs read them deliberately -- they are where the published
        # data starts. The index is computed on read rather than materialised:
        # a second 12 GB copy of a table that already exists, to add one
        # derived column, is not worth the bucket. The cleaned copy carries the
        # column already, so only the raw one needs this.
        "index_on_read": ["research/tracks"],
        # The endpoint candidate cache for this period. `process_dai` builds
        # the cache when it is missing for the month, and its default target is
        # the production table -- which holds 2025 and which the write guard
        # would refuse anyway. Naming the period's own cache is what lets the
        # `endpoint` modes run here at all.
        "candidates": "research/cand_2024",
    },
}


def redirect_candidates(table: str) -> None:
    """Point the endpoint candidate cache at a period's own copy.

    The 2025 cache lives at the pipeline's own table name; the 2024 one cannot,
    because that name is taken by a different period's data. Mapping it here
    rather than parameterising ``flights.py`` keeps a benchmark's need out of
    the production code path.

    Both reads and writes are mapped, deliberately and together: a run that read
    one period's candidates and wrote another's would produce a flight list
    nothing in the output could identify as wrong.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_v7_cand_redirect", False):
        return
    orig_path = StorageManager._s3_path
    # `_s3_path`, not the table name: `table_ref` registers a Spark temp view
    # named after the table, and `research/cand_2024` is not a legal SQL
    # identifier.
    def _s3_path(self, name, *a, **kw):
        return orig_path(self, table if name == "opdi_endpoint_candidates"
                         else name, *a, **kw)

    StorageManager._s3_path = _s3_path
    StorageManager._v7_cand_redirect = True
    print(f"  endpoint candidates redirected to {table}")


def index_on_read(tables) -> None:
    """Attach ``h3_res_7`` when reading a table built before step 02 computed it.

    The flight list's `trend` path joins aerodrome detection zones on that
    column, so a track table without it cannot be read by the real detection
    code at all. Computing it on read costs one cheap column expression per
    scan and no storage; materialising a second copy of an 11.9 GB table to add
    one derived column would cost the bucket a sixth of its free space to store
    something reproducible from what is already there.

    Guarded on the column being absent, so this is a no-op the day step 02's
    output replaces the research copy.
    """
    if not tables:
        return
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_v7_h3_on_read", False):
        return
    orig_read = StorageManager.read_table
    wanted = set(tables)

    def read_table(self, name, *a, **kw):
        df = orig_read(self, name, *a, **kw)
        if name in wanted and "h3_res_7" not in df.columns:
            import h3_pyspark

            df = (df.withColumn("_res", F.lit(7))
                    .withColumn("h3_res_7",
                                h3_pyspark.geo_to_h3("lat", "lon", "_res"))
                    .drop("_res"))
        return df

    StorageManager.read_table = read_table
    StorageManager._v7_h3_on_read = True
    print(f"  h3_res_7 computed on read for: {', '.join(sorted(wanted))}")


def redirect_tracks(assign_table: str, tracks_table: str) -> None:
    """Point the flight list at a research segmentation's ``track_id``.

    The arm wrote only ``(icao24, event_time, track_id)``. The flight list
    needs full tracks, so the assignment is joined onto the shared cleaned
    track table on read and the original ``track_id`` replaced. Reading one
    shared track table for every arm is deliberate: it means an ADEP/ADES
    difference between arms cannot be an artefact of a different track build.

    ``tracks_table`` must be the exact table name ``FlightListProcessor``
    resolves for this run -- ``self.tracks_table`` in ``flights.py`` -- not a
    fixed pair of names. It calls ``self.storage.read_table(self.tracks_table)``
    exactly once per data pull, and that resolved name varies by period and by
    run: ``"osn_tracks_clean"`` when 2025 resolves it from the config,
    ``"research/tracks_clean"`` when the 2024 period passes it as an explicit
    ``tracks_table_override``, or ``"osn_tracks"``/``"research/tracks"`` for a
    ladder rung that reads raw tracks. A wrapper matching a hardcoded
    ``("osn_tracks", "osn_tracks_clean")`` pair -- an earlier draft of this
    function -- would silently pass the 2024 ``shipped`` run straight through
    unredirected, because ``"research/tracks_clean"`` is in neither name.
    Matching the single resolved name the caller passes in is what keeps the
    redirect and the run in sync for both periods.

    Reads only. Writes are already confined to ``research/`` by
    ``guard_writes``.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_tc_track_redirect", False):
        return
    orig_read = StorageManager.read_table

    def read_table(self, name, *a, **kw):
        df = orig_read(self, name, *a, **kw)
        if name != tracks_table:
            return df
        assign = self.spark.read.parquet(assign_table).withColumnRenamed(
            "track_id", "_new_track_id"
        )
        return (
            df.drop("track_id")
            .join(assign, ["icao24", "event_time"], "inner")
            .withColumnRenamed("_new_track_id", "track_id")
        )

    StorageManager.read_table = read_table
    StorageManager._tc_track_redirect = True
    print(f"  tracks redirected: {tracks_table} + {assign_table}")


def guard_writes(allowed_prefix: str = "research/") -> None:
    """Refuse any write that would land outside ``research/``.

    ``process_dai``'s default table name is the published flight list, so a
    dropped argument overwrites production. This makes that failure loud
    instead of silent and irreversible.

    **The check is on the destination path, not the table name.** Those differ
    whenever ``redirect_candidates`` is active: it maps ``_s3_path``, so a write
    of the logical name ``opdi_endpoint_candidates`` physically lands at
    ``research/cand_...``. Guarding the name alone rejected that legitimate
    write while a *genuine* production write -- the thing this guard exists to
    stop -- was caught only because the production table happens not to be
    named ``research/``-anything. Both readings were wrong for the same reason:
    the name is not where the bytes go. Resolving the path fixes the false
    negative and the false positive together.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_v7_guarded", False):
        return
    orig_write = StorageManager.write_table

    def write_table(self, df, table_name, *a, **kw):
        name = str(table_name)
        dest = self._s3_path(name)
        allowed_root = f"{self.base_path}/{allowed_prefix}"
        if not (name.startswith(allowed_prefix) or dest.startswith(allowed_root)):
            raise RuntimeError(
                f"refusing to write {name!r} (resolves to {dest!r}): this "
                f"benchmark writes only under {allowed_prefix!r}")
        print(f"  -> writing {name}" + (f" -> {dest}" if not
                                        name.startswith(allowed_prefix) else ""))
        return orig_write(self, df, table_name, *a, **kw)

    StorageManager.write_table = write_table
    StorageManager._v7_guarded = True


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------


#: Marks a run that must read the *uncleaned* tracks. Everything else resolves
#: its track table from the configuration, which since v4.0.0 means the cleaned
#: one. Named rather than passed as a boolean so the plan reads as data.
RAW = "raw"


def build_plan(args, period) -> dict:
    """Every run this module can build: name -> (detection, adep, ades, tracks).

    Written as one function returning one dict so the whole experiment is
    readable in one place. Nothing here reads a sweep CSV: V6 derived its
    "tuned" configuration from an argmax at run time, which made the run depend
    on a file that could be regenerated underneath it. The values these runs use
    are the ones in ``DetectionConfig``, and the sweeps' job is to justify those
    rather than to supply them.

    ``tracks`` is the track table the run reads: ``None`` to resolve it from the
    configuration, or an explicit name.
    """
    from opdi.config import DetectionConfig

    L = DetectionConfig.legacy
    S = DetectionConfig
    raw, clean = period["raw_tracks"], period["tracks"]

    def frm(base, **kw):
        """A config differing from *base* in exactly the named fields."""
        return dataclasses.replace(base(), **kw)

    # --- the cumulative ladder ------------------------------------------
    #
    # Each rung adds one change to the rung above and keeps it, so the marginal
    # worth of a change is the difference between adjacent rows, and the last
    # rung is the shipped pipeline exactly.
    #
    # Order is causal, not alphabetical, and it runs in three blocks:
    #
    # 1. **The trend algorithm**, both roles from `trend` so every change is
    #    attributable to its own line. The three geometry fixes come before the
    #    thresholds because they decide what the thresholds are even measuring:
    #    under ring selection a tuned flight-level cap *loses* ground, and under
    #    exact distance the same value gains it. Tuning first would attribute
    #    the whole difference to whichever step happened to come last.
    # 2. **Which algorithm serves which role**, and the endpoint geometry that
    #    only matters once a role uses it.
    # 3. **The input**, last: what cleaning is worth given everything else
    #    already adopted.
    #
    # `endpoint_height_ft`, `endpoint_sched_penalty_nm` and the candidate radius
    # are not rungs because they did not change -- the study confirmed them
    # where they already were, and a rung that changes nothing measures nothing.
    # `verify_plan` refuses a ladder containing one.
    # The per-role algorithm lives in the configuration, so a rung states it the
    # same way it states every other change and the two cannot disagree. An
    # earlier version carried the roles alongside the config and `verify_plan`
    # caught them differing on the very first run -- which is the check working,
    # and a good argument for one source of truth rather than two.
    steps = [
        # 1 -- the trend algorithm, on raw tracks, both roles from trend
        ("L00_legacy", {}, raw),
        ("L01_exact_rank", dict(trend_rank_by="haversine"), raw),
        ("L02_exact_radius", dict(trend_radius_exact=True), raw),
        ("L03_smooth_first", dict(trend_smooth_before_cut=True), raw),
        ("L04_penalty",
         dict(trend_sched_penalty_nm=S().trend_sched_penalty_nm), raw),
        ("L05_flcap", dict(trend_max_fl=S().trend_max_fl), raw),
        ("L06_margin", dict(trend_vote_margin=S().trend_vote_margin), raw),
        ("L07_bearing",
         dict(trend_bearing_tiebreak_nm=S().trend_bearing_tiebreak_nm), raw),
        # 2 -- roles and endpoint geometry. The endpoint values start at their
        # legacy settings so the switch of algorithm and the tuning of that
        # algorithm are two separate lines rather than one confounded one.
        ("L08_endpoint_departures", dict(adep_mode="endpoint"), raw),
        ("L09_endpoint_radius",
         dict(endpoint_radius_nm=S().endpoint_radius_nm), raw),
        # 3 -- the input. Identical configuration, cleaned tracks.
        ("L10_clean_tracks", {}, clean),
    ]

    # Two changes measured in V7 and **not** adopted, so they are not rungs:
    # the ladder walks to what ships, and a rung for a rejected change would
    # make the last rung something other than `DetectionConfig()`.
    #
    #   * the arrival radius at 20 NM -- the only step whose sign differed
    #     between the two samples, so it stays at its legacy 30 NM;
    #   * out-of-area on `trend` -- 50% precision on arrivals, which is a losing
    #     trade against a silence.
    #
    # Both are measured by `rejected`, below, against the shipped configuration
    # rather than mid-ladder, which is the comparison that answers "should this
    # be on?" rather than "what would it have done at this point in a sequence?"

    ladder, acc = {}, {}
    for name, delta, tracks in steps:
        acc = {**acc, **delta}
        d = frm(L, **acc)
        ladder[name] = (d, d.adep_mode, d.ades_mode, tracks)

    # --- whole configurations, to choose between -------------------------
    # Alternatives, not increments. Every one reads the cleaned tracks, because
    # that is what the pipeline does; `legacy` here is the legacy *algorithm* on
    # current input, and the ladder's first rung is the legacy algorithm on
    # legacy input. The two together separate the algorithm from the data.
    # Each rejected change, applied to the shipped configuration on its own.
    # The delta against `shipped` is what says whether it should be on.
    rejected = {
        "reject_radius20": (frm(S, trend_radius_nm=20.0),
                            S().adep_mode, S().ades_mode, clean),
        "reject_trend_ooa": (frm(S, trend_ooa=True),
                             S().adep_mode, S().ades_mode, clean),
        # `reject_margin2` rather than `margin0`: zero is now what ships, and
        # this measures the value it replaced. Kept because V6 shipped 2 and a
        # reader deserves the number that moved it.
        "reject_margin2": (frm(S, trend_vote_margin=2),
                           S().adep_mode, S().ades_mode, clean),
    }

    modes = {
        "legacy": (L(), L().adep_mode, L().ades_mode, clean),
        "trend": (S(), "trend", "trend", clean),
        "endpoint": (S(), "endpoint", "endpoint", clean),
        "nearest": (S(), "nearest", "nearest", clean),
        "shipped": (S(), S().adep_mode, S().ades_mode, clean),
    }

    # --- the flight-level grid -------------------------------------------
    # Closed at the top this time. V6 stopped at FL120 with departures still
    # rising, which read as "higher is always better" when the curve had simply
    # not been followed far enough.
    grid = {}
    for fl in args.grid_fl:
        for rd in args.grid_radius:
            grid[f"grid_fl{fl}_r{rd:g}"] = (
                frm(S, trend_max_fl=int(fl), trend_radius_nm=float(rd)),
                "trend", "trend", clean)

    return {**ladder, **modes, **rejected, **grid}


def ladder_rungs(plan: dict) -> list:
    """The ladder, in order. Names sort correctly because they are zero-padded."""
    return sorted(k for k in plan if k.startswith("L") and k[1:3].isdigit())


def verify_plan(plan: dict) -> None:
    """Assert the plan says what it means, before anything expensive runs.

    Four checks, every one of which corresponds to a defect this study has
    actually shipped or come close to:

    * ``shipped`` must be exactly ``DetectionConfig()``. V6's equivalent was
      assembled by hand, missed one field, and produced a row labelled
      *recommended* that scored below the baseline it claimed to beat -- and
      nothing said so.
    * **the last ladder rung must be the shipped configuration**, in its
      detection settings, its per-role algorithms *and* its track table. A
      ladder that ends anywhere else is not a decomposition of the change: it is
      a decomposition of something adjacent to it, and the difference is
      invisible in the output.
    * no two adjacent rungs may be identical. A rung that changes nothing scores
      identically to its predecessor, which reads as "this parameter does not
      matter" and means "this parameter is not reaching the code". That is
      exactly how the inert scheduled-service penalty hid for a whole version.
    * the first rung must be the legacy algorithm on uncleaned tracks, so the
      ladder starts where the published data actually starts.
    """
    from opdi.config import DetectionConfig

    def diff(a, b):
        return [f.name for f in dataclasses.fields(DetectionConfig)
                if getattr(a, f.name) != getattr(b, f.name)]

    shipped_cfg, shipped_adep, shipped_ades, shipped_tracks = plan["shipped"]
    if shipped_cfg != DetectionConfig():
        raise SystemExit(
            f"the run named 'shipped' is not DetectionConfig(): "
            f"{diff(shipped_cfg, DetectionConfig())}. Every figure attributed "
            f"to the shipped configuration would describe something else.")

    rungs = ladder_rungs(plan)
    if rungs:
        cfg, adep, ades, tracks = plan[rungs[-1]]
        problems = []
        if cfg != shipped_cfg:
            problems.append(f"detection differs in {diff(cfg, shipped_cfg)}")
        if (adep, ades) != (shipped_adep, shipped_ades):
            problems.append(
                f"roles are {adep}/{ades}, shipped is "
                f"{shipped_adep}/{shipped_ades}")
        if tracks != shipped_tracks:
            problems.append(f"tracks are {tracks!r}, shipped reads "
                            f"{shipped_tracks!r}")
        if problems:
            raise SystemExit(
                f"the last ladder rung ({rungs[-1]}) is not the shipped "
                f"configuration: " + "; ".join(problems) + ". The ladder would "
                "decompose a change that is not the one being made.")

        first_cfg, _, _, first_tracks = plan[rungs[0]]
        if first_cfg != DetectionConfig.legacy():
            raise SystemExit(
                f"the first ladder rung ({rungs[0]}) is not the legacy "
                f"configuration: {diff(first_cfg, DetectionConfig.legacy())}.")
        if first_tracks == shipped_tracks:
            raise SystemExit(
                "the first ladder rung already reads the cleaned tracks, so "
                "the cleaning rung would measure nothing.")

        for lo, hi in zip(rungs, rungs[1:]):
            if plan[lo] == plan[hi]:
                raise SystemExit(
                    f"ladder rungs {lo} and {hi} are identical. A step that "
                    f"changes nothing is not a measurement of that step.")

    print(f"plan verified: {len(plan)} runs, {len(rungs)} ladder rungs, "
          f"ending at the shipped configuration")


# ---------------------------------------------------------------------------
# Running and scoring
# ---------------------------------------------------------------------------


def build(spark, cfg, month, run, detection, adep_mode, ades_mode, log_dir,
          tracks_table):
    """Run step 03 for one configuration and return the table it wrote."""
    from opdi.pipeline.flights import FlightListProcessor

    cfg.detection = detection
    # Grid cells are scored and discarded, so they share one table rather than
    # leaving a prefix behind for each of twenty-eight combinations.
    table = OUT_TMPL.format(run="grid" if run.startswith("grid_") else run)

    proc = FlightListProcessor(spark, cfg, log_dir=str(log_dir),
                               tracks_table=tracks_table)
    print(f"\n=== {run}: ADEP={adep_mode} ADES={ades_mode} "
          f"tracks={proc.tracks_table} ===")
    print(f"    {detection}")
    proc.process_dai(
        month=month,
        skip_if_processed=False,
        adep_mode=adep_mode,
        ades_mode=ades_mode,
        abstention_radius_nm=detection.endpoint_radius_nm,
        abstention_height_ft=detection.endpoint_height_ft,
        sched_penalty_nm=detection.endpoint_sched_penalty_nm,
        table_name=table,
        write_mode="overwrite",
    )
    # The *resolved* track table, not the argument: `None` means "ask the
    # config", and a row recording "config" tells a reader nothing about which
    # data the number came from.
    return table, proc.tracks_table


def load_predictions(spark, table):
    """(predictions, identities) from a flight list this run just wrote."""
    fl = spark.read.parquet(f"s3a://eurocontrol/opdi/{table}")
    pred = fl.select(
        F.col("ID").alias("track_id"),
        F.col("ADEP").alias("adep"),
        F.col("ADES").alias("ades"),
    )
    ident = fl.select(
        F.col("ID").alias("track_id"),
        F.lower(F.col("ICAO24")).alias("icao24"),
        # Trimmed: ADS-B callsigns are space-padded to eight characters and the
        # reference callsign is not, so an untrimmed join matches nothing --
        # which shows up as every run scoring 0.00%, not as an error.
        F.trim(F.col("FLT_ID")).alias("callsign"),
        F.to_date(F.col("FIRST_SEEN")).alias("day"),
        F.col("FIRST_SEEN").alias("t_start"),
    )
    return pred, ident


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(PERIODS), default="2025")
    ap.add_argument("--runs", nargs="+", required=True,
                    help="run names, or the groups 'ladder', 'modes', "
                         "'cleaning', 'grid', 'all'")
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-name", default=None,
                    help="CSV basename; defaults to mode_comparison_v7.csv")
    ap.add_argument("--k", type=float, default=2.0,
                    help="wrong-answer weight in score = correct - k * wrong")
    ap.add_argument("--per-airport", action="store_true",
                    help="also emit per-aerodrome and per-type counts")
    ap.add_argument("--pipeline-logs", type=Path,
                    default=REPO / "OPDI_live" / "logs")
    # FL25..FL300 in steps of 25, plus the legacy 40 and the shipped 60 so both
    # stay on the grid rather than being interpolated between neighbours.
    ap.add_argument("--grid-fl", nargs="+", type=int,
                    default=sorted({40, 60} | set(range(25, 301, 25))))
    ap.add_argument("--grid-radius", nargs="+", type=float, default=[20.0, 30.0])
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--ui-port", type=int, default=4057)
    ap.add_argument("--track-assign", default=None,
                    help="assignment table whose track_id replaces the tracks' own "
                         "(see redirect_tracks)")
    ap.add_argument("--candidates-table", default=None,
                    help="endpoint-candidate table for this run, overriding the "
                         "period's own. Required alongside --track-assign: "
                         "candidates are derived from track_id, so a research "
                         "arm must not share the production cache")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the plan and exit; no cluster needed")
    args = ap.parse_args()

    # Fail here rather than forty minutes in, at the write. A redirected
    # track_id makes the endpoint candidates arm-specific, and the production
    # cache is written with mode="overwrite", so the pairing is mandatory. The
    # write guard does catch this, but only after a full build has been paid
    # for, and only because the production table happens not to sit under
    # research/ -- which is luck, not a check.
    if args.track_assign and not args.candidates_table:
        ap.error("--track-assign requires --candidates-table: endpoint candidates "
                 "are derived from track_id, so a research arm cannot share the "
                 "production cache (it is written with mode='overwrite')")

    sys.stdout.reconfigure(line_buffering=True)
    period = PERIODS[args.period]

    plan = build_plan(args, period)
    verify_plan(plan)

    groups = {
        "ladder": ladder_rungs(plan),
        "modes": ["legacy", "trend", "endpoint", "nearest", "shipped"],
        "rejected": ["reject_radius20", "reject_trend_ooa", "reject_margin2"],
        "grid": [k for k in plan if k.startswith("grid_")],
    }
    groups["all"] = groups["ladder"] + groups["modes"] + groups["rejected"]

    runs, seen = [], set()
    for name in args.runs:
        for r in groups.get(name, [name]):
            if r not in seen:
                seen.add(r)
                runs.append(r)
    unknown = [r for r in runs if r not in plan]
    if unknown:
        raise SystemExit(f"unknown run(s): {unknown}")

    print(f"period {args.period}: {period['days'][0]} .. {period['days'][-1]}")
    print(f"runs ({len(runs)}): {', '.join(runs)}")
    if args.dry_run:
        for r in runs:
            d, a, s, tr = plan[r]
            print(f"  {r:<24} ADEP={a:<9} ADES={s:<9} tracks={str(tr):<22} "
                  f"FL{d.trend_max_fl} r{d.trend_radius_nm:g} "
                  f"m{d.trend_vote_margin} pen{d.trend_sched_penalty_nm:g} "
                  f"rank={d.trend_rank_by} exact_r={d.trend_radius_exact} "
                  f"smooth1st={d.trend_smooth_before_cut} "
                  f"tie={d.trend_bearing_tiebreak_nm:g} ooa={d.trend_ooa} "
                  f"e_r={d.endpoint_radius_nm:g}")
        return

    out = args.results_dir
    out.mkdir(parents=True, exist_ok=True)

    # The pipeline decides whether to rebuild the endpoint candidate cache from
    # a *local* progress log, so a fresh log directory reads as "not built" and
    # triggers a rebuild the write guard would refuse. Copying that one log in
    # makes the existing cache visible; the DAI log is deliberately left out so
    # this run records its own progress rather than appending to production's.
    log_dir = out / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cand_log = "03_osn-endpoint_candidates-etl-log.parquet"
    src_log = args.pipeline_logs / cand_log
    if src_log.exists() and not (log_dir / cand_log).exists():
        import shutil
        (shutil.copytree if src_log.is_dir() else shutil.copy2)(
            src_log, log_dir / cand_log)
        print(f"reusing endpoint candidate cache per {src_log}")


    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    osn_sample.UI_PORT = args.ui_port
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    guard_writes()
    index_on_read(period.get("index_on_read"))
    # --candidates-table wins over the period's own mapping. Endpoint candidates
    # are track-derived -- `build_endpoint_candidates` groups state vectors by
    # `track_id` and carries it into the cache -- so a run whose `track_id` came
    # from a research arm MUST NOT read or write the shared cache. Two failures
    # it prevents, both silent: the write is `mode="overwrite"`, so an unguarded
    # run destroys the production cache; and with one shared table every arm
    # would be scored against whichever arm happened to write last.
    if args.candidates_table:
        redirect_candidates(args.candidates_table)
    elif period.get("candidates"):
        redirect_candidates(period["candidates"])
    if args.track_assign:
        # The table name a research arm's assignment replaces: the same
        # `clean` table `build_plan`'s "shipped" entry reads for this period
        # -- `None` (config-resolved, i.e. "osn_tracks_clean") for 2025, an
        # explicit "research/tracks_clean" for 2024. This flag is only ever
        # used with `--runs shipped`, so that is the one table this run
        # actually pulls tracks from.
        redirect_tracks(args.track_assign, period["tracks"] or "osn_tracks_clean")

    from opdi.config import OPDIConfig

    # The factory, not OPDIConfig() plus an attribute: the environment decides
    # the storage backend, and setting the field afterwards leaves the
    # StorageManager unwired, so every table looks absent.
    cfg = OPDIConfig.for_environment("opensky")
    month = datetime.strptime(period["months"][0], "%Y%m").date().replace(day=1)

    # A period with its own candidate cache must be recorded as already built,
    # or `process_dai` rebuilds it the first time an `endpoint` run asks for a
    # month the log does not mention -- and it would rebuild from whatever
    # track table *that rung* reads, which for the early ladder rungs is the
    # raw one. The result would be a cache silently derived from uncleaned
    # data, replacing one derived from cleaned data, with nothing in the output
    # to say so. The write guard catches the attempt, but a crash nine rungs in
    # is a poor substitute for not trying.
    #
    # This sits *after* `month` is derived, deliberately. An earlier revision
    # put it beside the log-directory setup, where `month` does not yet exist,
    # and `--dry-run` returns before reaching it -- so the dry run passed and
    # the real run died on an UnboundLocalError.
    if period.get("candidates"):
        import pandas as pd

        log_path = log_dir / cand_log
        months = list(pd.read_parquet(log_path).months) if log_path.exists() else []
        if month not in months:
            months.append(month)
            pd.DataFrame({"months": months}).to_parquet(log_path)
        print(f"  {month:%Y-%m} marked as built: this period uses "
              f"{period['candidates']}, made by the chain's own stage")

    # Restricted to the days actually ingested. Without this the reference
    # spans the whole month while the flight lists cover three days, so every
    # flight on the other twenty-seven counts as an abstention and coverage
    # comes out about ten times too low.
    gt = label_ground_truth(
        load_ground_truth(spark, period["months"], period["days"]),
        airport_locations(spark))
    n_gt = gt.count()
    print(f"ground-truth flights: {n_gt:,}")
    if n_gt == 0:
        raise SystemExit(
            "no ground-truth flights for this period. Every run would score "
            "zero coverage, which is not a result -- check the reference data "
            "covers these days.")
    types = airport_types(spark) if args.per_airport else None

    rows, per_apt, per_type = [], [], []
    for run in runs:
        detection, adep_mode, ades_mode, tracks = plan[run]
        table, resolved_tracks = build(spark, cfg, month, run, detection,
                                       adep_mode, ades_mode, log_dir, tracks)

        pred, ident = load_predictions(spark, table)
        m = score(pred, ident, gt, k=args.k)
        if m["adep_coverage"] == 0 and m["ades_coverage"] == 0:
            raise SystemExit(
                f"{run!r} scored zero coverage on both roles. With "
                f"{n_gt:,} reference flights that means the identity join "
                f"matched nothing, not that detection failed. Check icao24 "
                f"case and callsign padding before reading anything into it.")
        # Two updates, in this order, and the order is the point.
        #
        # First every detection field, which records the configuration. Then the
        # run's identity and **the modes that actually executed**, which
        # overwrite the config's copies of them.
        #
        # They differ. A single-mode run like `endpoint` is built from
        # `DetectionConfig()` -- whose `adep_mode` is "endpoint" and `ades_mode`
        # is "trend" -- and then *run* with both roles forced to `endpoint`. If
        # the column recorded the config, the row for that run would claim a
        # configuration it did not use. That is precisely the mislabelling
        # version 6 shipped and version 7 exists to prevent, so the executed
        # value wins and the configured one is not kept: a reader of this CSV
        # wants to know what ran.
        m.update({f.name: getattr(detection, f.name)
                  for f in dataclasses.fields(detection)})
        m.update(run=run, period=args.period, tracks_table=resolved_tracks,
                 adep_mode=adep_mode, ades_mode=ades_mode)
        rows.append(m)
        print(f"  ADEP cov={m['adep_coverage']:.2%} acc={m['adep_accuracy']:.2%} "
              f"correct={m['adep_correct']:,} wrong={m['adep_wrong']:,} "
              f"score={m['adep_score']:,.0f}")
        print(f"  ADES cov={m['ades_coverage']:.2%} acc={m['ades_accuracy']:.2%} "
              f"correct={m['ades_correct']:,} wrong={m['ades_wrong']:,} "
              f"score={m['ades_score']:,.0f}")

        if args.per_airport:
            for r in per_airport_counts(pred, ident, gt).collect():
                per_apt.append({**r.asDict(), "run": run, "period": args.period})
            for r in per_type_counts(pred, ident, gt, types).collect():
                per_type.append({**r.asDict(), "run": run, "period": args.period})

    import pandas as pd

    name = args.out_name or "mode_comparison_v7.csv"
    pd.DataFrame(rows).to_csv(out / name, index=False)
    if args.per_airport:
        pd.DataFrame(per_apt).to_csv(out / "per_airport_v7.csv", index=False)
        pd.DataFrame(per_type).to_csv(out / "per_type_v7.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
