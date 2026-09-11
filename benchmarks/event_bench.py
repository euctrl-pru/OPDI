#!/usr/bin/env python
"""Run step 04 through its real code path and score what it produced.

The point of running ``FlightEventProcessor`` rather than reimplementing the
detectors is that a benchmark of a reimplementation measures the
reimplementation. Version 6 learned this the expensive way on the flight list;
this follows ``flight_list_v7.py``'s shape for the same reason.

**The write guard is not optional.** Step 04 writes ``opdi_flight_events`` and
``opdi_measurements`` by name, and until recently did so in append mode by
default. An unguarded benchmark run would therefore add a duplicate copy of a
month to the *published* tables -- silently, since nothing errors and the rows
are merely counted twice afterwards. Every run here refuses any write outside
``research/``.

The ladder is cumulative in the V7 style: rung 0 is ``EventConfig.legacy()``,
which reproduces published ``events_v0.0.2``, and each subsequent rung turns on
one more change. What that buys over scoring only the endpoints is
attribution -- if the shipped configuration is better, the ladder says which
change made it so, and if a change is worth nothing it is visible rather than
carried along inside a net gain.
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F

import events_gt
import events_score
from opdi.config import EventConfig, OPDIConfig

#: Where a benchmark run may write. Anything else is a bug, loudly.
RESEARCH_PREFIX = "research/"

#: Which track tables each period actually lives in. The 2025 sample is in the
#: production tables; the 2024 sample was never ingested there and lives under
#: research/ -- so a run that does not redirect reads 2025 tracks, filters them
#: to June 2024, finds nothing, and reports every rung as empty. That happened.
#: `flight_list_v7.PERIODS` carries the same mapping for the same reason.
PERIOD_TRACKS = {
    "2025": {"raw": "osn_tracks", "clean": "osn_tracks_clean", "index_on_read": [],
             # The production flight list covers this period, so identity comes
             # from it -- the same source step 03 and the V7 study used.
             "identity": "flight_list"},
    "2024": {
        "raw": "research/tracks",
        "clean": "research/tracks_clean",
        # The raw 2024 tracks pre-date step 02's H3 indexing, and the early
        # rungs read them deliberately. Computing the index on read costs one
        # column expression per scan; materialising a second 12 GB copy to add
        # a derived column would cost the bucket a sixth of its free space.
        "index_on_read": ["research/tracks", "research/tracks_clean"],
        # No flight list exists for this period, so identity is derived from
        # the tracks. Stated per period rather than inferred: the flight list
        # is not *empty* for 2024, it is *the wrong period*, and a row count
        # cannot tell those apart.
        "identity": "tracks",
    },
    "2026": {
        # The V4 period. Its tracks were built by the A8 ``recommended``
        # segmentation arm, so `track_id` is `{hash}_{offset}` with no
        # `_{year}_{month}` suffix -- which is also how an A8 table is told
        # apart from a legacy one.
        "raw": "research/tracks_2026",
        "clean": "research/tracks_clean_2026",
        # Both 2026 tables already carry `h3_res_12` and `baro_altitude_c`
        # (verified against the parquet schema before spending cluster time),
        # so nothing has to be computed on read.
        "index_on_read": [],
        # Built by benchmarks/flight_list_2026.py. The production flight list
        # covers 2025-06-05/06/07 only -- verified, 360,298 rows -- so a run
        # that does not redirect finds no aerodrome for any 2026 track and
        # reports every aerodrome-anchored family as empty.
        "identity": "flight_list",
        "flight_list": "research/flight_list_2026",
    },
}

#: The logical table name step 04 and the scorers ask for when nothing
#: overrides it. A period that carries its own ``flight_list`` key answers with
#: that table instead -- see `redirect_event_tables` for the pipeline side and
#: `detected_events` for the scoring side.
DEFAULT_FLIGHT_LIST = "opdi_flight_list"


def flight_list_table(period: str) -> str:
    """Which flight list this period's aerodrome joins must read."""
    return PERIOD_TRACKS[period].get("flight_list", DEFAULT_FLIGHT_LIST)

#: The ladder. Each rung is (name, {field: value}) applied cumulatively on top
#: of `EventConfig.legacy()`, so rung 0 is the published algorithm and the last
#: rung must equal the shipped configuration -- `verify_plan` asserts both.
#:
#: .. warning::
#:
#:    **Rungs L00-L12 inherit ``events_version="events_v0.0.2"`` from
#:    ``EventConfig.legacy()``, and only ``L13_shipped`` overrides it. Step 04's
#:    callsign resolution is guarded on that string, so those twelve rungs run
#:    *unresolved*.**
#:
#:    That is correct only over tracks built by the legacy segmentation, where
#:    a track carries one callsign by construction and resolution would be a
#:    no-op anyway. Over tracks built by the ``standard`` segmentation, which
#:    groups on the airframe alone, a track can carry several callsigns --
#:    `calculate_airport_events` groups on ``flight_id`` without aggregating it,
#:    so one aircraft crossing one runway once emits one airport event *per
#:    callsign it happened to broadcast*.
#:
#:    So once the segmentation default flips and ``osn_tracks`` is rebuilt, a
#:    re-run of L00-L12 measures that fan-out rather than the rung, and the
#:    ``L12 -> L13`` delta bundles multiple unrelated things: the version bump,
#:    the callsign resolution the bump switches on, and eight V4 behaviour flags
#:    (emit_runway_milestones, emit_pru_tops, level_method, level_floors_above_field,
#:    level_radius_enforced, level_anchor, airport_gate_above_field,
#:    airport_admit_on_ground) added when
#:    EventConfig's defaults moved to events_v0.2.0. An event-count change would be attributed to a rung that
#:    did multiple things at once.
#:
#:    ``L13_shipped`` is not a single-behaviour rung; it is the catch-all that
#:    keeps the top of this ladder equal to whatever ``EventConfig()`` currently
#:    is, which ``verify_plan`` asserts. It therefore **moves every time a new
#:    behaviour ships**, and its delta is not comparable across publications.
#:    Anything added to ``EventConfig`` with a non-default shipped value has to
#:    be listed here or the guard goes red -- which is the guard working, not a
#:    test to relax.
#:
#:    **Read the deltas below L13 as valid for the tracks they were computed
#:    over, not as reproducible against a rebuilt table.** Deliberately not
#:    fixed by re-cutting the ladder: this is the flight-events V3 study's
#:    published instrument, and changing its rungs would silently redefine
#:    numbers that study already reports. A V4 ladder (a later task) will
#:    replace this one with a properly attributed baseline.
#:
#:    There is no check that could catch the unsafe case automatically. It
#:    would have to fail fast when the tracks were built by ``standard``, and
#:    nothing in the track table records which segmentation produced it -- the
#:    ``track_id`` format differs, but a benchmark cannot tell a rebuilt table
#:    from a legacy one without parsing ids and guessing.
LADDER = [
    ("L00_legacy", {}),
    ("L01_clean_tracks", {"feeds_from_clean_tracks": True}),
    ("L02_smoothing", {"phase_twindow_seconds": 60.0}),
    ("L03_complete_rules", {"phase_require_complete_rules": True}),
    ("L04_field_elevation", {"phase_ground_above_field": True}),
    ("L05_ordered_airport", {"airport_events_ordered": True}),
    ("L06_deterministic_ids", {"deterministic_event_ids": True}),
    ("L07_all_crossings", {"crossing_all_occurrences": True}),
    ("L08_interpolated", {"crossing_interpolate": True}),
    ("L09_rings", {"ring_radii_nm": (40.0, 100.0)}),
    ("L10_runway", {"emit_runway_events": True}),
    ("L11_blocks", {"emit_block_events": True}),
    ("L12_level_offs", {"emit_level_offs": True}),
    (
        "L13_shipped",
        {
            "emit_runway_milestones": True,
            "emit_pru_tops": True,
            "level_method": "pru",
            "level_floors_above_field": True,
            "level_radius_enforced": True,
            "level_anchor": "pru",
            "airport_gate_above_field": True,
            # Shipped with the layout on_ground fix. Surface position messages
            # carry no altitude at all, so without this the layout gate drops
            # 99.9% of in-stand samples at EBBR and the block-time family reads
            # as reception-bound when it is gate-bound.
            "airport_admit_on_ground": True,
            # `velocity` is frequently absent or NULLed (including by
            # cleaning's own stale-broadcast mask) on exactly the samples the
            # block-time family reads. Off under `legacy()`; must be re-armed
            # here or the shipped configuration stops matching `EventConfig()`.
            "ground_speed_derive_from_position": True,
            "events_version": "events_v0.2.0",
        },
    ),
]

#: The v0.1.0 shipped configuration, reconstructed. `EventConfig()` moved to
#: v0.2.0 when the A-CDM families landed, so the V3 baseline no longer has a
#: constructor; these are exactly the seven new behaviour fields at their off
#: values plus the old version string. Every V4 rung is applied cumulatively on
#: top of this, so V00 *is* what V3 shipped and V07 must equal `EventConfig()`
#: field for field -- `verify_plan_v4` asserts both rather than trusting them.
V4_BASE = dict(
    emit_runway_milestones=False,
    emit_pru_tops=False,
    level_method="icao",
    level_floors_above_field=False,
    level_radius_enforced=False,
    level_anchor="phase",
    airport_gate_above_field=False,
    events_version="events_v0.1.0",
)

#: The V4 ladder. Cumulative on top of :data:`V4_BASE`, one behaviour per rung.
#:
#: **Why not extend `LADDER`.** Its rungs inherit ``events_v0.0.2`` and
#: therefore run with callsign resolution off; once the segmentation default
#: flipped to A8 ``recommended`` a re-run of them measures airport-event fan-out
#: rather than the rung (the warning on `LADDER` says why in full). V4 needs a
#: baseline that is reproducible against a rebuilt track table, so it starts
#: from the v0.1.0 configuration instead of the published v0.0.2 one.
#:
#: ``V04`` carries two fields, not one, and they are one idea: both bind the
#: level classification to the flight's aerodrome geometry -- the floors to its
#: elevation and the analysis window to its distance. Splitting them would give
#: two rungs whose deltas are not separately interpretable, because a segment
#: excluded by the radius is one the floor never got to judge.
#:
#: ``V01`` carries no config change because the cross-track tie-break is not
#: configurable -- it is a bug fix, and a bug fix behind a flag is a bug you
#: have promised to keep. Its effect is read off ``runway_2026.csv`` against
#: V3's ``runway_2025.csv``, not off a rung, and the paper has to say so rather
#: than letting a zero-delta rung imply the fix did nothing.
LADDER_V4 = [
    ("V00_v3_shipped", {}),
    ("V01_runway_tiebreak", {}),
    ("V02_layout_agl", {"airport_gate_above_field": True}),
    ("V03_pru_level", {"level_method": "pru"}),
    ("V04_level_geometry",
     {"level_floors_above_field": True, "level_radius_enforced": True}),
    ("V05_pru_tops", {"emit_pru_tops": True, "level_anchor": "pru"}),
    ("V06_runway_milestones", {"emit_runway_milestones": True}),
    ("V07_shipped", {"events_version": "events_v0.2.0"}),
]

#: Rungs that are allowed to be config-identical to the rung before them,
#: **by name**. A code-only rung has no configuration to differ in; a *new*
#: accidental no-op has no entry here and still fails `verify_plan_v4`.
V4_NOOP_EXEMPT = {"V01_runway_tiebreak"}

LADDERS = {"v3": LADDER, "v4": LADDER_V4}


def guard_writes(allowed_prefix: str = RESEARCH_PREFIX) -> None:
    """Refuse any write outside ``research/``.

    Copied from ``flight_list_v7.py`` deliberately rather than imported: that
    module carries a plan and a CLI this one does not want, and a benchmark's
    safety rail should not depend on another benchmark staying importable.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_events_guarded", False):
        return
    orig_write = StorageManager.write_table

    def write_table(self, df, table_name, *a, **kw):
        # Check the resolved *path*, not the name. `redirect_event_tables`
        # sends `opdi_flight_events` to a research location by patching
        # `_s3_path`, so a name-based guard would reject exactly the writes the
        # redirect has already made safe -- and, worse, would pass a write whose
        # name looked safe but whose path had been redirected somewhere else.
        landing = str(self._s3_path(table_name))
        if f"/{allowed_prefix}" not in landing:
            raise RuntimeError(
                f"refusing to write {table_name!r} -> {landing!r}: this "
                f"benchmark writes only under {allowed_prefix!r}. Step 04 "
                f"writes opdi_flight_events by name, so without this an "
                f"unguarded run would append a duplicate month to the "
                f"published table."
            )
        print(f"  -> writing {table_name} -> {landing}")
        return orig_write(self, df, table_name, *a, **kw)

    StorageManager.write_table = write_table
    StorageManager._events_guarded = True


def redirect_event_tables(target: str, flight_list: str = None) -> None:
    """Send step 04's two output tables to a per-rung research location.

    Patches ``_s3_path`` rather than the table *name*, following
    ``flight_list_v7.redirect_candidates``: ``table_ref`` registers a temp view
    named after the table, and ``research/events_2025_L00`` is not a legal SQL
    identifier. Also forces overwrite, because a re-run of one rung must
    replace its own output rather than append to it -- the same append trap the
    published tables have, in a place where it would quietly double a score.

    ``flight_list`` redirects the *input* the aerodrome-anchored detectors read.
    ``calculate_airport_events``, the runway-milestone family and the PRU tops
    all ask ``storage.read_table`` for ``opdi_flight_list`` by its fixed name,
    and the published table holds 2025 only -- so a 2026 run without this finds
    no ADEP/ADES for any track and reports every aerodrome family as empty
    rather than as broken. Done here rather than by threading a table name
    through step 04 because the redirect is a property of the *benchmark run*,
    not of the pipeline.

    Only the two output tables are forced to overwrite. The flight list is read
    and never written, and a redirect that also relaxed its write mode would be
    a loaded gun pointed at whatever it happens to name.
    """
    from opdi.utils.storage import StorageManager

    outputs = {"opdi_flight_events": target, "opdi_measurements": target + "_meas"}
    redirected = dict(outputs)
    if flight_list and flight_list != DEFAULT_FLIGHT_LIST:
        redirected[DEFAULT_FLIGHT_LIST] = flight_list
    orig_path = getattr(StorageManager, "_events_orig_path", StorageManager._s3_path)
    StorageManager._events_orig_path = orig_path

    def _s3_path(self, table_name):
        return orig_path(self, redirected.get(table_name, table_name))

    StorageManager._s3_path = _s3_path

    orig_write = getattr(StorageManager, "_events_orig_write", None)
    if orig_write is None:
        orig_write = StorageManager.write_table
        StorageManager._events_orig_write = orig_write

    def write_table(self, df, table_name, mode="append", partition_by=None):
        if table_name in outputs:
            mode = "overwrite"
        return orig_write(self, df, table_name, mode)

    StorageManager.write_table = write_table


def redirect_tracks(period: str, clean_table: str = None) -> None:
    """Point the track reads at the period's own tables.

    Patches ``_s3_path`` rather than the table name, as
    ``redirect_event_tables`` does and for the same reason: ``table_ref``
    registers a temp view named after the table, and ``research/tracks_clean``
    is not a legal SQL identifier.

    ``clean_table``, if given, overrides ``PERIOD_TRACKS[period]["clean"]`` --
    for pointing the cleaned-track read at a different copy (e.g. one rebuilt
    with a cleaning fix) without touching the raw-track mapping or any other
    period's tables.
    """
    spec = PERIOD_TRACKS[period]
    clean = clean_table or spec["clean"]
    if spec["raw"] == "osn_tracks" and clean == "osn_tracks_clean":
        return
    from opdi.utils.storage import StorageManager

    mapping = {"osn_tracks": spec["raw"], "osn_tracks_clean": clean}
    orig = getattr(StorageManager, "_events_track_path", None)
    if orig is None:
        orig = StorageManager._s3_path
        StorageManager._events_track_path = orig

    def _s3_path(self, table_name):
        return orig(self, mapping.get(table_name, table_name))

    StorageManager._s3_path = _s3_path


def index_on_read(tables) -> None:
    """Backfill the columns step 02 adds, for tables written before it did.

    Two are needed and both are guarded on absence, so this is a no-op the day
    the research copies are replaced by step 02's own output.

    ``h3_res_12`` -- step 04's airport events join the layout table on it, so a
    track table without it cannot be read by the real detector at all.

    ``baro_altitude_c`` -- step 02's rolling-mean altitude repair. The 2024
    research tracks predate it, and every event detector reads it. Falling back
    to the raw ``baro_altitude`` is the honest substitute rather than a
    silently different one: it is what the column *was* before the repair
    existed, so a 2024 legacy rung reading it is closer to what
    ``events_v0.0.2`` actually saw in 2024 than a repaired column would be.
    The repair itself is not reproduced here -- doing so would mean running
    step 02 over the period, which is a different experiment.
    """
    if not tables:
        return
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_events_h3_on_read", False):
        return
    orig_read = StorageManager.read_table
    wanted = set(tables)

    def read_table(self, table_name):
        df = orig_read(self, table_name)
        resolved = getattr(self, "_s3_path")(table_name)
        if any(w in str(resolved) for w in wanted):
            if "h3_res_12" not in df.columns:
                import h3_pyspark

                df = df.withColumn(
                    "h3_res_12", h3_pyspark.geo_to_h3("lat", "lon", F.lit(12))
                )
            if "baro_altitude_c" not in df.columns and "baro_altitude" in df.columns:
                df = df.withColumn("baro_altitude_c", F.col("baro_altitude"))
        return df

    StorageManager.read_table = read_table
    StorageManager._events_h3_on_read = True


def build_plan(only=None, ladder: str = "v3") -> dict:
    """name -> EventConfig, cumulative from the ladder's own base.

    ``v3`` starts from `EventConfig.legacy()` -- published ``events_v0.0.2``.
    ``v4`` starts from :data:`V4_BASE`, the reconstructed v0.1.0 configuration,
    because after the A-CDM families landed no constructor produces it.
    """
    from dataclasses import replace

    if ladder not in LADDERS:
        raise SystemExit(f"unknown ladder {ladder!r}: choose from {sorted(LADDERS)}")
    current = EventConfig.legacy() if ladder == "v3" else EventConfig(**V4_BASE)
    plan = {}
    for name, delta in LADDERS[ladder]:
        current = replace(current, **delta) if delta else current
        plan[name] = current
    if only:
        missing = [n for n in only if n not in plan]
        if missing:
            raise SystemExit(f"unknown rung(s): {', '.join(missing)}")
        plan = {n: plan[n] for n in only}
    return plan


def verify_plan(plan: dict) -> None:
    """Four assertions before anything expensive runs.

    Every one of these has a V6/V7 precedent: a ladder whose first rung was not
    actually the published algorithm, or whose last was not what ships, measures
    something nobody asked about -- and costs two hours per rung to discover.
    """
    names = list(plan)
    if names and names[0] == "L00_legacy":
        assert plan["L00_legacy"] == EventConfig.legacy(), (
            "rung 0 must be exactly the published algorithm, or the baseline "
            "every gain is measured against is not the baseline"
        )
    if names and names[-1] == "L13_shipped":
        shipped = EventConfig()
        got = plan["L13_shipped"]
        differing = [
            f
            for f in EventConfig().__dataclass_fields__
            if getattr(got, f) != getattr(shipped, f)
        ]
        assert not differing, (
            f"the last rung must equal the shipped configuration; differs on "
            f"{', '.join(differing)}"
        )
    for a, b in zip(names, names[1:]):
        assert plan[a] != plan[b], f"rungs {a} and {b} are identical -- one is a no-op"


def verify_plan_v4(plan: dict) -> None:
    """The V4 ladder's assertions, before anything expensive runs.

    Three of the four are `verify_plan`'s, restated against a base that has no
    constructor: the baseline has to be *exactly* the seven fields V4 changes,
    or it is not v0.1.0 and the paper's "V3 shipped" column is about a
    configuration nobody ran. The fourth guards the scoring map -- see
    `milestone_map`.
    """
    names = list(plan)
    shipped = EventConfig()
    fields = list(EventConfig().__dataclass_fields__)

    if names and names[0] == "V00_v3_shipped":
        differing = {
            f for f in fields if getattr(plan[names[0]], f) != getattr(shipped, f)
        }
        assert differing == set(V4_BASE), (
            f"the V4 baseline must differ from the shipped configuration in "
            f"exactly the {len(V4_BASE)} reconstructed v0.1.0 fields; it "
            f"differs in {sorted(differing)}. Extra fields mean the baseline "
            f"is not v0.1.0; missing ones mean a behaviour shipped without a "
            f"rung measuring it."
        )
    if names and names[-1] == "V07_shipped":
        differing = [
            f for f in fields if getattr(plan[names[-1]], f) != getattr(shipped, f)
        ]
        assert not differing, (
            f"the last rung must equal the shipped configuration; differs on "
            f"{', '.join(differing)}"
        )
    for a, b in zip(names, names[1:]):
        if b in V4_NOOP_EXEMPT:
            continue
        assert plan[a] != plan[b], f"rungs {a} and {b} are identical -- one is a no-op"

    for name, cfg in plan.items():
        if cfg.emit_runway_milestones:
            assert "landing" not in milestone_map(cfg), (
                f"{name} emits the A-CDM runway family, where `landing` is the "
                f"ICAO T16 threshold-plane crossing -- an event APDF does not "
                f"record. Scoring it against ALDT would report a touchdown "
                f"error for a thing that is not a touchdown."
            )


#: Which verifier belongs to which ladder. Two functions rather than one with a
#: branch: V3's is a published instrument and its assertions must not move.
VERIFIERS = {"v3": verify_plan, "v4": verify_plan_v4}


#: Which OPDI event type claims to be which APDF milestone. `take-off` and
#: `landing` are the published fuzzy-phase pair; ATOT and ALDT are the new
#: runway-anchored ones. Both are scored, separately, against the same truth --
#: that comparison *is* the question the ladder exists to answer, and collapsing
#: them would hide it.
TYPE_TO_MILESTONE = {
    # The A-CDM vocabulary of events_v0.2.0.
    "airborne": "ATOT",
    "touchdown": "ALDT",
    "off-block": "AOBT",
    "on-block": "AIBT",
    # V3's names, kept so the V00 baseline rung scores against the same truth.
    "ATOT": "ATOT",
    "ALDT": "ALDT",
    "AOBT": "AOBT",
    "AIBT": "AIBT",
    "take-off": "ATOT",
    "landing": "ALDT",
}

#: Event types that carry a runway designator, so their identity can be checked
#: against ``AP_C_RWY``. The phase pair (``take-off``/``landing``) is *not*
#: here: it names no runway, and letting it into the runway comparison would
#: put nulls in the denominator of an exact-match rate.
RUNWAY_IDENTITY_TYPES = {"ATOT": "ATOT", "ALDT": "ALDT",
                         "airborne": "ATOT", "touchdown": "ALDT"}


def milestone_map(config) -> dict:
    """The type -> milestone map that is correct for *this* configuration.

    One map cannot serve both vocabularies, because ``landing`` means two
    different physical things depending on ``emit_runway_milestones``:

    * **off** (v0.1.0 and earlier) -- the fuzzy phase transition into GND, i.e.
      ground contact. That is what ALDT is, so it is scored.
    * **on** (v0.2.0) -- ICAO T16, the instant the aircraft crosses the runway
      threshold plane, tens of seconds and roughly a kilometre before the
      wheels touch. ``touchdown`` is the ALDT of that vocabulary. APDF records
      no threshold crossing at all, so a ``landing`` scored against ALDT would
      report a large systematic "error" that is really a comparison between two
      different events -- and it would land in the same table as the honest
      figures, indistinguishable from them.

    So the map is narrowed per rung rather than per period or per version
    string: the ladder's own configuration is the only thing that knows which
    vocabulary its output speaks. ``take-off`` is left in place because it is
    simply not emitted when the milestones are on (``events.py`` gates the pair
    on the same flag), so a ``take-off`` row appearing under v0.2.0 would be a
    bug worth seeing scored rather than hidden.
    """
    mapping = dict(TYPE_TO_MILESTONE)
    if getattr(config, "emit_runway_milestones", False):
        mapping.pop("landing", None)
    return mapping


def runway_identity_types(config) -> dict:
    """`RUNWAY_IDENTITY_TYPES` narrowed to the types this config emits.

    Both vocabularies name a runway, under different type strings *and*
    different ``info`` keys -- ``runway`` for v0.1.0's ATOT/ALDT, ``rwy_ident``
    for the A-CDM family. Mixing the two into one comparison would align two
    detections onto the same truth row and score whichever landed nearer.
    """
    acdm = getattr(config, "emit_runway_milestones", False)
    keep = {"airborne", "touchdown"} if acdm else {"ATOT", "ALDT"}
    return {t: m for t, m in RUNWAY_IDENTITY_TYPES.items() if t in keep}


def detected_events(spark, table: str, storage=None, tracks=None,
                    identity: str = "flight_list",
                    flight_list: str = DEFAULT_FLIGHT_LIST,
                    mapping: dict = None):
    """Reshape the written event table into what the scorer expects.

    The event table keys on ``flight_id``, which is the ``track_id``; the
    ground truth keys on ``(icao24, callsign, day)``. Those are different
    identity spaces and the join between them has to be made explicitly --
    the flight list is what holds both, so it is the bridge.

    This was missed on the first run and cost an hour: the scorer's unit tests
    fed it synthetic frames that already carried ``icao24``, so they validated
    the scoring arithmetic and never the identity resolution. A test that
    constructs its own inputs cannot catch a mismatch between two real
    schemas.

    ``callsign`` is trimmed because ADS-B pads to eight characters, and
    ``icao24`` lowered because the reference carries it uppercase; both are the
    same traps the ground-truth loader closes on its side.

    ``mapping`` defaults to the whole of :data:`TYPE_TO_MILESTONE`; the ladder
    passes `milestone_map(cfg)`, which is narrower for the A-CDM vocabulary.
    ``flight_list`` names the table identity is resolved through, so an
    out-of-sample period can point at its own -- see `flight_list_table`.
    """
    ev = spark.read.parquet(table) if table.startswith("s3a://") else spark.table(table)
    # Two vocabularies, two keys for the same fact: v0.1.0's ATOT/ALDT write
    # `runway`, the A-CDM family writes `rwy_ident` (runway_ops._info). Parsing
    # only the first left every v0.2.0 runway designator NULL, which reads as
    # "the detector named no runway" rather than as "the reader looked in the
    # wrong field".
    info = F.from_json(
        F.col("info"),
        "runway string, rwy_ident string, apt_icao string, crossing_seq int, "
        "direction string",
    )
    mapping = dict(mapping or TYPE_TO_MILESTONE)
    milestone = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    ev = ev.withColumn("_i", info).select(
        F.col("flight_id").alias("_track_id"),
        F.col("type").alias("det_type"),
        milestone[F.col("type")].alias("milestone"),
        F.col("event_time"),
        F.col("latitude").alias("det_lat"),
        F.col("longitude").alias("det_lon"),
        F.coalesce(F.col("_i.runway"), F.col("_i.rwy_ident")).alias("det_runway"),
    )

    if identity != "tracks":
        fl = storage.read_table(flight_list).select(
            F.col("ID").alias("_fl_id"),
            F.lower(F.col("ICAO24")).alias("icao24"),
            F.trim(F.col("FLT_ID")).alias("callsign"),
            F.to_date(F.col("FIRST_SEEN")).alias("day"),
        )
    else:
        # The flight list is period-specific and only the 2025 one exists in
        # the production table, so an out-of-sample period would resolve no
        # identity at all and every rung would score zero. Identity does not
        # need the flight list: the tracks carry icao24, the callsign and a
        # first-seen day, which is what `adep_ades.track_identity` uses. The
        # flight list is still required by the *detectors* that need ADEP/ADES
        # -- airport events, rings, runway, blocks -- so a period without one
        # yields the phase and crossing families only, and says so.
        fl = tracks.groupBy("track_id").agg(
            F.lower(F.first("icao24", ignorenulls=True)).alias("icao24"),
            F.trim(F.first("callsign", ignorenulls=True)).alias("callsign"),
            F.to_date(F.min("event_time")).alias("day"),
        ).withColumnRenamed("track_id", "_fl_id")

    return (
        ev.filter(F.col("milestone").isNotNull())
        .join(F.broadcast(fl), ev._track_id == F.col("_fl_id"), "inner")
        .drop("_fl_id", "_track_id")
    )


def extraction_counts(spark, table: str, rung: str):
    """What step 04 actually emitted, by event type.

    The scorer only ever sees the milestones APDF can reach, which is four
    types out of roughly twenty. Everything else -- the level-offs, the
    top-of-climb and top-of-descent, the airport entry and exit events, the
    first and last seen -- is invisible in a scored table, so a family that
    stopped being emitted entirely would not show up as a regression. It would
    show up as nothing at all, which is worse.

    This is also the number consumers need: it says how much bigger the
    published event table becomes, per type, which is what they are being asked
    to ingest.
    """
    ev = spark.read.parquet(table) if table.startswith("s3a://") else spark.table(table)
    return [
        {"rung": rung, "type": r["type"], "n_events": r["n_events"]}
        for r in ev.groupBy("type")
        .agg(F.count(F.lit(1)).alias("n_events"))
        .orderBy("type")
        .collect()
    ]


def write_csv(rows, path):
    import csv

    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=sorted(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"  wrote {path}")


def osn_sample_driver_default() -> str:
    """The driver heap default, read from the one place that documents it.

    Imported lazily because argparse builds its defaults before ``main`` does
    its heavyweight imports, and ``osn_sample`` pulls in Spark.
    """
    import osn_sample

    return osn_sample.RESEARCH_DRIVER_MEMORY


def newest_object_mtime(prefix: str):
    """Last-modified of the newest object under ``opdi/<prefix>/``, or None.

    Used to prove a reused table belongs to the run being resumed. Parquet
    carries no write timestamp of its own that Spark exposes cheaply, and a
    row count cannot tell a fresh table from a stale one of the same shape --
    the object mtimes can.
    """
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="https://s3.opensky-network.org",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    newest = None
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket="eurocontrol", Prefix=f"opdi/{prefix.rstrip('/')}/"
    )
    for page in pages:
        for obj in page.get("Contents", []):
            if newest is None or obj["LastModified"] > newest:
                newest = obj["LastModified"]
    return newest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(events_gt.PERIODS), required=True)
    ap.add_argument(
        "--ladder", choices=sorted(LADDERS), default="v3",
        help="which ladder to run: v3 is the published instrument from "
             "events_v0.0.2 upwards; v4 starts from the reconstructed v0.1.0 "
             "configuration. Default v3 so the V3 chain's arguments keep "
             "meaning what they meant.",
    )
    ap.add_argument(
        "--airports", choices=("study", "all"), default="all",
        help="restrict the ground truth to events_gt.STUDY_AIRPORTS. The "
             "denominator of every coverage figure, so it has to match the "
             "bridge job's -- the V4 entrypoint passes 'study' to both.",
    )
    ap.add_argument("--runs", nargs="*", default=None, help="ladder rung names")
    ap.add_argument("--results-dir", default=None)
    ap.add_argument("--out-name", default=None)
    ap.add_argument(
        "--clean-table", default=None,
        help="override PERIOD_TRACKS[period]['clean'] -- read the cleaned "
             "tracks from a different table, e.g. a copy rebuilt with a "
             "cleaning fix, without touching the period's usual mapping.",
    )
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4059)
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument(
        "--driver-memory",
        default=osn_sample_driver_default(),
        help=(
            "Driver heap. Applies to distributed runs too -- it did not before, "
            "so a distributed run took the environment's 10 GB whatever was asked."
        ),
    )
    ap.add_argument(
        "--reuse-table",
        nargs="*",
        default=[],
        metavar="RUNG",
        help=(
            "Score these rungs from the event table already on S3 instead of "
            "recomputing them. For resuming a ladder that died partway: the "
            "scores live only in the driver's memory until the final CSV "
            "write, so a crash loses every rung's numbers while leaving every "
            "completed rung's table intact. Requires --reuse-newer-than."
        ),
    )
    ap.add_argument(
        "--reuse-newer-than",
        default=None,
        metavar="ISO8601",
        help=(
            "A reused table is accepted only if its newest object postdates "
            "this instant. Mandatory with --reuse-table, and not paranoia: "
            "research/events_2026_V07_shipped still held a table from an "
            "earlier, pre-fix run at the moment the ladder died, and reusing "
            "it would have published those numbers as this run's result."
        ),
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    plan = build_plan(args.runs, ladder=args.ladder)
    VERIFIERS[args.ladder](plan)
    print(f"plan verified ({args.ladder}): {len(plan)} rung(s) -- {', '.join(plan)}")
    reuse = set(args.reuse_table)
    unknown = reuse - set(plan)
    if unknown:
        raise SystemExit(
            f"--reuse-table names rungs that are not in this plan: {sorted(unknown)}"
        )
    if reuse and not args.reuse_newer_than:
        raise SystemExit("--reuse-table requires --reuse-newer-than")
    reuse_cutoff = None
    if args.reuse_newer_than:
        import datetime as _dt

        reuse_cutoff = _dt.datetime.fromisoformat(args.reuse_newer_than)
        if reuse_cutoff.tzinfo is None:
            reuse_cutoff = reuse_cutoff.replace(tzinfo=_dt.timezone.utc)

    if args.dry_run:
        return 0

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    osn_sample.RESEARCH_DRIVER_MEMORY = args.driver_memory
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "96")
    redirect_tracks(args.period, clean_table=args.clean_table)
    index_on_read(PERIOD_TRACKS[args.period]["index_on_read"])
    guard_writes()

    truth, rings, report = events_gt.build(spark, args.period, airports=args.airports)
    truth.cache()

    # Fail before the expensive part, not an hour into it. The first run of
    # this harness spent 68 minutes computing events and then died on the
    # scorer's join because the detected side had no `icao24` -- a check that
    # costs nothing here would have caught it in seconds.
    required = {"icao24", "callsign", "day", "milestone", "gt_time"}
    missing = required - set(truth.columns)
    if missing:
        raise SystemExit(f"ground truth is missing join keys: {sorted(missing)}")

    import datetime as dt

    from opdi.pipeline.events import FlightEventProcessor

    month = dt.datetime.strptime(events_gt.PERIODS[args.period]["month"], "%Y%m").date()
    scored, inventory = [], []
    for name, cfg in plan.items():
        print(f"\n=== {name} ===")
        target = f"research/events_{args.period}_{name}"
        redirect_event_tables(target, flight_list=flight_list_table(args.period))

        config = OPDIConfig.for_environment("opensky")
        config.events = cfg
        # Clear the per-family progress log first. `process_month` decides what
        # to compute from those logs *independently* of skip_if_processed, so a
        # rung whose name was used by an earlier run computes nothing, writes
        # nothing, and the scorer then reads the earlier run's table and reports
        # it as this rung's result. That happened: L00_legacy scored the
        # previous run's contaminated output and looked entirely plausible.
        log_dir = f"logs/events_{args.period}_{name}"
        if name not in reuse:
            # Before the processor is constructed, never after. The constructor
            # creates this directory, and `process_month` writes its per-family
            # progress markers into it as its final act. Clearing it after
            # construction leaves the processor holding paths beneath a
            # directory that no longer exists, so the run dies on the last line
            # of its last family -- with every event table already written and
            # nothing to show for it. That cost two hours of V07 compute.
            shutil.rmtree(log_dir, ignore_errors=True)
        proc = FlightEventProcessor(spark, config, log_dir=log_dir)
        if name in reuse:
            # Deliberate, named-on-the-command-line reuse. The failure mode the
            # comment above describes is *silent* reuse; this one is announced,
            # and the timestamp gate makes a stale table an error rather than a
            # plausible-looking result.
            newest = newest_object_mtime(f"research/events_{args.period}_{name}")
            if newest is None:
                raise SystemExit(f"{name}: --reuse-table but no table at {target}")
            if newest < reuse_cutoff:
                raise SystemExit(
                    f"{name}: table at {target} was last written {newest:%Y-%m-%d %H:%M:%S %Z}, "
                    f"which predates --reuse-newer-than "
                    f"{reuse_cutoff:%Y-%m-%d %H:%M:%S %Z}. That is a table from "
                    f"an earlier run, not this one. Recompute the rung."
                )
            print(f"  reusing existing {target} (written {newest:%Y-%m-%d %H:%M:%S %Z})")
        else:
            proc.process_month(month, skip_if_processed=False)

        table = f"s3a://eurocontrol/opdi/{target}"
        inventory.extend(extraction_counts(spark, table, name))
        tracks = proc.storage.read_table(
            "osn_tracks_clean" if cfg.feeds_from_clean_tracks else "osn_tracks"
        ).select("track_id", "icao24", "callsign", "event_time")
        detected = detected_events(
            spark, table, proc.storage, tracks,
            identity=PERIOD_TRACKS[args.period]["identity"],
            flight_list=flight_list_table(args.period),
            # Per rung, not per run: `landing` is ground contact under v0.1.0
            # and a threshold crossing under v0.2.0. See `milestone_map`.
            mapping=milestone_map(cfg),
        ).cache()
        types = [r.det_type for r in detected.select("det_type").distinct().collect()]
        rung_rows = []
        for det_type in sorted(types):
            aligned = events_score.align(
                truth, detected.filter(F.col("det_type") == det_type)
            )
            for row in events_score.score(aligned).collect():
                rung_rows.append({"rung": name, "det_type": det_type, **row.asDict()})
        if not rung_rows:
            raise SystemExit(
                f"{name}: emitted no scorable milestone type at all. A rung "
                f"that produces nothing is a configuration or input fault, not "
                f"a result -- on 2024 this meant the run was reading the 2025 "
                f"track tables and filtering them to a month they do not "
                f"contain."
            )
        else:
            events_score.guard_not_all_zero(
                spark.createDataFrame(rung_rows).select("n_detected")
            )
        scored.extend(rung_rows)
        print(f"  scored {len(scored)} rows so far")

    if args.results_dir:
        out = Path(args.results_dir)
        write_csv(scored, out / (args.out_name or f"ladder_{args.period}.csv"))
        write_csv(inventory, out / f"inventory_{args.period}.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
