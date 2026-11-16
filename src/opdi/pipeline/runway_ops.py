"""Runway milestones of Airport Collaborative Decision Making (A-CDM).

Part of step 04. Finds, for every flight, the runway it used and the times
of ``line-up``, ``take-off-roll``, ``airborne``, ``xing-runway-threshold``
(crossing of the threshold), ``touchdown`` and ``runway-vacated``, the runway crossings of a
taxiing aircraft, and ``go-around``. All are read from passages over a
runway, built from the H3 cells of the runway zones (``h3_runway_zones``).
See https://www.opdi.aero/content/methods/runway-milestones.html.

Nine of the eleven A-CDM milestones come out of one pass over the same object:
a *traversal*, a maximal run of consecutive samples whose H3 res-12 cell falls
inside a runway polygon of an aerodrome the flight list names for that flight.
A gap longer than ``airport_trace_gap_seconds`` starts a new one, so an
aircraft that sits on the runway across a coverage hole is two traversals
rather than one implausible ten-minute occupancy.

**One object rather than nine detectors.** ``line-up`` and ``take-off-roll``
and ``airborne`` are not independent questions; they are three readings of one
departure. Deriving them from a shared, explicitly classified traversal means
they cannot disagree about which runway, which aerodrome, or even whether the
movement was a departure at all -- and the classification that produced them
travels with every event in ``info``, so the decision is auditable from the
published row rather than reconstructible only from the code.

Each traversal is classified by four measurements over its own samples:

* **alignment** -- the angle between the traversal's median ground track and
  the runway's bearing, folded to [0, 90] so a reciprocal landing counts as
  aligned;
* **speed** -- the maximum groundspeed reached;
* **height** -- the height at entry and at exit above the field elevation of
  **the traversal's own aerodrome**. Not above the more permissive of the
  flight's two fields: that reading is right for asking whether a sample is
  near enough to the ground to be matched against a layout, and wrong here,
  where the answer is a threshold comparison that gets published. A departure
  from the lower of a flight's two aerodromes has a negative height throughout
  under the permissive reading, and the detector abstains on the whole
  movement;
* **ground flag** -- whether the report that gives the exit height, or the
  last report of the pass, is flagged on the ground by the transponder. A
  take-off ends in the air, so a pass that ends on the ground is no
  take-off, unless it also climbed clearly into the air before (a take-off
  and a landing on the same runway a few minutes apart, as in a circuit);
  see :attr:`~opdi.config.EventConfig.runway_take_off_needs_air_report`.
  On the ground the barometric height can read 20 to 60 ft, and a landing
  roll read from the far end of the runway, with no approach before it,
  otherwise starts low and ends high like a take-off;

giving ``departure``, ``arrival``, ``crossing`` -- or NULL, which means
abstain. The classes cannot overlap: ``runway_align_max_deg`` (30) and
``runway_cross_min_deg`` (45) leave a band between them where the detector says
nothing, and the speed limits separate a rolling aircraft from a taxiing one.
The band is deliberate, not an oversight.

**Timing is interpolated, not sampled.** ``airborne`` and ``touchdown`` are the
instants the height above field elevation crosses ``runway_airborne_height_ft``,
and ``xing-runway-threshold`` the instant the along-track distance from the
threshold changes sign. All three go through :func:`opdi.pipeline.crossings.threshold_crossings`
-- the same crossing detector (a dead band plus interpolation) that produces
the flight-level and ring crossings. The take-off and landing times of the
runway window method (``ATOT``/``ALDT``) are instead the first or last
report of a window, which places them systematically late or early.

``xing-runway-threshold`` needs one sample the traversal does not contain: the
threshold is the near edge of the runway polygon, so the point short of it is
outside the traversal by definition. The arrival sample window therefore reaches back
:data:`ARRIVAL_LEAD_SECONDS` before entry, and only for that one crossing --
entry, exit and every reported position stay bounded by the polygon.

**This module reads no table.** ``grid`` and ``thresholds`` arrive as
DataFrames, and ``sv`` must already carry the flight list's aerodrome array,
its ``adep``/``ades`` idents and the two field elevations. The join to the
flight list therefore happens once for every detector that needs it rather
than once per family, and every function here is testable on a laptop with no storage layer at all.
"""

from typing import TYPE_CHECKING, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

if TYPE_CHECKING:  # pragma: no cover - avoids an import cycle
    from opdi.config import EventConfig

from opdi.pipeline.crossings import missing_value_options, threshold_crossings
from opdi.pipeline.elevation import (
    field_elevation_ft,
    height_above_aerodrome_ft,
    height_above_elevation_ft,
)
from opdi.pipeline.runways import cross_track_nm
from opdi.utils.geometry import angle_between, bearing_deg, haversine_nm

FT_PER_M = 3.28084
FTMIN_PER_MPS = 196.850394
KT_PER_MPS = 1.94384

#: Half-width of the dead band around ``runway_airborne_height_ft``, in feet.
#: Not a configuration field because it is a property of the *signal*, not of
#: the milestone: barometric altitude at the field is noisy at the foot level,
#: and without a dead band a rotation that settles briefly re-crosses 15 ft and
#: emits a second ``airborne``. Five feet is a third of the threshold itself,
#: which is small enough that no real lift-off is straddled by it.
AIRBORNE_HYSTERESIS_FT = 5.0

#: Half-width of the dead band around the threshold plane, in nautical miles.
#: 0.05 NM is 304 ft, which at 140 kt (236 ft/s) is about 1.3 s. The band
#: only confirms a crossing; it does not move the reported instant.
#: ``xing-runway-threshold`` is interpolated to the sample pair that straddles
#: the bare plane, which is earlier than the confirming sample and unaffected
#: by how wide the band is.
#: What the width buys is that a sample sitting on the plane cannot toggle the
#: trigger.
THRESHOLD_PLANE_HYSTERESIS_NM = 0.05

#: Longest gap between two consecutive samples that a take-off roll may span,
#: in seconds. The hold that confirms ``take-off-roll`` is wall-clock -- the
#: time between the first fast sample and a later one -- and a traversal is
#: allowed to span a gap of up to ``airport_trace_gap_seconds`` (300 s), so
#: without this bound two fast samples either side of a four-minute coverage
#: hole satisfy a 5 s hold and stamp a roll that was never observed. Thirty
#: seconds is six times the 5 s nominal ADS-B cadence: generous enough that
#: ordinary reception dropouts do not break a real roll, short enough that the
#: hold measures continuous observation rather than elapsed time.
ROLL_MAX_GAP_SECONDS = 30.0

#: How far back before a traversal's entry the *arrival* sample window reaches,
#: in seconds.
#:
#: ``xing-runway-threshold`` (T16) is the crossing of the threshold plane, and
#: the threshold is the near edge of the runway polygon: the sample short of it is by
#: definition **outside** the polygon and so outside the traversal. Confirming
#: the crossing needs one, so the arrival window is extended backwards by this
#: much -- the pre-threshold final-approach samples the T16 crossing needs, and
#: nothing else. Without it the Schmitt trigger never sees a negative
#: along-track distance and ``xing-runway-threshold`` cannot fire at all in
#: production.
#:
#: Sixty seconds is about 2.3 NM at 140 kt: comfortably more than any runway's
#: displaced threshold or the gap between the polygon edge and the last
#: approach sample, and far short of the previous traversal an aircraft could
#: have made at the same aerodrome. Only :func:`_threshold_plane` reads the
#: extended window; entry, exit and every reported position stay bounded by the
#: polygon, because those are statements about occupancy.
ARRIVAL_LEAD_SECONDS = 60.0

#: The approach corridor's extent, mirrored from the ``h3_runway_zones`` grid
#: (:data:`opdi.reference.h3_runway_grid.APPROACH_NM` /
#: ``APPROACH_FAR_HALF_WIDTH_NM``). ``runway_traversals`` prunes on the grid
#: cells first, so these only bound the *geometry* check that follows -- they
#: reject a sample that shares an approach cell with this strip but sits outside
#: the corridor (a wide shared cell, a parallel runway's final). Duplicated as
#: literals rather than imported so this module keeps its no-reference-tables,
#: laptop-testable contract; they are bounds, not a published threshold, so a
#: small drift from the grid would only widen or narrow the reject margin.
APPROACH_CORRIDOR_NM = 3.0
APPROACH_CORRIDOR_HALF_WIDTH_NM = 0.5

#: Half-width of the dead band around the go-around trigger and recovery
#: heights, in feet. Wider than ``AIRBORNE_HYSTERESIS_FT`` because it covers a
#: whole excursion rather than an instant: an aircraft levelling at 500 ft on a
#: normal approach must not enter and leave the trigger repeatedly.
GOAROUND_HYSTERESIS_FT = 50.0

#: Heights above the airport, in feet, between which a report not flagged on
#: the ground shows the aircraft clearly in the air. On a measured worldwide
#: day, the reports not flagged on the ground in the two minutes after a
#: landing roll misread as a take-off were at most 25 ft above the airport (the pressure of
#: the day, and altitudes rounded to 25 or 100 ft), while a light aircraft
#: climbing out of a circuit passes 150 to 300 ft within the approach area at
#: the far end of the runway; hence the lower bound. A garbled altitude reads
#: tens of thousands of feet, hence the upper one. A pass with two such
#: reports that ends on the ground is a take-off and a later landing, not a
#: landing roll. Properties of the signal, like :data:`AIRBORNE_HYSTERESIS_FT`.
CLEARLY_AIRBORNE_MIN_FT = 100.0
CLEARLY_AIRBORNE_MAX_FT = 3000.0

#: The track's ground altitude at an airport (see
#: :func:`_ground_altitude_ft`). A report is evidence of the ground when it is
#: flagged on the ground, or slower than :data:`GROUND_EVIDENCE_MAX_KT` with a
#: vertical rate of at most :data:`GROUND_EVIDENCE_MAX_FTMIN`. Reports with an
#: altitude within :data:`GROUND_NEAR_SECONDS` of such a report are read too,
#: because airliners send no altitude on the ground: their last report before
#: touchdown and their first reports on the take-off roll are the nearest
#: altitudes to the ground there are. Of those, the highest at most
#: :data:`GROUND_PLATEAU_FT` above the lowest, and not climbing or descending
#: faster than :data:`GROUND_PLATEAU_MAX_FTMIN`, is the ground altitude;
#: altitudes come in steps of 25 ft, so the plateau of a take-off roll reads
#: as one or two steps. A ground altitude more than :data:`GROUND_MAX_OFFSET_FT` from the
#: airport's elevation is not a pressure error (about 27 ft per hPa, so
#: 1,000 ft is about 37 hPa from standard) and is not used. Properties of the
#: signal, like :data:`AIRBORNE_HYSTERESIS_FT`.
GROUND_EVIDENCE_MAX_KT = 30.0
GROUND_EVIDENCE_MAX_FTMIN = 300.0
GROUND_NEAR_SECONDS = 30.0
GROUND_PLATEAU_FT = 30.0
GROUND_PLATEAU_MAX_FTMIN = 500.0
GROUND_MAX_OFFSET_FT = 1000.0

#: How far back before a departure pass the reports are read, in seconds,
#: to tell whether the take-off started on the ground
#: (:attr:`~opdi.config.EventConfig.runway_line_up_needs_ground_report`).
#: Sixty seconds holds the taxi onto the runway, or the final approach of a
#: touch-and-go.
DEPARTURE_LEAD_SECONDS = 60.0

#: Speed, in knots, below which a report without altitude counts as on the
#: ground for a go-around (a landing roll or a taxi).
GOAROUND_GROUND_MAX_KT = 50.0

#: What a traversal is, for windowing purposes.
#:
#: ``apt_ident`` must be in the key. ``trace_id`` restarts at 0 for every
#: ``(track_id, strip_id, rwy_ident)``, and runway designators repeat between
#: airports (a flight can use a runway 07 at both ends), so without the airport
#: two passages at different airports would share one key.
#:
#: The four together are unique: a runway *direction* belongs to exactly one
#: strip at one aerodrome, and ``trace_id`` separates repeat traversals of it.
TRAVERSAL_KEY = ("track_id", "apt_ident", "rwy_ident", "trace_id")

#: Constant-per-traversal columns carried through :func:`threshold_crossings`
#: by riding along in ``partition_cols``. They do not change the partitioning
#: -- they are functionally dependent on :data:`TRAVERSAL_KEY` -- and carrying
#: them this way avoids a second join back to the traversal frame purely to
#: rebuild ``info``. The same trick ``calculate_ring_crossing_events`` uses for
#: the aerodrome position. Disjoint from :data:`TRAVERSAL_KEY`, because the two
#: are concatenated into one ``partition_cols`` list.
TRAVERSAL_INFO = (
    "traversal_class",
    "align_deg",
    "max_gs_kt",
    "osn_flight_id",
    # The runway's true bearing. Functionally dependent on ``rwy_ident``, like
    # everything else here, and it has to ride along rather than be rejoined:
    # ``_info`` builds the published JSON on the far side of
    # ``threshold_crossings``, which keeps only the columns it is told to.
    "rwy_bearing",
    # Whether heights were measured above the track's own ground altitude
    # (``track``) or the airport's elevation (``elevation``); NULL, and left
    # out of ``info``, without
    # :attr:`~opdi.config.EventConfig.runway_height_above_track_ground`.
    "height_reference",
)

#: The standard event frame, as ``events.py`` shapes it. Declared explicitly so
#: a disabled configuration can return an empty frame of exactly this shape
#: rather than ``None``: a caller that unions families must never have to
#: branch on nullity.
MILESTONE_SCHEMA = StructType(
    [
        StructField("track_id", StringType()),
        StructField("type", StringType()),
        StructField("event_time", TimestampType()),
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType()),
        StructField("altitude_ft", DoubleType()),
        StructField("cumulative_distance_nm", DoubleType()),
        StructField("cumulative_time_s", DoubleType()),
        StructField("info", StringType()),
    ]
)

#: Event type of the moment an arriving aircraft crosses the runway threshold
#: (ICAO milestone T16), a few seconds before the wheels touch the runway.
#: The ``landing`` of release v0.0.2 is a different event: the report at which
#: the flight phase changes from descent to ground.
RUNWAY_THRESHOLD_CROSSING = "xing-runway-threshold"

#: ICAO milestone numbers. ``go-around`` and the two runway crossings have
#: none, and are absent rather than invented.
MILESTONE_NUMBERS = {
    "line-up": "T06",
    "take-off-roll": "T07",
    "airborne": "T08",
    "ATOT": "T08",
    RUNWAY_THRESHOLD_CROSSING: "T16",
    "touchdown": "T17",
    "ALDT": "T17",
    "runway-vacated": "T19",
}

#: What the lift-off and touchdown instants are called. Under
#: ``merge_duplicate_milestones`` they take the ``ATOT``/``ALDT`` names and
#: carry ``info.method = "acdm"``, so they merge with the legacy detector's
#: output for the same two questions rather than sitting beside it.
ACDM_NAMES = {"airborne": "ATOT", "touchdown": "ALDT"}


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def classify_traversal(
    align_deg,
    max_gs_kt,
    entry_height_ft,
    exit_height_ft,
    duration_seconds,
    config: "EventConfig",
    exit_on_ground=None,
    last_on_ground=None,
    air_reports=None,
    crossing=None,
):
    """Departure, arrival, crossing -- or NULL, which means abstain.

    The three classes cannot overlap: ``runway_align_max_deg`` (30 deg) and
    ``runway_cross_min_deg`` (45 deg) leave a band in between where the
    detector says nothing, and the speed limits separate a rolling aircraft from
    a taxiing one. Abstention is a deliberate output, not a gap: naming the
    wrong milestone corrupts a movement count, naming none is an explicit null
    a consumer can see.

    With :attr:`~opdi.config.EventConfig.runway_take_off_needs_air_report`,
    a pass that ends on the ground is not a departure:

    * ``exit_on_ground``: the report that gives ``exit_height_ft`` is flagged
      on the ground. The transponder says the wheels are on the runway, so a
      height above ``runway_airborne_height_ft`` there is an altitude error,
      not a lift-off;
    * ``last_on_ground``: the last report of the pass is flagged on the
      ground, and fewer than two reports of the pass (``air_reports``) are in
      the air between :data:`CLEARLY_AIRBORNE_MIN_FT` and
      :data:`CLEARLY_AIRBORNE_MAX_FT`. A pass that climbed that high and
      ends on the ground is a take-off followed by a landing on the same
      runway less than ``airport_trace_gap_seconds`` later (a circuit), and
      its take-off stays.

    A NULL flag counts as not on the ground.

    ``crossing``, when given, is the crossing test computed by the caller
    (:func:`_corrected_traversals` reads it from the reports on the runway
    only) and replaces the one built here from the other arguments.
    """
    aligned = align_deg <= F.lit(config.runway_align_max_deg)
    fast = max_gs_kt > F.lit(config.runway_roll_speed_kt)
    airborne_h = F.lit(config.runway_airborne_height_ft)

    departure = aligned & fast & (entry_height_ft < airborne_h) & (exit_height_ft >= airborne_h)
    if config.runway_take_off_needs_air_report:
        if exit_on_ground is not None:
            departure = departure & ~F.coalesce(exit_on_ground, F.lit(False))
        if last_on_ground is not None and air_reports is not None:
            ends_low_on_ground = F.coalesce(last_on_ground, F.lit(False)) & (
                F.coalesce(air_reports, F.lit(0)) < F.lit(2)
            )
            departure = departure & ~ends_low_on_ground
    arrival = aligned & fast & (entry_height_ft >= airborne_h) & (exit_height_ft < airborne_h)
    if crossing is None:
        crossing = (
            (align_deg >= F.lit(config.runway_cross_min_deg))
            & (max_gs_kt <= F.lit(config.runway_taxi_speed_kt))
            & (duration_seconds <= F.lit(config.runway_crossing_max_seconds))
            & (entry_height_ft < airborne_h)
            & (exit_height_ft < airborne_h)
        )
    return (
        F.when(departure, F.lit("departure"))
        .when(arrival, F.lit("arrival"))
        .when(crossing, F.lit("crossing"))
    )


# ---------------------------------------------------------------------------
# Traversals
# ---------------------------------------------------------------------------


def _col_or_null(sdf: DataFrame, name: str, dtype: str):
    """``name`` if ``sdf`` carries it, otherwise a typed NULL.

    Used only for ``flight_id``, which step 04 derives from the callsign and a
    benchmark harness may not have. A missing callsign must leave
    ``osn_flight_id`` null, not make the whole family unavailable.
    """
    return F.col(name) if name in sdf.columns else F.lit(None).cast(dtype)


def along_track_nm(lat, lon, thr_lat, thr_lon, rwy_bearing):
    """Signed along-track distance from a runway threshold, in nautical miles.

    Positive towards the far end of the runway, negative on the approach short
    of the threshold. Two things read the sign: the geometric occupancy test,
    which admits a sample only when its along-track distance lies between the
    two thresholds; and ``xing-runway-threshold`` (T16), which is the instant
    this distance changes sign and so interpolates linearly across the
    threshold plane. It is the companion of
    :func:`~opdi.pipeline.runways.cross_track_nm` -- one gives distance
    *along* the centreline, the other distance *across* it.
    """
    d = haversine_nm(thr_lat, thr_lon, lat, lon)
    brg = bearing_deg(thr_lat, thr_lon, lat, lon)
    return d * F.cos(F.radians(brg - rwy_bearing))


def runway_traversals(
    sv: DataFrame,
    grid: DataFrame,
    thresholds: DataFrame,
    config: "EventConfig",
) -> DataFrame:
    """One row per (track, runway, traversal), classified.

    ``sv`` must already carry ``adep``/``ades`` and
    ``elev_adep_ft``/``elev_ades_ft`` (from
    ``vertical_pru.attach_aerodrome_geometry``; the idents are what let a
    height be measured against the traversal's *own* field) and ``apt`` -- the
    flight list's aerodrome array, built exactly as ``calculate_airport_events``
    builds it from ``adep``/``ades``/``adep_p``/``ades_p``. Both are attached
    once by the caller, so the join to the flight list and OurAirports happens a
    single time for every detector that needs it rather than once per family.

    ``grid`` is ``h3_runway_zones`` -- one row per (res-12 cell, strip), tagged
    ``zone`` ``"runway"`` or ``"approach"``. An equi-join on the cell prunes
    ``sv`` to the neighbourhood of a runway at every covered aerodrome, which is
    a wider coverage than the airport layout polygons
    (``hexaero_airport_layouts``) give.
    ``thresholds`` is :func:`runways.runway_thresholds` -- one row per runway
    *direction*, carrying ``rwy_length_nm``/``rwy_half_width_nm``.

    **A traversal is built per runway *direction*, and an arrival's samples span
    its approach corridor, not just its runway occupancy.** Each cell that
    survives the prune is matched against *both* directions of its strip, and a
    sample joins a direction when it is either

    * on that direction's runway -- ``zone == "runway"`` with along-track in
      ``[0, length]`` and cross-track within the half-width (occupancy); or
    * on that direction's *approach* -- ``zone == "approach"`` with a
      **negative** along-track (short of that threshold) inside the corridor.

    The negative-along-track condition keeps this from polluting departures
    and crossings: a departing aircraft's own direction has no samples short of
    its threshold, so it is still classified from runway occupancy exactly as
    before; only the *landing* direction of a descending track picks up the
    approach corridor. That corridor is the fix for the arrival gap -- a real
    arrival whose surface samples are sparse or nulled by cleaning has no
    on-runway flare sample above the airborne height, so a traversal built from
    occupancy alone never satisfies ``classify_traversal``'s arrival test and no
    ``touchdown`` is ever emitted. Reaching back across the descent gives the
    classification a high entry (on final) and a low exit (after the flare),
    which is the shape an arrival actually has.
    """
    grid_cols = grid.select(
        F.col("h3_id"),
        F.col("apt_icao"),
        F.col("strip_id"),
        F.col("le_ident"),
        F.col("he_ident"),
        F.col("zone"),
    )
    # Not broadcast. ``grid_cols`` is the network-wide h3_runway_zones (about
    # 7 million cells); a network-wide run leaves it near its full size, which
    # does not fit in the driver's memory. Spark chooses the join itself, and
    # broadcasts the grid only when it is small enough.
    near = sv.join(
        grid_cols,
        (sv.h3_res_12 == grid_cols.h3_id) & F.array_contains(sv.apt, grid_cols.apt_icao),
        "inner",
    )

    # Both directions of the strip, at the sample level: the along-track sign
    # is direction-specific, and it is the sign that decides whether an
    # approach sample belongs to a direction. ``le``/``he`` bound the join to
    # this strip's two directions rather than every runway at the aerodrome.
    dir_thr = thresholds.select(
        F.col("apt_ident").alias("_t_apt"),
        F.col("rwy_ident"),
        F.col("thr_lat"),
        F.col("thr_lon"),
        F.col("rwy_bearing"),
        F.col("rwy_length_nm"),
        F.col("rwy_half_width_nm"),
    )
    if _traversal_corrections_on(config) and "thr_elev_ft" in thresholds.columns:
        # The threshold's own elevation, for an airport the flight list names
        # only as an alternative (see ``own_field_elevation_ft``).
        dir_thr = thresholds.select(
            F.col("apt_ident").alias("_t_apt"),
            F.col("rwy_ident"),
            F.col("thr_lat"),
            F.col("thr_lon"),
            F.col("rwy_bearing"),
            F.col("rwy_length_nm"),
            F.col("rwy_half_width_nm"),
            F.col("thr_elev_ft"),
        )
    cand = near.join(
        F.broadcast(dir_thr),
        (F.col("apt_icao") == F.col("_t_apt"))
        & ((F.col("rwy_ident") == F.col("le_ident")) | (F.col("rwy_ident") == F.col("he_ident"))),
        "inner",
    ).drop("_t_apt")
    cand = cand.withColumn(
        "_along",
        along_track_nm(
            F.col("lat"),
            F.col("lon"),
            F.col("thr_lat"),
            F.col("thr_lon"),
            F.col("rwy_bearing"),
        ),
    ).withColumn(
        "_cross",
        cross_track_nm(
            F.col("lat"),
            F.col("lon"),
            F.col("thr_lat"),
            F.col("thr_lon"),
            F.col("rwy_bearing"),
        ),
    )

    # Occupancy is measured from the LANDING threshold (``0 <= along``), so a
    # runway cell short of the threshold -- pre-threshold paved area, a
    # displaced threshold -- is intentionally outside it. That is a known,
    # small late bias on ``line-up``/``take-off-roll`` (the roll may begin a few
    # metres behind the painted threshold), traded for a clean, published
    # definition of "on the runway" that both directions and the arrival
    # threshold-plane crossing agree on.
    on_runway = (
        (F.col("zone") == F.lit("runway"))
        & (F.col("_along") >= F.lit(0.0))
        & (F.col("_along") <= F.col("rwy_length_nm"))
        & (F.col("_cross") <= F.col("rwy_half_width_nm"))
    )
    # The approach corridor, for the *landing* direction only: short of this
    # threshold (negative along-track), inside the grid-rasterised corridor.
    on_approach = (
        (F.col("zone") == F.lit("approach"))
        & (F.col("_along") < F.lit(0.0))
        & (F.col("_along") >= -F.lit(APPROACH_CORRIDOR_NM))
        & (F.col("_cross") <= F.lit(APPROACH_CORRIDOR_HALF_WIDTH_NM))
    )
    if _traversal_corrections_on(config):
        return _corrected_traversals(sv, cand, on_runway, on_approach, thresholds, config)

    work = cand.filter(on_runway | on_approach).drop("_along", "_cross")

    # One direction is one candidate traversal, so the session -- and the trace
    # id -- is per direction. An arrival's approach samples are contiguous in
    # time with its runway samples, so they fall in one trace; the reciprocal
    # direction sees only the runway occupancy and its own (empty here) approach.
    ordered = Window.partitionBy("track_id", "strip_id", "rwy_ident").orderBy("event_time")
    work = work.withColumn(
        "_gap",
        F.col("event_time").cast("long") - F.lag(F.col("event_time").cast("long")).over(ordered),
    )
    work = work.withColumn(
        "trace_id",
        F.sum(F.when(F.col("_gap") > F.lit(config.airport_trace_gap_seconds), 1).otherwise(0))
        .over(ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .cast("string"),
    )

    # Measured against **this traversal's own aerodrome**, not against the more
    # permissive of the flight's two fields. The classification's height limits
    # are thresholds that get reported, not a membership test: measured against
    # the higher field, every movement at the lower one has a negative height,
    # `classify_traversal` abstains, and the whole departure or arrival is
    # silently absent. See `elevation.height_above_field_ft`'s warning.
    work = work.withColumn("_h_ft", height_above_aerodrome_ft(work, F.col("apt_icao")))
    work = work.withColumn("_gs_kt", F.col("velocity") * F.lit(KT_PER_MPS))
    work = work.withColumn("_alt_ft", F.col("baro_altitude_c") * F.lit(FT_PER_M))
    work = work.withColumn("_flight_id", _col_or_null(sv, "flight_id", "string"))
    work = work.withColumn("_on_ground", _col_or_null(sv, "on_ground", "boolean"))

    # ``min_by``/``max_by``, never ``first``/``last``: the latter take partition
    # order, not time order (see ``airport_events_ordered``). For an arrival the earliest sample is
    # now on final and the latest after the flare, so ``entry_height_ft`` is the
    # height on approach and ``exit_height_ft`` the height after touchdown --
    # the two readings ``classify_traversal`` needs, from the descent rather
    # than from scarce on-runway samples. The callsign is aggregated rather than
    # grouped on for the reason ``calculate_airport_events`` documents at length.
    agg = work.groupBy(
        "track_id",
        "apt_icao",
        "strip_id",
        "rwy_ident",
        "thr_lat",
        "thr_lon",
        "rwy_bearing",
        "rwy_length_nm",
        "rwy_half_width_nm",
        "le_ident",
        "he_ident",
        "trace_id",
    ).agg(
        F.min("event_time").alias("entry_time"),
        F.max("event_time").alias("exit_time"),
        F.max("_gs_kt").alias("max_gs_kt"),
        F.expr("percentile_approx(heading, 0.5)").alias("median_track_deg"),
        # First and last height **among samples that have one**. Ordering on a
        # bare ``event_time`` takes the earliest/latest sample's height
        # outright -- but ADS-B surface position messages carry no barometric
        # altitude, so a ground-roll or post-touchdown sample has NULL
        # ``_h_ft``. When that null sample is the earliest (a departure's roll)
        # or latest (an arrival's rollout), ``min_by``/``max_by`` on
        # ``event_time`` return NULL, and ``classify_traversal``'s
        # ``entry_height_ft < airborne_h`` is then NULL -> false: the whole
        # movement is silently dropped. Masking the ordering key to the
        # non-null rows takes the first/last height that actually exists.
        F.min_by("_h_ft", F.when(F.col("_h_ft").isNotNull(), F.col("event_time"))).alias(
            "entry_height_ft"
        ),
        F.max_by("_h_ft", F.when(F.col("_h_ft").isNotNull(), F.col("event_time"))).alias(
            "exit_height_ft"
        ),
        # The ground flag of the report that gives ``exit_height_ft``: a
        # height read on a report flagged on the ground is not a lift-off.
        F.coalesce(
            F.max_by("_on_ground", F.when(F.col("_h_ft").isNotNull(), F.col("event_time"))),
            F.lit(False),
        ).alias("exit_on_ground"),
        # The ground flag of the last report of the pass, with or without a
        # height, and the reports clearly in the air (see ``classify_traversal``).
        F.coalesce(F.max_by("_on_ground", "event_time"), F.lit(False)).alias("last_on_ground"),
        F.sum(
            F.when(
                ~F.coalesce(F.col("_on_ground"), F.lit(False))
                & (F.col("_h_ft") > F.lit(CLEARLY_AIRBORNE_MIN_FT))
                & (F.col("_h_ft") < F.lit(CLEARLY_AIRBORNE_MAX_FT)),
                1,
            ).otherwise(0)
        ).alias("air_reports"),
        F.min_by("lat", "event_time").alias("entry_lat"),
        F.min_by("lon", "event_time").alias("entry_lon"),
        F.max_by("lat", "event_time").alias("exit_lat"),
        F.max_by("lon", "event_time").alias("exit_lon"),
        F.min_by("_alt_ft", "event_time").alias("entry_altitude_ft"),
        F.max_by("_alt_ft", "event_time").alias("exit_altitude_ft"),
        F.min_by("_flight_id", "event_time").alias("osn_flight_id"),
        F.count(F.lit(1)).alias("n_samples"),
    )
    agg = agg.withColumn(
        "duration_seconds",
        (F.col("exit_time").cast("double") - F.col("entry_time").cast("double")),
    )

    # Folded to [0, 90] so a reciprocal counts as aligned. This is the
    # *classification* angle and it is deliberately direction-blind.
    cand2 = agg.withColumn(
        "align_deg",
        F.least(
            angle_between(F.col("median_track_deg"), F.col("rwy_bearing")),
            angle_between(F.col("median_track_deg"), F.col("rwy_bearing") + F.lit(180.0)),
        ),
    )
    # The *direction* angle, unfolded. It is a separate quantity from
    # ``align_deg`` and has to be: folded, the two ends of one strip score
    # identically, so nothing but the alphabet would choose between 07 and 25.
    cand2 = cand2.withColumn(
        "bearing_error_deg",
        angle_between(F.col("median_track_deg"), F.col("rwy_bearing")),
    )
    cand2 = cand2.withColumn(
        "cross_track_nm",
        cross_track_nm(
            F.col("entry_lat"),
            F.col("entry_lon"),
            F.col("thr_lat"),
            F.col("thr_lon"),
            F.col("rwy_bearing"),
        ),
    )
    # Rounded to the metre before it is ordered on. Cross-track distance picks
    # the *strip* -- parallel runways are hundreds of metres apart -- but the
    # two ends of one strip share a centreline and differ only in floating
    # point noise. Unrounded, that noise would decide the direction before the
    # bearing error was ever consulted.
    cand2 = cand2.withColumn("_xt", F.round(F.col("cross_track_nm"), 3))
    cand2 = cand2.withColumn(
        "class",
        classify_traversal(
            F.col("align_deg"),
            F.col("max_gs_kt"),
            F.col("entry_height_ft"),
            F.col("exit_height_ft"),
            F.col("duration_seconds"),
            config,
            exit_on_ground=F.col("exit_on_ground"),
            last_on_ground=F.col("last_on_ground"),
            air_reports=F.col("air_reports"),
        ),
    )
    # Classify before the direction pick and drop the abstentions first, so a
    # direction that classifies is never lost to a better-aligned one that
    # abstains. Both directions of a genuine movement classify the same way;
    # the pick then chooses between them by geometry.
    cand2 = cand2.filter(F.col("class").isNotNull())

    best = Window.partitionBy("track_id", "strip_id", "trace_id").orderBy(
        F.col("_xt").asc(),
        F.col("bearing_error_deg").asc(),
        F.col("rwy_ident").asc(),
    )
    out = (
        cand2.withColumn("_r", F.row_number().over(best)).filter(F.col("_r") == 1).drop("_r", "_xt")
    )

    return out.select(
        F.col("track_id"),
        F.col("apt_icao").alias("apt_ident"),
        F.col("rwy_ident"),
        F.col("rwy_bearing"),
        F.col("thr_lat"),
        F.col("thr_lon"),
        F.col("strip_id"),
        F.col("trace_id"),
        F.col("entry_time"),
        F.col("exit_time"),
        F.col("duration_seconds"),
        F.col("max_gs_kt"),
        F.col("median_track_deg"),
        F.col("align_deg"),
        F.col("bearing_error_deg"),
        F.col("cross_track_nm"),
        F.col("entry_height_ft"),
        F.col("exit_height_ft"),
        F.col("exit_on_ground"),
        F.col("last_on_ground"),
        F.col("air_reports"),
        F.col("entry_lat"),
        F.col("entry_lon"),
        F.col("exit_lat"),
        F.col("exit_lon"),
        F.col("entry_altitude_ft"),
        F.col("exit_altitude_ft"),
        F.col("n_samples"),
        F.col("osn_flight_id"),
        F.col("class"),
    )


# ---------------------------------------------------------------------------
# Traversals with the corrections of work package 2
# ---------------------------------------------------------------------------


def _traversal_corrections_on(config: "EventConfig") -> bool:
    """Whether any setting that changes how traversals are built is on."""
    return bool(
        config.runway_height_above_track_ground
        or config.runway_direction_by_motion
        or config.runway_events_on_runway_reports
        or config.runway_crossing_ground_without_altitude
    )


def own_field_elevation_ft(sdf: DataFrame, apt_ident):
    """The published elevation of the aerodrome ``apt_ident`` itself.

    :func:`~opdi.pipeline.elevation.field_elevation_ft` for the flight's two
    airports. For another airport of the flight's airport list (a nearby
    airport the flight list names as an alternative), the elevation of the
    runway threshold (``thr_elev_ft``, from OurAirports) rather than the
    higher of the flight's two airports, which can be hundreds of feet off.
    """
    two_fields = field_elevation_ft(sdf, apt_ident)
    if "thr_elev_ft" not in sdf.columns or "adep" not in sdf.columns:
        return two_fields
    named = (apt_ident == F.col("adep")) | (apt_ident == F.col("ades"))
    return F.when(F.coalesce(named, F.lit(False)), two_fields).otherwise(
        F.coalesce(F.col("thr_elev_ft"), two_fields)
    )


def _ground_altitude_ft(work: DataFrame, alt_ft, elevation_ft, gap_seconds: float = 300.0):
    """The track's barometric altitude on the ground at the airport, in feet.

    One value per visit of the airport: per ``(track_id, apt_icao)`` and
    stretch of the pass frame without a gap of more than ``gap_seconds``
    (a landing and the take-off after a turnaround are two visits). Returned
    as a column of ``work`` (window functions, so no join). Read from the
    reports of the pass frame (runway and approach area of the airport):

    * evidence of the ground: a report flagged on the ground, or slower than
      :data:`GROUND_EVIDENCE_MAX_KT` with a vertical rate of at most
      :data:`GROUND_EVIDENCE_MAX_FTMIN`;
    * the altitudes read: those of such reports, and those of reports within
      :data:`GROUND_NEAR_SECONDS` of one (airliners send no altitude on the
      ground, so the last report before touchdown and the first reports of
      the take-off roll stand for it);
    * the value, from the reports that show the ground when they carry
      altitudes: the highest of their altitudes at most
      :data:`GROUND_PLATEAU_FT` above the lowest; otherwise, from the reports
      near them: the highest altitude at most :data:`GROUND_PLATEAU_FT` from
      the altitude nearest in time to a report on the ground. Reports
      climbing or descending faster than :data:`GROUND_PLATEAU_MAX_FTMIN` are
      left out, except that first one. Altitudes come in steps of 25 ft, so a
      take-off roll reads two values, and the higher is taken: measured from
      the lower, the roll itself would read 25 ft, above
      ``runway_airborne_height_ft``. The lowest altitude is not the start for
      the reports near the ground: at rotation the altitude often dips by
      25 to 50 ft for a report or two.

    NULL when the track has no such report at this airport, or when the value
    is more than :data:`GROUND_MAX_OFFSET_FT` from ``elevation_ft``.
    """
    t = F.col("event_time").cast("double")
    # One value per visit of the airport: the reports of the pass frame split
    # where more than ``gap_seconds`` pass between two of them (a landing and
    # the take-off after a turnaround read the ground apart, and the air
    # pressure changes in between).
    visits = Window.partitionBy("track_id", "apt_icao").orderBy("event_time")
    work = work.withColumn(
        "_g_visit",
        F.sum(F.when(t - F.lag(t).over(visits) > F.lit(float(gap_seconds)), 1).otherwise(0)).over(
            visits.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )
    by_apt = Window.partitionBy("track_id", "apt_icao", "_g_visit")
    ordered = by_apt.orderBy("event_time")
    gs_kt = F.col("velocity") * F.lit(KT_PER_MPS)
    vr = F.abs(_col_or_null(work, "vert_rate", "double") * F.lit(FTMIN_PER_MPS))
    evidence = F.coalesce(F.col("_on_ground"), F.lit(False)) | F.coalesce(
        (gs_kt < F.lit(GROUND_EVIDENCE_MAX_KT))
        & (F.coalesce(vr, F.lit(0.0)) <= F.lit(GROUND_EVIDENCE_MAX_FTMIN)),
        F.lit(False),
    )
    ev_t = F.when(evidence, t)
    prev_ev = F.last(ev_t, ignorenulls=True).over(
        ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    next_ev = F.first(ev_t, ignorenulls=True).over(
        ordered.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )
    # A report descending or climbing fast is not on the plateau (the flare
    # before a touchdown), unless it is the lowest there is.
    steady = F.coalesce(vr <= F.lit(GROUND_PLATEAU_MAX_FTMIN), F.lit(True))

    def plateau_top(values):
        low = F.min(values).over(by_apt)
        on_plateau = (values <= low + F.lit(GROUND_PLATEAU_FT)) & (steady | (values == low))
        top = F.max(F.when(on_plateau, values)).over(by_apt)
        return F.when(F.abs(top - elevation_ft) <= F.lit(GROUND_MAX_OFFSET_FT), top)

    # The reports that show the ground, where they carry an altitude.
    direct = plateau_top(F.when(evidence, alt_ft))

    # Else the reports near them, starting from the altitude nearest in time
    # to a report on the ground (the last before a touchdown, the first of the
    # take-off roll), not from the lowest: at rotation the altitude often dips
    # by 25 to 50 ft for a report or two.
    # Each window result is kept as a column before the next window reads it.
    work = work.withColumn("_g_prev", prev_ev).withColumn("_g_next", next_ev)
    has_alt = (
        evidence
        | F.coalesce(t - F.col("_g_prev") <= F.lit(GROUND_NEAR_SECONDS), F.lit(False))
        | F.coalesce(F.col("_g_next") - t <= F.lit(GROUND_NEAR_SECONDS), F.lit(False))
    ) & alt_ft.isNotNull()
    gap = F.least(
        F.coalesce(t - F.col("_g_prev"), F.lit(1.0e9)),
        F.coalesce(F.col("_g_next") - t, F.lit(1.0e9)),
    )
    work = work.withColumn("_g_has", has_alt).withColumn(
        "_g_near",
        F.min(F.when(F.col("_g_has"), F.struct(gap.alias("gap"), alt_ft.alias("alt")))).over(
            by_apt
        )["alt"],
    )
    on_step = (
        F.col("_g_has")
        & (F.abs(alt_ft - F.col("_g_near")) <= F.lit(GROUND_PLATEAU_FT))
        & (steady | (alt_ft == F.col("_g_near")))
    )
    work = work.withColumn("_g_top", F.max(F.when(on_step, alt_ft)).over(by_apt))
    nearby = F.when(
        F.abs(F.col("_g_top") - elevation_ft) <= F.lit(GROUND_MAX_OFFSET_FT), F.col("_g_top")
    )
    work = work.withColumn("_g_direct", direct)
    direct = F.col("_g_direct")
    work = work.withColumn("_g_ref", F.coalesce(direct, nearby))
    return work.drop("_g_prev", "_g_next", "_g_has", "_g_near", "_g_top", "_g_direct", "_g_visit")


def _corrected_traversals(
    sv: DataFrame,
    cand: DataFrame,
    on_runway,
    on_approach,
    thresholds: DataFrame,
    config: "EventConfig",
) -> DataFrame:
    """:func:`runway_traversals` with the settings of work package 2.

    Each setting changes one part and leaves the others as they are:

    * :attr:`~opdi.config.EventConfig.runway_height_above_track_ground`:
      heights above the track's ground altitude at the airport
      (:func:`_ground_altitude_ft`), else above the airport's own elevation
      (:func:`own_field_elevation_ft`);
    * :attr:`~opdi.config.EventConfig.runway_direction_by_motion`: passes
      numbered per runway (both directions together), the direction named by
      the motion, one pass per runway, class and number, and one runway among
      parallel ones;
    * :attr:`~opdi.config.EventConfig.runway_events_on_runway_reports`: the
      times and positions of the first and last report on the runway itself,
      and crossings classified from those reports only;
    * :attr:`~opdi.config.EventConfig.runway_crossing_ground_without_altitude`:
      a report flagged on the ground without altitude is at height 0 for a
      crossing, and no crossing is kept in a pass that is a take-off or a
      landing on the same runway.
    """
    by_motion = config.runway_direction_by_motion
    on_rwy_only = config.runway_events_on_runway_reports
    ground_x = config.runway_crossing_ground_without_altitude

    work = (
        cand.withColumn("_on_rwy", on_runway)
        .filter(on_runway | on_approach)
        .withColumn("_alt_ft", F.col("baro_altitude_c") * F.lit(FT_PER_M))
        .withColumn("_gs_kt", F.col("velocity") * F.lit(KT_PER_MPS))
        .withColumn("_flight_id", _col_or_null(sv, "flight_id", "string"))
        .withColumn("_on_ground", _col_or_null(sv, "on_ground", "boolean"))
        .withColumn("_cum_nm", _col_or_null(sv, "cumulative_distance_nm", "double"))
        .withColumn("_cum_s", _col_or_null(sv, "cumulative_time_s", "double"))
    )
    if "thr_elev_ft" not in work.columns:
        work = work.withColumn("thr_elev_ft", F.lit(None).cast("double"))

    if config.runway_height_above_track_ground:
        elevation = own_field_elevation_ft(work, F.col("apt_icao"))
        work = work.withColumn("_elev_ft", elevation)
        work = _ground_altitude_ft(
            work,
            F.col("_alt_ft"),
            F.col("_elev_ft"),
            gap_seconds=float(config.airport_trace_gap_seconds),
        )
        work = work.withColumn("_ref_ft", F.coalesce(F.col("_g_ref"), F.col("_elev_ft")))
        work = work.withColumn(
            "_h_ref",
            F.when(F.col("_g_ref").isNotNull(), F.lit("track")).otherwise(F.lit("elevation")),
        )
        work = work.withColumn("_h_ft", F.col("_alt_ft") - F.col("_ref_ft"))
    else:
        work = work.withColumn("_ref_ft", F.lit(None).cast("double"))
        work = work.withColumn("_h_ref", F.lit(None).cast("string"))
        work = work.withColumn("_h_ft", height_above_aerodrome_ft(work, F.col("apt_icao")))

    # Pass numbers: per runway (both directions together) with the motion
    # setting, so the two readings of one passage carry one number; per
    # direction otherwise.
    session_keys = (
        ("track_id", "apt_icao", "strip_id") if by_motion else ("track_id", "strip_id", "rwy_ident")
    )
    ordered = Window.partitionBy(*session_keys).orderBy("event_time")
    work = work.withColumn(
        "_gap",
        F.col("event_time").cast("long") - F.lag(F.col("event_time").cast("long")).over(ordered),
    )
    work = work.withColumn(
        "trace_id",
        F.sum(F.when(F.col("_gap") > F.lit(config.airport_trace_gap_seconds), 1).otherwise(0))
        .over(ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .cast("string"),
    )

    # A report flagged on the ground with no altitude is on the ground, for
    # the crossing test only.
    if ground_x:
        work = work.withColumn(
            "_hx",
            F.when(
                F.col("_h_ft").isNull() & F.coalesce(F.col("_on_ground"), F.lit(False)),
                F.lit(0.0),
            ).otherwise(F.col("_h_ft")),
        )
    else:
        work = work.withColumn("_hx", F.col("_h_ft"))

    has_h = F.when(F.col("_h_ft").isNotNull(), F.col("event_time"))
    rwy_t = F.when(F.col("_on_rwy"), F.col("event_time"))
    rwy_hx_t = F.when(F.col("_on_rwy") & F.col("_hx").isNotNull(), F.col("event_time"))
    hx_t = F.when(F.col("_hx").isNotNull(), F.col("event_time"))
    agg = work.groupBy(
        "track_id",
        "apt_icao",
        "strip_id",
        "rwy_ident",
        "thr_lat",
        "thr_lon",
        "rwy_bearing",
        "rwy_length_nm",
        "rwy_half_width_nm",
        "le_ident",
        "he_ident",
        "trace_id",
    ).agg(
        F.min("event_time").alias("entry_time"),
        F.max("event_time").alias("exit_time"),
        F.max("_gs_kt").alias("max_gs_kt"),
        F.expr("percentile_approx(heading, 0.5)").alias("median_track_deg"),
        F.min_by("_h_ft", has_h).alias("entry_height_ft"),
        F.max_by("_h_ft", has_h).alias("exit_height_ft"),
        F.coalesce(F.max_by("_on_ground", has_h), F.lit(False)).alias("exit_on_ground"),
        F.coalesce(F.max_by("_on_ground", "event_time"), F.lit(False)).alias("last_on_ground"),
        F.sum(
            F.when(
                ~F.coalesce(F.col("_on_ground"), F.lit(False))
                & (F.col("_h_ft") > F.lit(CLEARLY_AIRBORNE_MIN_FT))
                & (F.col("_h_ft") < F.lit(CLEARLY_AIRBORNE_MAX_FT)),
                1,
            ).otherwise(0)
        ).alias("air_reports"),
        F.min_by("lat", "event_time").alias("entry_lat"),
        F.min_by("lon", "event_time").alias("entry_lon"),
        F.max_by("lat", "event_time").alias("exit_lat"),
        F.max_by("lon", "event_time").alias("exit_lon"),
        F.min_by("_alt_ft", "event_time").alias("entry_altitude_ft"),
        F.max_by("_alt_ft", "event_time").alias("exit_altitude_ft"),
        F.min_by("_flight_id", "event_time").alias("osn_flight_id"),
        F.count(F.lit(1)).alias("n_samples"),
        # The ground altitude and which reference it is (constant per pass).
        F.first("_ref_ft", ignorenulls=True).alias("ground_ref_ft"),
        F.first("_h_ref", ignorenulls=True).alias("height_reference"),
        # The motion: the median track of the fast reports (rolling,
        # climbing out, on final), where a heading is known.
        F.expr(
            f"percentile_approx(CASE WHEN _gs_kt >= {float(config.runway_roll_speed_kt)} "
            "THEN heading END, 0.5)"
        ).alias("fast_track_deg"),
        F.expr("percentile_approx(_cross, 0.5)").alias("median_cross_nm"),
        # The reports on the runway itself.
        F.sum(F.when(F.col("_on_rwy"), 1).otherwise(0)).alias("n_rwy"),
        F.min(rwy_t).alias("rwy_entry_time"),
        F.max(rwy_t).alias("rwy_exit_time"),
        F.min_by("lat", rwy_t).alias("rwy_entry_lat"),
        F.min_by("lon", rwy_t).alias("rwy_entry_lon"),
        F.min_by("_alt_ft", rwy_t).alias("rwy_entry_altitude_ft"),
        F.min_by("_cum_nm", rwy_t).alias("rwy_entry_cum_nm"),
        F.min_by("_cum_s", rwy_t).alias("rwy_entry_cum_s"),
        F.max_by("lat", rwy_t).alias("rwy_exit_lat"),
        F.max_by("lon", rwy_t).alias("rwy_exit_lon"),
        F.max_by("_alt_ft", rwy_t).alias("rwy_exit_altitude_ft"),
        F.max_by("_cum_nm", rwy_t).alias("rwy_exit_cum_nm"),
        F.max_by("_cum_s", rwy_t).alias("rwy_exit_cum_s"),
        F.max(F.when(F.col("_on_rwy"), F.col("_gs_kt"))).alias("rwy_max_gs_kt"),
        F.expr("percentile_approx(CASE WHEN _on_rwy THEN heading END, 0.5)").alias("rwy_track_deg"),
        F.min_by("_hx", rwy_hx_t).alias("rwy_entry_height_ft"),
        F.max_by("_hx", rwy_hx_t).alias("rwy_exit_height_ft"),
        F.min_by("_hx", hx_t).alias("x_entry_height_ft"),
        F.max_by("_hx", hx_t).alias("x_exit_height_ft"),
    )
    agg = agg.withColumn(
        "duration_seconds",
        (F.col("exit_time").cast("double") - F.col("entry_time").cast("double")),
    )

    def folded(track):
        return F.least(
            angle_between(track, F.col("rwy_bearing")),
            angle_between(track, F.col("rwy_bearing") + F.lit(180.0)),
        )

    cand2 = agg.withColumn("align_deg", folded(F.col("median_track_deg")))
    cand2 = cand2.withColumn(
        "bearing_error_deg", angle_between(F.col("median_track_deg"), F.col("rwy_bearing"))
    )
    cand2 = cand2.withColumn(
        "cross_track_nm",
        cross_track_nm(
            F.col("entry_lat"),
            F.col("entry_lon"),
            F.col("thr_lat"),
            F.col("thr_lon"),
            F.col("rwy_bearing"),
        ),
    )
    cand2 = cand2.withColumn("_xt", F.round(F.col("cross_track_nm"), 3))

    # The crossing test, on the reports on the runway with
    # ``runway_events_on_runway_reports``, else on the whole pass; heights
    # with the ground flag standing in for a missing altitude under
    # ``runway_crossing_ground_without_altitude``.
    if on_rwy_only:
        x_align = folded(F.col("rwy_track_deg"))
        x_gs = F.col("rwy_max_gs_kt")
        x_dur = F.col("rwy_exit_time").cast("double") - F.col("rwy_entry_time").cast("double")
        x_entry_h, x_exit_h = F.col("rwy_entry_height_ft"), F.col("rwy_exit_height_ft")
        x_present = F.col("n_rwy") >= F.lit(1)
    else:
        x_align, x_gs, x_dur = F.col("align_deg"), F.col("max_gs_kt"), F.col("duration_seconds")
        x_entry_h, x_exit_h = F.col("x_entry_height_ft"), F.col("x_exit_height_ft")
        x_present = F.lit(True)
    airborne_h = F.lit(config.runway_airborne_height_ft)
    crossing = (
        x_present
        & (x_align >= F.lit(config.runway_cross_min_deg))
        & (x_gs <= F.lit(config.runway_taxi_speed_kt))
        & (x_dur <= F.lit(config.runway_crossing_max_seconds))
        & (x_entry_h < airborne_h)
        & (x_exit_h < airborne_h)
    )
    cand2 = cand2.withColumn(
        "class",
        classify_traversal(
            F.col("align_deg"),
            F.col("max_gs_kt"),
            F.col("entry_height_ft"),
            F.col("exit_height_ft"),
            F.col("duration_seconds"),
            config,
            exit_on_ground=F.col("exit_on_ground"),
            last_on_ground=F.col("last_on_ground"),
            air_reports=F.col("air_reports"),
            crossing=crossing,
        ),
    )
    cand2 = cand2.filter(F.col("class").isNotNull())
    move = F.col("class").isin("departure", "arrival")

    if ground_x:
        # A take-off, a landing or the turn-off after it is not a crossing of
        # the same runway: drop a crossing reading of a pass that also reads
        # as a movement on this runway.
        same_pass = Window.partitionBy("track_id", "apt_icao", "strip_id", "trace_id")
        cand2 = cand2.withColumn(
            "_pass_moves", F.max(F.when(move, 1).otherwise(0)).over(same_pass)
        ).filter(move | (F.col("_pass_moves") == 0))
        cand2 = cand2.drop("_pass_moves")

    if not by_motion:
        best = Window.partitionBy("track_id", "strip_id", "trace_id").orderBy(
            F.col("_xt").asc(),
            F.col("bearing_error_deg").asc(),
            F.col("rwy_ident").asc(),
        )
        out = (
            cand2.withColumn("_r", F.row_number().over(best))
            .filter(F.col("_r") == 1)
            .drop("_r", "_xt")
        )
    else:
        out = _by_motion(cand2.drop("_xt"), thresholds, move)

    return out.select(
        F.col("track_id"),
        F.col("apt_icao").alias("apt_ident"),
        F.col("rwy_ident"),
        F.col("rwy_bearing"),
        F.col("thr_lat"),
        F.col("thr_lon"),
        F.col("strip_id"),
        F.col("trace_id"),
        F.col("entry_time"),
        F.col("exit_time"),
        F.col("duration_seconds"),
        F.col("max_gs_kt"),
        F.col("median_track_deg"),
        F.col("align_deg"),
        F.col("bearing_error_deg"),
        F.col("cross_track_nm"),
        F.col("entry_height_ft"),
        F.col("exit_height_ft"),
        F.col("exit_on_ground"),
        F.col("last_on_ground"),
        F.col("air_reports"),
        F.col("entry_lat"),
        F.col("entry_lon"),
        F.col("exit_lat"),
        F.col("exit_lon"),
        F.col("entry_altitude_ft"),
        F.col("exit_altitude_ft"),
        F.col("n_samples"),
        F.col("osn_flight_id"),
        F.col("class"),
        F.col("ground_ref_ft"),
        F.col("height_reference"),
        F.col("n_rwy"),
        F.col("rwy_entry_time"),
        F.col("rwy_exit_time"),
        F.col("rwy_entry_lat"),
        F.col("rwy_entry_lon"),
        F.col("rwy_entry_altitude_ft"),
        F.col("rwy_entry_cum_nm"),
        F.col("rwy_entry_cum_s"),
        F.col("rwy_exit_lat"),
        F.col("rwy_exit_lon"),
        F.col("rwy_exit_altitude_ft"),
        F.col("rwy_exit_cum_nm"),
        F.col("rwy_exit_cum_s"),
    )


def _by_motion(cand2: DataFrame, thresholds: DataFrame, move) -> DataFrame:
    """Name each movement by its direction of motion and keep one per pass.

    1. The motion is the median track of the fast reports (``fast_track_deg``),
       else of all reports. A take-off or landing read in the direction more
       than 90 degrees from it is renamed to the opposite direction of the
       same runway (its threshold and bearing).
    2. Per runway, pass number and class, one reading is kept: for a
       take-off, the reading over the runway and the climb-out beyond its far
       end (the one that was renamed); for a landing, the reading over the
       approach and the runway (not renamed). Then the smallest bearing error.
    3. Of passes of one class at one airport whose times overlap (parallel
       runways), the one with reports on its runway and the smallest median
       distance from its centre line.
    """
    motion = F.coalesce(F.col("fast_track_deg"), F.col("median_track_deg"))
    cand2 = cand2.withColumn(
        "_renamed",
        F.coalesce(
            move & (angle_between(motion, F.col("rwy_bearing")) > F.lit(90.0)), F.lit(False)
        ),
    )
    recip = thresholds.select(
        F.col("apt_ident").alias("_o_apt"),
        F.col("rwy_ident").alias("_o_ident"),
        F.col("thr_lat").alias("_o_lat"),
        F.col("thr_lon").alias("_o_lon"),
        F.col("rwy_bearing").alias("_o_bearing"),
    )
    other = F.when(F.col("rwy_ident") == F.col("le_ident"), F.col("he_ident")).otherwise(
        F.col("le_ident")
    )
    cand2 = cand2.withColumn("_other", other).join(
        F.broadcast(recip),
        (F.col("apt_icao") == F.col("_o_apt")) & (F.col("_other") == F.col("_o_ident")),
        "left",
    )
    # Renamed only where the opposite direction exists in the thresholds.
    swap = F.col("_renamed") & F.col("_o_ident").isNotNull()
    for target, source in (
        ("rwy_ident", "_o_ident"),
        ("thr_lat", "_o_lat"),
        ("thr_lon", "_o_lon"),
        ("rwy_bearing", "_o_bearing"),
    ):
        cand2 = cand2.withColumn(target, F.when(swap, F.col(source)).otherwise(F.col(target)))
    cand2 = cand2.withColumn("_renamed", swap).drop(
        "_other", "_o_apt", "_o_ident", "_o_lat", "_o_lon", "_o_bearing"
    )
    cand2 = cand2.withColumn("bearing_error_deg", angle_between(motion, F.col("rwy_bearing")))

    natural = F.when(F.col("class") == "departure", F.col("_renamed")).otherwise(~F.col("_renamed"))
    per_pass = Window.partitionBy("track_id", "apt_icao", "strip_id", "trace_id", "class").orderBy(
        natural.desc(), F.col("bearing_error_deg").asc_nulls_last(), F.col("rwy_ident").asc()
    )
    cand2 = cand2.withColumn("_r", F.row_number().over(per_pass)).filter(F.col("_r") == 1)

    # Parallel runways: passes of one class at one airport whose times
    # overlap are one movement.
    by_time = Window.partitionBy("track_id", "apt_icao", "class").orderBy(
        "entry_time", "exit_time", "strip_id"
    )
    cand2 = cand2.withColumn(
        "_prev_exit",
        F.max("exit_time").over(by_time.rowsBetween(Window.unboundedPreceding, -1)),
    )
    cand2 = cand2.withColumn(
        "_group",
        F.sum(
            F.when(
                F.col("_prev_exit").isNull() | (F.col("entry_time") > F.col("_prev_exit")), 1
            ).otherwise(0)
        ).over(by_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )
    one = Window.partitionBy("track_id", "apt_icao", "class", "_group").orderBy(
        (F.col("n_rwy") > 0).desc(),
        F.col("median_cross_nm").asc_nulls_last(),
        F.col("strip_id").asc(),
        F.col("rwy_ident").asc(),
    )
    return (
        cand2.withColumn("_r", F.row_number().over(one))
        .filter(F.col("_r") == 1)
        .drop("_r", "_renamed", "_prev_exit", "_group")
    )


# ---------------------------------------------------------------------------
# Milestones
# ---------------------------------------------------------------------------


def _session(sdf: DataFrame):
    """The DataFrame's SparkSession, for building an empty frame."""
    return getattr(sdf, "sparkSession", None) or sdf.sql_ctx.sparkSession


def _empty_milestones(sdf: DataFrame) -> DataFrame:
    """An empty frame of the standard event shape.

    Returned rather than ``None`` when a family is switched off, so a caller
    unioning families never branches on nullity -- the same contract
    ``crossings._empty_result`` provides for a configuration with no rings.
    """
    return _session(sdf).createDataFrame([], MILESTONE_SCHEMA)


def _info(milestone: Optional[str], method: Optional[str] = None, bracket: bool = False):
    """The ``info`` JSON every runway milestone carries.

    ``milestone`` is the ICAO number (``T16`` for ``xing-runway-threshold``).
    The classification, the runway and the alignment travel with it so the
    decision that produced the row is auditable from the row.

    ``method`` names the detector on the two types the merge makes ambiguous:
    ``ATOT`` and ``ALDT`` may come from here (``"acdm"``) or from the legacy
    window detector (``"legacy"``), and the two have different biases. It is
    NULL on every type with only one source, where there is nothing to
    disambiguate.

    With ``bracket``, ``bracket_s`` gives the seconds between the two reports
    the time was interpolated between (``bracket_seconds`` of
    :func:`opdi.pipeline.crossings.threshold_crossings`).
    """
    return F.to_json(
        F.struct(
            # ``runway`` is the canonical key: the legacy ATOT/ALDT detector writes
            # it under that name, and under ``merge_duplicate_milestones`` both
            # detectors publish the same ``type``, so the runway of an ATOT event
            # is read the same way whichever detector gave it.
            # ``rwy_ident`` is kept beside it as the name this family shipped with.
            F.col("rwy_ident").alias("runway"),
            F.col("rwy_ident").alias("rwy_ident"),
            # The centreline's TRUE bearing, from the threshold geometry -- a
            # property of the pavement, identical for every movement on it. Not a
            # heading: a heading is magnetic and wind-corrected, so two aircraft
            # using this runway in the same minute report different ones. The
            # designator fixes the direction only to the nearest ten degrees, and
            # is null whenever the runway could not be named, so this is what makes
            # "which way was the runway used" answerable.
            F.col("rwy_bearing").cast("double").alias("runway_bearing_deg"),
            F.col("apt_ident").alias("apt_icao"),
            F.col("traversal_class").alias("traversal_class"),
            F.col("align_deg").alias("align_deg"),
            F.col("max_gs_kt").alias("max_gs_kt"),
            F.col("osn_flight_id").alias("osn_flight_id"),
            F.lit(milestone).cast("string").alias("milestone"),
            F.lit(method).cast("string").alias("method"),
            (
                F.col("bracket_seconds").cast("double") if bracket else F.lit(None).cast("double")
            ).alias("bracket_s"),
            F.col("height_reference").cast("string").alias("height_reference"),
        )
    )


def _event(
    sdf: DataFrame,
    type_: str,
    time_col: str,
    method: Optional[str] = None,
    bracket: bool = False,
) -> DataFrame:
    """Project onto the standard event frame (``bracket``: see :func:`_info`)."""
    return sdf.select(
        F.col("track_id"),
        F.lit(type_).alias("type"),
        F.col(time_col).alias("event_time"),
        F.col("lon").cast("double").alias("lon"),
        F.col("lat").cast("double").alias("lat"),
        F.col("altitude_ft").cast("double").alias("altitude_ft"),
        F.col("cumulative_distance_nm").cast("double").alias("cumulative_distance_nm"),
        F.col("cumulative_time_s").cast("double").alias("cumulative_time_s"),
        _info(MILESTONE_NUMBERS.get(type_), method, bracket).alias("info"),
    )


#: Columns carried across the traversal join so the height can be computed on
#: the far side of it -- where the traversal's own ``apt_ident`` is known -- and
#: dropped again immediately afterwards.
_HEIGHT_INPUTS = ("adep", "ades", "elev_adep_ft", "elev_ades_ft")


def _traversal_samples(
    sv: DataFrame,
    traversals: DataFrame,
    lead_seconds: float = 0.0,
    departure_lead_seconds: float = 0.0,
) -> DataFrame:
    """The state vectors belonging to each traversal, one row per pair.

    A range join rather than a second sessionisation: the traversal frame
    already says which interval each movement occupies, and re-deriving it here
    would let the two disagree about the very thing the classification was
    made on.

    ``height_ft`` is computed **after** the join, not before it. Before the
    join there is no ``apt_ident``, so the only height available is the
    permissive two-field minimum -- which is the wrong reference for every
    threshold the milestones are defined by. See
    :func:`~opdi.pipeline.elevation.height_above_field_ft`.

    ``lead_seconds`` extends the window backwards for ``arrival`` traversals
    only, and the rows it admits are flagged ``_in_polygon = False``. One join
    serves both readings: :func:`_threshold_plane` takes the extended frame
    because the T16 crossing happens short of the threshold and therefore
    outside the polygon, and everything else filters to ``_in_polygon``,
    because entry, exit and the reported positions are statements about
    occupancy. See :data:`ARRIVAL_LEAD_SECONDS`.

    ``departure_lead_seconds`` does the same for ``departure`` traversals
    (:data:`DEPARTURE_LEAD_SECONDS`), for the test of whether a take-off
    started on the ground.

    Where the traversal carries ``ground_ref_ft`` (the track's ground
    altitude, see :func:`_ground_altitude_ft`), ``height_ft`` is measured
    above it.
    """
    carried = [c for c in _HEIGHT_INPUTS if c in sv.columns]
    s = sv.select(
        F.col("track_id").alias("_s_track"),
        F.col("event_time"),
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
        (F.col("baro_altitude_c") * F.lit(FT_PER_M)).alias("altitude_ft"),
        F.col("baro_altitude_c"),
        *[F.col(c) for c in carried],
        (F.col("velocity") * F.lit(KT_PER_MPS)).alias("gs_kt"),
        F.col("cumulative_distance_nm").cast("double").alias("cumulative_distance_nm"),
        F.col("cumulative_time_s").cast("double").alias("cumulative_time_s"),
        _col_or_null(sv, "on_ground", "boolean").alias("on_ground"),
    )
    t = traversals.select(
        F.col("track_id"),
        F.col("apt_ident"),
        F.col("rwy_ident"),
        F.col("rwy_bearing"),
        F.col("thr_lat"),
        F.col("thr_lon"),
        F.col("trace_id"),
        F.col("entry_time"),
        F.col("exit_time"),
        F.col("max_gs_kt"),
        F.col("align_deg"),
        _col_or_null(traversals, "osn_flight_id", "string").alias("osn_flight_id"),
        F.col("class").alias("traversal_class"),
        _col_or_null(traversals, "height_reference", "string").alias("height_reference"),
        _col_or_null(traversals, "ground_ref_ft", "double").alias("ground_ref_ft"),
    )
    t = t.withColumn(
        "_window_start",
        F.when(
            F.col("traversal_class") == F.lit("arrival"),
            F.timestamp_seconds(F.col("entry_time").cast("double") - F.lit(float(lead_seconds))),
        )
        .when(
            (F.col("traversal_class") == F.lit("departure"))
            & F.lit(float(departure_lead_seconds) > 0.0),
            F.timestamp_seconds(
                F.col("entry_time").cast("double") - F.lit(float(departure_lead_seconds))
            ),
        )
        .otherwise(F.col("entry_time")),
    )
    joined = t.join(
        s,
        (F.col("_s_track") == t["track_id"])
        & (F.col("event_time") >= t["_window_start"])
        & (F.col("event_time") <= t["exit_time"]),
        "inner",
    ).drop("_s_track", "_window_start")
    joined = joined.withColumn("_in_polygon", F.col("event_time") >= F.col("entry_time"))
    height = height_above_aerodrome_ft(joined, F.col("apt_ident"))
    height = F.when(
        F.col("ground_ref_ft").isNotNull(),
        F.col("baro_altitude_c") * F.lit(FT_PER_M) - F.col("ground_ref_ft"),
    ).otherwise(height)
    return joined.withColumn("height_ft", height).drop("baro_altitude_c", "ground_ref_ft", *carried)


def _at_extreme(sdf: DataFrame, ascending: bool) -> DataFrame:
    """The sample at the start (or end) of each traversal, whole.

    ``row_number`` over an explicit ``event_time`` ordering rather than
    ``first``/``last`` in a ``groupBy``: those take partition order, so the
    reported position need not be the position at the reported time.
    """
    order = F.col("event_time").asc() if ascending else F.col("event_time").desc()
    w = Window.partitionBy(*TRAVERSAL_KEY).orderBy(order)
    return sdf.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1).drop("_r")


def _roll_start(sdf: DataFrame, config: "EventConfig", speed_col: str = "gs_kt") -> DataFrame:
    """The sample at which the take-off roll *began*.

    Run-length over the ordered window: mark the fast samples, sessionise where
    fastness begins, and measure each run from its own start.

    **The hold is a confirmation criterion, not a delay.**
    ``runway_roll_min_seconds`` decides *whether* a run of fast samples is a
    take-off roll; it does not decide *when* the roll started, and the answer to
    that is the run's first sample. Reporting the sample at which the hold was
    satisfied instead would put ``take-off-roll`` one hold-duration plus up to a
    sample interval late on every departure -- a systematic bias, always in the
    same direction, of exactly the kind this module's docstring indicts
    ``ATOT`` for. So the run is qualified on its *maximum* hold and then
    reported at its *first* row, which also keeps the position and the
    timestamp on the same sample.

    A single fast sample -- a high-speed turn-off, a spike in the velocity
    field -- forms a run whose maximum hold is zero and never qualifies, which
    is what the threshold is for.

    The run also breaks across a gap wider than :data:`ROLL_MAX_GAP_SECONDS`.
    Without that, two fast samples either side of a coverage hole are a run
    holding for the width of the hole, and a traversal is allowed to span up to
    ``airport_trace_gap_seconds`` of one: the hold has to measure continuous
    observation, not elapsed time.

    ``speed_col`` is the ground speed in knots that is read (the reported
    one by default).
    """
    ordered = Window.partitionBy(*TRAVERSAL_KEY).orderBy("event_time")
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    work = sdf.withColumn("_fast", F.col(speed_col) >= F.lit(config.runway_roll_speed_kt))
    work = work.withColumn("_prev_fast", F.lag("_fast").over(ordered))
    work = work.withColumn(
        "_gap",
        F.col("event_time").cast("double")
        - F.lag(F.col("event_time").cast("double")).over(ordered),
    )
    work = work.withColumn(
        "_new_run",
        F.when(
            F.col("_fast")
            & (
                ~F.coalesce(F.col("_prev_fast"), F.lit(False))
                | (F.coalesce(F.col("_gap"), F.lit(0.0)) > F.lit(ROLL_MAX_GAP_SECONDS))
            ),
            1,
        ).otherwise(0),
    )
    work = work.withColumn("_run", F.sum("_new_run").over(running))
    work = work.filter(F.col("_fast"))

    run_w = Window.partitionBy(*TRAVERSAL_KEY, "_run")
    work = work.withColumn("_run_start", F.min("event_time").over(run_w))
    work = work.withColumn(
        "_held",
        F.col("event_time").cast("double") - F.col("_run_start").cast("double"),
    )
    # Qualify the whole run, then report its first row: the confirmation and
    # the timestamp are deliberately different rows.
    work = work.withColumn("_run_held", F.max("_held").over(run_w))
    work = work.filter(F.col("_run_held") >= F.lit(config.runway_roll_min_seconds))
    return (
        work.withColumn("_r", F.row_number().over(ordered))
        .filter(F.col("_r") == 1)
        .drop(
            "_r",
            "_fast",
            "_prev_fast",
            "_gap",
            "_new_run",
            "_run",
            "_run_start",
            "_held",
            "_run_held",
        )
    )


def _position_between_reports(sdf: DataFrame, config: "EventConfig") -> DataFrame:
    """Give a report without a position the one interpolated between its neighbours.

    Within each runway pass, a report with no ``lat``/``lon`` takes the
    position interpolated in time between the last report with a position
    before it and the first one after it, when both are at most
    :attr:`~opdi.config.EventConfig.crossing_value_max_gap_seconds` (30 s
    when that is not set) away. Read by the lift-off and touchdown only
    (:attr:`~opdi.config.EventConfig.runway_times_position_between_reports`).
    """
    gap = float(config.crossing_value_max_gap_seconds or 30.0)
    t = F.col("event_time").cast("double")
    order = Window.partitionBy(*TRAVERSAL_KEY).orderBy(t)
    positioned = F.when(
        F.col("lat").isNotNull() & F.col("lon").isNotNull(),
        F.struct(t.alias("t"), F.col("lat").alias("lat"), F.col("lon").alias("lon")),
    )
    prev = F.last(positioned, ignorenulls=True).over(
        order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    nxt = F.first(positioned, ignorenulls=True).over(
        order.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )
    out = sdf.withColumn("_pp", prev).withColumn("_pn", nxt)
    p, n = F.col("_pp"), F.col("_pn")
    near = (
        F.col("lat").isNull()
        & p.isNotNull()
        & n.isNotNull()
        & (n["t"] > p["t"])
        & (t - p["t"] <= F.lit(gap))
        & (n["t"] - t <= F.lit(gap))
    )
    frac = (t - p["t"]) / (n["t"] - p["t"])
    return (
        out.withColumn(
            "lat", F.when(near, p["lat"] + frac * (n["lat"] - p["lat"])).otherwise(F.col("lat"))
        )
        .withColumn(
            "lon",
            # Along the shorter way round the 180th meridian.
            F.when(
                near,
                F.pmod(
                    p["lon"]
                    + frac * (F.pmod(n["lon"] - p["lon"] + F.lit(540.0), F.lit(360.0)) - 180.0)
                    + F.lit(540.0),
                    F.lit(360.0),
                )
                - F.lit(180.0),
            ).otherwise(F.col("lon")),
        )
        .drop("_pp", "_pn")
    )


def _height_crossing(sdf: DataFrame, config: "EventConfig", direction: str) -> DataFrame:
    """The interpolated crossing of ``runway_airborne_height_ft``.

    ``up`` is the lift-off, ``down`` the touchdown. Same engine, same
    threshold, opposite sense -- which is the point: the two milestones are one
    geometric fact read in two directions, and giving them separate detectors
    would let them drift apart.

    Over reports without altitude, the crossing is placed between the two
    known heights only when they are at most
    ``runway_airborne_max_bracket_seconds`` apart, shorter than the limit of
    the other crossings (``track_hole_seconds``): near the runway the height
    changes fast.
    """
    options = missing_value_options(config)
    if options["over_missing_values"]:
        options["max_bracket_seconds"] = float(config.runway_airborne_max_bracket_seconds)
    if config.runway_times_position_between_reports:
        sdf = _position_between_reports(sdf, config)
        # A report still without a position takes no part in the crossing,
        # which is then placed between reports that have both.
        sdf = sdf.withColumn(
            "height_ft",
            F.when(F.col("lat").isNotNull() & F.col("lon").isNotNull(), F.col("height_ft")),
        )
    return threshold_crossings(
        sdf,
        value_col="height_ft",
        thresholds=[config.runway_airborne_height_ft],
        hysteresis=AIRBORNE_HYSTERESIS_FT,
        partition_cols=list(TRAVERSAL_KEY) + list(TRAVERSAL_INFO),
        interpolate_cols=(
            "lat",
            "lon",
            "altitude_ft",
            "cumulative_distance_nm",
            "cumulative_time_s",
        ),
        up_label="up",
        down_label="down",
        interpolate=True,
        all_occurrences=True,
        **options,
    ).filter(F.col("direction") == direction)


def _threshold_plane(sdf: DataFrame, config: "EventConfig") -> DataFrame:
    """The interpolated instant the aircraft crosses the runway threshold.

    Along-track distance from the threshold,
    ``d = haversine_nm(thr, P) * cos(bearing(thr, P) - rwy_bearing)``, is
    negative short of the threshold and positive beyond it, so the crossing is
    a sign change and interpolates linearly in exactly the way the height
    crossings do.

    Only crossings above ``runway_airborne_height_ft`` are kept. An aircraft
    rolling past the threshold on the ground -- backtracking, or vacating via a
    turn-off beyond it -- has already landed, and stamping a second
    ``xing-runway-threshold`` there would double the arrival count.
    """
    work = sdf.withColumn(
        "along_track_nm",
        haversine_nm(F.col("thr_lat"), F.col("thr_lon"), F.col("lat"), F.col("lon"))
        * F.cos(
            F.radians(
                bearing_deg(F.col("thr_lat"), F.col("thr_lon"), F.col("lat"), F.col("lon"))
                - F.col("rwy_bearing")
            )
        ),
    )
    crossings = threshold_crossings(
        work,
        value_col="along_track_nm",
        thresholds=[0.0],
        hysteresis=THRESHOLD_PLANE_HYSTERESIS_NM,
        partition_cols=list(TRAVERSAL_KEY) + list(TRAVERSAL_INFO),
        interpolate_cols=(
            "lat",
            "lon",
            "altitude_ft",
            "height_ft",
            "cumulative_distance_nm",
            "cumulative_time_s",
        ),
        up_label="inbound",
        down_label="outbound",
        interpolate=True,
        all_occurrences=True,
        **missing_value_options(config),
    )
    return crossings.filter(
        (F.col("direction") == "inbound")
        & (F.col("height_ft") >= F.lit(config.runway_airborne_height_ft))
    )


def _with_speed(sdf: DataFrame) -> DataFrame:
    """Add ``_speed_kt``: the reported ground speed, else one from positions.

    The speed from positions is the distance from the previous report of the
    traversal frame that has a position, over the time between the two, when
    that is more than 0 and at most :data:`ROLL_MAX_GAP_SECONDS`. It is given
    on reports with a position only. Ground reports almost never carry a
    speed (about 1 in 100 on a measured day), while their positions show the
    take-off roll clearly.
    """
    ordered = Window.partitionBy(*TRAVERSAL_KEY).orderBy("event_time")
    before = ordered.rowsBetween(Window.unboundedPreceding, -1)
    positioned = F.col("lat").isNotNull() & F.col("lon").isNotNull()
    prev = F.last(
        F.when(
            positioned,
            F.struct(
                F.col("event_time").cast("double").alias("t"),
                F.col("lat").alias("lat"),
                F.col("lon").alias("lon"),
            ),
        ),
        ignorenulls=True,
    ).over(before)
    work = sdf.withColumn("_prev_pos", prev)
    dt = F.col("event_time").cast("double") - F.col("_prev_pos.t")
    derived = F.when(
        positioned & (dt > F.lit(0.0)) & (dt <= F.lit(ROLL_MAX_GAP_SECONDS)),
        haversine_nm(F.col("_prev_pos.lat"), F.col("_prev_pos.lon"), F.col("lat"), F.col("lon"))
        / (dt / F.lit(3600.0)),
    )
    return work.withColumn("_speed_kt", F.coalesce(F.col("gs_kt"), derived)).drop("_prev_pos")


def _ground_starts(dep: DataFrame, config: "EventConfig") -> DataFrame:
    """The departure traversals that start on the ground, as their keys.

    ``dep`` holds the departure samples with ``_speed_kt`` (see
    :func:`_with_speed`), including the :data:`DEPARTURE_LEAD_SECONDS`
    before the traversal (``_in_polygon`` false). The *fast run* starts at
    the first report of the traversal at :attr:`runway_roll_speed_kt` or
    more (or at its end, when there is none). A take-off starts on the ground
    when, before the fast run, a report is flagged on the ground or slower
    than that speed; or, when no fast report in the air comes before the
    fast run (reception starts during the roll), when the first fast report
    is flagged on the ground. A touch-and-go has its final approach before
    the fast run, a track that reappears in the air has neither.
    """
    roll_kt = F.lit(config.runway_roll_speed_kt)
    key = Window.partitionBy(*TRAVERSAL_KEY)
    flagged = F.coalesce(F.col("on_ground"), F.lit(False))
    fast_in = F.col("_in_polygon") & F.coalesce(F.col("_speed_kt") >= roll_kt, F.lit(False))
    work = dep.withColumn(
        "_anchor",
        F.coalesce(
            F.min(F.when(fast_in, F.col("event_time"))).over(key),
            F.max("event_time").over(key),
        ),
    )
    before = F.col("event_time") < F.col("_anchor")
    ground_like = flagged | F.coalesce(F.col("_speed_kt") < roll_kt, F.lit(False))
    air_fast = ~flagged & F.coalesce(F.col("_speed_kt") >= roll_kt, F.lit(False))
    agg = work.groupBy(*TRAVERSAL_KEY).agg(
        F.max(F.when(before & ground_like, 1).otherwise(0)).alias("_ground_before"),
        F.max(F.when(before & air_fast, 1).otherwise(0)).alias("_air_before"),
        F.max(F.when((F.col("event_time") == F.col("_anchor")) & flagged, 1).otherwise(0)).alias(
            "_anchor_ground"
        ),
    )
    return agg.filter(
        (F.col("_ground_before") == 1)
        | ((F.col("_air_before") == 0) & (F.col("_anchor_ground") == 1))
    ).select(*TRAVERSAL_KEY)


def _on_runway_event(traversals: DataFrame, cls: str, type_: str, which: str) -> DataFrame:
    """An event at the first or last report on the runway of each traversal.

    ``which`` is ``"entry"`` (first) or ``"exit"`` (last); only traversals of
    class ``cls``; read from the traversal row. None when the traversal holds
    no report on the runway
    (:attr:`~opdi.config.EventConfig.runway_events_on_runway_reports`).
    """
    t = traversals.filter(
        (F.col("class") == F.lit(cls)) & F.col(f"rwy_{which}_time").isNotNull()
    ).select(
        *[F.col(c) for c in TRAVERSAL_KEY],
        F.col("rwy_bearing"),
        F.col("class").alias("traversal_class"),
        F.col("align_deg"),
        F.col("max_gs_kt"),
        F.col("osn_flight_id"),
        F.col("height_reference"),
        F.col(f"rwy_{which}_time").alias("event_time"),
        F.col(f"rwy_{which}_lat").alias("lat"),
        F.col(f"rwy_{which}_lon").alias("lon"),
        F.col(f"rwy_{which}_altitude_ft").alias("altitude_ft"),
        F.col(f"rwy_{which}_cum_nm").alias("cumulative_distance_nm"),
        F.col(f"rwy_{which}_cum_s").alias("cumulative_time_s"),
    )
    return _event(t, type_, "event_time")


def _last_low_threshold_crossing(crossings: DataFrame, config: "EventConfig") -> DataFrame:
    """The last threshold crossing of each landing traversal, if low enough.

    Crossings more than
    :attr:`~opdi.config.EventConfig.runway_threshold_crossing_max_height_ft`
    above the ground are dropped (a low pass, an overflight), then the last
    one before the end of the traversal is kept: earlier ones belong to a
    circuit or a low approach flown before the landing.
    """
    low = crossings.filter(
        F.col("height_ft") <= F.lit(config.runway_threshold_crossing_max_height_ft)
    )
    last = Window.partitionBy(*TRAVERSAL_KEY).orderBy(F.col("event_time").desc())
    return low.withColumn("_r", F.row_number().over(last)).filter(F.col("_r") == 1).drop("_r")


def _one_row_per_instant(events: DataFrame) -> DataFrame:
    """One row per ``(track_id, type, event_time)``.

    The event id is built from these three (and the version), so two rows
    with the same three would share an id. Kept: the first by ``info``, so
    the choice does not depend on how Spark split the data.
    """
    w = Window.partitionBy("track_id", "type", "event_time").orderBy(F.col("info").asc())
    return events.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1).drop("_r")


def runway_milestones(sv: DataFrame, traversals: DataFrame, config: "EventConfig") -> DataFrame:
    """The eight traversal-derived A-CDM milestones, as standard events.

    ``sv`` is the state vector frame carrying ``baro_altitude_c``, the two
    aerodrome idents, the two field elevations, ``velocity`` and the cumulative
    measures; ``traversals``
    is the output of :func:`runway_traversals`. Returns an *empty* frame of
    :data:`MILESTONE_SCHEMA` when the family is switched off, never ``None``.

    ``go-around`` is not here: it is emitted by :func:`go_arounds`, because a
    go-around may never touch a runway polygon and so has no traversal to hang
    from.
    """
    if not config.emit_runway_milestones:
        return _empty_milestones(sv)

    # One join, two readings. The polygon-bounded frame is what "the aircraft
    # was on the runway" means and is what every occupancy milestone reads; the
    # extended one exists solely so the threshold-plane crossing has a sample
    # short of the threshold to interpolate from. See ARRIVAL_LEAD_SECONDS.
    # Under the merge these two are ``ATOT``/``ALDT`` and are stamped with the
    # detector that produced them; without it they keep the A-CDM names and have no
    # second source to be told apart from.
    merged = config.merge_duplicate_milestones
    lift_off = ACDM_NAMES["airborne"] if merged else "airborne"
    touch_down = ACDM_NAMES["touchdown"] if merged else "touchdown"
    method = "acdm" if merged else None

    ground_start = config.runway_line_up_needs_ground_report
    on_rwy_only = config.runway_events_on_runway_reports and "rwy_entry_time" in traversals.columns
    corrected = _traversal_corrections_on(config) or (
        config.runway_roll_speed_from_positions
        or ground_start
        or config.runway_threshold_crossing_last_only
    )
    if corrected:
        # Kept for the step: about ten readings below take these two frames,
        # and computing them again for each would read the reports again.
        traversals = traversals.cache()
    samples = _traversal_samples(
        sv,
        traversals,
        lead_seconds=ARRIVAL_LEAD_SECONDS,
        departure_lead_seconds=DEPARTURE_LEAD_SECONDS if ground_start else 0.0,
    )
    if corrected:
        samples = samples.cache()
    bounded = samples.filter(F.col("_in_polygon"))

    dep = bounded.filter(F.col("traversal_class") == "departure")
    arr = bounded.filter(F.col("traversal_class") == "arrival")
    xing = bounded.filter(F.col("traversal_class") == "crossing")
    arr_approach = samples.filter(F.col("traversal_class") == "arrival")

    # The first report on the runway and the start of the roll; with
    # ``runway_line_up_needs_ground_report``, only for a take-off that started
    # on the ground.
    dep_trav = traversals
    if config.runway_roll_speed_from_positions or ground_start:
        dep_speed = _with_speed(samples.filter(F.col("traversal_class") == "departure"))
    if ground_start:
        starts = _ground_starts(dep_speed, config)
        dep_trav = traversals.join(starts, list(TRAVERSAL_KEY), "left_semi")
        dep_lined = dep.join(starts, list(TRAVERSAL_KEY), "left_semi")
        dep_speed = dep_speed.join(starts, list(TRAVERSAL_KEY), "left_semi")
    else:
        dep_lined = dep
    line_up = (
        _on_runway_event(dep_trav, "departure", "line-up", "entry")
        if on_rwy_only
        else _event(_at_extreme(dep_lined, True), "line-up", "entry_time")
    )
    if config.runway_roll_speed_from_positions:
        rolling = dep_speed.filter(
            F.col("_in_polygon")
            & F.col("lat").isNotNull()
            & F.col("lon").isNotNull()
            & F.col("_speed_kt").isNotNull()
        )
        roll = _event(_roll_start(rolling, config, "_speed_kt"), "take-off-roll", "event_time")
    else:
        roll = _event(_roll_start(dep_lined, config), "take-off-roll", "event_time")

    threshold = _threshold_plane(arr_approach, config)
    if config.runway_threshold_crossing_last_only:
        threshold = _last_low_threshold_crossing(threshold, config)

    parts = [
        # A departure: lined up, rolling, then off the deck.
        line_up,
        roll,
        _event(_height_crossing(dep, config, "up"), lift_off, "event_time", method, bracket=True),
        # An arrival: over the threshold, wheels down, off the strip.
        _event(threshold, RUNWAY_THRESHOLD_CROSSING, "event_time"),
        _event(
            _height_crossing(arr, config, "down"), touch_down, "event_time", method, bracket=True
        ),
        (
            _on_runway_event(traversals, "arrival", "runway-vacated", "exit")
            if on_rwy_only
            else _event(_at_extreme(arr, False), "runway-vacated", "exit_time")
        ),
        # A crossing is not a movement: it gets its two instants and nothing
        # else, so it can never be counted as one.
        (
            _on_runway_event(traversals, "crossing", "runway-crossing-entry", "entry")
            if on_rwy_only
            else _event(_at_extreme(xing, True), "runway-crossing-entry", "entry_time")
        ),
        (
            _on_runway_event(traversals, "crossing", "runway-crossing-vacated", "exit")
            if on_rwy_only
            else _event(_at_extreme(xing, False), "runway-crossing-vacated", "exit_time")
        ),
    ]

    out = parts[0]
    for part in parts[1:]:
        out = out.unionByName(part)
    if config.runway_direction_by_motion:
        out = _one_row_per_instant(out)
    return out


# ---------------------------------------------------------------------------
# Go-around
# ---------------------------------------------------------------------------


def _in_approach_corridor(
    excursion: DataFrame, thresholds: DataFrame, config: "EventConfig"
) -> DataFrame:
    """The excursions whose lowest point lies on a runway's extended centre line.

    The lowest point (``lat``/``lon``) is in the corridor of a runway of the
    destination (``ades``) when its along-track distance from that runway's
    threshold is between ``-goaround_corridor_length_nm`` and the runway's
    length, and its distance from the centre line is at most
    ``goaround_corridor_half_width_nm``. An excursion is kept when one runway
    has it in its corridor, and also when the destination has no runway in
    ``thresholds`` or the lowest point has no position: an unknown fact keeps
    the event.
    """
    thr = F.broadcast(
        thresholds.select(
            F.col("apt_ident").alias("_c_apt"),
            F.col("thr_lat").alias("_c_lat"),
            F.col("thr_lon").alias("_c_lon"),
            F.col("rwy_bearing").alias("_c_brg"),
            F.col("rwy_length_nm").alias("_c_len"),
        )
    )
    along = along_track_nm(
        F.col("lat"), F.col("lon"), F.col("_c_lat"), F.col("_c_lon"), F.col("_c_brg")
    )
    across = cross_track_nm(
        F.col("lat"), F.col("lon"), F.col("_c_lat"), F.col("_c_lon"), F.col("_c_brg")
    )
    inside = (
        (F.col("ades") == F.col("_c_apt"))
        & (along >= -F.lit(config.goaround_corridor_length_nm))
        & (along <= F.col("_c_len"))
        & (across <= F.lit(config.goaround_corridor_half_width_nm))
    )
    in_corridor = excursion.join(thr, inside, "left_semi")
    unknown = excursion.filter(F.col("lat").isNull() | F.col("lon").isNull()).unionByName(
        excursion.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull()).join(
            thr, F.col("ades") == F.col("_c_apt"), "left_anti"
        )
    )
    return in_corridor.unionByName(unknown)


def go_arounds(
    sv: DataFrame, config: "EventConfig", thresholds: Optional[DataFrame] = None
) -> DataFrame:
    """Go-arounds: a descent near the destination followed by a climb back.

    A go-around is a descent below ``goaround_trigger_height_ft`` near the
    destination that climbs back above ``goaround_recovery_height_ft`` with no
    touchdown between.

    Independent of the traversals, because a go-around may never touch a runway
    polygon at all -- an approach abandoned at 400 ft is still a go-around and
    the aircraft was never over the strip.

    Detected on height above field elevation with the same Schmitt trigger the
    crossings use, so a trajectory oscillating around the trigger height emits
    one event rather than a burst. ``go-around`` is stamped at the *lowest*
    point of the excursion -- the instant the approach was abandoned, which is
    the operationally meaningful one and the only one a controller would
    recognise.

    "No touchdown between" is evaluated as the excursion never reaching
    ``runway_airborne_height_ft``, i.e. against the same threshold that
    *defines* ``touchdown``. Reading the touchdown events themselves would make
    this family depend on a runway polygon covering the aircraft, which is
    exactly the dependency this detector exists to avoid.

    ``sv`` must carry ``ades_lat``/``ades_lon`` -- the destination position from
    the flight list -- alongside the usual altitude, elevation and cumulative
    columns. Heights here are measured against ``elev_ades_ft`` alone: the
    detector is anchored at the destination by construction, so there is no
    ambiguity to be permissive about.

    With :attr:`~opdi.config.EventConfig.goaround_excludes_landings`, an
    excursion that holds a report on the ground (flagged, or slower than
    :data:`GOAROUND_GROUND_MAX_KT` without altitude), lasts longer than
    ``goaround_max_excursion_seconds`` or holds a gap longer than
    ``goaround_max_gap_seconds`` is not a go-around. With
    :attr:`~opdi.config.EventConfig.goaround_in_approach_corridor` and
    ``thresholds`` (:func:`opdi.pipeline.runways.runway_thresholds`), the
    lowest point must lie in the approach corridor of a runway of the
    destination (see :func:`_in_approach_corridor`).
    """
    if not config.emit_runway_milestones:
        return _empty_milestones(sv)

    work = sv.select(
        F.col("track_id"),
        F.col("event_time"),
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
        (F.col("baro_altitude_c") * F.lit(FT_PER_M)).alias("altitude_ft"),
        # Arrival-anchored by definition -- the excursion is measured against
        # the *destination*'s field, never against the more permissive minimum
        # of the two. At a destination lower than the origin the permissive
        # height is negative throughout, the excursion reads as having reached
        # the deck, and the go-around is suppressed as a landing.
        height_above_elevation_ft(_col_or_null(sv, "elev_ades_ft", "double")).alias("height_ft"),
        (F.col("vert_rate") * F.lit(FTMIN_PER_MPS)).alias("roc_ft_min"),
        haversine_nm(F.col("lat"), F.col("lon"), F.col("ades_lat"), F.col("ades_lon")).alias(
            "ades_dist_nm"
        ),
        F.col("cumulative_distance_nm").cast("double").alias("cumulative_distance_nm"),
        F.col("cumulative_time_s").cast("double").alias("cumulative_time_s"),
        _col_or_null(sv, "ades", "string").alias("ades"),
        _col_or_null(sv, "flight_id", "string").alias("osn_flight_id"),
        *(
            [
                # A report on the ground: flagged, or slow without an
                # altitude (a landing roll or a taxi; airliners send no
                # altitude on the ground).
                (
                    F.coalesce(_col_or_null(sv, "on_ground", "boolean"), F.lit(False))
                    | F.coalesce(
                        F.col("baro_altitude_c").isNull()
                        & (F.col("velocity") * F.lit(KT_PER_MPS) < F.lit(GOAROUND_GROUND_MAX_KT)),
                        F.lit(False),
                    )
                ).alias("_ground")
            ]
            if config.goaround_excludes_landings
            else []
        ),
    )

    crossings = threshold_crossings(
        work.drop("_ground") if "_ground" in work.columns else work,
        value_col="height_ft",
        thresholds=[config.goaround_trigger_height_ft, config.goaround_recovery_height_ft],
        hysteresis=GOAROUND_HYSTERESIS_FT,
        partition_cols=["track_id"],
        interpolate_cols=("lat", "lon"),
        up_label="up",
        down_label="down",
        interpolate=True,
        all_occurrences=True,
        **missing_value_options(config),
    )

    trigger_down = (F.col("threshold") == F.lit(float(config.goaround_trigger_height_ft))) & (
        F.col("direction") == "down"
    )
    recovery_up = (F.col("threshold") == F.lit(float(config.goaround_recovery_height_ft))) & (
        F.col("direction") == "up"
    )
    marked = crossings.filter(trigger_down | recovery_up).withColumn(
        "_kind", F.when(trigger_down, F.lit("down")).otherwise(F.lit("up"))
    )

    # Pair each recovery with the most recent descent below the trigger. A
    # recovery with no preceding descent is an ordinary climb -- a departure,
    # or a track that was first seen above the trigger -- and pairs with
    # nothing.
    ordered = (
        Window.partitionBy("track_id")
        .orderBy("event_time")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    marked = marked.withColumn(
        "_from",
        F.last(F.when(F.col("_kind") == "down", F.col("event_time")), ignorenulls=True).over(
            ordered
        ),
    )
    windows = marked.filter((F.col("_kind") == "up") & F.col("_from").isNotNull()).select(
        F.col("track_id").alias("_w_track"),
        F.col("_from"),
        F.col("event_time").alias("_to"),
    )
    # One excursion per descent: an aircraft that climbs, levels, and climbs
    # again registers one abandoned approach, not two.
    first_recovery = Window.partitionBy("_w_track", "_from").orderBy("_to")
    windows = (
        windows.withColumn("_r", F.row_number().over(first_recovery))
        .filter(F.col("_r") == 1)
        .drop("_r")
    )
    if config.goaround_excludes_landings:
        # An excursion longer than the limit is dropped below whatever its
        # reports show, so its reports are not read: the window from the last
        # descent before a landing to the take-off after a turnaround holds
        # every report of the stay on the ground.
        windows = windows.filter(
            F.col("_to").cast("double") - F.col("_from").cast("double")
            <= F.lit(float(config.goaround_max_excursion_seconds))
        )

    inside = work.join(
        windows,
        (F.col("_w_track") == work["track_id"])
        & (F.col("event_time") >= F.col("_from"))
        & (F.col("event_time") <= F.col("_to")),
        "inner",
    ).drop("_w_track")

    extra = []
    if config.goaround_excludes_landings:
        by_excursion = Window.partitionBy("track_id", "_from").orderBy("event_time")
        inside = inside.withColumn(
            "_step",
            F.col("event_time").cast("double")
            - F.lag(F.col("event_time").cast("double")).over(by_excursion),
        )
        extra = [
            F.max(F.when(F.col("_ground"), 1).otherwise(0)).alias("_landed"),
            F.max("_step").alias("_max_step"),
        ]
    excursion = inside.groupBy("track_id", "_from", "_to").agg(
        *extra,
        F.min("height_ft").alias("low_height_ft"),
        F.max("roc_ft_min").alias("max_roc_ft_min"),
        F.min_by("event_time", "height_ft").alias("low_time"),
        F.min_by("lat", "height_ft").alias("lat"),
        F.min_by("lon", "height_ft").alias("lon"),
        F.min_by("altitude_ft", "height_ft").alias("altitude_ft"),
        F.min_by("ades_dist_nm", "height_ft").alias("low_dist_nm"),
        F.min_by("cumulative_distance_nm", "height_ft").alias("cumulative_distance_nm"),
        F.min_by("cumulative_time_s", "height_ft").alias("cumulative_time_s"),
        F.min_by("ades", "height_ft").alias("ades"),
        F.min_by("osn_flight_id", "height_ft").alias("osn_flight_id"),
    )

    excursion = excursion.filter(
        # A touchdown inside the window means the aircraft landed, however
        # briefly; the climb away is a departure, not an abandoned approach.
        (F.col("low_height_ft") > F.lit(config.runway_airborne_height_ft))
        # A climb away, not a descent that merely stopped descending.
        & (F.col("max_roc_ft_min") >= F.lit(config.goaround_min_roc_ftmin))
        # Initiated on final, not somewhere else entirely.
        & (F.col("low_dist_nm") <= F.lit(config.goaround_radius_nm))
    )
    if config.goaround_excludes_landings:
        duration = F.col("_to").cast("double") - F.col("_from").cast("double")
        excursion = excursion.filter(
            (F.col("_landed") == 0)
            & (duration <= F.lit(config.goaround_max_excursion_seconds))
            & (F.coalesce(F.col("_max_step"), F.lit(0.0)) <= F.lit(config.goaround_max_gap_seconds))
        )
    if config.goaround_in_approach_corridor and thresholds is not None:
        excursion = _in_approach_corridor(excursion, thresholds, config)

    return excursion.select(
        F.col("track_id"),
        F.lit("go-around").alias("type"),
        F.col("low_time").alias("event_time"),
        F.col("lon").cast("double").alias("lon"),
        F.col("lat").cast("double").alias("lat"),
        F.col("altitude_ft").cast("double").alias("altitude_ft"),
        F.col("cumulative_distance_nm").cast("double").alias("cumulative_distance_nm"),
        F.col("cumulative_time_s").cast("double").alias("cumulative_time_s"),
        F.to_json(
            F.struct(
                F.lit(None).cast("string").alias("rwy_ident"),
                F.col("ades").alias("apt_icao"),
                F.lit(None).cast("string").alias("traversal_class"),
                F.lit(None).cast("double").alias("align_deg"),
                F.lit(None).cast("double").alias("max_gs_kt"),
                F.col("osn_flight_id").alias("osn_flight_id"),
                F.lit(None).cast("string").alias("milestone"),
                F.col("low_height_ft").alias("low_height_ft"),
                F.col("max_roc_ft_min").alias("recovery_roc_ft_min"),
                (F.col("_to").cast("double") - F.col("_from").cast("double")).alias(
                    "excursion_seconds"
                ),
            )
        ).alias("info"),
    )
