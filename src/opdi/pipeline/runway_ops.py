"""A-CDM runway milestones, from classified runway traversals.

Nine of the eleven A-CDM milestones come out of one pass over the same object:
a *traversal*, a maximal run of consecutive samples whose H3 res-12 cell falls
inside a runway polygon of an aerodrome the flight list names for that flight.
A gap longer than ``airport_trace_gap_seconds`` starts a new one, so an
aircraft that sits on the runway across a coverage hole is two traversals
rather than one implausible ten-minute occupancy.

**Why one object rather than nine detectors.** ``line-up`` and ``take-off-roll``
and ``airborne`` are not independent questions; they are three readings of one
departure. Deriving them from a shared, explicitly classified traversal means
they cannot disagree about which runway, which aerodrome, or even whether the
movement was a departure at all -- and the classification that produced them
travels with every event in ``info``, so the decision is auditable from the
published row rather than reconstructible only from the code.

Each traversal is classified by three measurements over its own samples:

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
  movement.

giving ``departure``, ``arrival``, ``crossing`` -- or NULL, which means
abstain. The classes cannot overlap: ``runway_align_max_deg`` (30) and
``runway_cross_min_deg`` (45) leave a band between them where the detector says
nothing, and the speed gates separate a rolling aircraft from a taxiing one.
The band is deliberate, not an oversight.

**Timing is interpolated, not sampled.** ``airborne`` and ``touchdown`` are the
instants the height above field elevation crosses ``runway_airborne_height_ft``,
and ``landing`` the instant the along-track distance from the threshold changes
sign. All three go through :func:`opdi.pipeline.crossings.threshold_crossings`
-- the same Schmitt-trigger-plus-interpolation engine that produces the
flight-level and ring crossings. This is the substantive improvement over
``ATOT``/``ALDT``, which took the *extreme sample* of a detection window and
paid for it with a +19 s median bias on departures. A bias that is always in
the same direction is not noise a larger sample averages away.

``landing`` needs one sample the traversal does not contain: the threshold is
the near edge of the runway polygon, so the point short of it is outside the
traversal by definition. The arrival sample window therefore reaches back
:data:`ARRIVAL_LEAD_SECONDS` before entry, and only for that one crossing --
entry, exit and every reported position stay bounded by the polygon.

**This module reads no table.** ``grid`` and ``thresholds`` arrive as
DataFrames, and ``sv`` must already carry the flight list's aerodrome array,
its ``adep``/``ades`` idents and the two field elevations. The join to the flight list therefore happens once
for every detector that needs it rather than once per family, and every
function here is testable on a laptop with no storage layer at all.
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

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from opdi.config import EventConfig

from opdi.pipeline.crossings import threshold_crossings
from opdi.pipeline.elevation import (
    height_above_aerodrome_ft,
    height_above_elevation_ft,
)
from opdi.pipeline.flights import angle_between, bearing_deg, haversine_nm
from opdi.pipeline.runways import cross_track_nm

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
#: 0.05 NM is 304 ft, which at 140 kt (236 ft/s) is about 1.3 s -- not a
#: fraction of a second, as an earlier version of this comment claimed. It does
#: not matter, and the reason it does not matter is the thing to remember: the
#: dead band gates *confirmation*, not the reported instant. ``landing`` is
#: interpolated to the sample pair that straddles the bare plane, which is
#: earlier than the confirming sample and unaffected by how wide the band is.
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
#:
#: A module constant rather than an ``EventConfig`` field only because the
#: config surface is fixed this round; it is a tuning parameter and belongs
#: there eventually.
ROLL_MAX_GAP_SECONDS = 30.0

#: How far back before a traversal's entry the *arrival* sample window reaches,
#: in seconds.
#:
#: ``landing`` (T16) is the crossing of the threshold plane, and the threshold
#: is the near edge of the runway polygon: the sample short of it is by
#: definition **outside** the polygon and so outside the traversal. Confirming
#: the crossing needs one, so the arrival window is extended backwards by this
#: much -- the pre-threshold final-approach samples the T16 crossing needs, and
#: nothing else. Without it the Schmitt trigger never sees a negative
#: along-track distance and ``landing`` cannot fire at all in production.
#:
#: Sixty seconds is about 2.3 NM at 140 kt: comfortably more than any runway's
#: displaced threshold or the gap between the polygon edge and the last
#: approach sample, and far short of the previous traversal an aircraft could
#: have made at the same aerodrome. Only :func:`_threshold_plane` reads the
#: extended window; entry, exit and every reported position stay bounded by the
#: polygon, because those are statements about occupancy.
ARRIVAL_LEAD_SECONDS = 60.0

#: Half-width of the dead band around the go-around trigger and recovery
#: heights, in feet. Wider than ``AIRBORNE_HYSTERESIS_FT`` because it guards a
#: whole excursion rather than an instant: an aircraft levelling at 500 ft on a
#: normal approach must not enter and leave the trigger repeatedly.
GOAROUND_HYSTERESIS_FT = 50.0

#: What a traversal is, for windowing purposes.
#:
#: ``apt_ident`` is in the key and has to be. ``trace_id`` restarts at 0 for
#: every ``(track_id, strip_id)``, so it carries no information across
#: aerodromes -- and runway designators repeat: a flight departing 07 at its
#: origin and crossing a runway also called 07 at its destination produced two
#: traversals with an identical ``(track_id, "07", "0")``. Everything windowed
#: on that key then collapsed them into one, giving a single crossing pair
#: whose entry came from the origin and whose exit came from the destination,
#: straddling the entire flight. 07/25 and 09/27 are among the commonest
#: designators in Europe, so this was not a corner case.
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
TRAVERSAL_INFO = ("traversal_class", "align_deg", "max_gs_kt", "osn_flight_id")

#: The standard event frame, as ``events.py`` shapes it. Declared explicitly so
#: a disabled configuration can return an empty frame of exactly this shape
#: rather than ``None``: a caller that unions families must never have to
#: branch on nullity.
MILESTONE_SCHEMA = StructType([
    StructField("track_id", StringType()),
    StructField("type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("lon", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("altitude_ft", DoubleType()),
    StructField("cumulative_distance_nm", DoubleType()),
    StructField("cumulative_time_s", DoubleType()),
    StructField("info", StringType()),
])

#: ICAO milestone numbers. ``go-around`` and the two runway crossings have
#: none, and are absent rather than invented.
MILESTONE_NUMBERS = {
    "line-up": "T06",
    "take-off-roll": "T07",
    "airborne": "T08",
    "landing": "T16",
    "touchdown": "T17",
    "runway-vacated": "T19",
}


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_traversal(align_deg, max_gs_kt, entry_height_ft, exit_height_ft,
                       duration_seconds, config: "EventConfig"):
    """Departure, arrival, crossing -- or NULL, which means abstain.

    The three classes cannot overlap: ``runway_align_max_deg`` (30 deg) and
    ``runway_cross_min_deg`` (45 deg) leave a band in between where the
    detector says nothing, and the speed gates separate a rolling aircraft from
    a taxiing one. Abstention is a deliberate output, not a gap: naming the
    wrong milestone corrupts a movement count, naming none is an explicit null
    a consumer can see.
    """
    aligned = align_deg <= F.lit(config.runway_align_max_deg)
    fast = max_gs_kt > F.lit(config.runway_roll_speed_kt)
    airborne_h = F.lit(config.runway_airborne_height_ft)

    departure = (
        aligned & fast
        & (entry_height_ft < airborne_h) & (exit_height_ft >= airborne_h)
    )
    arrival = (
        aligned & fast
        & (entry_height_ft >= airborne_h) & (exit_height_ft < airborne_h)
    )
    crossing = (
        (align_deg >= F.lit(config.runway_cross_min_deg))
        & (max_gs_kt <= F.lit(config.runway_taxi_speed_kt))
        & (duration_seconds <= F.lit(config.runway_crossing_max_seconds))
        & (entry_height_ft < airborne_h) & (exit_height_ft < airborne_h)
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
    two thresholds; and ``landing`` (T16), which is the instant this distance
    changes sign and so interpolates linearly across the threshold plane. It is
    the companion of :func:`~opdi.pipeline.runways.cross_track_nm` -- one gives
    distance *along* the centreline, the other distance *across* it.
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
    the coverage the retired ``hexaero_airport_layouts`` polygons never had:
    a sample survives the prune when its cell is one the grid rasterised for a
    strip of an aerodrome this flight names. ``thresholds`` is
    :func:`runways.runway_thresholds` -- one row per runway *direction*, now
    carrying ``rwy_length_nm``/``rwy_half_width_nm`` so the prune can be refined
    to a genuine on-runway test rather than trusting cell membership alone.
    """
    # 1. Prune to the neighbourhood of a runway with an equi-join on the res-12
    #    grid, restricted to aerodromes this flight actually names. Broadcast
    #    because the grid is small next to a month of state vectors.
    grid_cols = grid.select(
        F.col("h3_id"),
        F.col("apt_icao"),
        F.col("strip_id"),
        F.col("le_ident"),
        F.col("he_ident"),
        F.col("zone"),
    )
    near = sv.join(
        F.broadcast(grid_cols),
        (sv.h3_res_12 == grid_cols.h3_id)
        & F.array_contains(sv.apt, grid_cols.apt_icao),
        "inner",
    )

    # 2. Refine the cell prune to a geometric occupancy test. One direction of
    #    the strip fixes the centreline, and whether a sample sits between the
    #    two thresholds is the same question from either end -- the two
    #    along-track distances sum to the length -- so the ``le`` threshold
    #    alone decides membership. The two *directions* are weighed later, at
    #    the traversal level, exactly as the old code weighed them.
    memb = thresholds.select(
        F.col("apt_ident").alias("_m_apt"),
        F.col("rwy_ident").alias("_m_rwy"),
        F.col("thr_lat").alias("_m_thr_lat"),
        F.col("thr_lon").alias("_m_thr_lon"),
        F.col("rwy_bearing").alias("_m_bearing"),
        F.col("rwy_length_nm").alias("_m_length"),
        F.col("rwy_half_width_nm").alias("_m_half_width"),
    )
    near = near.join(
        F.broadcast(memb),
        (F.col("apt_icao") == F.col("_m_apt")) & (F.col("le_ident") == F.col("_m_rwy")),
        "inner",
    )
    near = near.withColumn(
        "_along",
        along_track_nm(
            F.col("lat"), F.col("lon"),
            F.col("_m_thr_lat"), F.col("_m_thr_lon"), F.col("_m_bearing"),
        ),
    ).withColumn(
        "_cross",
        cross_track_nm(
            F.col("lat"), F.col("lon"),
            F.col("_m_thr_lat"), F.col("_m_thr_lon"), F.col("_m_bearing"),
        ),
    )
    # Only ``runway`` cells inside the strip's own extent are occupancy. The
    # ``approach`` cells and anything off either end fall away here; the
    # arrival window that needs the pre-threshold samples is rebuilt from ``sv``
    # by time in ``_traversal_samples``. See ARRIVAL_LEAD_SECONDS.
    work = near.filter(
        (F.col("zone") == F.lit("runway"))
        & (F.col("_along") >= F.lit(0.0))
        & (F.col("_along") <= F.col("_m_length"))
        & (F.col("_cross") <= F.col("_m_half_width"))
    ).drop(
        "_m_apt", "_m_rwy", "_m_thr_lat", "_m_thr_lon", "_m_bearing",
        "_m_length", "_m_half_width", "_along", "_cross",
    )

    ordered = Window.partitionBy("track_id", "strip_id").orderBy("event_time")
    work = work.withColumn(
        "_gap",
        F.col("event_time").cast("long")
        - F.lag(F.col("event_time").cast("long")).over(ordered),
    )
    work = work.withColumn(
        "trace_id",
        F.sum(
            F.when(F.col("_gap") > F.lit(config.airport_trace_gap_seconds), 1)
            .otherwise(0)
        ).over(ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .cast("string"),
    )

    # Measured against **this traversal's own aerodrome**, not against the more
    # permissive of the flight's two fields. The classification's height gates
    # are thresholds that get reported, not a membership test: measured against
    # the higher field, every movement at the lower one has a negative height,
    # `classify_traversal` abstains, and the whole departure or arrival is
    # silently absent. See `elevation.height_above_field_ft`'s warning.
    work = work.withColumn(
        "_h_ft", height_above_aerodrome_ft(work, F.col("apt_icao"))
    )
    work = work.withColumn("_gs_kt", F.col("velocity") * F.lit(KT_PER_MPS))
    work = work.withColumn("_alt_ft", F.col("baro_altitude_c") * F.lit(FT_PER_M))
    work = work.withColumn("_flight_id", _col_or_null(sv, "flight_id", "string"))

    # ``min_by``/``max_by``, never ``first``/``last``: the latter take partition
    # order, not time order, which is the correctness fix
    # ``airport_events_ordered`` records. The callsign is aggregated rather than
    # grouped on for the reason ``calculate_airport_events`` documents at
    # length -- a track that broadcast two callsigns while occupying one runway
    # would otherwise become two traversals and two movements. ``le_ident`` and
    # ``he_ident`` ride the grouping so both directions of the strip are
    # available as candidates without a second join back to the grid.
    agg = work.groupBy(
        "track_id", "apt_icao", "strip_id", "le_ident", "he_ident", "trace_id"
    ).agg(
        F.min("event_time").alias("entry_time"),
        F.max("event_time").alias("exit_time"),
        F.max("_gs_kt").alias("max_gs_kt"),
        F.expr("percentile_approx(heading, 0.5)").alias("median_track_deg"),
        F.min_by("_h_ft", "event_time").alias("entry_height_ft"),
        F.max_by("_h_ft", "event_time").alias("exit_height_ft"),
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

    # A strip carries two directions and a movement is reported against one, so
    # both directions of this strip are candidates and geometry picks one.
    cand = agg.join(
        F.broadcast(thresholds),
        (F.col("apt_icao") == thresholds.apt_ident)
        & (
            (thresholds.rwy_ident == F.col("le_ident"))
            | (thresholds.rwy_ident == F.col("he_ident"))
        ),
        "inner",
    )
    # Folded to [0, 90] so a reciprocal counts as aligned. This is the
    # *classification* angle and it is deliberately direction-blind.
    cand = cand.withColumn(
        "align_deg",
        F.least(
            angle_between(F.col("median_track_deg"), F.col("rwy_bearing")),
            angle_between(F.col("median_track_deg"), F.col("rwy_bearing") + F.lit(180.0)),
        ),
    )
    # The *direction* angle, unfolded. It is a separate quantity from
    # ``align_deg`` and has to be: folded, the two ends of one strip score
    # identically, so nothing but the alphabet would choose between 07 and 25.
    cand = cand.withColumn(
        "bearing_error_deg",
        angle_between(F.col("median_track_deg"), F.col("rwy_bearing")),
    )
    cand = cand.withColumn(
        "cross_track_nm",
        cross_track_nm(
            F.col("entry_lat"), F.col("entry_lon"),
            F.col("thr_lat"), F.col("thr_lon"), F.col("rwy_bearing"),
        ),
    )
    # Rounded to the metre before it is ordered on. Cross-track distance picks
    # the *strip* -- parallel runways are hundreds of metres apart -- but the
    # two ends of one strip share a centreline and differ only in floating
    # point noise. Unrounded, that noise would decide the direction before the
    # bearing error was ever consulted.
    cand = cand.withColumn("_xt", F.round(F.col("cross_track_nm"), 3))

    best = Window.partitionBy("track_id", "strip_id", "trace_id").orderBy(
        F.col("_xt").asc(),
        F.col("bearing_error_deg").asc(),
        F.col("rwy_ident").asc(),
    )
    out = (
        cand.withColumn("_r", F.row_number().over(best))
        .filter(F.col("_r") == 1)
        .drop("_r", "_xt")
    )
    out = out.withColumn("class", classify_traversal(
        F.col("align_deg"), F.col("max_gs_kt"), F.col("entry_height_ft"),
        F.col("exit_height_ft"), F.col("duration_seconds"), config,
    )).filter(F.col("class").isNotNull())

    return out.select(
        F.col("track_id"),
        F.col("apt_ident"),
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


def _info(milestone: Optional[str]):
    """The ``info`` JSON every runway milestone carries.

    ``milestone`` is the ICAO number, and it is what lets a consumer tell the
    new threshold-crossing ``landing`` from the retired ground-contact one
    without parsing version strings. The classification, the runway and the
    alignment travel with it so the decision that produced the row is auditable
    from the row.
    """
    return F.to_json(F.struct(
        F.col("rwy_ident").alias("rwy_ident"),
        F.col("apt_ident").alias("apt_icao"),
        F.col("traversal_class").alias("traversal_class"),
        F.col("align_deg").alias("align_deg"),
        F.col("max_gs_kt").alias("max_gs_kt"),
        F.col("osn_flight_id").alias("osn_flight_id"),
        F.lit(milestone).cast("string").alias("milestone"),
    ))


def _event(sdf: DataFrame, type_: str, time_col: str) -> DataFrame:
    """Project onto the standard event frame."""
    return sdf.select(
        F.col("track_id"),
        F.lit(type_).alias("type"),
        F.col(time_col).alias("event_time"),
        F.col("lon").cast("double").alias("lon"),
        F.col("lat").cast("double").alias("lat"),
        F.col("altitude_ft").cast("double").alias("altitude_ft"),
        F.col("cumulative_distance_nm").cast("double").alias("cumulative_distance_nm"),
        F.col("cumulative_time_s").cast("double").alias("cumulative_time_s"),
        _info(MILESTONE_NUMBERS.get(type_)).alias("info"),
    )


#: Columns carried across the traversal join so the height can be computed on
#: the far side of it -- where the traversal's own ``apt_ident`` is known -- and
#: dropped again immediately afterwards.
_HEIGHT_INPUTS = ("adep", "ades", "elev_adep_ft", "elev_ades_ft")


def _traversal_samples(
    sv: DataFrame, traversals: DataFrame, lead_seconds: float = 0.0
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
    )
    t = t.withColumn(
        "_window_start",
        F.when(
            F.col("traversal_class") == F.lit("arrival"),
            F.timestamp_seconds(
                F.col("entry_time").cast("double") - F.lit(float(lead_seconds))
            ),
        ).otherwise(F.col("entry_time")),
    )
    joined = t.join(
        s,
        (F.col("_s_track") == t["track_id"])
        & (F.col("event_time") >= t["_window_start"])
        & (F.col("event_time") <= t["exit_time"]),
        "inner",
    ).drop("_s_track", "_window_start")
    joined = joined.withColumn(
        "_in_polygon", F.col("event_time") >= F.col("entry_time")
    )
    return joined.withColumn(
        "height_ft", height_above_aerodrome_ft(joined, F.col("apt_ident"))
    ).drop("baro_altitude_c", *carried)


def _at_extreme(sdf: DataFrame, ascending: bool) -> DataFrame:
    """The sample at the start (or end) of each traversal, whole.

    ``row_number`` over an explicit ``event_time`` ordering rather than
    ``first``/``last`` in a ``groupBy``: those take partition order, so the
    reported position need not be the position at the reported time.
    """
    order = F.col("event_time").asc() if ascending else F.col("event_time").desc()
    w = Window.partitionBy(*TRAVERSAL_KEY).orderBy(order)
    return sdf.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1).drop("_r")


def _roll_start(sdf: DataFrame, config: "EventConfig") -> DataFrame:
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
    """
    ordered = Window.partitionBy(*TRAVERSAL_KEY).orderBy("event_time")
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    work = sdf.withColumn("_fast", F.col("gs_kt") >= F.lit(config.runway_roll_speed_kt))
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
        .drop("_r", "_fast", "_prev_fast", "_gap", "_new_run", "_run",
              "_run_start", "_held", "_run_held")
    )


def _height_crossing(
    sdf: DataFrame, config: "EventConfig", direction: str
) -> DataFrame:
    """The interpolated crossing of ``runway_airborne_height_ft``.

    ``up`` is the lift-off, ``down`` the touchdown. Same engine, same
    threshold, opposite sense -- which is the point: the two milestones are one
    geometric fact read in two directions, and giving them separate detectors
    would let them drift apart.
    """
    return threshold_crossings(
        sdf,
        value_col="height_ft",
        thresholds=[config.runway_airborne_height_ft],
        hysteresis=AIRBORNE_HYSTERESIS_FT,
        partition_cols=list(TRAVERSAL_KEY) + list(TRAVERSAL_INFO),
        interpolate_cols=(
            "lat", "lon", "altitude_ft",
            "cumulative_distance_nm", "cumulative_time_s",
        ),
        up_label="up",
        down_label="down",
        interpolate=True,
        all_occurrences=True,
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
    turn-off beyond it -- has already landed, and stamping a second ``landing``
    there would double the arrival count.
    """
    work = sdf.withColumn(
        "along_track_nm",
        haversine_nm(F.col("thr_lat"), F.col("thr_lon"), F.col("lat"), F.col("lon"))
        * F.cos(F.radians(
            bearing_deg(F.col("thr_lat"), F.col("thr_lon"), F.col("lat"), F.col("lon"))
            - F.col("rwy_bearing")
        )),
    )
    crossings = threshold_crossings(
        work,
        value_col="along_track_nm",
        thresholds=[0.0],
        hysteresis=THRESHOLD_PLANE_HYSTERESIS_NM,
        partition_cols=list(TRAVERSAL_KEY) + list(TRAVERSAL_INFO),
        interpolate_cols=(
            "lat", "lon", "altitude_ft", "height_ft",
            "cumulative_distance_nm", "cumulative_time_s",
        ),
        up_label="inbound",
        down_label="outbound",
        interpolate=True,
        all_occurrences=True,
    )
    return crossings.filter(
        (F.col("direction") == "inbound")
        & (F.col("height_ft") >= F.lit(config.runway_airborne_height_ft))
    )


def runway_milestones(
    sv: DataFrame, traversals: DataFrame, config: "EventConfig"
) -> DataFrame:
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
    samples = _traversal_samples(sv, traversals, lead_seconds=ARRIVAL_LEAD_SECONDS)
    bounded = samples.filter(F.col("_in_polygon"))

    dep = bounded.filter(F.col("traversal_class") == "departure")
    arr = bounded.filter(F.col("traversal_class") == "arrival")
    xing = bounded.filter(F.col("traversal_class") == "crossing")
    arr_approach = samples.filter(F.col("traversal_class") == "arrival")

    parts = [
        # A departure: lined up, rolling, then off the deck.
        _event(_at_extreme(dep, True), "line-up", "entry_time"),
        _event(_roll_start(dep, config), "take-off-roll", "event_time"),
        _event(_height_crossing(dep, config, "up"), "airborne", "event_time"),
        # An arrival: over the threshold, wheels down, off the strip.
        _event(_threshold_plane(arr_approach, config), "landing", "event_time"),
        _event(_height_crossing(arr, config, "down"), "touchdown", "event_time"),
        _event(_at_extreme(arr, False), "runway-vacated", "exit_time"),
        # A crossing is not a movement: it gets its two instants and nothing
        # else, so it can never be counted as one.
        _event(_at_extreme(xing, True), "runway-crossing-entry", "entry_time"),
        _event(_at_extreme(xing, False), "runway-crossing-vacated", "exit_time"),
    ]

    out = parts[0]
    for part in parts[1:]:
        out = out.unionByName(part)
    return out


# ---------------------------------------------------------------------------
# Go-around
# ---------------------------------------------------------------------------

def go_arounds(sv: DataFrame, config: "EventConfig") -> DataFrame:
    """A descent below ``goaround_trigger_height_ft`` near the destination that
    climbs back above ``goaround_recovery_height_ft`` with no touchdown between.

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
        height_above_elevation_ft(
            _col_or_null(sv, "elev_ades_ft", "double")
        ).alias("height_ft"),
        (F.col("vert_rate") * F.lit(FTMIN_PER_MPS)).alias("roc_ft_min"),
        haversine_nm(
            F.col("lat"), F.col("lon"), F.col("ades_lat"), F.col("ades_lon")
        ).alias("ades_dist_nm"),
        F.col("cumulative_distance_nm").cast("double").alias("cumulative_distance_nm"),
        F.col("cumulative_time_s").cast("double").alias("cumulative_time_s"),
        _col_or_null(sv, "ades", "string").alias("ades"),
        _col_or_null(sv, "flight_id", "string").alias("osn_flight_id"),
    )

    crossings = threshold_crossings(
        work,
        value_col="height_ft",
        thresholds=[config.goaround_trigger_height_ft,
                    config.goaround_recovery_height_ft],
        hysteresis=GOAROUND_HYSTERESIS_FT,
        partition_cols=["track_id"],
        interpolate_cols=("lat", "lon"),
        up_label="up",
        down_label="down",
        interpolate=True,
        all_occurrences=True,
    )

    trigger_down = (
        (F.col("threshold") == F.lit(float(config.goaround_trigger_height_ft)))
        & (F.col("direction") == "down")
    )
    recovery_up = (
        (F.col("threshold") == F.lit(float(config.goaround_recovery_height_ft)))
        & (F.col("direction") == "up")
    )
    marked = crossings.filter(trigger_down | recovery_up).withColumn(
        "_kind", F.when(trigger_down, F.lit("down")).otherwise(F.lit("up"))
    )

    # Pair each recovery with the most recent descent below the trigger. A
    # recovery with no preceding descent is an ordinary climb -- a departure,
    # or a track that was first seen above the trigger -- and pairs with
    # nothing.
    ordered = Window.partitionBy("track_id").orderBy("event_time").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    marked = marked.withColumn(
        "_from",
        F.last(
            F.when(F.col("_kind") == "down", F.col("event_time")), ignorenulls=True
        ).over(ordered),
    )
    windows = marked.filter(
        (F.col("_kind") == "up") & F.col("_from").isNotNull()
    ).select(
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

    inside = work.join(
        windows,
        (F.col("_w_track") == work["track_id"])
        & (F.col("event_time") >= F.col("_from"))
        & (F.col("event_time") <= F.col("_to")),
        "inner",
    ).drop("_w_track")

    excursion = inside.groupBy("track_id", "_from", "_to").agg(
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

    return excursion.select(
        F.col("track_id"),
        F.lit("go-around").alias("type"),
        F.col("low_time").alias("event_time"),
        F.col("lon").cast("double").alias("lon"),
        F.col("lat").cast("double").alias("lat"),
        F.col("altitude_ft").cast("double").alias("altitude_ft"),
        F.col("cumulative_distance_nm").cast("double").alias("cumulative_distance_nm"),
        F.col("cumulative_time_s").cast("double").alias("cumulative_time_s"),
        F.to_json(F.struct(
            F.lit(None).cast("string").alias("rwy_ident"),
            F.col("ades").alias("apt_icao"),
            F.lit(None).cast("string").alias("traversal_class"),
            F.lit(None).cast("double").alias("align_deg"),
            F.lit(None).cast("double").alias("max_gs_kt"),
            F.col("osn_flight_id").alias("osn_flight_id"),
            F.lit(None).cast("string").alias("milestone"),
            F.col("low_height_ft").alias("low_height_ft"),
            F.col("max_roc_ft_min").alias("recovery_roc_ft_min"),
            (F.col("_to").cast("double") - F.col("_from").cast("double"))
            .alias("excursion_seconds"),
        )).alias("info"),
    )
