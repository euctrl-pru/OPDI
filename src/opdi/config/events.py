"""Thresholds of flight event detection (step 04): :class:`EventConfig`."""

from dataclasses import dataclass
from typing import Optional

from opdi.versions import EVENTS_VERSION, LEGACY_EVENTS_VERSION


@dataclass
class EventConfig:
    """Thresholds of flight event detection (step 04).

    Step 04 reads the tracks and the flight list and writes the flight events
    (``opdi_flight_events``) and their measurements (``opdi_measurements``).
    The fields are grouped by event family: flight phases and the events
    read from them, flight-level and distance-circle crossings, runway and
    off-block/in-block times, level segments, holding, point merge, FIR
    crossings and airport layout events. Each family has its page on the
    OPDI website, ``https://www.opdi.aero/content/methods/``.

    Units are in the field names: ``_ft`` feet, ``_fl`` flight level,
    ``_nm`` nautical miles, ``_kt`` knots, ``_ftmin`` feet per minute,
    ``_deg`` degrees, ``_seconds`` or ``_s`` seconds, ``_pct`` percent.

    The defaults are those of release v2.0.0. :meth:`legacy` gives the
    behaviour of release v0.0.2 (event version ``events_v0.0.2``).
    """

    # -- flight phases (fuzzy logic) --------------------------------------
    #
    # Every report is given a flight phase (ground, climb, descent, level,
    # cruise) by fuzzy logic on altitude, vertical rate and speed, with the
    # membership functions of OpenAP. Take-off and landing, top of climb and
    # top of descent, and level-start/level-end are read from the changes of
    # phase. See https://www.opdi.aero/content/methods/vertical-events.html.
    phase_twindow_seconds: float = 60.0
    """Length, in seconds, of the window over which the phase labels are
    smoothed by majority vote before the changes of phase are read. A single
    misclassified report then no longer creates a spurious level segment or
    breaks a take-off. 0 turns smoothing off (release v0.0.2)."""

    phase_ground_ceiling_ft: float = 200.0
    """Height, in feet, at which the "ground" membership of a report falls to
    zero. Below it a report can be classified as on the ground. Measured from
    the airport's elevation when :attr:`phase_ground_above_field` is set."""

    phase_ground_above_field: bool = True
    """Measure the ground membership by height above the airport's elevation
    rather than by pressure altitude.

    Pressure altitude at an airport equals its elevation only on a standard
    day, so with a fixed 200 ft limit on pressure altitude no report at an
    airport higher than about 200 ft is ever classified as on the ground, and
    its flights get no take-off or landing. With this setting, the elevations
    of the flight's departure and destination airports are subtracted and the
    lower height of the two is used; an unknown elevation counts as 0 ft.
    Off in release v0.0.2."""

    phase_require_complete_rules: bool = True
    """A fuzzy rule can win only when all its inputs (altitude, vertical
    rate, speed) are present. Without this, a missing input is skipped and the
    rule is judged on fewer inputs, which raises its score. Off in release
    v0.0.2."""

    phase_cruise_speed_kt: float = 600.0
    """Centre of the "cruise" speed membership, in knots; see
    :attr:`phase_cruise_speed_sigma_kt`."""
    phase_cruise_speed_sigma_kt: float = 100.0
    """Centre and width, in knots, of the bell-shaped "cruise" speed
    membership (OpenAP values: 600 kt and 100 kt). An aircraft much slower
    than the centre, for example a turboprop at 250 kt, has a cruise
    membership close to zero and may get no top of climb or top of descent
    from the phase method."""

    # -- flight-level crossings and distance circles ----------------------
    crossing_levels_fl: tuple = (50, 70, 100, 245)
    """Flight levels whose crossings are detected (events
    ``first-xing-fl<level>`` and ``last-xing-fl<level>``). Default FL050,
    FL070, FL100 and FL245."""

    crossing_hysteresis_ft: float = 300.0
    """Half-width, in feet, of a band around each crossed level. A crossing
    counts only when the aircraft passes from below the band to above it (or
    the reverse), so an aircraft flying at the level does not create a
    crossing at every small altitude change. A flight that levels off at the
    level itself never leaves the band and has no crossing of it."""

    crossing_all_occurrences: bool = True
    """Keep every crossing of a level, not only the first and the last.
    Off in release v0.0.2."""

    crossing_interpolate: bool = True
    """Give the time and position at which the aircraft was exactly at the
    level, interpolated between the two reports around it, instead of the
    last report before it (up to 5 seconds early). Off in release v0.0.2."""

    interpolate_crossings_over_missing_values: bool = True
    """Place a crossing between the last report that has a value (altitude,
    distance or height) and the report after the crossing, skipping the
    reports in between that have no value.

    Cleaning blanks implausible altitudes, so a track can hold reports with
    no altitude in the middle of a climb. Without this setting, a crossing
    that happens over such reports is read from a report without a value: it
    gets no time at all, or the time of an earlier crossing of the same
    level. With it, a crossing whose two known values are more than
    :attr:`track_hole_seconds` apart is not given, because its time and
    position cannot be placed. Off in release v0.0.2."""

    ring_radii_nm: tuple = (40.0, 50.0, 60.0, 100.0, 110.0, 120.0)
    """Radii, in NM, of the circles around the departure and destination
    airports whose crossings are detected. 40 NM is the area used by the
    ICAO indicator of additional time in the arrival sequencing area (ASMA)
    and 100 NM its variant for airports that hold further out. Each radius
    also gives a column of the flight list (for example ``C40_ARR``). The
    distance is the great-circle distance to the airport, so it is not
    limited by the extent of the airport zones. Empty in release v0.0.2.
    See https://www.opdi.aero/content/methods/distance-rings.html."""

    ring_hysteresis_nm: float = 1.0
    """Half-width, in NM, of a band around each circle, playing the same role
    as :attr:`crossing_hysteresis_ft`: a track running along the circle does
    not cross it again and again."""

    # -- event families that release v0.0.2 did not publish ---------------
    emit_runway_events: bool = True
    """Publish the actual take-off and landing times (``ATOT``, ``ALDT``)
    detected on a runway. Off in release v0.0.2.
    See https://www.opdi.aero/content/methods/takeoff-landing.html."""

    emit_block_events: bool = True
    """Publish the off-block and in-block times (``AOBT``, ``AIBT``),
    detected from the first and last movement at the stand. Off in release
    v0.0.2. See https://www.opdi.aero/content/methods/block-times.html."""

    emit_level_offs: bool = True
    """Publish the level-off events (``level-off-start``, ``level-off-end``)
    of climbs and descents. Separate from ``level-start``/``level-end``,
    which are published in both releases. Off in release v0.0.2."""

    # -- ground movement (off-block and in-block) -------------------------
    ground_speed_threshold_kt: float = 2.0
    """Ground speed, in knots, above which an aircraft on the ground counts
    as moving (the value of the ``traffic`` library)."""

    ground_move_min_seconds: float = 30.0
    """Seconds the movement must last before it counts, which tells a real
    push-back from noise in the speed value."""

    ground_speed_derive_from_position: bool = True
    """Where the reported ground speed is missing, use the speed computed
    from two successive positions to detect ground movement.

    Ground speed is often missing on the airport surface, and cleaning
    removes repeated values, which is exactly what a parked aircraft that
    starts to move sends. The reported speed is used wherever it exists.
    Off in release v0.0.2."""

    block_times_on_ground_only: bool = True
    """Find each off-block and in-block time on the ground, in the part of
    the track that belongs to its own leg.

    An off-block time is then the start of the first sustained movement on
    the ground before the take-off, and an in-block time the end of the last
    sustained movement on the ground after the landing; each needs a stand
    passage in the track, and names the stand when that passage lies in the
    same part of the track.
    A track that lands and departs again gets the in-block time of its
    arrival and the off-block time of its departure. Movement is read only
    from reports that may be on the ground: flagged on the ground, without
    altitude, or at most :attr:`airport_max_height_agl_ft` above the
    airport of the leg, and a block time is the start (end) of a movement
    from (to) a standstill (:attr:`block_time_max_start_speed_kt`). The
    event names the stand and its airport (``info.stand``,
    ``info.apt_icao``).

    Without this setting, the off-block time is the first sustained movement
    anywhere in the track and the in-block time the last one, flight
    included, whenever the track passed any stand: an arrival then gets an
    off-block time at its first report in flight. Off in release v0.0.2,
    which publishes no block times. See
    https://www.opdi.aero/content/methods/block-times.html."""

    block_time_max_start_speed_kt: float = 30.0
    """Highest ground speed, in knots, at which a movement counts as starting
    from a standstill (off-block) or ending in one (in-block), with
    :attr:`block_times_on_ground_only`.

    An off-block time is the start of a movement from a standstill. The
    report where the movement starts must be at most this fast, or be
    flagged on the ground; the same holds for the report where the last
    movement ends, for an in-block time. A movement that starts at the
    track's first report while faster than this (or ends at its last report)
    gives no block time: the aircraft was already moving when it was first
    received (or still moving when last received), so the moment it left or
    reached the stand was not seen. The speed is the lower of the reported
    ground speed and the speed derived from successive positions, because
    the reported value is sometimes stale on the ground. 30 kt is the top of
    normal taxi speeds; a push-back moves at 2 to 5 kt."""

    # -- block times and stands (work package 3 of the 2026-06-14 corrections)
    ground_movement_through_reports_without_speed: bool = True
    """Read ground movement across reports whose speed is not known.

    On the airport surface about 40% of the reports carry no position and
    under 1% a reported ground speed, so the speed is mostly derived from
    two positions. With this setting the speed is derived between a report
    with a position and the previous report with a position, when the two
    are at most :attr:`ground_speed_max_position_gap_seconds` apart, and a
    report whose speed is not known keeps the moving or standing state of
    the last report whose speed is known. A movement then ends only at a
    report that shows the aircraft standing (a speed of at most
    :attr:`ground_speed_threshold_kt`) or after a silence of more than
    :attr:`airport_trace_gap_seconds` without any known speed. A movement
    still starts and ends at a report whose speed is known.

    Without it, the speed is derived from the report just before, so it is
    unknown after every report without a position, and an unknown speed ends
    the movement: a taxi whose positioned reports alternate with reports
    without position looks like a standstill, and the take-off or landing
    roll is taken as the movement. Off in release v0.0.2."""

    ground_speed_max_position_gap_seconds: float = 30.0
    """Longest time, in seconds, between two reports with a position from
    which a ground speed is derived, with
    :attr:`ground_movement_through_reports_without_speed`. Over a longer
    time the aircraft may have stopped and moved again in between, so the
    average speed would say little."""

    block_time_off_the_runway: bool = True
    """Give no block time on the runway or next to the take-off or landing,
    and judge a standstill on speed.

    With this setting an off-block time is not given when it lies less than
    :attr:`block_time_min_gap_to_runway_time_seconds` before the take-off of
    its leg, or between the line-up and that take-off; an in-block time is
    not given when it lies less than that after the landing of its leg, or
    between that landing and the ``runway-vacated`` that follows it. A
    movement starts from (ends in) a standstill when the report where it
    starts (ends) is at most :attr:`block_time_max_start_speed_kt` fast; the
    flag "on the ground" counts only where no speed of that report is known,
    because the take-off and landing rolls are flagged on the ground too.
    Off in release v0.0.2."""

    block_time_min_gap_to_runway_time_seconds: float = 60.0
    """Shortest time, in seconds, between a block time and the take-off or
    landing of its leg, with :attr:`block_time_off_the_runway`. No taxi
    takes less than a minute; a block time closer than that is the take-off
    or landing roll."""

    block_time_stand_at_same_airport: bool = True
    """Name the stand of a block time only from a stand passage in the same
    ground part (so at the same airport) that lasts at least
    :attr:`block_time_min_stand_dwell_seconds` or reaches to within a
    minute of the block time (an aircraft that parks is often in the
    stand's cells for a few seconds only, because its later reports carry no
    position).

    The ground part is the part of the track between the track's start, a
    take-off, a landing and the track's end in which the block time lies;
    an off-block time names a stand exit in it, an in-block time a stand
    entry. A block time without such a passage is still given, as before
    (a stand passage anywhere in the track is needed), and names no stand
    and no airport: the stand at a leg's own airport is often not received,
    and requiring it would lose block times that airport records confirm.
    Without this setting a passage of a few seconds (taxiing past a stand)
    can be named. Off in release v0.0.2."""

    block_time_min_stand_dwell_seconds: float = 30.0
    """Shortest stand passage, in seconds (``info.opdi_time_in_use_s`` of the
    stand events), that counts as a stop at the stand for the block times.
    Taxiing across the cells of a stand takes 5 to 15 s."""

    block_time_from_last_stand_stop: bool = True
    """Take the off-block time from the stand stop the aircraft left last
    before the take-off, and the in-block time from the stand stop it
    reached first after the landing.

    For an off-block time, the stand stop is the last stand passage of the
    ground part that lasts at least :attr:`block_time_long_stand_stop_seconds`
    (a shorter passage after the push-back is often where the engines are
    started); the off-block time is the start of the first movement that
    starts at or after the entry of that passage and still goes on 30 s
    later. Without such a stop the first movement of the part is kept. For
    an in-block time, the stand stop is the first stand passage after the
    landing of at least that long, or one that lasts to the end of the
    track; without one, the last movement of the part is kept. The in-block
    time is the
    end of the movement before the longest stop between two movements (or
    between the last movement and the end of the track) that overlaps that
    passage. The block time names that stand. Without this setting the
    off-block time is the first movement of the part and the in-block time
    the last, so a tow hours before the departure or
    after the arrival is taken instead. Off in release v0.0.2."""

    block_time_long_stand_stop_seconds: float = 600.0
    """Stand passage, in seconds, long enough to be a stand stop rather than
    a short wait, with :attr:`block_time_from_last_stand_stop`: an aircraft
    that is repositioned stands on each stand for longer than this."""

    block_time_stand_longest_dwell: bool = True
    """Name the stand of a block time by the longest stand passage of its
    ground part, rather than by the passage nearest in time.

    The passage nearest in time is often a few seconds of taxiing across the
    cells of a neighbouring stand. Off in release v0.0.2."""

    block_time_one_part_per_leg: bool = True
    """Read each block time in one ground part only. A track with a landing
    but no take-off has one ground part after its landing, and a track with
    a take-off but no landing one before its take-off. Without this setting
    such a track also gets a ground part without take-off or landing, and
    its block time can lose the landing or take-off in ``info``, so the
    order checks cannot read it. Off in release v0.0.2."""

    block_time_needs_flight: bool = True
    """Give no block time to a track that never leaves the ground: one with
    no take-off or landing time and no reports in flight for at least
    :attr:`ground_move_min_seconds`. Airport vehicles and aircraft under
    tow have such tracks. Off in release v0.0.2."""

    layout_ref_missing_as_null: bool = True
    """Write a missing name of an airport layout element (a stand, taxiway
    or apron without a ``ref`` tag in OpenStreetMap) as no value, not as
    the text ``"None"``, ``"nan"`` or an empty text. Applies where step 00b
    builds the layout table and where step 04 reads it (``info.osm_ref``,
    ``info.stand``). Off in release v0.0.2."""

    # -- runway identification for take-off and landing times ------------
    runway_max_dist_nm: float = 5.0
    """Only reports within this distance of the airport, in NM, are used to
    identify the runway (the value of the ``traffic`` library)."""

    runway_max_height_ft: float = 1500.0
    """Only reports at most this high above the airport, in feet, are used
    to identify the runway."""

    runway_min_vert_rate_ftmin: float = 257.0
    """Vertical rate, in ft/min, that tells the initial climb or final
    descent from movement on the ground (1.3 m/s)."""

    runway_min_groundspeed_kt: float = 30.0
    """Lowest ground speed, in knots, of a report used to identify the
    runway, which leaves out taxiing."""

    runway_max_bearing_deg: float = 10.0
    """Largest difference, in degrees, between the median track of the
    reports and a runway's direction for that runway to be a candidate."""

    # -- level segments ---------------------------------------------------
    #
    # Values of the ICAO and EUROCONTROL definitions of level flight in climb
    # and descent, used by the level-off events.
    # See https://www.opdi.aero/content/methods/vertical-events.html.
    level_analysis_radius_nm: float = 200.0
    """Radius, in NM, around the departure airport within which the climb
    is analysed, and around the destination airport within which the descent
    is analysed. Measured as great-circle distance to the airport."""

    level_vertical_speed_limit_ftmin: float = 300.0
    """Largest vertical rate, in ft/min, of a report that belongs to a level
    segment."""

    level_band_limit_ft: float = 200.0
    """Height of the altitude band, in feet, within which the reports must
    stay to belong to one level segment."""

    level_min_duration_seconds: float = 20.0
    """Shortest level segment reported, in seconds."""

    level_exclusion_box_pct: float = 90.0
    """Lower edge of the cruise exclusion box, as a percentage of the top of
    climb (or top of descent) altitude. A level segment above this edge and
    longer than :attr:`level_exclusion_box_seconds` is cruise, not a
    level-off, and is left out."""

    level_exclusion_box_seconds: float = 300.0
    """Duration, in seconds, above which a level segment inside the
    exclusion box counts as cruise."""

    level_min_altitude_climb_ft: float = 3000.0
    """Height, in feet, from which level segments are looked for in the
    climb; the climb below it is not analysed."""

    level_min_altitude_descent_ft: float = 1800.0
    """Height, in feet, below which level segments are no longer looked for
    in the descent. Lower than for the climb, because an aircraft on final
    approach is close to level."""

    # -- runway milestones -------------------------------------------------
    emit_runway_milestones: bool = True
    """Publish the runway milestones of Airport Collaborative Decision
    Making (A-CDM): ``line-up``, ``take-off-roll``, ``airborne``,
    ``xing-runway-threshold``, ``touchdown``, ``runway-vacated``,
    ``go-around`` and the two runway-crossing events. Off in release v0.0.2, which published
    ``take-off``, ``landing``, ``entry-runway`` and ``exit-runway`` instead.
    See https://www.opdi.aero/content/methods/runway-milestones.html."""

    runway_airborne_height_ft: float = 15.0
    """Height above the airport, in feet, at which the wheels are taken to
    leave or meet the runway. ``airborne`` and ``touchdown`` are the times,
    interpolated between reports, at which the height crosses this value.
    It is small rather than zero because barometric altitude near the
    ground is noisy by a few feet."""

    runway_airborne_max_bracket_seconds: float = 60.0
    """Longest time, in seconds, between the two reports with an altitude
    that ``airborne`` or ``touchdown`` is interpolated between, when the
    reports in between have no altitude. A crossing over a longer gap is not
    given: the aircraft climbs or descends several hundred feet in a minute
    near the runway, so its time could not be placed. The time between the
    two reports is written in ``info.bracket_s``."""

    runway_roll_speed_kt: float = 50.0
    """Ground speed, in knots, above which a departing aircraft is rolling
    for take-off rather than taxiing (taxi speed is about 20 kt, rotation
    about 140 kt)."""

    runway_roll_min_seconds: float = 5.0
    """Seconds the roll speed must last before ``take-off-roll`` is set, so
    a fast turn-off is not read as a take-off roll."""

    runway_take_off_needs_air_report: bool = True
    """Read a pass over a runway as a take-off only if it ends in the air:
    the report that shows the aircraft above
    :attr:`runway_airborne_height_ft` at the end of the pass is not flagged
    on the ground, and neither is the last report of the pass, unless the
    pass also holds two reports in the air 100 to 3,000 ft above the airport
    (a take-off and a landing on the same runway a few minutes apart, as in
    a circuit).

    A take-off ends with the aircraft in the air. On the ground, the
    barometric altitude of a report can read 20 to 60 ft above the airport's
    elevation, because it is measured against standard pressure and not the
    pressure of the day. Without this setting, a landing roll then looks like
    a take-off when it is read in the opposite direction of the runway (from
    the far end, with no approach before it): it starts low, just after the
    touchdown, and ends "above 15 ft" on a report at or just before the
    reports that the transponder flags on the ground. The take-off time,
    ``line-up`` and ``take-off-roll`` are then placed on the landing roll.
    Reports without the flag count as not on the ground. Off in release
    v0.0.2. See
    https://www.opdi.aero/content/methods/runway-milestones.html."""

    runway_taxi_speed_kt: float = 40.0
    """Highest ground speed, in knots, of a passage over a runway that is
    read as a taxiing crossing. Take-offs and landings exceed it."""

    runway_crossing_max_seconds: float = 120.0
    """Longest passage over a runway, in seconds, that is read as a crossing."""

    runway_align_max_deg: float = 30.0
    """Largest difference, in degrees, between the median track of a passage
    and the runway direction for the passage to be a take-off or landing."""

    runway_cross_min_deg: float = 45.0
    """Smallest angle, in degrees, between a passage and the runway
    direction for the passage to be a crossing. A passage between
    :attr:`runway_align_max_deg` and this angle is classified as neither and
    gets no event."""

    goaround_trigger_height_ft: float = 500.0
    """Height above the airport, in feet, below which an arrival must
    descend for a later climb to be a go-around rather than a level-off."""

    goaround_recovery_height_ft: float = 1500.0
    """Height above the airport, in feet, that the aircraft must climb back
    to, without touching down, for ``go-around`` to be published."""

    goaround_min_roc_ftmin: float = 500.0
    """Rate of climb, in ft/min, that the climb back must reach."""

    goaround_radius_nm: float = 10.0
    """Largest distance from the airport, in NM, at which a go-around is
    looked for."""

    # -- runway events: corrections of work package 2 (spot checks of one day)
    runway_height_above_track_ground: bool = True
    """Measure the height of the runway events above the track's own
    barometric altitude on the ground at that airport, rather than above the
    airport's published elevation.

    Barometric altitude is measured from standard pressure, so an aircraft
    standing on the runway reads several hundred feet above or below the
    elevation on a day of low or high pressure (from -571 to +232 ft on one
    measured day). The track's ground altitude is read from its reports at the
    runways of that airport that are flagged on the ground or slower than
    30 kt, and from the reports within 30 s of one (airliners send no
    altitude on the ground): the highest of their altitudes at most 30 ft
    above the lowest (altitudes come in steps of 25 ft). Without such
    reports, or when their altitude is more than 1,000 ft from the elevation,
    the published elevation of the pass's own airport is used.
    ``info.height_reference`` says which (``track`` or ``elevation``). Off in
    release v0.0.2."""

    runway_direction_by_motion: bool = True
    """Name the runway direction of a take-off or landing by the direction
    the aircraft moves, and give one runway per pass.

    The two directions of a runway share one centre line. A take-off's
    climb-out lies short of the opposite threshold, so a pass read in the
    opposite direction also looks like a take-off. With this setting, the
    passes over one runway are numbered together for both directions, the
    direction is the one within 90 degrees of the median track of the reports
    faster than :attr:`runway_roll_speed_kt`, one pass per runway, class and
    pass number is kept, and of passes of one class on parallel runways at
    overlapping times, the one with reports on its runway and the smallest
    median distance from its centre line. Off in release v0.0.2."""

    runway_events_on_runway_reports: bool = True
    """Take ``line-up``, ``runway-vacated`` and the runway crossings from the
    reports on the runway itself (between the two thresholds and within half
    its width), not from the approach area before the threshold, and give
    none of them when the pass holds no report on the runway. A crossing is
    also classified from those reports only. Off in release v0.0.2."""

    runway_roll_speed_from_positions: bool = True
    """Find ``take-off-roll`` from the ground speed worked out from
    consecutive positions (at most 30 s apart) where a report carries no
    speed (almost every report on the ground), skip reports with no speed
    instead of letting them end the roll, and use only reports with a
    position. Off in release v0.0.2."""

    runway_line_up_needs_ground_report: bool = True
    """Give ``line-up`` and ``take-off-roll`` only for a take-off that starts
    on the ground: a report flagged on the ground, or slower than
    :attr:`runway_roll_speed_kt`, before the first fast report of the pass
    (looking up to 60 s before it starts), or, when reception starts during
    the roll, a first fast report flagged on the ground. A touch-and-go, a low
    approach or a track that reappears in the air keeps its take-off time but
    gets no ``line-up`` or ``take-off-roll``. Off in release v0.0.2."""

    runway_threshold_crossing_last_only: bool = True
    """Give one ``xing-runway-threshold`` per landing: the last crossing of
    the threshold towards the runway in the pass, and only when it is at most
    :attr:`runway_threshold_crossing_max_height_ft` above the ground. A low
    pass, an overflight or a circuit over the runway gives none. Off in
    release v0.0.2."""

    runway_threshold_crossing_max_height_ft: float = 300.0
    """Highest height above the ground, in feet, of a threshold crossing.
    On a 3 degree approach an aircraft crosses the threshold about 50 ft
    above it."""

    goaround_excludes_landings: bool = True
    """Do not give a go-around when the aircraft was on the ground in between
    (a report flagged on the ground, or slower than 50 kt with no altitude),
    when the low part lasts more than :attr:`goaround_max_excursion_seconds`,
    or when it holds a gap of more than :attr:`goaround_max_gap_seconds`
    between two reports. A landing followed by a take-off, or a data hole,
    is then not read as an abandoned approach. Off in release v0.0.2."""

    goaround_max_excursion_seconds: float = 600.0
    """Longest time, in seconds, from the descent below
    :attr:`goaround_trigger_height_ft` to the climb back above
    :attr:`goaround_recovery_height_ft`. The go-arounds read as correct on
    one measured day took 42 to 547 s."""

    goaround_max_gap_seconds: float = 60.0
    """Longest time, in seconds, between two consecutive reports in a
    go-around."""

    goaround_in_approach_corridor: bool = True
    """Give a go-around only when its lowest point lies on the extended centre
    line of a runway of the destination airport: at most
    :attr:`goaround_corridor_length_nm` before a threshold or over the runway,
    and at most :attr:`goaround_corridor_half_width_nm` from the centre line.
    A low point at another airfield, a heliport or a glider site near the
    destination is then not a go-around. When the destination's runways are
    not known, the go-around is kept. Off in release v0.0.2."""

    goaround_corridor_length_nm: float = 5.0
    """Distance before the threshold, in NM, of the approach corridor a
    go-around's lowest point must lie in."""

    goaround_corridor_half_width_nm: float = 0.5
    """Half the width, in NM, of that corridor."""

    runway_crossing_ground_without_altitude: bool = True
    """Read a report flagged on the ground that carries no altitude as on the
    ground when classifying a runway crossing (airliners send no altitude on
    the ground), and do not give a crossing of a runway in a pass that is a
    take-off or landing on that same runway. Off in release v0.0.2."""

    runway_window_times_checked: bool = True
    """Check the take-off and landing times of the runway window method
    (``info.method = "legacy"``): use only reports at most
    :attr:`runway_window_max_height_ft` above the track's ground altitude (or
    the elevation; see that setting for when it acts) and at most
    :attr:`runway_window_max_cross_track_nm` from the runway's extended
    centre line; for a landing, require a report on the
    ground (flagged, or slower than 50 kt) after it when the track goes on for
    more than 120 s; publish the position and altitude of the report the time
    is taken from; and say in ``info.at_track_edge`` whether that report is
    the first (take-off) or last (landing) of the track. Off in release
    v0.0.2."""

    runway_window_max_height_ft: float = 1500.0
    """Highest height, in feet, above the track's ground altitude at the
    airport, of a report that gives a take-off or landing time of the runway
    window method. The ground altitude is the median altitude of the track's
    reports near the airport, on the same visit of the airport, that are
    flagged on the ground or slower than 30 kt, when it is within 1,000 ft
    of the published elevation; otherwise the published elevation is used.

    The reports that can give these times are already at most
    :attr:`runway_max_height_ft` (1,500 ft) above the published elevation.
    So with both limits at 1,500 ft, this one removes a time only when the
    ground altitude reads below the elevation (a day of high pressure, with
    the altimeter set to standard pressure): a report 1,400 ft above the
    elevation is then more than 1,500 ft above the ground. Where the ground
    reads at or above the elevation, or no ground report is known, it
    removes nothing."""

    runway_window_max_cross_track_nm: float = 0.5
    """Largest distance, in NM, from the runway's extended centre line of a
    report that gives a take-off or landing time of the runway window
    method."""

    runway_times_position_between_reports: bool = True
    """Give the lift-off and touchdown a position when one of the two reports
    around the crossing of :attr:`runway_airborne_height_ft` has an altitude
    but no position. That report takes the position interpolated in time
    between the positioned reports before and after it, when both are at
    most :attr:`crossing_value_max_gap_seconds` away (30 s when that is not
    set); a report that still has no position takes no part in the
    crossing, which is then placed between the nearest reports that have
    both an altitude and a position (within
    :attr:`runway_airborne_max_bracket_seconds`). Without it the crossing
    can be placed against a report without a position, and is then
    published without one."""

    # -- counting movements ------------------------------------------------
    supersede_window_seconds: float = 10800.0
    """Two departures (or two arrivals) of one aircraft at one airport closer
    than this, in seconds, may be one movement. The earlier one is not
    counted as a movement when it has no take-off: a departure is often split
    into a short track that never leaves the stand and the flight itself.
    Three hours is longer than any such split and shorter than a real second
    flight. See https://www.opdi.aero/content/methods/airports.html."""

    # -- reception gaps inside a track -----------------------------------
    track_hole_seconds: float = 1800.0
    """A gap longer than this, in seconds, between two reports of one track
    is a hole. The default track rule keeps one ``track_id`` across a hole
    in reception when the two sides fit one continuous flight. No event
    detector interpolates across a hole: level segments and crossings are
    found in each piece between holes. Equal to
    :attr:`SegmentationConfig.gap_minutes`."""

    # -- holding -----------------------------------------------------------
    emit_holding: bool = True
    """Publish ``holding-start`` and ``holding-end`` (version
    ``holding_v0.2.0``).

    A hold is a period in which the aircraft turns through at least one
    nearly complete circle in one direction while staying in a small area at
    roughly one level, above circuit height and at flying speed. Racetrack
    holds and single orbits both count; ``info.pattern`` says which. The
    thresholds follow published holding detection methods and ICAO Doc 8168.
    See https://www.opdi.aero/content/methods/holding.html."""

    holding_window_seconds: float = 360.0
    """Length, in seconds, of the window over which the turn is summed. One
    holding circuit takes 4 to 7 minutes."""

    holding_sample_period_seconds: float = 5.0
    """Time between reports, in seconds, used to count the window in rows:
    the window spans ``holding_window_seconds / holding_sample_period_seconds``
    rows, which is much faster to compute than a window bounded in time. A
    window whose two ends are not 0.8 to 1.25 window lengths apart in time
    (because of a gap inside it) is not judged."""

    holding_max_sample_gap_seconds: float = 60.0
    """A heading change across a gap longer than this, in seconds, is not
    counted as a turn."""

    holding_turn_rate_spike_deg: float = 6.0
    """Turn rate, in degrees per second, above which a heading change is
    noise rather than a turn (twice the ICAO maximum of 3 deg/s)."""

    holding_turn_rate_min_deg: float = 0.4
    """Turn rate, in degrees per second, from which a report counts as
    turning. Airliners in a hold turn at 1 to 2 deg/s; track noise at the
    5-second report interval is about 0.2 deg/s."""

    holding_net_turn_min_deg: float = 300.0
    """Net turn, in degrees, a window must contain. A full circle is 360; 300
    allows for a window that includes a straight leg."""

    holding_same_sense_min_pct: float = 80.0
    """Smallest share, in percent, of the turn made in the window's main
    direction. Radar vectoring turns both ways and a figure of eight nets to
    zero; a hold turns one way."""

    holding_max_displacement_nm: float = 8.0
    """Largest distance, in NM, between the first and last positions of the
    window. A hold is 8 to 10 NM long; a go-around loop or a vectoring
    detour is wider."""

    holding_min_path_nm: float = 15.0
    """Smallest distance flown in the window, in NM (6 minutes at 150 kt),
    which leaves out slow traffic."""

    holding_max_altitude_range_ft: float = 2500.0
    """Largest total altitude change in one window, in feet (the sum of the
    changes between reports): two 1,000 ft steps down a holding stack. A
    climb or descent through the window exceeds it."""

    holding_min_height_ft: float = 2500.0
    """Height above the airport, in feet, below which nothing is a hold:
    circuits and go-arounds happen below it."""

    holding_min_speed_kt: float = 140.0
    """Lowest ground speed, in knots; helicopters and training circuits are
    slower."""

    holding_bridge_gap_seconds: float = 120.0
    """Two holding stretches closer than this, in seconds, are one hold."""

    holding_min_duration_seconds: float = 240.0
    """Shortest hold kept, in seconds (one standard circuit)."""

    holding_min_episode_turn_deg: float = 330.0
    """Smallest net turn over the whole hold, in degrees: at least one nearly
    complete circle."""

    holding_orbit_max_straight_pct: float = 15.0
    """Share of reports that are not turning, in percent, below which the
    hold is an orbit rather than a racetrack (a racetrack is 35 to 55 %
    straight)."""

    holding_orbit_max_turn_deg: float = 450.0
    """Total turn, in degrees, above which a hold is a racetrack even
    without straight legs (more than one and a quarter circles)."""

    holding_goaround_exclusion_seconds: float = 180.0
    """A hold that starts or ends within this many seconds of a
    ``go-around`` of the same track is left out: repeated approaches are not
    holds."""

    # Work package 6: which label and context a hold gets, where it starts
    # and ends.
    holding_pattern_by_straight_share: bool = True
    """Decide ``info.pattern`` on the share of straight reports alone: an
    ``orbit`` below :attr:`holding_orbit_max_straight_pct`, else a
    ``racetrack``. Without it (:attr:`holding_orbit_max_turn_deg`) a
    continuous circle of more than one and a quarter turns was a
    ``racetrack``. Off in release v0.0.2."""

    holding_trim_to_main_direction: bool = True
    """Start and end a hold at its first and last report turning in its main
    direction (the direction of its net turn). Without it, a turn the other
    way just before or after the hold (a lead-in turn, the turn back after
    leaving the stack) is counted in the hold. The straight legs of a
    racetrack lie between turning reports and stay inside. Off in release
    v0.0.2."""

    holding_altitude_from_known_reports: bool = True
    """Take the altitude of ``holding-start`` and ``holding-end`` from the
    first and last report of the hold that has one, rather than from its
    first and last report, which can have none. The time and position stay
    those of the first and last report. Off in release v0.0.2."""

    holding_and_go_arounds_at_corrected_airports: bool = True
    """Find the go-arounds, and give each hold its context, at the airports
    the flight list publishes: those of step 03 after the correction by the
    runway events of step 04b
    (:func:`opdi.pipeline.flight_list_milestones.correct_aerodromes`),
    computed in step 04 from the events found before these two families.
    A flight whose destination step 04b replaces then has its go-arounds
    looked for, and its holds read as an arrival, at the airport it landed
    at. Without it (release v0.0.2) both use the airports of step 03."""

    holding_context: bool = True
    """Write ``info.context`` on each hold:

    * ``arrival``: the flight lands at its destination at most
      :attr:`holding_arrival_max_seconds` after the end of the hold, which
      ends at most :attr:`holding_arrival_max_distance_nm` from that airport;
    * ``en-route``: the flight lands, but later or further away;
    * ``other``: a local flight (departure equal to destination, or no
      destination), no landing after the hold, a track with
      :attr:`holding_other_min_episodes` holds or more, or holds making up
      more than :attr:`holding_other_max_share_pct` of the flight's time in
      the air. Aerial work, survey, firefighting and training flights that
      circle meet the holding definition; they fall here.

    The landings are the take-off and landing times (``ATOT``, ``ALDT``) of
    the same step. Delay statistics should use ``arrival``. Off in release
    v0.0.2."""

    holding_arrival_max_seconds: float = 3600.0
    """Longest time, in seconds, from the end of a hold to the landing at
    the destination for the hold to be an ``arrival`` hold."""

    holding_arrival_max_distance_nm: float = 150.0
    """Largest distance, in NM, from the end of a hold to the destination
    for the hold to be an ``arrival`` hold."""

    holding_other_min_episodes: int = 3
    """A track with this many holds or more is circling for another purpose
    than waiting to land: its holds are ``other``."""

    holding_other_max_share_pct: float = 50.0
    """Share, in percent, of the time in the air (from the take-off to the
    landing) above which the holds of a track are ``other``. Not applied to a
    track whose take-off was not seen."""

    # -- point merge -------------------------------------------------------
    emit_point_merge: bool = True
    """Publish ``pms-leg-entry``, ``pms-leg-exit`` and ``pms-merge-point``
    (version ``pms_v0.2.0``) for arrivals at airports with a point merge
    system listed in ``reference/data/point_merge_systems.csv``.

    In a point merge system, arrivals fly an arc (a sequencing leg) at a
    constant distance from a merge point until they are sent direct to it.
    A leg is found as a stretch flown at constant distance from the merge
    point, inside the published band, moving along the arc, and followed by
    a direct to the merge point.
    See https://www.opdi.aero/content/methods/point-merge.html."""

    pms_candidate_radius_nm: float = 60.0
    """Reports farther than this, in NM, from every merge point of the
    destination are not looked at (the longest legs listed are 46 NM
    out)."""

    pms_candidate_max_altitude_ft: float = 25000.0
    """Reports above this altitude, in feet, are not looked at (legs are
    flown between FL070 and FL270)."""

    pms_leg_window_seconds: float = 45.0
    """Half-length, in seconds, of the window over which constant distance
    is tested (90 seconds in all)."""

    pms_leg_distance_band_nm: float = 1.5
    """Largest spread, in NM, of the distance to the merge point inside the
    window. A published leg is a chain of straight segments, not a perfect
    arc, and navigation has a tolerance."""

    pms_leg_distance_margin_nm: float = 2.0
    """Margin, in NM, around the listed band of leg distances."""

    pms_leg_min_bearing_sweep_deg: float = 8.0
    """Smallest change, in degrees, of the bearing from the merge point over
    the window. An aircraft on an 18 NM arc at 250 kt sweeps about 20 degrees
    in 90 seconds; an aircraft holding at the same distance sweeps none."""

    pms_leg_tangential_min_pct: float = 80.0
    """Smallest share, in percent, of reports in the window whose track is 60
    to 120 degrees off the bearing to the merge point, that is, moving along
    the arc."""

    pms_leg_min_duration_seconds: float = 90.0
    """Shortest leg kept, in seconds."""

    pms_direct_lookahead_seconds: float = 600.0
    """A leg counts only if the track reaches the merge point within this
    many seconds."""

    pms_direct_align_deg: float = 5.0
    """Largest difference, in degrees, between the track and the bearing to
    the merge point for the aircraft to be flying direct to it. Reported as
    ``info.direct_aligned``; it does not decide whether a leg is kept."""

    pms_direct_min_duration_seconds: float = 60.0
    """Shortest time, in seconds, flown direct that sets
    ``info.direct_aligned``."""

    pms_merge_point_radius_nm: float = 3.0
    """The track must come this close to the merge point, in NM, from a leg
    at least twice as far out, for the leg to count. ``pms-merge-point`` is
    set at the closest approach."""

    # Work package 6: where a leg ends, split legs, straight or climbing
    # legs, and which passages of a merge point count.
    pms_extend_leg_exit: bool = True
    """Move the exit of a leg past the last report the window test accepts,
    report by report, while the aircraft is still moving along the arc
    (track 60 to 120 degrees off the bearing to the merge point) and within
    :attr:`pms_leg_exit_tolerance_nm` of the distance of the last accepted
    report. The window test looks 45 s ahead and fails as soon as the
    turn towards the merge point enters the window, so without this the exit
    is about 25 s before the aircraft leaves the arc and leg times are about
    15 % short. The entry is not moved. Off in release v0.0.2."""

    pms_leg_exit_tolerance_nm: float = 0.5
    """Largest change, in NM, of the distance to the merge point over which
    the exit of a leg is moved (see :attr:`pms_extend_leg_exit`)."""

    pms_join_split_legs: bool = True
    """Count two stretches on the arc of one merge point as one leg when the
    second starts at most :attr:`pms_join_max_gap_seconds` after the first
    ends and both turn the same way about the merge point. A gap of a minute
    in the positions otherwise splits one leg into two, each with a shorter
    time and distance. Off in release v0.0.2."""

    pms_join_max_gap_seconds: float = 120.0
    """Longest time, in seconds, between two stretches joined into one leg."""

    pms_leg_must_turn: bool = True
    """Keep a leg only if it turns with the arc and does not climb.

    On an arc the heading turns with the bearing from the merge point; on a
    straight line that only grazes the circle at the leg distance it does
    not. A leg shorter than :attr:`pms_leg_turn_test_max_seconds` must turn
    by at least :attr:`pms_leg_min_turn_share_pct` of its bearing sweep. A leg
    whose altitude rises by more than :attr:`pms_leg_max_climb_ft` from
    entry to exit is a missed approach or a climbing turn near the airport,
    not a sequencing leg. Off in release v0.0.2."""

    pms_leg_min_turn_share_pct: float = 30.0
    """Smallest heading change of a short leg, in percent of its bearing
    sweep (real arcs measure 60 to 80 %, straight segments 0 to 3 %)."""

    pms_leg_turn_test_max_seconds: float = 180.0
    """Legs shorter than this, in seconds, must turn with the arc (see
    :attr:`pms_leg_must_turn`). Published legs are chains of straight
    segments, so a longer leg may be straight for a while."""

    pms_leg_max_climb_ft: float = 500.0
    """Largest rise in altitude, in feet, from the entry to the exit of a
    leg."""

    pms_merge_point_conditions: bool = True
    """Publish ``pms-merge-point`` only for a passage that can be an arrival
    through the system: with no take-off of the track between the passage
    and the landing at the airport that follows it (with no such landing, no
    take-off after the passage), descending or
    level (at most :attr:`pms_merge_point_max_climb_ftmin`), at
    :attr:`pms_merge_point_min_speed_kt` or more, for a system that serves
    the runway landed on where the system's name and the landing say which
    (``RJTT-34L`` serves runways 31 to 01, within
    :attr:`pms_runway_direction_tolerance_deg`; ``LIME-ALL`` any runway), and one per
    approach (the passage that follows a leg, else the closest to its merge
    point). Without it every passage within :attr:`pms_merge_point_radius_nm`
    of a listed merge point counts, departures and light aircraft included.
    Off in release v0.0.2."""

    pms_runway_direction_tolerance_deg: float = 30.0
    """Largest difference, in degrees, between the runway direction in a
    system's name and the runway landed for the system to serve it, so that
    one system serves the parallel runways of a flow (LTFM-16NW: 16L, 16R,
    17L, 17R and 18)."""

    pms_merge_point_min_speed_kt: float = 150.0
    """Lowest ground speed, in knots, of a merge-point passage."""

    pms_merge_point_max_climb_ftmin: float = 500.0
    """Highest vertical rate, in feet per minute, of a merge-point passage."""

    # -- FIR crossings -----------------------------------------------------
    emit_fir_crossings: bool = True
    """Publish ``xing-fir`` (version ``fir_v0.2.0``): one event at each
    change of flight information region (FIR) along a track, from the
    airspace table step 00c builds (``opdi_h3_airspace_ref``). The regions
    are those of the Network Manager airspace model, with lower FIRs and
    upper UIRs, so climbing through their boundary level is a crossing as
    well. Each event names the AIRAC cycle of the airspace definition.
    See https://www.opdi.aero/content/methods/fir-crossings.html."""

    fir_min_dwell_seconds: float = 60.0
    """A stay in one region shorter than this, in seconds, is counted as part
    of the region before it. A track along a boundary would otherwise cross
    it every few seconds."""

    distance_from_last_known_position: bool = True
    """Compute the distance flown from the last known position.

    Cleaning sets untrusted positions to NULL. With this setting (version
    ``distance_v0.0.3``, or ``distance_v0.0.4`` with
    :attr:`distance_max_implied_speed_kt` or
    :attr:`distance_between_positions`), each step of the distance flown is measured from the
    last report that has a position, so a missing position no longer drops
    the distance before and after it; across a gap the straight line between
    its two sides is added, which is the least the aircraft can have flown.
    Off (version ``distance_v0.0.2``, release v0.0.2), a report next to a
    missing position adds no distance. The level segments' ``distance_nm``
    values depend on it as well.
    See https://www.opdi.aero/content/methods/measurements.html."""

    # -- crossings, rings, FIR and the two measurements (work package 5) --
    crossing_max_implied_speed_kt: Optional[float] = 1000.0
    """Highest ground speed, in knots, that the two reports a crossing is
    placed between may imply. The speed is the great-circle distance between
    their positions divided by the time between them. A flight-level, ring or
    FIR crossing whose two reports imply more is not given: the position
    jumped (a spoofed or corrupted position), and a jump across all six
    distance circles at once would otherwise give six crossings within
    seconds. A crossing whose reports lack a position is not tested. Like a
    crossing that cannot be placed, it still counts in ``crossing_seq``.
    Real reception gaps over the ocean imply 390 to 560 kt and are kept.
    The positions may also be :attr:`position_jump_allowance_nm` further
    apart than the speed allows. None in release v0.0.2 (no test)."""

    crossing_skips_position_jumps: bool = True
    """Leave out of the crossings and of the distance flown a position that
    jumps away from the track and back.

    A report's position counts as a jump when the step to it from the
    positioned report before it, and the step from it to the next one, each
    imply more than :attr:`crossing_max_implied_speed_kt` (plus
    :attr:`position_jump_allowance_nm`), while the reports either side are
    consistent with each other; two such reports in a row count too. The
    flight-level, distance-circle and FIR crossings and the distance flown
    then read that report as one without a position, so a crossing is
    placed between the good reports either side instead of being dropped
    for the jump (40 crossings of the 40 and 100 NM circles that airport
    records confirm, most near Istanbul where positions are spoofed).
    Without it the crossing is dropped. Off in release v0.0.2."""

    ring_crossing_keeps_single_steps: bool = True
    """Keep a distance-circle crossing whose two reports imply more than
    :attr:`crossing_max_implied_speed_kt` when no other circle radius lies
    between the two distances: the position stepped (by 5 to 20 NM near
    Istanbul, where positions are spoofed) and stayed, which does not carry
    the track across several circles at once as the jumps the speed limit
    is for do. Such a crossing is placed between the two reports as before
    and has ``info.position_jump`` = true. Off in release v0.0.2."""

    position_jump_allowance_nm: float = 2.0
    """Distance, in NM, by which two positions may be further apart than
    :attr:`crossing_max_implied_speed_kt` and
    :attr:`distance_max_implied_speed_kt` allow before the step counts as a
    jump. Over 5 s an aircraft at 500 kt moves 0.7 NM, and a position that
    lags and then catches up can make two reports 5 s apart look faster than
    1,000 kt while the aircraft flew normally; the jumps the limit is for
    move 4 to 400 NM at once (a glitch, a spoofed position, two aircraft
    under one address)."""

    crossing_value_max_gap_seconds: Optional[float] = 30.0
    """Seconds within which a crossing takes the values its own two reports
    lack from the nearest reports that have them. A flight-level crossing is
    placed between two reports with an altitude, and one of them often has no
    position: the position is then interpolated in time between the nearest
    report with a position before and the nearest one after, each at most this
    many seconds away. A distance-circle crossing is placed between two
    reports with a position, and one of them can have no altitude: each then
    takes its last known altitude, at most this many seconds old. Without a
    report close enough the value stays empty. None in release v0.0.2."""

    rings_around_flight_list_airports: bool = True
    """Draw the distance circles around the departure and destination airports
    that the flight list publishes. Step 04b lets a runway event replace an
    airport found from the trajectory
    (:func:`opdi.pipeline.flight_list_milestones.correct_aerodromes`). With
    this setting step 04 applies the same function to the events it writes
    before it computes the circles, so a circle never names an airport the
    flight list does not have, and running steps 04 and 04b again gives the
    same circles. Off in release v0.0.2 (the airports of step 03)."""

    fir_needs_position: bool = True
    """Look up the FIR only of reports whose position survived cleaning.
    Cleaning blanks an untrusted latitude and longitude but keeps the map
    cell ``h3_res_7`` computed from them, so without this setting a FIR
    crossing can come from a position the pipeline itself rejected, and be
    published without a position."""

    fir_flag_vertical: bool = True
    """Write ``info.vertical`` on each FIR crossing: true when the two regions
    lie one above the other (their flight-level bands do not overlap), as a
    lower FIR and the upper UIR of the same area. Such a crossing is a climb
    or descent through the boundary level, not a crossing of a lateral
    boundary; it is published as before, and the field lets a user count
    lateral crossings only."""

    distance_max_implied_speed_kt: Optional[float] = 1000.0
    """Highest ground speed, in knots, of a step of the distance flown. A step
    from the last known position to the next one that implies more is not
    added: the position jumped (two aircraft under one transponder address, a
    shifted or spoofed position). The next step is measured from the new
    position. Real reception gaps over the ocean imply 390 to 560 kt and are
    still bridged by the straight line. The positions may also be
    :attr:`position_jump_allowance_nm` further apart. None in release v0.0.2
    and in ``distance_v0.0.3`` (no test)."""

    distance_between_positions: bool = True
    """Give a report without a position the distance flown interpolated in
    time between the last report with a position before it and the next one
    after it, when both are in the same piece of track (no gap longer than
    :attr:`track_hole_seconds` between them). Without it the distance stays at
    its value at the last known position and catches up at the next one, so
    an event inside a stretch without positions shows a distance too small,
    and the distance can go down between two events. Before the first known
    position the distance is unknown (empty), not 0; after the last known
    position it stays at its last value."""

    measure_every_event: bool = True
    """Give every event both measurements, the distance flown and the time
    passed, from its own time, once all event families are joined. Events
    whose family gives no distance (level-offs, off-block and in-block times,
    the take-off and landing times of the runway window) get the distance
    interpolated in time between the events before and after them on the same
    track that have one. A measurement without a value is not written."""

    time_passed_from_first_seen: bool = True
    """Count the time passed of every event from the ``first_seen`` event of
    its track, by reading that event's time, so the two always agree; an
    event earlier than ``first_seen`` gets no time passed (version
    ``time_v0.0.2``). Without it (``time_v0.0.1``) the time passed counts
    from the track's first report, which can differ from ``first_seen``."""

    # -- one event type per question -----------------------------------
    merge_milestones_per_airport: bool = True
    """With :attr:`merge_duplicate_milestones`, keep one ``ATOT`` and one
    ``ALDT`` per track and airport (``info.apt_icao``) rather than per track.
    A track that holds several flights lands at several airports; the
    runway milestones may give the landing at a stop on the way and only
    the runway window the one at the destination, and both are landings.
    Two readings of one movement are at one airport and still give one
    event. Off in release v0.0.2."""

    merge_duplicate_milestones: Optional[bool] = None
    """Publish one event type per milestone, where two detectors give the
    same one.

    With the runway milestones on, ``airborne`` and the take-off time
    ``ATOT`` describe the same instant, as do ``touchdown`` and the landing
    time ``ALDT``. With this setting, one event is published::

        ATOT = airborne if detected, else the take-off time of the older method
        ALDT = touchdown if detected, else the landing time of the older method

    ``info.method`` of each such event says which detector gave it
    (``"acdm"`` or ``"legacy"``), because the two have different accuracy.
    The same setting publishes the stand times under the names ``AOBT`` and
    ``AIBT`` and gives the vertical profile tops (:attr:`emit_pru_tops`) the
    names ``top-of-climb`` and ``top-of-descent``.

    ``None`` (default) follows :attr:`emit_runway_milestones`. ``True`` with
    the runway milestones off is refused, as there is nothing to merge. Off
    in release v0.0.2.
    """

    # -- vertical profile tops and level segments --------------------------
    emit_pru_tops: bool = True
    """Publish the top of climb and top of descent of the vertical flight
    efficiency method of the EUROCONTROL Performance Review Unit (PRU), which
    are the first point of the cruise and the last. With
    :attr:`merge_duplicate_milestones` they are named ``top-of-climb`` and
    ``top-of-descent`` and are the only tops published; otherwise they are
    ``top-of-climb-cco`` and ``top-of-descent-cdo``, published beside the
    tops of the phase method.
    See https://www.opdi.aero/content/methods/vertical-events.html."""

    level_method: str = "pru"
    """Method of the level segments: ``"pru"`` (default), a rolling window of
    :attr:`level_window_seconds` whose height follows 300 ft per minute, or
    ``"icao"``, an altitude band anchored at the start of each segment
    (:attr:`level_band_limit_ft`)."""

    level_window_seconds: float = 10.0
    """Length, in seconds, of the rolling window of the ``"pru"`` method
    (10 s gives a window 50 ft high). The trajectory is interpolated to this
    time step first, as the method requires."""

    level_floors_above_field: bool = True
    """Measure :attr:`level_min_altitude_climb_ft` and
    :attr:`level_min_altitude_descent_ft` above the airport's elevation
    (default) rather than as pressure altitude, which would place them
    lower above the ground at a high airport."""

    level_anchor: str = "pru"
    """Tops that bound the cruise exclusion box of the level-off events:
    ``"pru"`` (default, the tops of :attr:`emit_pru_tops`) or ``"phase"``
    (the tops of the phase method)."""

    level_radius_enforced: bool = True
    """Look for level-offs only within :attr:`level_analysis_radius_nm` of
    the departure airport (climb) and the destination airport (descent).
    ``False`` looks at every level segment, whatever its distance."""

    # -- the altitude the detectors read (work package 1) -----------------
    altitude_from_cleaning: bool = True
    """Give the flight detectors of step 04 the barometric altitude that
    survived the cleaning of step 02a.

    The track table holds two barometric altitudes. ``baro_altitude_c`` is
    the altitude after one check of step 02 (a change of more than 200 ft/s
    from the previous report); ``baro_altitude`` in the cleaned tracks
    (``osn_tracks_clean``) is the altitude after the cleaning of step 02a,
    which also removes the values OpenSky repeats when no new altitude
    message arrived, isolated values and spikes. With this setting, when
    step 04 reads the cleaned tracks, the content of ``baro_altitude_c`` is
    replaced by the cleaned ``baro_altitude`` as the reports are read, so
    the flight detectors (tops, level-offs, level segments, flight-level,
    distance-circle and FIR crossings, holding, point merge, first and last
    seen) read the cleaned altitude under their usual column name. A value
    the step 02 check removed but the cleaning kept is used: it is a fresh
    altitude right after a run of repeated values, which the step 02 check
    compares with the repeated value. The runway, ground and airport
    detectors and the plausibility checks keep the step 02 altitude: the
    cleaning removes altitudes read on the take-off and landing roll, and
    there a report without altitude counts as a surface report. Off in
    release v0.0.2."""

    altitude_plausible_min_ft: Optional[float] = -2000.0
    """Lowest barometric altitude, in feet, that step 04 uses. A lower value
    is treated as missing (set to NULL as the reports are read), for every
    detector. ``None`` sets no lower limit (release v0.0.2)."""

    altitude_plausible_max_ft: Optional[float] = 60000.0
    """Highest barometric altitude, in feet, that step 04 uses; above it the
    value is treated as missing, for every detector. Airliners fly below
    45,000 ft and business jets below 51,000 ft. ``None`` sets no upper
    limit (release v0.0.2)."""

    vertical_ignores_ground_altitude: bool = True
    """Do not use the altitude of a report flagged on the ground as a flight
    altitude in the vertical detectors: the tops of climb and descent, the
    level segments of the level-offs and the flight-level crossings.

    Transponders on the ground sometimes send altitudes of 9,800 to
    126,000 ft at the stand, which would become a top of climb or a
    crossing of every flight level. The runway, ground and airport
    detectors still read these reports' altitude, as the altitude an
    aircraft reads on the ground is their reference. Off in release
    v0.0.2."""

    level_max_sample_gap_seconds: Optional[float] = 60.0
    """Longest time, in seconds, between two measured altitudes across which
    the ``"pru"`` level method interpolates.

    The method interpolates the altitude onto a regular time grid. With
    this setting the grid covers only the time from the first to the last
    measured altitude of each piece of track, and a grid window that lies
    between two measured altitudes more than this far apart is not level:
    the altitude there was not measured. Without it (``None``, release
    v0.0.2) the first and last altitudes are held flat to the ends of the
    piece and any gap is bridged by a straight line, which turns a parked
    aircraft or a lost reception into a long level-off."""

    level_segment_min_duration_applied: bool = True
    """Apply :attr:`level_min_duration_seconds` to the level segments of the
    phase method (``level-start``/``level-end``) as well: a segment shorter
    than that, a single report for instance, is not published. Off in
    release v0.0.2."""

    level_segment_not_on_ground: bool = True
    """Publish no ``level-start``/``level-end`` on the ground or on the
    take-off or landing roll: a report flagged on the ground, or less than
    :attr:`phase_ground_ceiling_ft` above the departure or destination
    airport, is not level flight, whatever its speed. The height is
    measured above the airport the report is near: above the departure
    airport within :attr:`level_segment_ground_radius_nm` of it, above the
    destination within that distance of the destination, and above the
    lower of the two airports for a report without a position. A level-off
    at 2,000 ft near a destination at sea level is therefore level flight
    even after a departure from a field at 2,000 ft. Off in release
    v0.0.2."""

    level_segment_ground_radius_nm: float = 10.0
    """Distance, in nautical miles, from an airport within which a report's
    height above that airport is measured for
    :attr:`level_segment_not_on_ground`. Farther from both airports, only a
    report flagged on the ground is taken as ground."""

    tops_position_from_nearest_report: bool = True
    """Place the top of climb and top of descent of the vertical profile
    method at the report with a position nearest in time to the top, within
    :attr:`vertical_position_window_s`; no position when there is none. The
    time passed is counted to the top's own time. Without this setting
    (release v0.0.2), the top takes the position of the last report at or
    before it, which has no position when that report carried only an
    altitude."""

    tops_observed_min_change_ft: Optional[float] = 3000.0
    """Change of altitude, in feet, that makes a top observed:
    ``info.observed`` is true on a top of climb when the track has an
    altitude at least this much below the top before it, and on a top of
    descent when it has one this much below the top after it. Otherwise the
    top sits at the edge of reception (a track that starts or ends in
    cruise) and ``info.observed`` is false. ``None`` writes no
    ``info.observed`` (release v0.0.2)."""

    level_offs_get_position: bool = True
    """Give the level-off events a position, a distance flown and a time
    passed, like other events: from the report with a position nearest in
    time to the start (end) of the level-off, within
    :attr:`vertical_position_window_s`. Without this setting (release
    v0.0.2) these are empty on every level-off."""

    vertical_position_window_s: float = 60.0
    """Seconds around a top or a level-off within which the report giving
    its position is searched, with :attr:`tops_position_from_nearest_report`
    and :attr:`level_offs_get_position`."""

    # -- airport layout events -------------------------------------------
    #
    # Entry and exit events of runways, taxiways, aprons, stands and other
    # parts of an airport, matched on the H3 cells of the airport layout.
    # See https://www.opdi.aero/content/methods/surface-events.html.
    airport_gate_above_field: bool = True
    """Admit a report to the layout matching by its height above the
    airport (:attr:`airport_max_height_agl_ft`) rather than by flight level
    (:attr:`airport_max_fl`). A flight level is measured from standard
    pressure, so its height above the ground changes with the weather and
    the airport's elevation. Off in release v0.0.2."""

    airport_max_height_agl_ft: float = 2000.0
    """Highest height above the airport, in feet, of a report matched to the
    layout when :attr:`airport_gate_above_field` is set."""

    airport_admit_on_ground: bool = True
    """Admit every report flagged on the ground, whatever its altitude says.

    A surface position report carries no altitude, so a height test alone
    would leave out nearly every report on the airport surface. The height
    test still applies to airborne reports. It stops an aircraft flying over
    an airport from being matched to its layout. Off in release v0.0.2."""

    # -- airport events ---------------------------------------------------
    airport_max_fl: int = 20
    """Highest flight level of a report matched to the layout when
    :attr:`airport_gate_above_field` is off (release v0.0.2)."""

    airport_trace_gap_seconds: float = 300.0
    """A gap longer than this, in seconds, inside one passage through one
    part of the airport starts a new passage."""

    airport_events_ordered: bool = True
    """Take the position, altitude and measurements of an entry or exit event
    from the report at the entry or exit time. Without it (release v0.0.2)
    they come from whichever report Spark reads first, which need not be the
    one at that time."""

    layout_deterministic_element: bool = True
    """When several layout elements of one kind (taxiways, aprons, stands)
    share an H3 cell, a passage gives one event, naming the element with the
    smallest identifier (``osm_id``); between elements with the same
    identifier, the first in the order of ``info``. Without it (release
    v0.0.2) the event names whichever of them Spark keeps, which can differ
    between two runs of the same day."""

    # -- surface events: passages, air reports, parked aircraft, vehicles --
    # (work package 6 of the corrections after the spot checks of one day)
    layout_element_per_passage: bool = True
    """Choose the element of overlapping layout elements once per passage,
    so that every entry has its exit on the same element.

    Elements of one kind often overlap (a named taxiway drawn as several
    ways, stands drawn over a taxilane). Of passages of one kind that start
    at the same report one is kept, then of passages that end at the same
    report: the longest, which covers the time of the others, but of stands
    the shortest (the stand itself rather than a larger area drawn around
    it, which the aircraft enters earlier and leaves later). Between passages of equal
    length, the element with the smallest identifier is kept. Without it,
    entries and exits choose their element separately
    (:attr:`layout_deterministic_element`), and about one taxiway entry in
    eight had no exit on the same element. Off in release
    v0.0.2."""

    layout_nested_stands_both_published: bool = True
    """Publish both passages when a stand is drawn inside a larger stand.

    With :attr:`layout_element_per_passage`, of overlapping stand passages
    that start (or end) at the same report only the shortest was kept,
    because two layout events at one report of one type got the same event
    identifier. With this setting, the identifier of a layout event
    (``entry-*`` / ``exit-*``) also includes the element (``info.osm_id``),
    so both passages are published; the longer one, which contains the
    other in time, carries ``info.encloses_stand`` = true. The block times
    and the flight list's stand columns read the inner stand only, as
    before. The identifiers of all layout events change with it; those of
    other events do not. Off in release v0.0.2."""

    layout_air_report_must_be_slow: bool = True
    """Admit a report that is not flagged on the ground to the layout
    matching only if it is slow and close to the ground: at most
    :attr:`layout_air_report_max_speed_kt` and at most
    :attr:`layout_air_report_max_height_ft` above the airport.

    The speed is the largest of the reported ground speed and the speeds
    between this report and the reports just before and after it in the same
    element (from their positions, when they are at most
    :attr:`layout_position_speed_max_gap_seconds` apart). A report with no
    known speed is admitted. Reports flagged on the ground are always
    admitted. This keeps aircraft on approach, climbing out, passing low
    over the airport or helicopters hovering over an apron out of the
    surface events, and keeps transponders that never set the ground flag
    while taxiing (at 2 to 8 kt). Off in release v0.0.2."""

    layout_air_report_max_speed_kt: float = 50.0
    """Highest ground speed, in knots, of a report not flagged on the ground
    that is still matched to the layout (taxiing is 5 to 30 kt)."""

    layout_air_report_max_height_ft: float = 300.0
    """Highest height above the airport, in feet, of a report not flagged on
    the ground that is still matched to the layout. Barometric altitude on
    the ground reads up to about 250 ft from the airport's elevation on days
    of high or low pressure."""

    layout_position_speed_max_gap_seconds: float = 30.0
    """Longest time, in seconds, between two reports whose positions give a
    ground speed for :attr:`layout_air_report_must_be_slow`."""

    layout_merge_parked_passages: bool = True
    """Join two consecutive passages of one element when the aircraft moved
    less than :attr:`layout_merge_max_move_ft` between them and entered no
    other element in between.

    A parked aircraft whose position jitters across the edge of an element by
    a few metres, or whose positions arrive only every few minutes inside a
    hangar, otherwise gives a new entry and exit after every absence of more
    than :attr:`airport_trace_gap_seconds` (42 entry/exit pairs for one
    aircraft parked at a hangar for 19 hours). Stand passages are not
    joined: an aircraft pushing back often sends no position until it is
    clear of the stand, and the exit before that gap is the off-block time.
    Off in release v0.0.2."""

    layout_merge_max_move_ft: float = 165.0
    """Largest distance, in feet (165 ft is about 50 m), between the exit of a passage and the
    entry of the next passage of the same element for the two to be joined."""

    layout_mark_track_edges: bool = True
    """Set ``info.at_track_start`` on an entry made at the first report with
    a position of its track, and ``info.at_track_end`` on an exit made at the
    last one. Such an entry or exit is when reception started or stopped
    (for example a transponder switched on at the stand), not a movement.
    Off in release v0.0.2."""

    layout_mark_ground_only: bool = True
    """Set ``info.ground_only`` on the layout events of a track that never
    leaves the ground and whose aircraft type is unknown or a vehicle type
    (:attr:`layout_vehicle_typecodes`): in practice airport vehicles with
    transponders. A track never leaves the ground when it has no report that
    is not flagged on the ground and has an altitude. Towed aircraft with a
    known type are not marked. The events are kept. Off in release
    v0.0.2."""

    layout_vehicle_typecodes: tuple = ("SERV", "GRND", "GND", "TWR")
    """Aircraft type codes of the aircraft database that are not aircraft,
    for :attr:`layout_mark_ground_only`: service vehicles (``SERV``), ground
    vehicles and marshallers (``GRND``, ``GND``) and fixed transmitters
    (``TWR``). On 14 June 2026 the flight list held 561 rows with these
    codes, all without a destination."""

    # -- plausibility checks ----------------------------------------------
    #
    # Events that the aircraft's own track clearly contradicts are removed
    # before they are written (``opdi.pipeline.plausibility``), and kept with
    # the reason in the working table ``opdi_flight_events_rejected``. See
    # https://www.opdi.aero/content/methods/plausibility-checks.html.
    plausibility_checks: bool = True
    """Remove the events the track contradicts: an event without a time, an
    off-block, in-block or airport layout event while the aircraft flies, an
    off-block time not before the track's last take-off time, an in-block
    time not after its first landing time. Only clear evidence removes an
    event; an unknown fact keeps it. A take-off or landing time more than
    :attr:`plausibility_max_runway_time_height_ft` above its airport is
    removed too. Off in release v0.0.2."""

    plausibility_max_ground_event_height_ft: float = 1500.0
    """Height above the airport, in feet, above which an off-block, in-block
    or airport layout event counts as in flight (together with the speed and
    the on-ground flag below)."""

    plausibility_max_ground_event_altitude_ft: float = 15000.0
    """Barometric altitude, in feet, used instead of the height when the
    event's airport or its elevation is unknown: above the highest airports
    in the world (about 14,500 ft)."""

    plausibility_max_ground_event_speed_kt: float = 80.0
    """Ground speed, in knots, above which such an event counts as in flight.
    Taxiing aircraft move at 10 to 30 kt."""

    plausibility_max_runway_time_height_ft: float = 1500.0
    """Height above the ground, in feet, above which a take-off or landing
    time (``ATOT``, ``ALDT``) is removed. A time close to the ground is kept:
    in the study of these checks, the airport records confirm most times
    found between 1,000 and 1,500 ft, and none above.

    The height is the event's barometric altitude minus the same track's
    barometric altitude on the ground at that airport (see
    :attr:`plausibility_ground_reference_window_s`), which cancels the
    error of barometric altitude on a day of low or high pressure. When the
    track has no such report, the height is measured above the airport's
    elevation and :attr:`plausibility_pressure_allowance_ft` is added to the
    limit."""

    plausibility_runway_time_window_s: float = 10.0
    """Seconds around a take-off or landing time within which the reports
    that give its altitude are read, when the event has no altitude of its
    own. The altitude is interpolated between the nearest report before and
    the nearest report after the time that have both an altitude and a
    position; without one on each side within this window the height is
    unknown and the event is kept. A jet climbs 2,000 to 4,000 ft in a
    minute, so a report further away says little about the height at the
    time."""

    plausibility_pressure_allowance_ft: float = 1000.0
    """Feet added to :attr:`plausibility_max_runway_time_height_ft` when the
    height of a take-off or landing time is measured above the airport's
    elevation rather than above the track's own altitude on the ground.
    Barometric altitude is measured from standard pressure (1013.25 hPa) and
    reads about 27 ft high per hPa below it: at 975 hPa, a true 500 ft above
    the airport reads about 1,530 ft. 1,000 ft covers pressures down to
    about 976 hPa."""

    plausibility_ground_reference_window_s: float = 1800.0
    """Seconds around a take-off or landing time within which the track's
    reports flagged on the ground, with an altitude, give its altitude on
    the ground (the median of those reports)."""

    plausibility_ground_reference_radius_nm: float = 5.0
    """Distance from the airport, in NM, within which those reports on the
    ground are read."""

    plausibility_context_window_s: int = 60
    """Seconds around the event in which the position reports are read. A
    report flagged on the ground in this window keeps the event."""

    # -- inputs and identifiers -------------------------------------------
    feeds_from_clean_tracks: bool = True
    """Detect events on the cleaned tracks ``osn_tracks_clean`` (default),
    which also carry the ``segment_id`` of each gap, rather than on the
    uncleaned ``osn_tracks`` (release v0.0.2)."""

    deterministic_event_ids: bool = True
    """Build each event id from the event's own content (track, type, time),
    so that processing a day again gives the same ids. Off (release v0.0.2),
    ids are row numbers counted over the day and change when the data
    change."""

    keep_each_family: bool = True
    """Compute the events of each family on its own and keep them before the
    families are joined (``localCheckpoint``). The joined events then have a
    small plan: with all corrections on, the plan of the joined families was
    too large for the driver's memory to analyse. The events are the same
    either way. Off in the v0.0.2 settings, which run as before."""

    events_version: str = EVENTS_VERSION
    """Version string written on the events this configuration produces
    (column ``version``). A published version string is never reused for
    different results."""

    # -- flight list and first/last seen (work package 4) ------------------
    aerodrome_correction_checks_track: bool = True
    """Let a runway event replace an airport of the flight list (step 04b)
    only where it fits the track.

    A departure runway event (``ATOT``, ``line-up``, ``take-off-roll``)
    replaces the departure airport only if it is the first departure event
    of the track and its airport lies within
    :attr:`aerodrome_correction_radius_nm` of the track's first report with
    a position. An arrival runway event (``ALDT``, ``touchdown``,
    ``runway-vacated``) replaces the destination only if it is the last
    arrival event, comes after every departure event, and its airport lies
    within that distance of the track's last report with a position. A
    take-off or landing time of the fallback method (``info.method`` =
    ``legacy``) never replaces an airport: that method searches only the
    airports the flight list already named. Otherwise the airport found
    from the trajectory stays.

    Without this setting the earliest departure and the latest arrival event
    anywhere on the track replaced the airports. On a track holding two
    flights (a stop under one call sign) both became the airport of the stop,
    and a go-around read as a take-off moved the departure to the
    destination. Off in release v0.0.2."""

    aerodrome_correction_radius_nm: float = 30.0
    """Largest distance, in NM, between the airport of a runway event and the
    track's first (departure) or last (arrival) report with a position, for
    the event to replace an airport (:attr:`aerodrome_correction_checks_track`).
    Equal to :attr:`DetectionConfig.endpoint_radius_nm`."""

    movement_flags_need_track_evidence: bool = True
    """Base ``SUPERSEDED_DEP``/``SUPERSEDED_ARR`` and the movement columns
    on what the track shows.

    * A superseded flag needs an airport: a row without ADEP (ADES) is not a
      superseded departure (arrival).
    * A row whose track flew more than :attr:`movement_airborne_height_ft`
      above its airport is not a ground fragment, so it is not superseded.
    * A departure with no take-off time, no destination and no circle
      crossing still counts as a movement when the track climbed more than
      :attr:`movement_airborne_height_ft` above the airport, or its last
      report with a position is more than :attr:`movement_left_distance_nm`
      from it; the arrival likewise, with the first report.

    The height is the highest altitude among the track's events, above the
    airport's elevation. Off in release v0.0.2."""

    movement_airborne_height_ft: float = 1000.0
    """Height above the airport, in feet, above which a track counts as
    having flown (see :attr:`movement_flags_need_track_evidence`)."""

    movement_left_distance_nm: float = 40.0
    """Distance from the airport, in NM, beyond which the far end of a track
    shows that the aircraft left it or came from elsewhere (see
    :attr:`movement_flags_need_track_evidence`). Equal to the smallest
    circle radius, 40 NM."""

    milestones_on_own_leg: bool = True
    """Take the flight list's block times, stands and circle crossings from
    the departure and the arrival of the row (step 04b).

    ``AOBT`` is the latest off-block at ADEP not after ``ATOT``, ``AIBT``
    the earliest in-block at ADES not before ``ALDT``, and ``STND_DEP`` /
    ``STND_ARR`` the stand of that same event, or else the last stand exit
    at ADEP before ``ATOT`` / the first stand entry at ADES after ``ALDT``.
    ``C{NM}_DEP`` is the first outbound crossing at ADEP after ``ATOT`` and
    ``C{NM}_ARR`` the last inbound crossing at ADES before ``ALDT``. Where
    the take-off or landing time is unknown, the time limit is not applied.
    An event that names no airport is accepted. Without this setting the
    block times were taken at any airport, and the first and last events of
    the track were used whatever the order. Off in release v0.0.2."""

    # One flight-list row per flight (flight legs). ---------------------
    flight_list_one_row_per_flight: bool = True
    """Give every flight of a track its own row in the published flight list
    (step 04b, :mod:`opdi.pipeline.flight_legs`).

    A track that lands and, after at least :attr:`flight_stop_min_ground_s`
    on the ground, takes off again holds two flights; one with ``n`` such
    stops holds ``n + 1``. Each gets a row named ``<track_id>-L<k>`` with
    its own departure and destination airports, first and last report
    times, and milestone, runway, stand and circle columns, and each event
    of the track is published under the flight it belongs to. The track's
    own row in the working flight list is kept as it is. Step 04 also draws
    the distance circles around the airports of the stops. Tracks without a
    stop are not changed. Off in release v0.0.2."""

    flight_stop_min_ground_s: float = 300.0
    """Shortest time, in seconds, from a landing (``ALDT``) to the next
    take-off (``ATOT``) of the same track for the two to belong to two
    flights (see :attr:`flight_list_one_row_per_flight`). A touch-and-go or
    a go-around climbs away within well under a minute; a turnaround or a
    full-stop circuit that taxis back takes several minutes."""

    seen_from_measured_reports: bool = True
    """Define ``first_seen`` and ``last_seen`` as the first and last report
    of the track with a measured position, in the flight list (step 03) and
    in the events (step 04) alike. For a track with no position at all, the
    first and last report with any measured value (altitude, speed, heading
    or vertical rate); for a track with none of these either, its first and
    last report.

    "Measured" means not blanked by the cleaning of step 02a. OpenSky keeps
    sending an aircraft's last state for about 300 s after it was last
    heard; cleaning blanks these repeated values, so the last report with a
    measured position is the last time the aircraft was really heard.
    Without this setting the events used the first and last report of any
    kind, which put ``last_seen`` about five minutes late on most flights,
    and the flight list used other definitions on different rows. Off in
    release v0.0.2."""

    def __post_init__(self) -> None:
        """Resolve :attr:`merge_duplicate_milestones` and check the methods.

        Raises:
            ValueError: If ``merge_duplicate_milestones`` is ``True`` without
                the runway milestones, or ``level_method`` or
                ``level_anchor`` is not a known value.
        """
        if self.merge_duplicate_milestones is None:
            object.__setattr__(self, "merge_duplicate_milestones", self.emit_runway_milestones)
        elif self.merge_duplicate_milestones and not self.emit_runway_milestones:
            raise ValueError(
                "merge_duplicate_milestones=True requires emit_runway_milestones. "
                "The merge exists to reconcile the A-CDM family with the legacy "
                "one; with A-CDM off there is nothing to merge, and the fuzzy "
                "take-off/landing pair would be published beside a vocabulary "
                "that has no place for it. Leave it unset to follow "
                "emit_runway_milestones."
            )
        if self.level_method not in ("icao", "pru"):
            raise ValueError(
                f"level_method must be 'icao' or 'pru', got {self.level_method!r}. "
                "Another value would give no level segments at all, which would "
                "look like missing data rather than a typing error."
            )
        if self.level_anchor not in ("pru", "phase"):
            raise ValueError(f"level_anchor must be 'pru' or 'phase', got {self.level_anchor!r}.")

    def level_window_height_ft(self) -> float:
        """Height of the rolling window of the ``"pru"`` level method, in feet.

        The method fixes the window's height over its length at 300 ft per
        minute, so the height is computed from
        :attr:`level_vertical_speed_limit_ftmin` and
        :attr:`level_window_seconds` rather than set separately.
        """
        return self.level_vertical_speed_limit_ftmin * self.level_window_seconds / 60.0

    def differs_from_release(self, names) -> bool:
        """Whether one of the settings ``names`` differs from release v0.0.2.

        The values of release v0.0.2 are those of :meth:`legacy`. A method
        whose result these settings change writes its new version then, and
        the version of release v0.0.2 only when none differs.
        """
        legacy = type(self).legacy()
        return any(getattr(self, name) != getattr(legacy, name) for name in names)

    @classmethod
    def legacy(cls) -> "EventConfig":
        """The settings of release v0.0.2.

        Every event published in release v0.0.2 (version ``events_v0.0.2``)
        was built with these, so a day processed with them reproduces that
        release.
        """
        return cls(
            # Not part of release v0.0.2.
            phase_twindow_seconds=0.0,
            phase_ground_above_field=False,
            emit_runway_events=False,
            emit_block_events=False,
            emit_level_offs=False,
            ground_speed_derive_from_position=False,
            block_times_on_ground_only=False,
            # Work package 3 of the 2026-06-14 corrections.
            ground_movement_through_reports_without_speed=False,
            block_time_off_the_runway=False,
            block_time_stand_at_same_airport=False,
            block_time_from_last_stand_stop=False,
            block_time_stand_longest_dwell=False,
            block_time_one_part_per_leg=False,
            block_time_needs_flight=False,
            layout_ref_missing_as_null=False,
            phase_require_complete_rules=False,
            crossing_all_occurrences=False,
            crossing_interpolate=False,
            interpolate_crossings_over_missing_values=False,
            airport_events_ordered=False,
            layout_deterministic_element=False,
            # Work package 6 (surface events, holding, point merge).
            layout_element_per_passage=False,
            layout_nested_stands_both_published=False,
            layout_air_report_must_be_slow=False,
            layout_merge_parked_passages=False,
            layout_mark_track_edges=False,
            layout_mark_ground_only=False,
            holding_pattern_by_straight_share=False,
            holding_trim_to_main_direction=False,
            holding_altitude_from_known_reports=False,
            holding_context=False,
            holding_and_go_arounds_at_corrected_airports=False,
            pms_extend_leg_exit=False,
            pms_join_split_legs=False,
            pms_leg_must_turn=False,
            pms_merge_point_conditions=False,
            feeds_from_clean_tracks=False,
            deterministic_event_ids=False,
            # No distance circles in release v0.0.2.
            ring_radii_nm=(),
            emit_runway_milestones=False,
            merge_duplicate_milestones=False,
            merge_milestones_per_airport=False,
            emit_pru_tops=False,
            level_method="icao",
            level_floors_above_field=False,
            level_radius_enforced=False,
            level_anchor="phase",
            airport_gate_above_field=False,
            airport_admit_on_ground=False,
            distance_from_last_known_position=False,
            emit_holding=False,
            emit_point_merge=False,
            emit_fir_crossings=False,
            plausibility_checks=False,
            runway_take_off_needs_air_report=False,
            # Work package 1: the altitude the detectors read.
            altitude_from_cleaning=False,
            altitude_plausible_min_ft=None,
            altitude_plausible_max_ft=None,
            vertical_ignores_ground_altitude=False,
            level_max_sample_gap_seconds=None,
            level_segment_min_duration_applied=False,
            level_segment_not_on_ground=False,
            tops_position_from_nearest_report=False,
            tops_observed_min_change_ft=None,
            level_offs_get_position=False,
            # Work package 2 (runway events).
            runway_height_above_track_ground=False,
            runway_direction_by_motion=False,
            runway_events_on_runway_reports=False,
            runway_roll_speed_from_positions=False,
            runway_line_up_needs_ground_report=False,
            runway_threshold_crossing_last_only=False,
            goaround_excludes_landings=False,
            goaround_in_approach_corridor=False,
            runway_crossing_ground_without_altitude=False,
            runway_window_times_checked=False,
            runway_times_position_between_reports=False,
            # Work package 5: crossings, rings, FIR and the two measurements.
            crossing_max_implied_speed_kt=None,
            crossing_skips_position_jumps=False,
            ring_crossing_keeps_single_steps=False,
            crossing_value_max_gap_seconds=None,
            rings_around_flight_list_airports=False,
            fir_needs_position=False,
            fir_flag_vertical=False,
            distance_max_implied_speed_kt=None,
            distance_between_positions=False,
            measure_every_event=False,
            time_passed_from_first_seen=False,
            # Work package 4, not part of release v0.0.2.
            aerodrome_correction_checks_track=False,
            movement_flags_need_track_evidence=False,
            milestones_on_own_leg=False,
            seen_from_measured_reports=False,
            flight_list_one_row_per_flight=False,
            keep_each_family=False,
            events_version=LEGACY_EVENTS_VERSION,
        )
