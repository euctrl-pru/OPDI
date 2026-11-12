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
    ``landing``, ``touchdown``, ``runway-vacated``, ``go-around`` and the
    two runway-crossing events. Off in release v0.0.2, which published
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
    ``holding_v0.1.0``).

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

    # -- point merge -------------------------------------------------------
    emit_point_merge: bool = True
    """Publish ``pms-leg-entry``, ``pms-leg-exit`` and ``pms-merge-point``
    (version ``pms_v0.1.0``) for arrivals at airports with a point merge
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

    # -- FIR crossings -----------------------------------------------------
    emit_fir_crossings: bool = True
    """Publish ``xing-fir`` (version ``fir_v0.1.0``): one event at each
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
    ``distance_v0.0.3``), each step of the distance flown is measured from the
    last report that has a position, so a missing position no longer drops
    the distance before and after it; across a gap the straight line between
    its two sides is added, which is the least the aircraft can have flown.
    Off (version ``distance_v0.0.2``, release v0.0.2), a report next to a
    missing position adds no distance. The level segments' ``distance_nm``
    values depend on it as well.
    See https://www.opdi.aero/content/methods/measurements.html."""

    # -- one event type per question -----------------------------------
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

    events_version: str = EVENTS_VERSION
    """Version string written on the events this configuration produces
    (column ``version``). A published version string is never reused for
    different results."""

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
            phase_require_complete_rules=False,
            crossing_all_occurrences=False,
            crossing_interpolate=False,
            interpolate_crossings_over_missing_values=False,
            airport_events_ordered=False,
            feeds_from_clean_tracks=False,
            deterministic_event_ids=False,
            # No distance circles in release v0.0.2.
            ring_radii_nm=(),
            emit_runway_milestones=False,
            merge_duplicate_milestones=False,
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
            events_version=LEGACY_EVENTS_VERSION,
        )
