"""How the position reports are split into tracks (step 02): :class:`SegmentationConfig`."""

import sys
from dataclasses import dataclass

#: Names of the track rules that are not part of the package. They are kept
#: with the study scripts, in ``benchmarks/study_variants/segmentation.py``,
#: which can register them for use (see :class:`SegmentationConfig`).
STUDY_SEGMENTATION_METHODS = (
    "no_month_suffix",
    "traffic_style",
    "airframe_only",
    "ground_anchored",
    "airport_anchored",
    "vertical_profile",
    "recommended",
    "debounced",
)


@dataclass
class SegmentationConfig:
    """How the position reports are split into tracks (step 02).

    A track is the sequence of reports of one aircraft that belong to one
    flight; its identifier is ``track_id``, which the flight list publishes
    as ``flight_id``. Two rules are available (:attr:`method`):

    * ``"standard"`` (default), the rule of release v2.0.0;
    * ``"legacy"``, the rule of release v0.0.2.

    Heights are in feet and times in minutes or seconds, with the unit in
    the field name. The thresholds reach the track builder through
    :meth:`opdi.pipeline.segmentation.base.SegmentationParams.from_config`.
    The ``legacy`` rule reads the gap thresholds of :class:`IngestionConfig`
    instead.
    See https://www.opdi.aero/content/methods/track-identity.html.
    """

    method: str = "standard"
    """The rule that builds ``track_id``: ``"standard"`` (default) or
    ``"legacy"``.

    ``"legacy"``, the rule of release v0.0.2, groups the reports by aircraft
    address and call sign, starts a new track at a gap longer than 30
    minutes (15 minutes below 5,000 ft), and ends each id with the year and
    month (``_2024_6``).

    ``"standard"`` (also reachable as ``"cruise_linked"``) groups the reports
    by aircraft address alone and starts a new track when:

    * the call sign changes to a new non-blank value that is then kept for at
      least :attr:`callsign_min_persistence_seconds`, so a call sign garbled
      for a few seconds does not split a flight; or
    * there is a gap longer than :attr:`gap_minutes` (or
      :attr:`low_alt_gap_minutes` at low altitude), unless the two sides of
      the gap fit one continuous flight at cruise level (the ``link_*``
      settings), as when a flight crosses an ocean out of reception.

    Because a track can then span a long gap, building one day reads the
    reports of up to about 11 hours before it (see
    :func:`opdi.pipeline.tracks.required_lookback_hours`), and the event
    detectors treat the gaps inside a track as holes
    (:attr:`EventConfig.track_hole_seconds`).

    The two rules give different ``track_id`` values for the same flight,
    and nothing in the data says which rule produced an id, so ids of
    different releases do not join. The study rules listed in
    :data:`STUDY_SEGMENTATION_METHODS` can be selected only after a study
    script has registered them; otherwise their names are refused.
    """

    gap_minutes: float = 30.0
    """A gap longer than this, in minutes, splits a track at any altitude,
    unless the two sides are linked as one flight."""

    low_alt_gap_minutes: float = 15.0
    """A gap longer than this, in minutes, splits a track when the aircraft
    is below :attr:`low_alt_ft`."""

    low_alt_ft: float = 5000.0
    """Altitude, in feet, below which the shorter gap applies."""

    callsign_lookback_minutes: float | None = None
    """How far back, in minutes, the last non-blank call sign is looked for
    when deciding whether the call sign changed. ``None`` uses
    :attr:`gap_minutes`."""

    callsign_min_persistence_seconds: float = 30.0
    """Seconds a new call sign must be kept before the change splits the
    track. A call sign that changes to a garbled value and back within this
    time does not split it."""

    link_max_gap_minutes: float = 600.0
    """Longest gap in reception, in minutes, that can be linked as one
    flight (10 hours, which covers the longest oceanic gaps). It also sets
    how far back step 02 reads (see
    :func:`opdi.pipeline.tracks.required_lookback_hours`)."""

    link_cruise_floor_ft: float = 18000.0
    """Lowest altitude, in feet, at each side of a gap that can be linked:
    above it the aircraft is en route rather than near an airport."""

    link_edge_vert_rate_fpm: float = 500.0
    """Vertical rate, in ft/min, beyond which the side of a gap counts as
    climbing or descending: a gap whose old side descends and whose new side
    climbs is read as a landing and a new flight."""

    link_callsign_lookahead_minutes: float = 10.0
    """How far after a gap, in minutes, the first non-blank call sign is
    looked for, to compare it with the call sign before the gap."""

    link_callsign_lookback_minutes: float = 60.0
    """How far before a gap, in minutes, the last call sign may have been
    seen and still count as the call sign before the gap; an older one counts
    as blank. This keeps the decision the same whichever day's run makes it.
    It adds to how far back step 02 reads (see
    :func:`opdi.pipeline.tracks.required_lookback_hours`)."""

    link_edge_position_minutes: float = 5.0
    """How far from each side of a gap, in minutes, reports without a
    position are skipped to find the side's position, altitude, vertical rate
    and speed (these arrive in separate messages). With no position or no
    altitude within this time, the gap splits the track; a missing vertical
    rate does not count either way at cruise level. Must be shorter than
    :attr:`low_alt_gap_minutes`."""

    link_min_implied_speed_kt: float = 400.0
    """Lowest speed, in knots, implied by the great-circle distance and the
    time across a gap, for the gap to be linked. A lower speed suggests the
    aircraft landed in between."""

    link_max_implied_speed_kt: float = 750.0
    """Highest implied speed across a gap, in knots, for it to be linked. A
    higher one means two aircraft share one address."""

    link_min_speed_ratio: float = 0.0
    """Lowest ratio of the implied speed across a gap to the ground speed at
    its sides for the gap to be linked. 0 turns the test off."""

    link_min_stop_minutes: float = 0.0
    """Time, in minutes, that would be left over when flying across the gap
    at :attr:`link_vfast_kt`, from which the gap counts as having room for a
    stop. 0 turns the test off."""

    link_vfast_kt: float = 650.0
    """Fastest plausible ground speed, in knots, used by
    :attr:`link_min_stop_minutes`."""

    link_old_edge_climb: bool = True
    """Allow a gap whose old side is below :attr:`link_cruise_floor_ft` to be
    linked when that side is climbing faster than
    :attr:`link_edge_vert_rate_fpm`: an aircraft lost while still climbing
    is outbound, so the next report is the same flight. The climb is judged
    on the mean of at least three vertical rates; missing ones do not
    count."""

    link_new_edge_descent: bool = True
    """Allow a gap whose new side is below :attr:`link_cruise_floor_ft` to be
    linked when that side is descending faster than
    :attr:`link_edge_vert_rate_fpm`, judged on at least three vertical
    rates. A gap that the low-altitude rule splits (below :attr:`low_alt_ft`)
    is still split. The implied speed test leaves out gaps that hide a
    landing."""

    def __post_init__(self) -> None:
        """Refuse a study rule that no study script has registered.

        Raises:
            ValueError: If :attr:`method` names a rule of
                :data:`STUDY_SEGMENTATION_METHODS` that is not registered.
        """
        methods = sys.modules.get("opdi.pipeline.segmentation.methods")
        registered = methods is not None and self.method in methods.ARMS
        if self.method in STUDY_SEGMENTATION_METHODS and not registered:
            raise ValueError(
                f"segmentation method {self.method!r} is a study rule and not part of "
                f"opdi; it lives in benchmarks/study_variants/segmentation.py, and a "
                f"study script calls register_study_arms() to select it"
            )
