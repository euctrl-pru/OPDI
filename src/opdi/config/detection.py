"""Thresholds of departure and destination airport detection (step 03): :class:`DetectionConfig`."""

from dataclasses import dataclass


@dataclass
class DetectionConfig:
    """Thresholds of departure and destination airport detection (step 03).

    Step 03 names, for every track, the airport it departed from (ADEP) and
    the airport it arrived at (ADES). Two methods are available, and each end
    of the flight can use either (:attr:`adep_mode`, :attr:`ades_mode`):

    ``trend``
        Looks at every report below a ceiling (:attr:`trend_max_height_ft`
        above the airport's elevation, or :attr:`trend_max_fl`) within
        :attr:`trend_radius_nm` of an airport, and decides from the smoothed
        altitude whether the aircraft was climbing away from it (a departure)
        or descending towards it (an arrival). It names no airport when the
        altitude neither clearly rises nor clearly falls.

    ``endpoint``
        Looks at the first report of the track (for the departure) or the
        last one (for the arrival) and accepts the nearest airport when that
        report is within :attr:`endpoint_radius_nm` and no more than
        :attr:`endpoint_height_ft` above the airport's elevation, or on the
        ground.

    Both rank candidate airports by distance plus a penalty for airports
    without scheduled service, so a small or military field is chosen only
    when it is clearly the nearest.

    Distances are in nautical miles (NM), heights in feet (ft). The defaults
    are those of release v2.0.0 (departures by ``endpoint``, arrivals by
    ``trend``); :meth:`legacy` gives the settings of release v0.0.2.
    See https://www.opdi.aero/content/methods/airports.html.
    """

    # -- trend -----------------------------------------------------------
    trend_max_fl: int = 60
    """Ceiling of the ``trend`` method as a flight level (60 means FL060,
    6,000 ft at standard pressure). Read only when :attr:`trend_max_datum`
    is ``"msl"``, as in release v0.0.2 (FL040)."""

    trend_max_height_ft: float = 6000.0
    """Ceiling of the ``trend`` method as a height above the airport's
    elevation, in feet. Read when :attr:`trend_max_datum` is ``"field"`` (the
    default).

    A ceiling measured from the airport rather than from sea level gives the
    method the same climb and descent to look at whatever the elevation of
    the airport."""

    trend_max_datum: str = "field"
    """What the ``trend`` ceiling is measured from.

    ``"field"``
        Height above the candidate airport's elevation, up to
        :attr:`trend_max_height_ft`. The default.
    ``"msl"``
        Flight level, up to :attr:`trend_max_fl`, as in release v0.0.2. The
        flight level is a whole number, so ``trend_max_fl = 60`` admits
        everything below 6,100 ft.
    """

    trend_radius_nm: float = 30.0
    """Radius around each airport, in NM, within which ``trend`` reads the
    reports. Default 30 NM."""

    trend_smooth_half_window: int = 2
    """Half-width, in reports, of the centred moving average applied to the
    barometric altitude before the climb or descent is decided. 2 means a
    window of five reports (about 25 seconds)."""

    trend_vote_margin: int = 0
    """Number of reports by which climbing must outnumber descending (or the
    reverse) for ``trend`` to decide. With fewer, the airport is not named.
    0 (the default) means a simple majority decides; release v0.0.2 used 4."""

    trend_rank_by: str = "haversine"
    """How ``trend`` ranks candidate airports: ``"haversine"`` (default) by
    the great-circle distance to each airport, or ``"ring"`` (release v0.0.2)
    by the number of H3 rings between the report and the airport, about 5 km
    each, with distance used only among airports at the same ring count."""

    trend_sched_penalty_nm: float = 10.0
    """Distance, in NM, added to an airport without scheduled service when
    ``trend`` ranks the candidates. 0 in release v0.0.2."""

    trend_smooth_before_cut: bool = True
    """Smooth the barometric altitude before the ceiling is applied (``True``)
    or after (``False``, release v0.0.2). Smoothing first keeps the moving
    average complete at the ceiling, where the altitude changes fastest."""

    trend_radius_exact: bool = True
    """Select the reports within :attr:`trend_radius_nm` by their exact
    great-circle distance (``True``), or by keeping only the H3 rings that lie
    wholly inside the radius (``False``, release v0.0.2), which leaves out
    reports in the rings that cross the radius."""

    trend_bearing_tiebreak_nm: float = 2.0
    """Distance band, in NM, within which ``trend`` chooses between candidate
    airports by the direction of flight rather than by distance. 0 turns it
    off.

    When two candidates are less than this much apart in ranked distance, the
    one the track's course points at (for an arrival) or comes from (for a
    departure) is chosen. Outside the band, distance decides alone."""

    # -- endpoint --------------------------------------------------------
    endpoint_radius_nm: float = 30.0
    """Largest distance, in NM, between the first or last report of a track
    and the airport ``endpoint`` names. Default 30 NM; 40 NM in release
    v0.0.2."""

    endpoint_height_ft: float = 15000.0
    """Largest height of the first or last report above the airport's
    elevation, in feet, for ``endpoint`` to name the airport. A report on the
    ground is accepted whatever its altitude."""

    endpoint_sched_penalty_nm: float = 10.0
    """Distance, in NM, added to an airport without scheduled service when
    ``endpoint`` ranks the candidates."""
    endpoint_candidate_radius_nm: float = 110.0
    """Radius, in NM, of the table of candidate airports kept for each track
    end (``opdi_endpoint_candidates``). Larger than any detection radius, so
    the radius can be changed without rebuilding the table."""

    # -- which method serves which end -----------------------------------
    adep_mode: str = "endpoint"
    """Method that names the departure airport: ``"endpoint"`` (default) or
    ``"trend"`` (release v0.0.2). The first report of a departing track is
    usually on or near the runway, so its position decides well."""

    ades_mode: str = "trend"
    """Method that names the destination airport: ``"trend"`` (default and
    release v0.0.2) or ``"endpoint"``. The last report of an arriving track
    is often where reception was lost rather than where it landed, so the
    altitude trend decides better."""

    def __post_init__(self) -> None:
        """Refuse an unknown :attr:`trend_max_datum`.

        Raises:
            ValueError: If it is neither ``"field"`` nor ``"msl"``.
        """
        if self.trend_max_datum not in ("field", "msl"):
            raise ValueError(
                f"trend_max_datum must be 'field' or 'msl', got "
                f"{self.trend_max_datum!r}. Falling back to a default here "
                f"would apply one altitude cut while reporting the other."
            )

    @classmethod
    def legacy(cls) -> "DetectionConfig":
        """The settings of release v0.0.2.

        Every flight list published in release v0.0.2 was built with these,
        so a day processed with them reproduces that release.
        """
        return cls(
            trend_max_fl=40,
            # Release v0.0.2 measured the ceiling as a flight level.
            trend_max_datum="msl",
            trend_radius_nm=30.0,
            trend_smooth_half_window=2,
            trend_vote_margin=4,
            trend_sched_penalty_nm=0.0,
            trend_rank_by="ring",
            # Not part of release v0.0.2.
            trend_smooth_before_cut=False,
            trend_radius_exact=False,
            trend_bearing_tiebreak_nm=0.0,
            endpoint_radius_nm=40.0,
            endpoint_height_ft=15000.0,
            endpoint_sched_penalty_nm=10.0,
            endpoint_candidate_radius_nm=110.0,
            # Release v0.0.2 used the trend method for both ends.
            adep_mode="trend",
            ades_mode="trend",
        )
