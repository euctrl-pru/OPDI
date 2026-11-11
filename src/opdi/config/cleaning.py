"""Thresholds of trajectory cleaning (step 02a): :class:`CleaningConfig`."""

from dataclasses import dataclass
from typing import Tuple


@dataclass
class CleaningConfig:
    """Thresholds of trajectory cleaning (pipeline step 02a).

    Cleaning reads the tracks (``osn_tracks``) and writes ``osn_tracks_clean``.
    A value that cannot be trusted is set to NULL and the row is kept, so
    "no data" stays distinct from "measured". The stages, in order:

    1. remove duplicate reports of the same track and second;
    2. set coordinates outside the possible range to NULL;
    3. set to NULL values that are only repeats of the previous report
       (ADS-B sends position and velocity in separate messages, so an
       unchanged value is often a repeat, not a new measurement);
    4. set to NULL the middle of spikes, found from the first and second
       differences of each column over time;
    5. set to NULL values with no other value of the same column within
       20 seconds on either side;
    6. start a new segment (``segment_id``) at every gap of 5 minutes or
       more, so later steps never interpolate across a hole in coverage.

    The method and thresholds follow the trajectory cleaning of Alligier and
    Gianazza (ENAC), winners of the 2024 PRC Data Challenge.
    See https://www.opdi.aero/content/methods/trajectory-cleaning.html.

    Thresholds are in aviation units, with the unit in the field name. The
    stored columns are in SI units (metres, metres per second), so each column
    is scaled to the threshold's unit only for the comparison, with the
    factors also used for the published events:

    ==============  ==================  =============
    Column          Aviation unit       Factor
    ==============  ==================  =============
    baro/geo alt    m -> ft             3.28084
    vert_rate       m/s -> ft/min       196.850394
    velocity        m/s -> kt           1.94384
    heading         deg (unchanged)     1.0
    lat / lon       deg (unchanged)     1.0
    ==============  ==================  =============

    The result is a NULL mask, which has no unit, so no converted value is
    ever stored.

    Both difference thresholds of a column have the same unit, column unit
    per second. The second difference is the change between two successive
    first differences divided by their mean time step, not a rate of a rate.
    """

    enabled: bool = True
    """Run the cleaning step. When ``False``, step 02a writes nothing and step
    03 reads the uncleaned tracks."""

    # -- Stage 1: duplicate removal -------------------------------------
    dedup_enabled: bool = True
    """Stage 1: keep one row per ``(track_id, event_time)``."""

    # -- Stage 2: range validity ----------------------------------------
    lat_min: float = -90.0
    """Lowest valid latitude, in degrees."""
    lat_max: float = 90.0
    """Highest valid latitude, in degrees."""
    lon_min: float = -180.0
    """Lowest valid longitude, in degrees."""
    lon_max: float = 180.0
    """Stage 2: possible latitude and longitude ranges, in degrees. A
    coordinate outside them is set to NULL."""

    # -- Stage 3: stale-broadcast removal --------------------------------
    stale_enabled: bool = True
    """Stage 3: set to NULL a value identical to the previous report's, as a
    repeat rather than a new measurement. Position (latitude and longitude
    together) and each of ``vert_rate``, ``heading`` and ``velocity`` are
    tested separately."""

    stale_position_uses_last_pos_update: bool = True
    """Use ``last_pos_update`` to tell a repeated position from an aircraft
    that is standing still.

    A parked aircraft reports exactly the same latitude and longitude every
    time, which the plain equality test of stage 3 would remove as repeats,
    leaving no position on the stand for the off-block and in-block
    detection. ``last_pos_update`` is the time at which ADS-B last measured
    the position. When it has advanced since the previous report, the
    position is a new measurement even if its value has not changed, and it
    is kept. Rows with no ``last_pos_update`` use the plain equality test.

    Applies to the position only; ``vert_rate``, ``heading`` and ``velocity``
    keep the plain equality test. Set to ``False`` for the plain test
    everywhere.
    """

    # -- Stage 4: derivative spike filter --------------------------------
    spike_enabled: bool = True
    """Stage 4: run the spike filter."""

    spike_min_votes: int = 2
    """Number of flagged difference windows a value must take part in before
    it is set to NULL.

    A difference is flagged when it exceeds the threshold of its column. The
    middle point of a spike takes part in the rise and in the fall, so it
    collects two flags, while a real step change collects only one and is
    kept. First and second differences are counted separately: a value is
    removed when it has at least this many first-difference flags, or at least
    this many second-difference flags."""

    baro_altitude_d1_max_ft_s: float = 200.0
    """Largest plausible first difference of barometric altitude, in ft/s."""
    baro_altitude_d2_max_ft_s: float = 50.0
    """Largest plausible second difference of barometric altitude, in ft/s."""

    geo_altitude_d1_max_ft_s: float = 200.0
    """Largest plausible first difference of geometric (GNSS) altitude, in
    ft/s."""
    geo_altitude_d2_max_ft_s: float = 150.0
    """Largest plausible second difference of geometric altitude, in ft/s.
    Looser than for barometric altitude, because GNSS altitude is noisier."""

    vert_rate_d1_max_ftmin_s: float = 1500.0
    """Largest plausible first difference of vertical rate, in ft/min per
    second."""
    vert_rate_d2_max_ftmin_s: float = 1000.0
    """Largest plausible second difference of vertical rate, in ft/min per
    second."""

    velocity_d1_max_kt_s: float = 12.0
    """Largest plausible first difference of ground speed, in kt/s."""
    velocity_d2_max_kt_s: float = 10.0
    """Largest plausible second difference of ground speed, in kt/s."""

    heading_d1_max_deg_s: float = 12.0
    """Largest plausible first difference of track angle, in degrees per
    second."""
    heading_d2_max_deg_s: float = 10.0
    """Largest plausible second difference of track angle, in degrees per
    second."""

    latlon_d1_max_deg_s: float = 0.01
    """Largest plausible first difference of latitude or longitude, in
    degrees per second (0.01 deg/s is about 1,100 m/s along a meridian)."""
    latlon_d2_max_deg_s: float = 0.06
    """Largest plausible second difference of latitude or longitude, in
    degrees per second."""

    # -- Stage 5: isolated-point removal ---------------------------------
    isolated_enabled: bool = True
    """Stage 5: run the isolated-value test."""
    isolated_max_gap_seconds: float = 20.0
    """A value with no other value of the same column within this many
    seconds on both sides cannot be checked against its neighbours and is set
    to NULL."""

    sparse_sources: Tuple[str, ...] = ("ADS-C",)
    """Values of the ``source`` column that skip stages 3 and 5.

    Both stages assume a dense feed: stage 5 removes a value with no
    neighbour within 20 seconds, and stage 3 reads an unchanged value as a
    repeat. ADS-C reports arrive about every 10 minutes and are each a new
    measurement, so without this exemption cleaning would remove every ADS-C
    value. The ``source`` column exists only when ADS-C is merged in
    (:attr:`AdscConfig.enabled`); otherwise this setting has no effect.
    """

    # -- Stage 6: gap segmentation ---------------------------------------
    segment_gap_seconds: float = 300.0
    """Stage 6: a gap of at least this many seconds between two reports of a
    track starts a new ``segment_id`` (default 300 s, 5 minutes)."""

    # -- Which tracks step 03 reads ---------------------------------------
    feeds_flight_list: bool = True
    """Build the flight list (step 03) from the cleaned tracks
    ``osn_tracks_clean`` (``True``) or from the uncleaned ``osn_tracks``
    (``False``, as in release v0.0.2).

    When cleaning is off (:attr:`enabled` is ``False``), step 03 reads the
    uncleaned tracks whatever this says. Cleaning keeps every row but sets
    untrusted values to NULL, and step 03 leaves out reports without a
    barometric altitude, so the cleaned tracks give step 03 fewer but more
    reliable reports."""
