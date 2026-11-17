"""The barometric altitude the detectors of step 04 read.

Every detector of step 04 reads the barometric altitude from the column
``baro_altitude_c`` (metres). This module decides what that column holds,
in two places:

* :func:`altitude_for_detectors`, applied once as step 04 reads the tracks.
  It puts in ``baro_altitude_c`` the altitude that survived the cleaning of
  step 02a (``EventConfig.altitude_from_cleaning``) and treats an altitude
  outside a plausible range as missing (``EventConfig.altitude_plausible_min_ft``
  and ``altitude_plausible_max_ft``). The flight detectors read it: tops,
  level-offs, level segments, flight-level, distance-circle and FIR
  crossings, holding, point merge, first and last seen.
* :func:`received_altitude` gives the runway, ground and airport detectors
  and the plausibility checks the altitude of step 02 instead, within the
  same range (see that function for why).
* :func:`flight_altitude_only`, applied in the vertical detectors only (the
  tops of climb and descent, the level segments of the level-offs and the
  flight-level crossings). It treats the altitude of a report flagged on the
  ground as missing (``EventConfig.vertical_ignores_ground_altitude``). The
  runway, ground and airport detectors keep these altitudes, as the altitude
  an aircraft reads on the ground is their reference.

**How the two altitudes of the cleaned tracks relate.** Step 02 writes
``baro_altitude_c``: the received altitude, set to NULL where it changes by
more than 200 ft/s from the previous report. Step 02a then cleans
``baro_altitude`` (it sets values to NULL and never changes one) and passes
``baro_altitude_c`` through untouched. So where both hold a value, they are
the same number. On the reports of the spot checks of 2026-06-14 (160,593
reports of 2,348 tracks), they agreed on all 98,879 reports where both held
a value. On 22,996 reports only ``baro_altitude_c`` held one: values the
cleaning removed, nearly all of them repeated by OpenSky on reports with no
new altitude message. On 100 reports only the cleaned ``baro_altitude`` held
one: each was a fresh altitude right after a run of repeated values, which
the step 02 check had compared with the repeated value (none of them was a
spike: the next measured altitude continued from it). The rule is therefore
to take the cleaned ``baro_altitude`` as it is.

The stored altitude is in metres; the limits are in feet and the comparison
is scaled, so nothing is converted on disk.
"""

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opdi.pipeline.runways import FT_PER_M

if TYPE_CHECKING:  # pragma: no cover - avoids an import cycle
    from opdi.config import EventConfig

__all__ = [
    "DETECTOR_ALTITUDE",
    "RECEIVED_ALTITUDE",
    "altitude_for_detectors",
    "flight_altitude_only",
    "received_altitude",
]

#: The altitude column every detector of step 04 reads, in metres.
DETECTOR_ALTITUDE = "baro_altitude_c"

#: Internal column of step 04: the altitude of step 02 (``baro_altitude_c`` as
#: stored), within the plausible range, kept beside the cleaned one for the
#: runway, ground and airport detectors (:func:`received_altitude`). Never
#: published.
RECEIVED_ALTITUDE = "_baro_altitude_received"


def altitude_for_detectors(
    sdf: DataFrame, config: "EventConfig", *, cleaned_tracks: bool
) -> DataFrame:
    """Put in ``baro_altitude_c`` the altitude every detector should read.

    Args:
        sdf: The reports as step 04 reads them. With ``cleaned_tracks`` and
            ``config.altitude_from_cleaning`` they must hold the cleaned
            ``baro_altitude`` beside ``baro_altitude_c``.
        config: The event settings.
        cleaned_tracks: Whether the reports come from the cleaned tracks
            (``osn_tracks_clean``). In the uncleaned tracks ``baro_altitude``
            is the received altitude, so it is not used.

    Returns:
        The same rows and columns, without ``baro_altitude`` if it was added
        for this. With every setting off, ``sdf`` itself.
    """
    altitude = F.col(DETECTOR_ALTITUDE)
    changed = False
    if cleaned_tracks and config.altitude_from_cleaning:
        altitude = F.col("baro_altitude")
        changed = True
    low, high = config.altitude_plausible_min_ft, config.altitude_plausible_max_ft
    if low is not None:
        altitude = F.when(altitude * F.lit(FT_PER_M) >= F.lit(float(low)), altitude)
        changed = True
    if high is not None:
        altitude = F.when(altitude * F.lit(FT_PER_M) <= F.lit(float(high)), altitude)
        changed = True
    if not changed:
        return sdf
    out = sdf
    if cleaned_tracks and config.altitude_from_cleaning:
        received = F.col(DETECTOR_ALTITUDE)
        if low is not None:
            received = F.when(received * F.lit(FT_PER_M) >= F.lit(float(low)), received)
        if high is not None:
            received = F.when(received * F.lit(FT_PER_M) <= F.lit(float(high)), received)
        out = out.withColumn(RECEIVED_ALTITUDE, received.cast("double"))
    out = out.withColumn(DETECTOR_ALTITUDE, altitude.cast("double"))
    if cleaned_tracks and config.altitude_from_cleaning:
        out = out.drop("baro_altitude")
    return out


def received_altitude(sdf: DataFrame) -> DataFrame:
    """The reports with the altitude of step 02 in ``baro_altitude_c``.

    For the runway, ground and airport detectors and the plausibility
    checks. The cleaning of step 02a removes altitudes these need: the
    altitudes read on the take-off and landing roll, where the altitude
    changes fast and the cleaning takes the change for a spike, and the
    altitude of reports that carry no position. Those reports count as
    "neither on the ground nor in flight" in the ground detectors, while a
    report without any altitude counts as a surface report. Measured on the
    verification sample of 2026-06-14: with the cleaned altitude, 1,878 of
    10,626 in-block times (``AIBT``) were lost, 105 of them confirmed by
    airport records, and about 100 confirmed runway events.

    Without :data:`RECEIVED_ALTITUDE` on the frame (the cleaned altitude
    was not used), ``sdf`` itself.
    """
    if RECEIVED_ALTITUDE not in sdf.columns:
        return sdf
    return sdf.withColumn(DETECTOR_ALTITUDE, F.col(RECEIVED_ALTITUDE))


def flight_altitude_only(sdf: DataFrame, config: "EventConfig") -> DataFrame:
    """The reports with no altitude on those flagged on the ground.

    For the vertical detectors only (see the module description). Without
    ``config.vertical_ignores_ground_altitude``, or without an ``on_ground``
    column, ``sdf`` itself.
    """
    if not config.vertical_ignores_ground_altitude or "on_ground" not in sdf.columns:
        return sdf
    return sdf.withColumn(
        DETECTOR_ALTITUDE,
        F.when(~F.coalesce(F.col("on_ground"), F.lit(False)), F.col(DETECTOR_ALTITUDE)),
    )
