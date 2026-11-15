"""The barometric altitude the detectors of step 04 read.

Every detector of step 04 reads the barometric altitude from the column
``baro_altitude_c`` (metres). This module decides what that column holds,
in two places:

* :func:`altitude_for_detectors`, applied once as step 04 reads the tracks,
  for every detector. It puts in ``baro_altitude_c`` the altitude that
  survived the cleaning of step 02a (``EventConfig.altitude_from_cleaning``)
  and treats an altitude outside a plausible range as missing
  (``EventConfig.altitude_plausible_min_ft`` and
  ``altitude_plausible_max_ft``).
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

if TYPE_CHECKING:  # pragma: no cover - avoids an import cycle
    from opdi.config import EventConfig

__all__ = ["DETECTOR_ALTITUDE", "altitude_for_detectors", "flight_altitude_only"]

#: The altitude column every detector of step 04 reads, in metres.
DETECTOR_ALTITUDE = "baro_altitude_c"

#: Feet per metre, the factor the events use for altitudes.
FT_PER_M = 3.28084


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
    out = sdf.withColumn(DETECTOR_ALTITUDE, altitude.cast("double"))
    if cleaned_tracks and config.altitude_from_cleaning:
        out = out.drop("baro_altitude")
    return out


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
