"""Where OPDI looks. One bounding box, asked for by everything that filters.

The box used to be written out six times in five files -- four of them
textually identical ``LAT_MIN``/``LAT_MAX``/``LON_MIN``/``LON_MAX`` class
constants with no link between them. Widening coverage therefore meant finding
all six, and missing one produced no error: the ingest would keep worldwide
rows while the detection zones covered Europe, so every flight outside Europe
got a track and no aerodrome. That is a silent wrong answer, which is the
failure mode this module exists to make impossible.

``bbox = None`` means worldwide. It is not a box of +/-180/90 because those are
not the same thing: a full-globe box still compares four columns on every row,
and at the antimeridian it is actively wrong -- a longitude range cannot
express "everywhere" once the coordinate wraps. ``None`` compiles to
``lit(True)``, which the optimiser deletes.
"""

from dataclasses import dataclass
from typing import Optional, Tuple

from pyspark.sql import Column
from pyspark.sql import functions as F

__all__ = ["EUROPE_BBOX", "CoverageConfig"]

#: The published OPDI coverage box, ``(min_lon, min_lat, max_lon, max_lat)``.
#:
#: **Do not edit.** It reaches published data through ``track_id`` -- a track
#: is the run of samples that survived this filter -- and every detection study
#: OPDI has published was tuned inside it. Changing coverage is done by
#: choosing a different ``CoverageConfig``, never by editing this tuple.
EUROPE_BBOX: Tuple[float, float, float, float] = (
    -25.86653, 26.74617, 49.65699, 70.25976
)


@dataclass
class CoverageConfig:
    """The geographic scope of a run.

    ``bbox`` is ``(min_lon, min_lat, max_lon, max_lat)`` in degrees, or ``None``
    for worldwide.
    """

    bbox: Optional[Tuple[float, float, float, float]] = EUROPE_BBOX
    """``None`` means worldwide. Anything else is a box in degrees."""

    bbox_offset_deg: float = 3.0
    """How far outside the box reference data is still built, in degrees.

    An aerodrome sitting just beyond the edge still receives traffic that is
    inside it, so its zones and layout must exist or those flights get a track
    and no ADEP. 3 degrees is the value all four reference generators used
    independently before this module existed. Ignored worldwide, where there
    is no edge to be just beyond.
    """

    airport_types: Tuple[str, ...] = ("large_airport", "medium_airport")
    """Which aerodromes get reference data.

    Measured against the released ``h3_airport_detection_zones``: it holds
    large and medium aerodromes and nothing else. The filter used to be a
    bounding box alone, and inside that box OurAirports lists 13,973
    aerodromes -- 6,782 small, 3,820 heliports, 1,952 *closed* -- against
    1,357 large or medium.

    Rebuilding without this therefore did two things at once. It changed the
    ADEP/ADES candidate set by a factor of ten, silently, against the set
    every detection study tuned on; and at H3 resolution 7 with rings out to
    110 NM it generated enough cells to OOM every executor in the namespace.

    All three reference tables -- detection zones, ground layouts, runway grid
    -- must use one set, or they join to each other with gaps nothing reports.
    """

    @property
    def is_worldwide(self) -> bool:
        return self.bbox is None

    @property
    def label(self) -> str:
        """``europe``, ``worldwide``, or ``custom``.

        Stamped into the warehouse coverage marker, so a third value is not
        cosmetic: a regional run must not be able to claim it is one of the two
        named coverages and land in that coverage's warehouse.
        """
        if self.bbox is None:
            return "worldwide"
        return "europe" if tuple(self.bbox) == EUROPE_BBOX else "custom"

    def _bounds(self, offset: bool):
        min_lon, min_lat, max_lon, max_lat = self.bbox
        d = self.bbox_offset_deg if offset else 0.0
        return min_lon - d, min_lat - d, max_lon + d, max_lat + d

    def spark_filter(self, lat_col: Column, lon_col: Column, *, offset: bool) -> Column:
        """A predicate selecting positions inside this coverage.

        ``offset`` is for the reference generators, which build slightly
        outside the box; ingest and the flight list pass ``False``.
        """
        if self.bbox is None:
            return F.lit(True)
        min_lon, min_lat, max_lon, max_lat = self._bounds(offset)
        return (
            (lat_col >= F.lit(min_lat)) & (lat_col <= F.lit(max_lat))
            & (lon_col >= F.lit(min_lon)) & (lon_col <= F.lit(max_lon))
        )

    def pandas_mask(self, lat, lon, *, offset: bool):
        """The same rule for a pandas frame. ``h3_airport_layouts`` filters its
        airport loop list in pandas, and the two must not be able to disagree.
        """
        import numpy as np

        if self.bbox is None:
            return np.ones(len(lat), dtype=bool)
        min_lon, min_lat, max_lon, max_lat = self._bounds(offset)
        return (
            lat.between(min_lat, max_lat) & lon.between(min_lon, max_lon)
        ).to_numpy()
