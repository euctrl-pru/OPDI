"""The geographic coverage of a run: worldwide, or a bounding box.

Every part of the pipeline that filters by position (ingestion, airport
detection, the three reference generators) reads :class:`CoverageConfig`, so
they always cover the same area. If they did not, the ingestion could keep
reports for which no airport zones exist, and those flights would get no
airport, without any error.

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
#: Do not edit: the European data published by OPDI were built inside this box
#: (it decides which reports form each track), and the detection thresholds
#: were tuned inside it. Another coverage is chosen with another
#: ``CoverageConfig``.
EUROPE_BBOX: Tuple[float, float, float, float] = (-25.86653, 26.74617, 49.65699, 70.25976)


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

    An airport just beyond the edge still has traffic inside it, so its zones
    and layout must exist, or those flights get no ADEP. Default 3 degrees.
    Ignored worldwide.
    """

    airport_types: Tuple[str, ...] = ("large_airport", "medium_airport")
    """OurAirports airport types that get reference data (default: large and
    medium airports).

    Small airports, heliports and closed airports outnumber large and medium
    ones by about ten to one, and would change the candidate airports of the
    flight list and multiply the size of the reference tables. All three
    reference tables (detection zones, layouts, runway grid) use this one set,
    so they always join to each other.
    """

    @property
    def is_worldwide(self) -> bool:
        """Whether the coverage is the whole world (no box)."""
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
            (lat_col >= F.lit(min_lat))
            & (lat_col <= F.lit(max_lat))
            & (lon_col >= F.lit(min_lon))
            & (lon_col <= F.lit(max_lon))
        )

    def pandas_mask(self, lat, lon, *, offset: bool):
        """The same filter for a pandas frame (used by ``h3_airport_layouts``)."""
        import numpy as np

        if self.bbox is None:
            return np.ones(len(lat), dtype=bool)
        min_lon, min_lat, max_lon, max_lat = self._bounds(offset)
        return (lat.between(min_lat, max_lat) & lon.between(min_lon, max_lon)).to_numpy()
