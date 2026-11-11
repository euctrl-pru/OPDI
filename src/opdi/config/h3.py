"""Resolutions of the H3 grid: :class:`H3Config`."""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class H3Config:
    """Resolutions of the H3 grid used to index positions and airports.

    H3 divides the Earth into hexagonal cells; a higher resolution means
    smaller cells. Resolution 7 cells are about 5 km across, resolution 12
    cells about 20 m.
    """

    airport_detection_resolution: int = 7
    """Resolution of the cells around each airport used to find departure and
    destination airports (about 5 km across)."""

    airport_layout_resolution: int = 12
    """Resolution of the cells that map runways, taxiways and stands (about
    20 m across)."""

    airport_layout_pbf_path: Optional[str] = None
    """Local OpenStreetMap extract (``.pbf`` file) that step 00b reads the
    airport layouts (runways, taxiways, stands) from.

    When ``None``, step 00b queries the public Overpass service instead,
    which is slow for many airports and can return nothing for an airport
    whose name it cannot resolve. The environment variable ``OPDI_OSM_PBF``
    replaces this setting.

    A file filtered to aeroway features is much smaller and faster to read
    than a raw regional extract, and a raw extract of a whole continent needs
    more than 16 GB of memory; see
    :func:`opdi.reference.pbf_filter.filter_aeroway_pbf`.
    """

    track_resolutions: List[int] = field(default_factory=lambda: [7, 12])
    """Resolutions at which every position of a track is indexed (columns
    ``h3_res_7`` and ``h3_res_12``)."""

    airspace_resolution: int = 7
    """Resolution of the cells that map airspaces (FIRs)."""
