"""
Render the H3 aerodrome detection zones to a self-contained kepler.gl page.

The zone table is tens of millions of (aerodrome, hex) rows, which no browser
will draw, so the viewer samples aerodromes rather than rows: every ring of a
chosen number of aerodromes, which is what makes the ring structure legible.
Sampling rows instead would scatter hexagons and show nothing useful.

Hexagons are handed to kepler.gl as H3 cell IDs rather than polygons. Its H3
layer resolves the geometry itself, so the page carries one string per cell
instead of seven coordinate pairs.

The radius filter is the point of the page: ``max_c_radius_nm`` is exposed as a
range filter defaulting to 30 NM, which is the flight list's detection radius
(``FlightListProcessor.DETECTION_RADIUS_NM``). Widening it shows what the ASMA
rings out to 110 NM cover.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Sequence

import pandas as pd

#: Default detection radius shown when the page opens, matching the flight list.
DEFAULT_MAX_RADIUS_NM = 30.0

#: Aerodromes to draw unless told otherwise. Busy, spread across the area, and
#: several are cases the ADEP/ADES study called out -- EHAM and LFPG for dense
#: terminal areas, LICC for its neighbouring military field.
DEFAULT_AERODROMES = [
    "EHAM", "EGLL", "LFPG", "EDDF", "LEMD", "LIRF", "LSZH", "EKCH", "LICC", "LTFM",
]


def load_zones(
    path: str | Path,
    aerodromes: Optional[Sequence[str]] = None,
    max_radius_nm: Optional[float] = None,
) -> pd.DataFrame:
    """Read the generated zone table, restricted to a few aerodromes.

    Args:
        path: Directory of parquet parts, as written by ``save_prepared``.
        aerodromes: ICAO idents to keep. Defaults to :data:`DEFAULT_AERODROMES`.
        max_radius_nm: Optional cap, mostly to keep a page small.

    Returns:
        One row per (aerodrome, hex cell).
    """
    path = Path(path)
    parts = sorted(path.glob("*.parquet")) if path.is_dir() else [path]
    if not parts:
        raise FileNotFoundError(f"no parquet parts under {path}")

    keep = list(aerodromes) if aerodromes is not None else DEFAULT_AERODROMES
    frames = []
    for part in parts:
        df = pd.read_parquet(part)
        df = df[df["apt_ident"].isin(keep)]
        # Tolerate both namings; see prepare_for_flight_list_spark.
        df = df.rename(columns={"min_c_radius_nm": "apt_min_c_radius_nm",
                                "max_c_radius_nm": "apt_max_c_radius_nm"})
        if max_radius_nm is not None:
            df = df[df["apt_max_c_radius_nm"] <= max_radius_nm]
        if len(df):
            frames.append(df)
    if not frames:
        raise ValueError(f"none of {keep} found in {path}")
    return pd.concat(frames, ignore_index=True)


def _config(default_max_nm: float, radius_bounds: tuple) -> dict:
    """kepler.gl config: an H3 layer coloured by ring, filtered by radius."""
    lo, hi = radius_bounds
    return {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [
                    {
                        # The reason the page exists: narrow the rings without
                        # regenerating anything. Opens at the flight list's
                        # detection radius.
                        "dataId": ["zones"],
                        "id": "radius_filter",
                        "name": ["apt_max_c_radius_nm"],
                        "type": "range",
                        "value": [lo, default_max_nm],
                        "enlarged": False,
                        "plotType": "histogram",
                        ".yAxis": None,
                    }
                ],
                "layers": [
                    {
                        "id": "zone_hexes",
                        "type": "hexagonId",
                        "config": {
                            "dataId": "zones",
                            "label": "Detection rings",
                            "columns": {"hex_id": "apt_hex_id"},
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.55,
                                "colorRange": {
                                    "name": "ColorBrewer RdYlBu-6",
                                    "type": "diverging",
                                    "category": "ColorBrewer",
                                    # Inner rings warm, outer rings cool, so the
                                    # concentric structure reads at a glance.
                                    "colors": ["#d73027", "#fc8d59", "#fee090",
                                               "#e0f3f8", "#91bfdb", "#4575b4"],
                                },
                                "coverage": 1,
                                "enable3d": False,
                                "sizeRange": [0, 500],
                                "coverageRange": [0, 1],
                                "elevationScale": 5,
                            },
                        },
                        "visualChannels": {
                            "colorField": {"name": "apt_max_c_radius_nm", "type": "real"},
                            "colorScale": "quantize",
                            "sizeField": None,
                            "sizeScale": "linear",
                            "coverageField": None,
                            "coverageScale": "linear",
                        },
                    }
                ],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "zones": [
                                {"name": "apt_ident", "format": None},
                                {"name": "apt_min_c_radius_nm", "format": None},
                                {"name": "apt_max_c_radius_nm", "format": None},
                                {"name": "distance_from_center", "format": None},
                            ]
                        },
                        "enabled": True,
                    },
                    "brush": {"size": 0.5, "enabled": False},
                },
            },
            "mapState": {
                "latitude": 50.0,
                "longitude": 8.0,
                "zoom": 4.0,
                "bearing": 0,
                "pitch": 0,
            },
            "mapStyle": {"styleType": "dark"},
        },
    }


def render(
    zones: pd.DataFrame,
    out_html: str | Path,
    default_max_nm: float = DEFAULT_MAX_RADIUS_NM,
) -> Path:
    """Write a self-contained kepler.gl page for *zones*."""
    from keplergl import KeplerGl

    cols = [
        "apt_ident", "apt_hex_id",
        "apt_min_c_radius_nm", "apt_max_c_radius_nm",
        "distance_from_center",
    ]
    data = zones[[c for c in cols if c in zones.columns]].copy()

    bounds = (
        float(data["apt_max_c_radius_nm"].min()),
        float(data["apt_max_c_radius_nm"].max()),
    )
    m = KeplerGl(height=800, data={"zones": data}, config=_config(default_max_nm, bounds))
    out_html = Path(out_html)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    m.save_to_html(file_name=str(out_html))
    return out_html


def build(
    zones_path: str | Path,
    out_html: str | Path,
    aerodromes: Optional[Sequence[str]] = None,
    default_max_nm: float = DEFAULT_MAX_RADIUS_NM,
    max_radius_nm: Optional[float] = None,
) -> Path:
    """Load, render, and report. Convenience wrapper for the two steps above."""
    zones = load_zones(zones_path, aerodromes=aerodromes, max_radius_nm=max_radius_nm)
    n_apt = zones["apt_ident"].nunique()
    print(f"{len(zones):,} hex cells across {n_apt} aerodromes")
    path = render(zones, out_html, default_max_nm=default_max_nm)
    size_mb = path.stat().st_size / 1e6
    print(f"-> {path} ({size_mb:.1f} MB), radius filter defaulting to {default_max_nm:g} NM")
    return path
