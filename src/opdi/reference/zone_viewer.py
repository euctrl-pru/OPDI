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

#: Carto's Dark Matter vector style. Free and token-free, unlike the Mapbox
#: styles kepler.gl defaults to -- a page built without a token shows the data
#: floating over nothing at all.
CARTO_DARK_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"

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
            # Carto Dark Matter, served as a free vector style. kepler.gl
            # otherwise defaults to Mapbox styles, and the generated page has
            # no access token -- which renders the data over a blank void
            # rather than a map.
            "mapStyle": {
                "styleType": "carto_dark",
                "topLayerGroups": {},
                "visibleLayerGroups": {
                    "label": True, "road": True, "border": False,
                    "building": True, "water": True, "land": True,
                },
                "threeDBuildingColor": [15.0, 15.0, 15.0],
                "mapStyles": {
                    "carto_dark": {
                        "id": "carto_dark",
                        "label": "Carto Dark Matter",
                        "url": CARTO_DARK_STYLE,
                        "icon": "",
                        "custom": True,
                        "accessToken": "",
                    }
                },
            },
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
    _finish_html(out_html)
    return out_html


#: kepler.gl's template renders into an unsized div, so the map occupies a
#: corner of the window instead of the window.
_FULLSCREEN_CSS = """
<style>
  html, body { margin: 0; padding: 0; width: 100%; height: 100%; overflow: hidden; }
  #app-content { position: absolute; inset: 0; width: 100vw; height: 100vh; }
</style>
"""


def _finish_html(path: Path) -> None:
    """Size the map to the viewport and strip the bundled analytics beacon."""
    import re

    html = path.read_text()
    if "#app-content" not in html.split("<body")[0]:
        html = html.replace("</head>", _FULLSCREEN_CSS + "</head>", 1)

    # The template ships a Google Analytics tag. This is an internal artefact
    # about aerodrome geometry; it should not call home when opened. The
    # loader appears both as a script tag and again inside a bundled JS
    # string, so the endpoint is neutralised rather than the code excised --
    # excising only the tag leaves the second copy live.
    html = re.sub(
        r"<script>\(function\(i,s,o,g,r,a,m\).*?</script>", "", html, flags=re.S
    )
    html = html.replace("https://www.google-analytics.com/analytics.js", "about:blank")
    path.write_text(html)


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
