"""
Render the aerodrome detection zones to a kepler.gl page.

Thin front end over ``opdi.reference.zone_viewer``; all behaviour lives in the
package.

    python benchmarks/view_zones.py --zones data/zones_110nm --out data/zones.html
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from opdi.reference.zone_viewer import DEFAULT_AERODROMES, DEFAULT_MAX_RADIUS_NM, build


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--zones", required=True, help="directory of generated parquet parts")
    ap.add_argument("--out", required=True, help="output .html")
    ap.add_argument("--aerodromes", nargs="+", default=DEFAULT_AERODROMES)
    ap.add_argument("--default-max-nm", type=float, default=DEFAULT_MAX_RADIUS_NM,
                    help="where the radius filter opens (default: the flight "
                         "list's 30 NM detection radius)")
    ap.add_argument("--max-radius-nm", type=float, default=None,
                    help="hard cap on what is loaded at all, to keep the page small")
    args = ap.parse_args()
    build(args.zones, args.out, aerodromes=args.aerodromes,
          default_max_nm=args.default_max_nm, max_radius_nm=args.max_radius_nm)


if __name__ == "__main__":
    main()
