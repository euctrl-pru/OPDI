# Point merge systems

`point_merge_systems.csv`: one row per merge point of a Point Merge System (PMS),
for every airport in EUROCONTROL's *Point Merge catalogue* (edition 1.0, July 2026,
61 airports). Curated 2026-11-10.

Columns: `apt_icao`, `pms_id` (airport plus runway group), `merge_point` (fix name),
`lat`, `lon` (WGS84 decimal degrees), `legs_mp_min_nm`/`legs_mp_max_nm` (distance from
the sequencing legs to the merge point, from the catalogue), `leg_level_min_ft`/
`leg_level_max_ft` (leg levels where the STAR coding states them, else blank),
`valid_from`/`valid_to` (dates; `valid_to` blank means still in operation), `source`,
`confidence` (`high`: official chart or fix-database coordinates confirmed by the
catalogue geometry; `medium`: the design in the navigation data differs from the
catalogue's description; `missing`: no coordinates found, row kept so the gap is
visible).

Sources. Leg geometry and dates come from the catalogue. Merge-point names and
coordinates come from state AIPs where reachable (Ireland, Portugal) and otherwise
from an open mirror of X-Plane 12 navigation data (AIRAC 2406, Jeppesen-derived,
github.com/CAG2Mark/cifp-visualizer), where the merge point was found the way the
catalogue's authors found it: fitting a circle through each run of three consecutive
STAR waypoints, snapping the centre to the nearest published fix, and checking it
against the catalogue's legs-to-merge-point distance and the approach transitions.
Fix coordinates are facts published in the state AIPs; no procedure coding is
reproduced here. Every placed point lies 7 to 45 NM from the airport reference point,
except two the catalogue itself places at the edge (ZYTL TL111 at 5.8 NM, VVTS DALAP at
45 NM). Four airports (RJBB, ZSAM, ZSWZ, ZUCK; 2025-2026 implementations) and three
sub-systems are `missing`. Details per airport are in the research notes that produced
the table.
