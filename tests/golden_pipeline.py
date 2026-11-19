"""A small synthetic day, run through the pipeline steps 02 to 09.

The golden-output test (``tests/test_golden_outputs.py``) and the script that
writes its reference files (``tools/make_golden.py``) both use this module.
It builds one day of state vectors for a handful of simulated flights and the
reference tables the steps read, writes them into a local warehouse of plain
parquet folders, runs the steps the way ``opdi.periodrun`` runs them, and
returns every table the steps wrote.

Two configurations are run:

* ``default``: today's default configuration (release v2.0.0).
* ``v002``: the configuration that reproduces the published v0.0.2 release
  (``segmentation.method = "legacy"``, ``decimation = "modulo"``,
  ``DetectionConfig.legacy()``, ``EventConfig.legacy()``, flight list from raw
  tracks, European coverage).

The inputs are made here from plain Python and the ``h3`` library only, so a
change in the package cannot change the inputs, only the outputs.
"""

import datetime as dt
import math
import os
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Tuple

import h3
import pandas as pd

#: The day whose flights are processed. The state vectors cover the day
#: before and after it as well, as the steps read past both ends of the day.
DAY = dt.date(2024, 6, 12)

#: Unit conversions, as used throughout the pipeline.
FT_PER_M = 3.28084
KT_PER_MPS = 1.94384
FPM_PER_MPS = 196.850394
NM_M = 1852.0
EARTH_RADIUS_M = 6371008.8


# --- geometry ---------------------------------------------------------------


def destination(lat: float, lon: float, bearing_deg: float, dist_nm: float) -> Tuple[float, float]:
    """Point reached from (lat, lon) after ``dist_nm`` on a great circle."""
    d = dist_nm * NM_M / EARTH_RADIUS_M
    b = math.radians(bearing_deg)
    p1, l1 = math.radians(lat), math.radians(lon)
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(b))
    l2 = l1 + math.atan2(
        math.sin(b) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2)
    )
    return math.degrees(p2), (math.degrees(l2) + 540.0) % 360.0 - 180.0


def distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a)) / NM_M


def bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial great-circle bearing in degrees from north."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


# --- aerodromes -------------------------------------------------------------


@dataclass(frozen=True)
class Aerodrome:
    """One aerodrome with one runway.

    ``threshold`` is the threshold of the runway direction ``le`` (heading
    ``heading`` degrees); the other end is ``length_nm`` further along.
    """

    icao: str
    name: str
    threshold: Tuple[float, float]
    heading: float
    elevation_ft: float
    le: str
    he: str
    length_nm: float = 2.0

    @property
    def far_end(self) -> Tuple[float, float]:
        return destination(*self.threshold, self.heading, self.length_nm)

    @property
    def reference_point(self) -> Tuple[float, float]:
        return destination(*self.threshold, self.heading, self.length_nm / 2)

    def stand(self, number: int) -> Tuple[float, float]:
        """A parking position beside the runway, 0.4 NM off its centreline."""
        along = destination(*self.threshold, self.heading, 0.2 + 0.3 * number)
        return destination(*along, self.heading - 90.0, 0.4)


AERODROMES = [
    Aerodrome("EBBR", "Brussels", (50.8958, 4.4626), 72.0, 184.0, "07", "25"),
    Aerodrome("EDDF", "Frankfurt", (50.0280, 8.5350), 70.0, 364.0, "07", "25"),
    Aerodrome("LFPG", "Paris", (49.0120, 2.5150), 90.0, 392.0, "09", "27"),
    Aerodrome("LOWW", "Vienna", (48.1100, 16.5400), 110.0, 600.0, "11", "29"),
]
BY_ICAO = {a.icao: a for a in AERODROMES}


# --- the flight simulator ---------------------------------------------------


@dataclass
class Flight:
    """A simulated flight: one airframe, gate to gate.

    ``holes`` are reception gaps (start and end, in seconds after the
    start of the flight) during which no state vector is received.
    ``callsign_changes`` replace the callsign in a time window
    (start, end, value), to simulate a garbled callsign.
    """

    icao24: str
    callsign: str
    origin: Optional[str]
    destination: Optional[str]
    start: dt.datetime
    cruise_ft: float = 35000.0
    holes: List[Tuple[float, float]] = field(default_factory=list)
    callsign_changes: List[Tuple[float, float, str]] = field(default_factory=list)
    holding_circuits: int = 0
    stand: int = 1
    # Only for an overflight: where it enters, its heading and how long it flies.
    overflight: Optional[Tuple[float, float, float, float]] = None
    # The golden configurations whose day includes this flight.
    configurations: Tuple[str, ...] = ("default", "v002")


def _simulate(f: Flight) -> List[dict]:
    """One state vector per second for the flight, in the OpenSky schema."""
    out: List[dict] = []
    t = 0.0
    lat = lon = alt = gs = hdg = 0.0
    on_ground = False

    def emit():
        out.append(dict(t=t, lat=lat, lon=lon, alt=alt, gs=gs, hdg=hdg, vr=vr, on_ground=on_ground))

    def move(dist_nm: float, heading: float):
        nonlocal lat, lon
        lat, lon = destination(lat, lon, heading, dist_nm)

    vr = 0.0
    if f.overflight is not None:
        lat, lon, hdg, duration = f.overflight
        alt, gs = f.cruise_ft, 460.0
        while t < duration:
            emit()
            move(gs / 3600.0, hdg)
            t += 1.0
        return out

    org, des = BY_ICAO[f.origin], BY_ICAO[f.destination]

    # Taxi from the stand to the threshold at 12 kt.
    lat, lon = org.stand(f.stand)
    alt, gs, on_ground = org.elevation_ft, 0.0, True
    for _ in range(60):  # parked, transponder on
        hdg = bearing(lat, lon, *org.threshold)
        emit()
        t += 1.0
    while distance_nm(lat, lon, *org.threshold) > 12.0 / 3600.0:
        gs = 12.0
        hdg = bearing(lat, lon, *org.threshold)
        emit()
        move(gs / 3600.0, hdg)
        t += 1.0
    lat, lon = org.threshold
    hdg, gs = org.heading, 0.0
    for _ in range(40):  # lined up
        emit()
        t += 1.0
    # Take-off roll at 3 kt/s up to 150 kt.
    while gs < 150.0:
        emit()
        gs += 3.0
        move(gs / 3600.0, hdg)
        t += 1.0
    on_ground = False

    # Final approach fix: 8 NM out on the extended centreline of runway ``le``.
    faf = destination(*des.threshold, des.heading + 180.0, 8.0)
    climb_away = 0.0
    held = 0
    hold_fix = destination(*faf, des.heading + 180.0, 12.0)
    hold_state = None  # (leg index, seconds left in the leg)
    while True:
        dist_to_thr = distance_nm(lat, lon, *des.threshold)
        dist_to_faf = distance_nm(lat, lon, *faf)
        if dist_to_faf < 0.6 and hold_state is None and held >= f.holding_circuits:
            break
        # Target altitude: cruise, limited by a 3:1 descent towards the fix.
        target = min(f.cruise_ft, des.elevation_ft + 318.0 * 8.0 + 300.0 * dist_to_faf)
        if hold_state is not None:
            target = alt
        if alt < target:
            vr = min(2500.0, (target - alt) * 60.0)
        elif alt > target:
            vr = -min(2500.0, (alt - target) * 60.0)
        else:
            vr = 0.0
        alt += vr / 60.0
        # Speed: 250 kt below 10,000 ft, 460 kt above.
        want = 460.0 if alt > 10000.0 else 250.0
        if dist_to_faf < 15.0:
            want = 180.0
        gs += max(-2.0, min(2.0, want - gs))
        if climb_away < 3.0:
            climb_away += gs / 3600.0
        elif hold_state is not None:
            leg, left = hold_state
            if leg % 2 == 1:
                hdg = (hdg + 3.0) % 360.0
            if left <= 1.0:
                leg += 1
                if leg == 4:
                    held += 1
                    leg = 0
                    if held >= f.holding_circuits:
                        hold_state = None
                        leg = None
                if leg is not None:
                    hold_state = (leg, 60.0)
            else:
                hold_state = (leg, left - 1.0)
        else:
            if held < f.holding_circuits and distance_nm(lat, lon, *hold_fix) < 1.0:
                hold_state = (0, 60.0)
            target_point = hold_fix if held < f.holding_circuits else faf
            want_hdg = bearing(lat, lon, *target_point)
            turn = (want_hdg - hdg + 540.0) % 360.0 - 180.0
            hdg = (hdg + max(-3.0, min(3.0, turn))) % 360.0
        emit()
        move(gs / 3600.0, hdg)
        t += 1.0
        if t > 4 * 3600:
            raise RuntimeError(f"{f.callsign} never reached its approach fix")

    # Final approach on a 3 degree slope at 140 kt.
    hdg = des.heading
    while True:
        dist_to_thr = distance_nm(lat, lon, *des.threshold)
        past = (bearing(lat, lon, *des.threshold) - hdg + 540.0) % 360.0 - 180.0
        beyond = abs(past) > 90.0
        height = 318.0 * dist_to_thr if not beyond else 50.0 - 200.0 * dist_to_thr
        if beyond and height <= 0.0:
            break
        new_alt = des.elevation_ft + max(height, 0.0)
        vr = (new_alt - alt) * 60.0
        alt = new_alt
        gs += max(-2.0, min(2.0, 140.0 - gs))
        hdg = (bearing(lat, lon, *des.far_end)) if not beyond else hdg
        emit()
        move(gs / 3600.0, hdg)
        t += 1.0
    # Roll-out, then taxi to the stand.
    alt, vr, on_ground = des.elevation_ft, 0.0, True
    while gs > 15.0:
        emit()
        gs -= 3.0
        move(gs / 3600.0, hdg)
        t += 1.0
    stand = des.stand(f.stand + 2)
    while distance_nm(lat, lon, *stand) > 12.0 / 3600.0:
        gs = 12.0
        hdg = bearing(lat, lon, *stand)
        emit()
        move(gs / 3600.0, hdg)
        t += 1.0
    lat, lon, gs = stand[0], stand[1], 0.0
    for _ in range(120):  # parked
        emit()
        t += 1.0
    return out


#: The flights of the synthetic day. They cover: a turnaround of one airframe
#: under two callsigns, a holding pattern, a garbled callsign for a few
#: seconds, a long reception gap at cruise, and flights that cross midnight at
#: both ends of the day, and one overflight at cruise level far from every
#: aerodrome, which step 03 adds to the flight list in its second pass.
#:
#: The overflight is flown in the default configuration only. Under the
#: v0.0.2 configuration, event and measurement ids are row numbers counted
#: over the whole day, so any added flight renumbers the events of the other
#: flights; leaving it out keeps the v0.0.2 reference files comparable.
FLIGHTS = [
    Flight("4ca001", "TST101", "EBBR", "EDDF", dt.datetime(2024, 6, 12, 8, 0, 0)),
    Flight(
        "4ca001",
        "TST102",
        "EDDF",
        "EBBR",
        dt.datetime(2024, 6, 12, 9, 30, 0),
        holding_circuits=2,
        cruise_ft=24000.0,
    ),
    Flight(
        "3c6002",
        "DLH4",
        "LFPG",
        "LOWW",
        dt.datetime(2024, 6, 12, 11, 0, 0),
        holes=[(1800.0, 3000.0)],
        callsign_changes=[(1500.0, 1510.0, "DLH4X")],
    ),
    Flight("440004", "AUA7", "LOWW", "EDDF", dt.datetime(2024, 6, 11, 23, 30, 0)),
    Flight("39c005", "AFR8", "EDDF", "LFPG", dt.datetime(2024, 6, 12, 23, 20, 0), stand=2),
    Flight(
        "4d0006",
        "SWR19",
        None,
        None,
        dt.datetime(2024, 6, 12, 14, 0, 0),
        overflight=(46.5, 0.0, 90.0, 1800.0),
        configurations=("default",),
    ),
]


def state_vectors(name: str = "default") -> pd.DataFrame:
    """Raw 1 Hz state vectors of every flight, as ingested from OpenSky.

    ``event_time`` is in Unix seconds, as in the OpenSky archive, so the
    ingestion thinning can be applied to it.

    Args:
        name: The golden configuration, ``"default"`` or ``"v002"``; only the
            flights flown in it are included.
    """
    rows = []
    for f in FLIGHTS:
        if name not in f.configurations:
            continue
        t0 = (f.start - dt.datetime(1970, 1, 1)).total_seconds()
        for s in _simulate(f):
            if any(a <= s["t"] < b for a, b in f.holes):
                continue
            callsign = f.callsign
            for a, b, value in f.callsign_changes:
                if a <= s["t"] < b:
                    callsign = value
            when = int(t0 + s["t"])
            rows.append(
                {
                    "event_time": when,
                    "icao24": f.icao24,
                    "lat": round(s["lat"], 6),
                    "lon": round(s["lon"], 6),
                    "velocity": round(s["gs"] / KT_PER_MPS, 3),
                    "heading": round(s["hdg"], 3),
                    "vert_rate": round(s["vr"] / FPM_PER_MPS, 3),
                    "callsign": callsign.ljust(8),
                    "on_ground": bool(s["on_ground"]),
                    "alert": False,
                    "spi": False,
                    "squawk": "1000",
                    "baro_altitude": round(s["alt"] / FT_PER_M, 2),
                    "geo_altitude": round(s["alt"] / FT_PER_M + 30.0, 2),
                    "last_pos_update": float(when),
                    "last_contact": float(when),
                    "serials": [1, 2],
                }
            )
    return pd.DataFrame(rows)


# --- reference tables -------------------------------------------------------


def _cells_along(a: Tuple[float, float], b: Tuple[float, float], res: int, k: int) -> set:
    """H3 cells covering the segment a-b, widened by ``k`` rings."""
    n = max(2, int(distance_nm(*a, *b) / 0.01))
    cells = set()
    for i in range(n + 1):
        lat = a[0] + (b[0] - a[0]) * i / n
        lon = a[1] + (b[1] - a[1]) * i / n
        cells.update(h3.grid_disk(h3.latlng_to_cell(lat, lon, res), k))
    return cells


def oa_airports() -> pd.DataFrame:
    """The ``oa_airports`` table (OurAirports)."""
    rows = []
    for i, a in enumerate(AERODROMES):
        lat, lon = a.reference_point
        rows.append(
            dict(
                id=i + 1,
                ident=a.icao,
                type="large_airport",
                name=a.name,
                latitude_deg=lat,
                longitude_deg=lon,
                elevation_ft=int(a.elevation_ft),
                continent="EU",
                iso_country="XX",
                iso_region="XX-1",
                municipality=a.name,
                scheduled_service="yes",
                gps_code=a.icao,
                iata_code=None,
                local_code=None,
                home_link=None,
                wikipedia_link=None,
                keywords=None,
            )
        )
    return pd.DataFrame(rows)


def oa_runways() -> pd.DataFrame:
    """The ``oa_runways`` table (OurAirports), one runway per aerodrome."""
    rows = []
    for i, a in enumerate(AERODROMES):
        far = a.far_end
        rows.append(
            dict(
                id=100 + i,
                airport_ref=i + 1,
                airport_ident=a.icao,
                length_ft=int(a.length_nm * 6076),
                width_ft=148,
                surface="ASP",
                lighted=True,
                closed=False,
                le_ident=a.le,
                le_latitude_deg=a.threshold[0],
                le_longitude_deg=a.threshold[1],
                le_elevation_ft=int(a.elevation_ft),
                le_heading_degT=a.heading,
                le_displaced_threshold_ft=None,
                he_ident=a.he,
                he_latitude_deg=far[0],
                he_longitude_deg=far[1],
                he_elevation_ft=int(a.elevation_ft),
                he_heading_degT=(a.heading + 180.0) % 360.0,
                he_displaced_threshold_ft=None,
            )
        )
    return pd.DataFrame(rows)


def detection_zones(max_nm: float = 40.0) -> pd.DataFrame:
    """The ``h3_airport_detection_zones`` table: rings of res-7 cells per aerodrome."""
    rings = list(range(0, int(max_nm) + 5, 5))
    rows = []
    for a in AERODROMES:
        lat, lon = a.reference_point
        centre = h3.latlng_to_cell(lat, lon, 7)
        for cell in sorted(h3.grid_disk(centre, 40)):
            clat, clon = h3.cell_to_latlng(cell)
            d = distance_nm(lat, lon, clat, clon)
            if d >= max_nm:
                continue
            lo = max(r for r in rings if r <= d)
            rows.append(
                dict(
                    apt_ident=a.icao,
                    apt_hex_id=cell,
                    distance_from_center=h3.grid_distance(centre, cell),
                    apt_latitude_deg=lat,
                    apt_longitude_deg=lon,
                    apt_min_c_radius_nm=float(lo),
                    apt_max_c_radius_nm=float(lo + 5),
                    apt_type="large_airport",
                    apt_scheduled="yes",
                )
            )
    return pd.DataFrame(rows)


def airport_layouts() -> pd.DataFrame:
    """The ``hexaero_airport_layouts`` table: runway, taxiway and stand cells."""
    rows = []
    osm = 1000
    for a in AERODROMES:
        parts = [("runway", f"{a.le}/{a.he}", [(a.threshold, a.far_end)], 1)]
        for n in (1, 2, 3, 4):
            stand = a.stand(n)
            on_runway = destination(*stand, a.heading + 90.0, 0.4)
            parts.append(("taxiway", f"T{n}", [(stand, on_runway)], 0))
            parts.append(("parking_position", f"S{n}", [(stand, stand)], 0))
        seen = set()
        for aeroway, ref, segments, k in parts:
            osm += 1
            cells = set()
            for p, q in segments:
                cells |= _cells_along(p, q, 12, k)
            for cell in sorted(cells - seen):
                seen.add(cell)
                clat, clon = h3.cell_to_latlng(cell)
                rows.append(
                    dict(
                        hexaero_apt_icao=a.icao,
                        hexaero_h3_id=cell,
                        hexaero_latitude=clat,
                        hexaero_longitude=clon,
                        hexaero_res=12,
                        hexaero_aeroway=aeroway,
                        hexaero_length=None,
                        hexaero_ref=ref,
                        hexaero_surface=None,
                        hexaero_width=None,
                        hexaero_osm_id=osm,
                        hexaero_type="way",
                    )
                )
    return pd.DataFrame(rows)


def runway_zones() -> pd.DataFrame:
    """The ``h3_runway_zones`` table: runway and 1 NM approach cells per runway."""
    rows = []
    for a in AERODROMES:
        strip = f"{a.icao}:{a.le}-{a.he}"
        runway = _cells_along(a.threshold, a.far_end, 12, 1)
        approach = (
            _cells_along(destination(*a.threshold, a.heading + 180.0, 1.0), a.threshold, 12, 2)
            | _cells_along(a.far_end, destination(*a.far_end, a.heading, 1.0), 12, 2)
        ) - runway
        for zone, cells in (("runway", runway), ("approach", approach)):
            for cell in sorted(cells):
                rows.append(
                    dict(
                        h3_id=cell,
                        apt_icao=a.icao,
                        strip_id=strip,
                        le_ident=a.le,
                        he_ident=a.he,
                        zone=zone,
                    )
                )
    return pd.DataFrame(rows)


def airspaces(sv: pd.DataFrame) -> pd.DataFrame:
    """The ``opdi_h3_airspace_ref`` table: three FIRs split by longitude.

    Only cells near the simulated positions are listed, which is all the
    flights can reach.
    """
    cells = set()
    for lat, lon in zip(sv["lat"], sv["lon"]):
        cells.add(h3.latlng_to_cell(lat, lon, 7))
    near = set()
    for c in cells:
        near.update(h3.grid_disk(c, 1))
    rows = []
    for cell in sorted(near):
        lon = h3.cell_to_latlng(cell)[1]
        code = "EBBU" if lon < 6.0 else ("EDGG" if lon < 12.0 else "LOVV")
        rows.append(
            dict(
                airspace_type="FIR",
                name=code,
                code=code,
                airac_cfmu="486",
                validity_start="2024-05-16",
                validity_end="2024-06-13",
                min_fl=0,
                max_fl=999,
                area_km2=1.0e5,
                h3_cell=cell,
                h3_res=7,
            )
        )
    return pd.DataFrame(rows)


def aircraft_db() -> pd.DataFrame:
    """The ``osn_aircraft_db`` table for the simulated airframes."""
    icao24s = sorted({f.icao24 for f in FLIGHTS})
    return pd.DataFrame(
        [
            dict(
                icao24=i,
                registration=f"OO-{n:03d}",
                model="A320",
                typecode="A320",
                icao_aircraft_class="L2J",
                icao_operator="TST",
            )
            for n, i in enumerate(icao24s)
        ]
    )


# --- running the steps ------------------------------------------------------

#: Steps run for the day, in order.
STEPS = ("02", "02a", "03", "04", "04b", "09")

#: The tables compared, with the columns that sort their rows. Columns that
#: record when a file was written are left out.
OUTPUT_TABLES = (
    "osn_statevectors_v2",
    "osn_tracks",
    "osn_tracks_clean",
    "opdi_flight_list",
    "opdi_flight_events",
    "opdi_measurements",
)
PUBLISHED_FILES = ("flight_list", "flight_events", "index", "catalogue")
UNSTABLE_COLUMNS = {"published_at"}


def configuration(name: str, warehouse: str):
    """The pipeline configuration of a golden run.

    Args:
        name: ``"default"`` or ``"v002"``.
        warehouse: Local folder that holds the tables.
    """
    from opdi.config import DetectionConfig, EventConfig, OPDIConfig

    config = OPDIConfig.for_environment("local", worldwide=(name == "default"), warehouse=warehouse)
    # Plain parquet folders, no catalog, as in the opensky environment.
    config.spark.enable_iceberg = False
    config.spark.enable_hive = False
    if name == "v002":
        config.segmentation.method = "legacy"
        config.ingestion.decimation = "modulo"
        config.detection = DetectionConfig.legacy()
        config.events = EventConfig.legacy()
        config.cleaning = replace(config.cleaning, feeds_flight_list=False)
    elif name != "default":
        raise ValueError(f"unknown golden configuration {name!r}")
    return config


def _spark_frame(spark, pdf: pd.DataFrame, ddl: str):
    return spark.createDataFrame(pdf.astype(object).where(pdf.notna(), None), ddl)


def write_inputs(spark, config, name: str = "default") -> None:
    """Write the state vectors (thinned by the configured rule) and the reference tables."""
    from pyspark.sql import functions as F

    from opdi.ingestion.osn_statevectors import StateVectorIngestion
    from opdi.utils.storage import StorageManager

    storage = StorageManager(spark, config)
    raw = state_vectors(name)
    sv_ddl = (
        "event_time bigint, icao24 string, lat double, lon double, velocity double, "
        "heading double, vert_rate double, callsign string, on_ground boolean, "
        "alert boolean, spi boolean, squawk string, baro_altitude double, "
        "geo_altitude double, last_pos_update double, last_contact double, "
        "serials array<int>"
    )
    sdf = spark.createDataFrame(raw, sv_ddl)
    scratch = os.path.join(config.project.warehouse_path, "_ingest_scratch")
    ingest = StateVectorIngestion(
        spark,
        config,
        local_download_path=os.path.join(scratch, "download"),
        log_file_path=os.path.join(scratch, "logs", "01.log"),
    )
    # The same transformations as ``StateVectorIngestion.ingest_from_s3``.
    sdf = ingest._apply_filters(sdf)
    sdf = sdf.withColumn("event_time", F.from_unixtime(F.col("event_time")).cast("timestamp"))
    sdf = sdf.withColumn("event_time_day", F.to_date(F.col("event_time")))
    storage.write_table(
        sdf, "osn_statevectors_v2", mode="overwrite", partition_by=["event_time_day"]
    )

    from opdi.reference.h3_airspaces import AIRSPACE_SCHEMA

    tables = {
        "oa_airports": (
            oa_airports(),
            "id int, ident string, type string, name string, latitude_deg double, "
            "longitude_deg double, elevation_ft int, continent string, "
            "iso_country string, iso_region string, municipality string, "
            "scheduled_service string, gps_code string, iata_code string, "
            "local_code string, home_link string, wikipedia_link string, "
            "keywords string",
        ),
        "oa_runways": (
            oa_runways(),
            "id int, airport_ref int, airport_ident string, length_ft int, "
            "width_ft int, surface string, lighted boolean, closed boolean, "
            "le_ident string, le_latitude_deg double, le_longitude_deg double, "
            "le_elevation_ft int, le_heading_degT double, "
            "le_displaced_threshold_ft int, he_ident string, "
            "he_latitude_deg double, he_longitude_deg double, he_elevation_ft int, "
            "he_heading_degT double, he_displaced_threshold_ft int",
        ),
        "h3_airport_detection_zones": (
            detection_zones(),
            "apt_ident string, apt_hex_id string, distance_from_center int, "
            "apt_latitude_deg double, apt_longitude_deg double, apt_min_c_radius_nm double, "
            "apt_max_c_radius_nm double, apt_type string, apt_scheduled string",
        ),
        "hexaero_airport_layouts": (
            airport_layouts(),
            "hexaero_apt_icao string, hexaero_h3_id string, hexaero_latitude double, "
            "hexaero_longitude double, hexaero_res int, hexaero_aeroway string, "
            "hexaero_length double, hexaero_ref string, hexaero_surface string, "
            "hexaero_width double, hexaero_osm_id bigint, hexaero_type string",
        ),
        "h3_runway_zones": (
            runway_zones(),
            "h3_id string, apt_icao string, strip_id string, le_ident string, "
            "he_ident string, zone string",
        ),
        "osn_aircraft_db": (
            aircraft_db(),
            "icao24 string, registration string, model string, "
            "typecode string, icao_aircraft_class string, "
            "icao_operator string",
        ),
    }
    for name, (pdf, ddl) in tables.items():
        storage.write_table(_spark_frame(spark, pdf, ddl), name, mode="overwrite")
    fir = spark.createDataFrame(
        [
            tuple(r)
            for r in airspaces(raw)[[f.name for f in AIRSPACE_SCHEMA.fields]].itertuples(
                index=False
            )
        ],
        AIRSPACE_SCHEMA,
    )
    fir = fir.withColumn("validity_start", F.col("validity_start").cast("timestamp")).withColumn(
        "validity_end", F.col("validity_end").cast("timestamp")
    )
    storage.write_table(fir, "opdi_h3_airspace_ref", mode="overwrite")


def run(spark, name: str, warehouse: str) -> Dict[str, pd.DataFrame]:
    """Run the golden day under configuration ``name`` and return every output.

    Returns:
        A frame per output table and per published file, with rows in a fixed
        order. Each frame's Spark column types are in ``frame.attrs["types"]``.
    """
    from opdi.output.published import PublishedLayout
    from opdi.periodrun import run_day_step
    from opdi.utils.storage import StorageManager

    os.environ["OPDI_COMMIT"] = "golden"
    config = configuration(name, warehouse)
    write_inputs(spark, config, name)
    logs = os.path.join(warehouse, "_logs")
    for step in STEPS:
        run_day_step(spark, config, step, DAY, {"log_dir": logs})
        spark.catalog.clearCache()

    storage = StorageManager(spark, config)
    frames = {}
    for table in OUTPUT_TABLES:
        frames[table] = _collect(storage.read_table(table))
    layout = PublishedLayout.for_storage(storage)
    paths = {
        "flight_list": layout.flight_list(DAY),
        "flight_events": layout.flight_events(DAY),
        "index": layout.flight_index(DAY),
        "catalogue": layout.catalogue,
    }
    for key, path in paths.items():
        frames[f"published_{key}"] = _collect(spark.read.parquet(path))
    return frames


def _collect(sdf) -> pd.DataFrame:
    """The frame as pandas with its rows sorted on every column."""
    from pyspark.sql import functions as F

    sdf = sdf.drop(*[c for c in sdf.columns if c in UNSTABLE_COLUMNS])
    types = [(f.name, f.dataType.simpleString()) for f in sdf.schema.fields]
    # Sorting on the text form of every column gives one order for any type,
    # including arrays and maps.
    order = [
        F.col(c).cast("string").asc_nulls_first()
        for c, t in types
        if not t.startswith(("map", "array", "struct"))
    ]
    order += [
        F.to_json(F.col(c)).asc_nulls_first()
        for c, t in types
        if t.startswith(("map", "array", "struct"))
    ]
    pdf = sdf.orderBy(*order).toPandas() if types else pd.DataFrame()
    pdf = pdf.reset_index(drop=True)
    pdf.attrs["types"] = types
    return pdf
