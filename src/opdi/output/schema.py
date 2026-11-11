"""The columns of the published OPDI tables.

OPDI publishes two files per day:

* the **flight list**, with one row per flight, and
* the **flight events** file, with one row per event. An event is a moment in a
  flight, such as the take-off or the start of a level segment. Each row also
  carries the measurements taken at that event (for example the distance flown
  so far) and the columns of the flight it belongs to.

This module declares every published column once: its name, its type, its
unit, a description and the working-table column it is built from. The
publishing step, the readers, the database table definitions and the data
dictionary on the website all read this declaration, so they always agree.

Naming rules: names are in lower case with underscores, and a numeric measure
ends with its unit (``_nm`` for nautical miles, ``_s`` for seconds, ``_ft`` for
feet, ``_min`` for minutes, ``_deg`` for degrees). All times are in UTC.
"""

import csv
import hashlib
import io
from dataclasses import dataclass
from typing import Dict, Iterable, Tuple

# RELEASE, the name of the data release, is part of the published path and is
# written in the ``release`` column of every published row.
from opdi.versions import (
    DISTANCE_VERSION,
    HOLDING_VERSION,
    PMS_VERSION,
    RELEASE,
    TIME_VERSION,
)

#: The groups a column can belong to. The data dictionary uses them as
#: section headings.
GROUPS = (
    "identity",
    "aircraft",
    "airports",
    "times",
    "milestones",
    "runway",
    "rings",
    "quality",
    "event",
    "position",
    "measurement",
    "release",
)


@dataclass(frozen=True)
class Column:
    """One published column.

    Attributes:
        name: Column name in the published file.
        dtype: Column type, written as a Spark SQL type (``string``,
            ``double``, ``timestamp``, ...).
        unit: Unit of the values, or an empty string when there is none.
        description: What the column holds, in one or a few sentences.
        source: Where the value comes from: the column of the pipeline's
            working table, the measurement type, or a short expression.
        group: The section of the data dictionary the column belongs to (one
            of :data:`GROUPS`).
    """

    name: str
    dtype: str
    unit: str
    description: str
    source: str
    group: str


_RING_RADII_NM = (40, 50, 60, 100, 110, 120)


def _ring(nm: int, arrival: bool) -> Column:
    if arrival:
        return Column(
            f"c{nm}_arr",
            "timestamp",
            "UTC",
            f"Time when the arriving flight last crossed, inbound, a circle of {nm} "
            "nautical miles (NM) radius around the destination airport. NULL when no "
            "such crossing was detected.",
            f"C{nm}_ARR",
            "rings",
        )
    return Column(
        f"c{nm}_dep",
        "timestamp",
        "UTC",
        f"Time when the departing flight first crossed, outbound, a circle of {nm} "
        "nautical miles (NM) radius around the departure airport. NULL when no "
        "such crossing was detected.",
        f"C{nm}_DEP",
        "rings",
    )


_MOVEMENT_RULE = (
    "the row names an airport, it is not a fragment counted on another row, and "
    "there is evidence that the aircraft flew (a detected runway time, an airport at "
    "the other end, or a crossing of one of the distance circles)"
)

#: The published flight list, in column order.
FLIGHT_LIST: Tuple[Column, ...] = (
    # Identity
    Column(
        "flight_id",
        "string",
        "",
        "Unique identifier of the flight. A flight is one track: the sequence of "
        "position reports of one aircraft that belong together.",
        "ID",
        "identity",
    ),
    Column(
        "icao24",
        "string",
        "",
        "ICAO 24-bit address of the aircraft's transponder, written as six hexadecimal "
        "characters. It normally stays with one aircraft.",
        "ICAO24",
        "identity",
    ),
    Column(
        "flt_id",
        "string",
        "",
        "Call sign broadcast by the aircraft, for example DLH123.",
        "FLT_ID",
        "identity",
    ),
    Column(
        "dof",
        "date",
        "UTC",
        "Date of the flight: the day on which it was first seen. A day's files hold "
        "the flights of that date.",
        "DOF",
        "identity",
    ),
    # Airports
    Column(
        "adep",
        "string",
        "",
        "Departure airport, as its four-letter ICAO code. 'OOA' (out of area) when "
        "the track began at the edge of the covered area, so the flight departed "
        "from somewhere outside it. This occurs only in data built with European "
        "coverage. NULL when no departure airport could be identified, which "
        "includes every overflight.",
        "ADEP",
        "airports",
    ),
    Column(
        "ades",
        "string",
        "",
        "Destination airport, as its four-letter ICAO code. 'OOA' (out of area) when "
        "the track ended at the edge of the covered area, so the flight continued "
        "to somewhere outside it. This occurs only in data built with European "
        "coverage. NULL when no destination airport could be identified, which "
        "includes every overflight.",
        "ADES",
        "airports",
    ),
    Column(
        "adep_p",
        "string",
        "",
        "Other airports near the start of the track that the flight could have left "
        "from, as a comma-separated list of ICAO codes, nearest first.",
        "ADEP_P",
        "airports",
    ),
    Column(
        "ades_p",
        "string",
        "",
        "Other airports near the end of the track that the flight could have gone to, "
        "as a comma-separated list of ICAO codes, nearest first.",
        "ADES_P",
        "airports",
    ),
    Column(
        "adep_source",
        "string",
        "",
        "How the departure airport was decided. 'aerodrome': adep names the airport "
        "near which the track began, found from the position and height of its first "
        "reports. 'runway_event': adep was taken from a take-off detected on one of "
        "that airport's runways, which takes precedence over the first method. "
        "'out_of_area': adep is 'OOA'. 'undetermined': no airport could be "
        "identified and adep is NULL, as for an overflight.",
        "ADEP_SOURCE",
        "airports",
    ),
    Column(
        "ades_source",
        "string",
        "",
        "How the destination airport was decided, with the same four values as "
        "adep_source: 'aerodrome' (found from the last reports of the track), "
        "'runway_event' (taken from a landing detected on one of the airport's "
        "runways), 'out_of_area' (ades is 'OOA') and 'undetermined' (ades is NULL).",
        "ADES_SOURCE",
        "airports",
    ),
    # Aircraft
    Column(
        "registration",
        "string",
        "",
        "Registration of the aircraft (its tail number), from the OpenSky aircraft "
        "database. NULL when the aircraft is not in that database.",
        "REGISTRATION",
        "aircraft",
    ),
    Column(
        "model",
        "string",
        "",
        "Aircraft model, from the OpenSky aircraft database.",
        "MODEL",
        "aircraft",
    ),
    Column(
        "typecode",
        "string",
        "",
        "ICAO aircraft type designator, for example A320, from the OpenSky aircraft " "database.",
        "TYPECODE",
        "aircraft",
    ),
    Column(
        "icao_aircraft_class",
        "string",
        "",
        "ICAO aircraft description, for example L2J for a landplane with two jet "
        "engines, from the OpenSky aircraft database.",
        "ICAO_AIRCRAFT_CLASS",
        "aircraft",
    ),
    Column(
        "icao_operator",
        "string",
        "",
        "Three-letter ICAO code of the aircraft operator, from the OpenSky aircraft " "database.",
        "ICAO_OPERATOR",
        "aircraft",
    ),
    # Times
    Column(
        "first_seen",
        "timestamp",
        "UTC",
        "Time of the first position report of the flight received by the OpenSky " "Network.",
        "FIRST_SEEN",
        "times",
    ),
    Column(
        "last_seen",
        "timestamp",
        "UTC",
        "Time of the last position report of the flight received by the OpenSky " "Network.",
        "LAST_SEEN",
        "times",
    ),
    # Milestones
    Column(
        "atot",
        "timestamp",
        "UTC",
        "Actual take-off time: when the aircraft left the runway at the departure "
        "airport. Taken from the flight's earliest ATOT event at that airport. NULL "
        "when no take-off was detected.",
        "ATOT",
        "milestones",
    ),
    Column(
        "aldt",
        "timestamp",
        "UTC",
        "Actual landing time: when the aircraft touched down at the destination "
        "airport. Taken from the flight's latest ALDT event at that airport. NULL "
        "when no landing was detected.",
        "ALDT",
        "milestones",
    ),
    Column(
        "aobt",
        "timestamp",
        "UTC",
        "Actual off-block time: when the aircraft started to move away from its "
        "parking position. Taken from the flight's earliest AOBT event. NULL when it "
        "was not detected, which is common because it needs reception on the ground.",
        "AOBT",
        "milestones",
    ),
    Column(
        "aibt",
        "timestamp",
        "UTC",
        "Actual in-block time: when the aircraft stopped at its parking position after "
        "landing. Taken from the flight's latest AIBT event. NULL when it was not "
        "detected, which is common because it needs reception on the ground.",
        "AIBT",
        "milestones",
    ),
    # Runway
    Column(
        "rwy_dep",
        "string",
        "",
        "Runway used for take-off, for example 25R. Taken from the same event as atot.",
        "RWY_DEP",
        "runway",
    ),
    Column(
        "rwy_dep_bearing_deg",
        "double",
        "degrees",
        "True bearing of the centre line of the take-off runway, in degrees clockwise "
        "from north. It describes the runway, not the heading of the aircraft.",
        "RWY_DEP_BEARING_DEG",
        "runway",
    ),
    Column(
        "rwy_arr",
        "string",
        "",
        "Runway used for landing, for example 07L. Taken from the same event as aldt.",
        "RWY_ARR",
        "runway",
    ),
    Column(
        "rwy_arr_bearing_deg",
        "double",
        "degrees",
        "True bearing of the centre line of the landing runway, in degrees clockwise "
        "from north. It describes the runway, not the heading of the aircraft.",
        "RWY_ARR_BEARING_DEG",
        "runway",
    ),
    Column(
        "stnd_dep",
        "string",
        "",
        "Parking position (stand) the flight left from, with the name it has in "
        "OpenStreetMap. NULL when it was not detected.",
        "STND_DEP",
        "milestones",
    ),
    Column(
        "stnd_arr",
        "string",
        "",
        "Parking position (stand) the flight arrived at, with the name it has in "
        "OpenStreetMap. NULL when it was not detected.",
        "STND_ARR",
        "milestones",
    ),
    # Distance circles
    *[_ring(nm, arrival=True) for nm in _RING_RADII_NM],
    *[_ring(nm, arrival=False) for nm in _RING_RADII_NM],
    # Quality
    Column(
        "track_duration_min",
        "double",
        "min",
        "Time between first_seen and last_seen, in minutes. Very short tracks are often "
        "pieces of a flight seen on the ground rather than whole flights. No row is "
        "removed on this value, so users can choose their own limit.",
        "TRACK_DURATION_MIN",
        "quality",
    ),
    Column(
        "superseded_dep",
        "boolean",
        "",
        "True when this row is a piece of a departure that is already counted on "
        "another row: it has no take-off, and the same aircraft departs again from the "
        "same airport within 3 hours (10,800 s). False otherwise.",
        "SUPERSEDED_DEP",
        "quality",
    ),
    Column(
        "superseded_arr",
        "boolean",
        "",
        "True when this row is a piece of an arrival that is already counted on "
        "another row: it has no landing, and the same aircraft arrived at the same "
        "airport within the 3 hours (10,800 s) before. False otherwise.",
        "SUPERSEDED_ARR",
        "quality",
    ),
    Column(
        "movement_dep",
        "boolean",
        "",
        f"True when this row counts as a departure: {_MOVEMENT_RULE}. Use this column "
        "to count departures.",
        "MOVEMENT_DEP",
        "quality",
    ),
    Column(
        "movement_arr",
        "boolean",
        "",
        f"True when this row counts as an arrival: {_MOVEMENT_RULE}. Use this column "
        "to count arrivals.",
        "MOVEMENT_ARR",
        "quality",
    ),
    # Release
    Column(
        "version",
        "string",
        "",
        "Version of the method that built this flight row, for example v5.0.0. It "
        "changes only when that method changes.",
        "VERSION",
        "release",
    ),
    Column(
        "release",
        "string",
        "",
        f"Name of the data release the row belongs to ({RELEASE}). It is the same on "
        "every row of a release.",
        "RELEASE",
        "release",
    ),
)

#: The columns describing the event itself, first in the flight events file.
EVENT_COLUMNS: Tuple[Column, ...] = (
    Column("event_id", "string", "", "Unique identifier of the event.", "id", "event"),
    Column(
        "flight_id",
        "string",
        "",
        "Identifier of the flight the event belongs to. It matches flight_id in the "
        "flight list.",
        "flight_id",
        "event",
    ),
    Column(
        "type",
        "string",
        "",
        "Event type, for example ATOT, level-start or xing-40nm. The data dictionary "
        "lists every type.",
        "type",
        "event",
    ),
    Column(
        "event_time",
        "timestamp",
        "UTC",
        "Time at which the event happened.",
        "event_time",
        "event",
    ),
    Column(
        "longitude",
        "double",
        "degrees",
        "Longitude of the aircraft at the event, in degrees (WGS 84, positive east).",
        "longitude",
        "position",
    ),
    Column(
        "latitude",
        "double",
        "degrees",
        "Latitude of the aircraft at the event, in degrees (WGS 84, positive north).",
        "latitude",
        "position",
    ),
    Column(
        "altitude_ft",
        "double",
        "ft",
        "Altitude of the aircraft at the event, in feet, from the barometric altitude "
        "the aircraft reports. For a flight-level crossing it is the flight level "
        "times 100. NULL for events that have no altitude.",
        "altitude",
        "position",
    ),
    Column(
        "h3_res_5",
        "string",
        "",
        "Cell of the H3 hexagonal grid at resolution 5 that contains the event "
        "position. Cells cover about 250 km2 each, so the column can be used to "
        "select events in a region quickly.",
        "h3(latitude, longitude, 5)",
        "position",
    ),
    Column(
        "source",
        "string",
        "",
        "Source of the position reports, OSN for the OpenSky Network.",
        "source",
        "event",
    ),
    Column(
        "version",
        "string",
        "",
        "Version of the method that produced this event type, for example "
        "events_v0.3.0. Different event types can have different versions.",
        "version",
        "event",
    ),
    Column(
        "info",
        "string",
        "",
        "Extra details about the event, as JSON text, for example the runway or the "
        "airport. The keys depend on the event type and are listed in the data "
        "dictionary.",
        "info",
        "event",
    ),
)

_ONLY_ON = "It is set only on {event} events and is NULL on every other event."
_PMS = (
    "A point merge system is an arrival procedure in which aircraft fly along an arc, "
    "called a leg, at a fixed distance from a merge point until they are sent "
    "directly to it."
)

#: The measurements, one column each, in the flight events file.
MEASUREMENT_COLUMNS: Tuple[Column, ...] = (
    Column(
        "distance_flown_nm",
        "double",
        "NM",
        "Distance flown from the start of the track to this event, along the received "
        "positions, in nautical miles (NM, 1 NM = 1,852 m).",
        "Distance flown (NM)",
        "measurement",
    ),
    Column(
        "time_passed_s",
        "double",
        "s",
        "Time from the first position of the track to this event, in seconds.",
        "Time Passed (s)",
        "measurement",
    ),
    Column(
        "holding_duration_s",
        "double",
        "s",
        "Time spent in a holding pattern, in seconds. A holding pattern is a loop "
        "flown while the aircraft waits for permission to continue. "
        + _ONLY_ON.format(event="holding-end"),
        "Holding duration (s)",
        "measurement",
    ),
    Column(
        "holding_distance_nm",
        "double",
        "NM",
        "Distance flown in the holding pattern, in nautical miles (NM). "
        + _ONLY_ON.format(event="holding-end"),
        "Holding distance (NM)",
        "measurement",
    ),
    Column(
        "holding_circuits",
        "double",
        "count",
        "Number of loops flown in the holding pattern: the total turn divided by 360 "
        "degrees, to one decimal. " + _ONLY_ON.format(event="holding-end"),
        "Holding circuits",
        "measurement",
    ),
    Column(
        "pms_leg_time_s",
        "double",
        "s",
        "Time spent on a point merge leg, in seconds. "
        + _PMS
        + " "
        + _ONLY_ON.format(event="pms-leg-exit"),
        "Point merge leg time (s)",
        "measurement",
    ),
    Column(
        "pms_leg_distance_nm",
        "double",
        "NM",
        "Distance flown along the point merge leg, in nautical miles (NM). "
        + _ONLY_ON.format(event="pms-leg-exit"),
        "Point merge leg distance (NM)",
        "measurement",
    ),
    Column(
        "pms_direct_to_distance_nm",
        "double",
        "NM",
        "Distance from the aircraft to the merge point when it left the leg, in "
        "nautical miles (NM). " + _ONLY_ON.format(event="pms-leg-exit"),
        "Point merge direct-to distance (NM)",
        "measurement",
    ),
)

_FLIGHT_VERSION = Column(
    "flight_version",
    "string",
    "",
    "Version of the method that built the flight's row in the flight list (the version "
    "column of the flight list).",
    "VERSION",
    "release",
)

#: The published flight events file, in column order: the event, its
#: measurements, then the columns of its flight.
FLIGHT_EVENTS: Tuple[Column, ...] = (
    EVENT_COLUMNS
    + MEASUREMENT_COLUMNS
    + tuple(c for c in FLIGHT_LIST if c.name not in ("flight_id", "version", "release"))
    + (_FLIGHT_VERSION, FLIGHT_LIST[-1])
)

_BY_NAME = {c.name: c for c in FLIGHT_LIST}

#: The monthly flight index: one row per published flight of the month, with
#: the file that holds it.
FLIGHT_INDEX: Tuple[Column, ...] = tuple(
    _BY_NAME[n]
    for n in ("flight_id", "icao24", "flt_id", "adep", "ades", "first_seen", "last_seen", "dof")
) + (
    Column(
        "file",
        "string",
        "",
        "Path of the flight list file that holds the flight, relative to the published "
        "folder, for example flight_list/flight_list_20260614.parquet.",
        "file name",
        "identity",
    ),
)

#: The dataset catalogue: one row per published day.
CATALOGUE: Tuple[Column, ...] = (
    Column("day", "date", "UTC", "Day the row describes.", "day", "release"),
    Column(
        "flights",
        "bigint",
        "count",
        "Number of rows in the day's flight list file.",
        "count",
        "release",
    ),
    Column(
        "events",
        "bigint",
        "count",
        "Number of rows in the day's flight events file.",
        "count",
        "release",
    ),
    Column(
        "first_event",
        "timestamp",
        "UTC",
        "Time of the earliest event in the day's flight events file. NULL when the file "
        "is empty.",
        "min(event_time)",
        "release",
    ),
    Column(
        "last_event",
        "timestamp",
        "UTC",
        "Time of the latest event in the day's flight events file. It can fall on the "
        "next day, because a flight keeps all its events in the file of the day it "
        "started. NULL when the file is empty.",
        "max(event_time)",
        "release",
    ),
    Column(
        "flight_list_bytes",
        "bigint",
        "bytes",
        "Size of the day's flight list file.",
        "file size",
        "release",
    ),
    Column(
        "flight_events_bytes",
        "bigint",
        "bytes",
        "Size of the day's flight events file.",
        "file size",
        "release",
    ),
    Column(
        "release",
        "string",
        "",
        f"Name of the data release ({RELEASE}).",
        "RELEASE",
        "release",
    ),
    Column(
        "flight_list_version",
        "string",
        "",
        "Version of the flight list method used for the day. When several were used, "
        "they are listed separated by commas.",
        "VERSION",
        "release",
    ),
    Column(
        "event_versions",
        "array<string>",
        "",
        "Versions of the event methods found in the day's flight events file.",
        "version",
        "release",
    ),
    Column(
        "commit",
        "string",
        "",
        "Identifier of the version of the OPDI software (the git commit) that "
        "published the day.",
        "commit",
        "release",
    ),
    Column(
        "published_at",
        "timestamp",
        "UTC",
        "Time when the day was published.",
        "current time",
        "release",
    ),
)


def _extra_suffix(measurement_type: str) -> str:
    # The same rule as the event step: "_m" plus six hexadecimal characters of
    # the SHA-256 hash of the measurement type.
    return "_m" + hashlib.sha256(measurement_type.encode("utf-8")).hexdigest()[:6]


_DISTANCE, _TIME = "Distance flown (NM)", "Time Passed (s)"

#: Measurement type -> the suffix that turns an event id into the id of that
#: measurement in the separate measurements table.
MEASUREMENT_SUFFIX: Dict[str, str] = {
    c.source: {_DISTANCE: "_d", _TIME: "_t"}.get(c.source) or _extra_suffix(c.source)
    for c in MEASUREMENT_COLUMNS
}

#: Measurement type -> version of the method that computes it.
MEASUREMENT_VERSION: Dict[str, str] = {
    _DISTANCE: DISTANCE_VERSION,
    _TIME: TIME_VERSION,
    "Holding duration (s)": HOLDING_VERSION,
    "Holding distance (NM)": HOLDING_VERSION,
    "Holding circuits": HOLDING_VERSION,
    "Point merge leg time (s)": PMS_VERSION,
    "Point merge leg distance (NM)": PMS_VERSION,
    "Point merge direct-to distance (NM)": PMS_VERSION,
}


#: The classic events table of release v0.0.2, which kept events and
#: measurements in separate files: published column -> classic column, in the
#: classic order.
CLASSIC_EVENT_COLUMNS: Dict[str, str] = {
    "event_id": "id",
    "flight_id": "flight_id",
    "type": "type",
    "event_time": "event_time",
    "longitude": "longitude",
    "latitude": "latitude",
    "altitude_ft": "altitude",
    "source": "source",
    "version": "version",
    "info": "info",
}

#: The columns of the classic measurements table, one row per measured value.
CLASSIC_MEASUREMENT_COLUMNS: Tuple[str, ...] = ("id", "event_id", "type", "value", "version")

#: The classic flight list of release v0.0.2: published column -> classic
#: column name in upper case, in the classic order.
CLASSIC_FLIGHT_LIST: Dict[str, str] = {
    name: name.upper().replace("FLIGHT_ID", "ID")
    for name in (
        "flight_id",
        "icao24",
        "flt_id",
        "dof",
        "adep",
        "ades",
        "adep_p",
        "ades_p",
        "registration",
        "model",
        "typecode",
        "icao_aircraft_class",
        "icao_operator",
        "first_seen",
        "last_seen",
        "version",
    )
}


def as_markdown(columns: Iterable[Column]) -> str:
    """The columns as a Markdown table with name, type, unit and description.

    Args:
        columns: The columns to list, for example :data:`FLIGHT_LIST`.

    Returns:
        The table: a header line, a separator line and one line per column.
    """
    lines = ["| name | type | unit | description |", "|---|---|---|---|"]
    for c in columns:
        text = c.description.replace("|", "\\|")
        lines.append(f"| `{c.name}` | {c.dtype} | {c.unit} | {text} |")
    return "\n".join(lines) + "\n"


def as_csv(columns: Iterable[Column]) -> str:
    """The columns as CSV text with name, type, unit and description.

    Args:
        columns: The columns to list, for example :data:`FLIGHT_EVENTS`.

    Returns:
        The CSV text, with a header row.
    """
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["name", "type", "unit", "description"])
    for c in columns:
        writer.writerow([c.name, c.dtype, c.unit, c.description])
    return out.getvalue()
