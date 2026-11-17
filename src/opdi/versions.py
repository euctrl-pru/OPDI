"""Every version string the pipeline writes, in one place.

OPDI marks its data at two levels. The **release** (``RELEASE``) names a
published data set as a whole and is written on every published row. Each
**method** also writes its own version beside its results (the ``version``
column of the flight list and of every event and measurement), so that a
later release that changes one method can be told apart from one that does
not. A published version string is never reused for different results: a
change to a method gets a new string.

The strings are defined here and imported by the modules that write them
(:mod:`opdi.output.schema`, :mod:`opdi.pipeline.flights`,
:mod:`opdi.pipeline.events` and the event family modules), which also make
them available under their former names. This module imports nothing, so it
can be read without Spark.
"""

#: The release of the published files (folder name and ``release`` column).
RELEASE = "v2.0.0"

# --- the flight list (step 03, column ``version``) --------------------------

#: The flight list of release v2.0.0. Under this version candidate airports are
#: ranked by great-circle distance; departures come from the ``endpoint``
#: method and arrivals from ``trend``; the thresholds are those of
#: :class:`~opdi.config.DetectionConfig`; ``trend`` measures its ceiling above
#: the airport's elevation and can give the out-of-area code; the flight list
#: is built from the cleaned tracks; and ``FLT_ID`` is the call sign the track
#: broadcast most often.
FLIGHT_LIST_VERSION = "v6.0.0"
#: The flight list built with the v0.0.2 settings (the ``trend`` method).
LEGACY_TREND_VERSION = "v2.0.0"
#: The ``endpoint`` method run with the v0.0.2 settings.
LEGACY_ENDPOINT_VERSION = "v3.0.0"

# --- events (step 04, column ``version``) ------------------------------------

#: The events of release v2.0.0 (the default of ``EventConfig.events_version``),
#: with the corrections of the spot checks of 2026-06-14.
EVENTS_VERSION = "events_v0.4.0"
#: The events of release v0.0.2 (``EventConfig.legacy()``).
LEGACY_EVENTS_VERSION = "events_v0.0.2"
#: Holding patterns: ``holding-start``, ``holding-end`` and their measurements.
HOLDING_VERSION = "holding_v0.2.0"
#: Holding patterns without the settings that change them
#: (:data:`opdi.pipeline.holding.HOLDING_SETTINGS`).
LEGACY_HOLDING_VERSION = "holding_v0.1.0"
#: Point merge: ``pms-leg-entry``, ``pms-leg-exit``, ``pms-merge-point`` and
#: their measurements.
PMS_VERSION = "pms_v0.2.0"
#: Point merge without the settings that change it
#: (:data:`opdi.pipeline.point_merge.PMS_SETTINGS`).
LEGACY_PMS_VERSION = "pms_v0.1.0"
#: FIR crossings: ``xing-fir``.
FIR_VERSION = "fir_v0.2.0"
#: FIR crossings without the settings that change them
#: (:data:`opdi.pipeline.fir.FIR_SETTINGS`).
LEGACY_FIR_VERSION = "fir_v0.1.0"

# --- measurements (step 04, column ``version`` of the measurements) ---------

#: Distance flown, measured from the last known position; a step that implies
#: more than a speed limit is skipped, and a report without a position gets
#: the distance between its positioned neighbours
#: (``EventConfig.distance_max_implied_speed_kt``,
#: ``EventConfig.distance_between_positions``).
DISTANCE_VERSION = "distance_v0.0.4"
#: Distance flown, measured from the last known position, without those two
#: settings.
LAST_KNOWN_DISTANCE_VERSION = "distance_v0.0.3"
#: Distance flown as in release v0.0.2, where a missing position adds no
#: distance on either side of it.
LEGACY_DISTANCE_VERSION = "distance_v0.0.2"
#: Time passed since the track's ``first_seen`` event
#: (``EventConfig.time_passed_from_first_seen``).
TIME_VERSION = "time_v0.0.2"
#: Time passed since the start of the track.
LEGACY_TIME_VERSION = "time_v0.0.1"
