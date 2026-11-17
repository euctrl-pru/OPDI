"""Step 03: the flight list, with departure and destination airports.

Reads the tracks (the cleaned ``osn_tracks_clean`` by default) and writes
``opdi_flight_list``: one row per track, with its departure airport (ADEP)
and destination airport (ADES), the other candidate airports, the first and
last time it was seen, and the aircraft's registration, type and operator
from the OpenSky aircraft database.

The rows are written in two passes into the same day folder: first the
tracks whose departure or destination could be placed near an airport, then
the remaining tracks of at least 5 minutes as overflights. The methods and
thresholds are those of :class:`opdi.config.DetectionConfig`.
See https://www.opdi.aero/content/methods/airports.html.
"""

import functools
import os
from datetime import date, datetime
from typing import List, Optional

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    atan2,
    broadcast,
    col,
    collect_list,
    concat_ws,
    cos,
    lit,
    radians,
    row_number,
    sin,
    sqrt,
    to_date,
    unix_timestamp,
    when,
)
from pyspark.sql.functions import (
    max as f_max,
)
from pyspark.sql.functions import (
    min as f_min,
)
from pyspark.sql.window import Window

from opdi.config import OPDIConfig
from opdi.coverage import EUROPE_BBOX
from opdi.utils.datetime_helpers import (
    generate_months,
)
from opdi.utils.geometry import (
    EARTH_R_NM,  # noqa: F401 - importable from here too
    angle_between,
    bearing_deg,
    haversine_nm,
)
from opdi.utils.storage import StorageManager

# FLIGHT_LIST_VERSION is written on every flight-list row of release v2.0.0;
# LEGACY_TREND_VERSION and LEGACY_ENDPOINT_VERSION by the v0.0.2 settings.
from opdi.versions import FLIGHT_LIST_VERSION, LEGACY_ENDPOINT_VERSION, LEGACY_TREND_VERSION

#: Default box for :func:`at_border`, the European coverage. The pipeline
#: passes ``config.coverage.bbox`` explicitly (see
#: :meth:`FlightListProcessor.build_endpoint_candidates`).
BBOX = EUROPE_BBOX

#: How close to the edge of the coverage box, in NM, the end of a track must be
#: to read as "left the covered area" rather than "stopped being received".
#: Reception fades before the edge, so the margin is wide.
BORDER_MARGIN_NM = 30.0

#: Code given as ADEP or ADES when the flight entered or left the covered area
#: in the air (European coverage only): the airport lies outside the area and
#: cannot be named from the data.
OOA = "OOA"

NM_PER_DEG = 60.0

#: Metres to feet, the factor used throughout the pipeline.
FT_PER_M = 3.28084


def height_above_field(baro_altitude_m: Column, elevation_ft: Column) -> Column:
    """Height in feet above an airport's elevation.

    Args:
        baro_altitude_m: Barometric altitude, in metres.
        elevation_ft: Elevation of the airport, in feet. A NULL elevation
            counts as 0 ft, so the height is then the pressure altitude: an
            airport whose elevation is unknown is still detected, and a
            report that matches no airport passes through unchanged.
    """
    return baro_altitude_m * FT_PER_M - F.coalesce(elevation_ft, F.lit(0.0))


def trend_altitude_cut(detection) -> Column:
    """The altitude condition of the ``trend`` method, for the configured datum.

    Uses ``baro_altitude`` and ``apt_elevation_ft`` when the ceiling is a
    height above the airport, ``flight_level`` when it is a flight level.
    """
    if getattr(detection, "trend_max_datum", "msl") == "field":
        return height_above_field(col("baro_altitude"), col("apt_elevation_ft")) <= lit(
            float(detection.trend_max_height_ft)
        )

    # Kept as written in release v0.0.2: `flight_level` is a whole number,
    # so FL60 admits everything below 6,100 ft, and a comparison in feet
    # would move the limit.
    return col("flight_level") <= lit(int(detection.trend_max_fl))


def trend_prefilter_ceiling_ft(detection, max_field_elevation_ft: float) -> float:
    """Ceiling in feet for a quick filter ahead of the join with the airports.

    The exact height test needs the airport and so runs after the join. A
    report can pass it at some airport only if it is below the ceiling plus
    the elevation of the highest airport in the reference table, so filtering
    at that altitude first removes nothing the exact test would keep, and
    makes the join much smaller.
    """
    if getattr(detection, "trend_max_datum", "msl") == "field":
        return float(detection.trend_max_height_ft) + float(max_field_elevation_ft)
    # The flight level is a whole number again: FL60 reaches 6,100 ft.
    return (float(detection.trend_max_fl) + 1.0) * 100.0


#: How far either side of a track's end, in seconds, its course is measured
#: (seven minutes).
COURSE_WINDOW_S = 7 * 60

#: With fewer reports, or a shorter time between them, a bearing is position
#: noise rather than a course, and the course is left NULL: the direction
#: tie-break then does not apply and distance decides alone.
COURSE_MIN_SAMPLES = 5
COURSE_MIN_BASELINE_S = 60


def at_border(lat, lon, margin_nm: float = BORDER_MARGIN_NM, bbox=BBOX):
    """True where a position lies within ``margin_nm`` of the coverage edge.

    ``bbox=None`` means worldwide and gives ``False`` everywhere: there is no
    edge (a box of +/-180 by +/-90 would put every flight crossing the 180th
    meridian at the edge). The longitude margin is divided by the cosine of
    the latitude, because a degree of longitude is shorter towards the poles.
    """
    if bbox is None:
        return lit(False)
    min_lon, min_lat, max_lon, max_lat = bbox
    dlat = margin_nm / NM_PER_DEG
    dlon = dlat / F.greatest(cos(radians(lat)), lit(0.1))
    return (
        (lat <= lit(min_lat) + dlat)
        | (lat >= lit(max_lat) - dlat)
        | (lon <= lit(min_lon) + dlon)
        | (lon >= lit(max_lon) - dlon)
    )


def dominant_flight_id(df: DataFrame, track_col: str = "track_id", trim: bool = False) -> DataFrame:
    """The call sign each track broadcast most often, one row per track.

    Blank call signs are not counted. Ties are broken on the call sign
    itself, so the result does not depend on how Spark split the data. A
    track that never broadcast a call sign has no row here;
    :func:`resolve_flight_id` gives it ``""``.

    Args:
        df: Reports with ``flight_id`` (the call sign) and ``track_col``.
        track_col: Column holding the track id.
        trim: Remove the spaces around each call sign before counting, so
            that ``'ABC1    '`` and ``'ABC1'`` are one call sign
            (:attr:`DetectionConfig.call_sign_trimmed`).

    Returns:
        One row per track: ``track_col`` and ``_dominant_flight_id``.
    """
    blank = F.trim(F.coalesce(F.col("flight_id"), lit(""))) == ""
    labelled = df.filter(~blank)
    if trim:
        labelled = labelled.withColumn("flight_id", F.trim(F.col("flight_id")))
    counts = labelled.groupBy(track_col, "flight_id").agg(F.count(lit(1)).alias("_n"))
    rank = Window.partitionBy(track_col).orderBy(F.col("_n").desc(), F.col("flight_id").asc())
    return (
        counts.withColumn("_r", row_number().over(rank))
        .filter(F.col("_r") == 1)
        .select(track_col, F.col("flight_id").alias("_dominant_flight_id"))
    )


def resolve_flight_id(sv: DataFrame, track_col: str = "track_id", trim: bool = False) -> DataFrame:
    """Give every report of a track the one call sign the track flew.

    This module groups and joins on ``flight_id`` (the call sign). That is
    safe only if every track carries one value, which the v0.0.2 track rule
    guarantees because the call sign is part of its group key. The default
    rule groups by aircraft address alone, so a track can carry several call
    signs, and would otherwise become several flight-list rows.

    The call sign is resolved on all reports of the track, before any step
    keeps only a few of them, which is why it is applied where the track
    table is read (:meth:`FlightListProcessor._get_data_within_timeframe`).
    A track that never broadcast a call sign keeps ``""``: an unlabelled
    flight, not a missing one.

    Args:
        sv: Reports with ``flight_id`` and ``track_col``.
        track_col: Column holding the track id.
        trim: Give the call sign without the spaces around it (see
            :func:`dominant_flight_id`).

    Returns:
        The same reports, with ``flight_id`` replaced by the track's call sign.
    """
    labels = dominant_flight_id(sv, track_col, trim=trim)
    return (
        sv.join(labels, on=track_col, how="left")
        .withColumn(
            "flight_id",
            F.coalesce(F.col("_dominant_flight_id"), lit("")),
        )
        .drop("_dominant_flight_id")
    )


#: Columns of a report that carry a measured value besides the position. The
#: cleaning of step 02a blanks a value that was repeated rather than measured
#: again, so a report with none of them left carries nothing new.
MEASURED_VALUE_COLUMNS = ("baro_altitude", "geo_altitude", "velocity", "heading", "vert_rate")


def seen_rank(df: DataFrame) -> Column:
    """How a report counts for ``first_seen`` and ``last_seen``.

    0 for a report with a measured position (``lat`` and ``lon`` not blank),
    1 for a report with another measured value (one of
    :data:`MEASURED_VALUE_COLUMNS` present in ``df``), 2 for a report with
    neither. The first (last) report of the lowest rank a track has is its
    first (last) seen (:attr:`EventConfig.seen_from_measured_reports`).
    """
    others = [col(c).isNotNull() for c in MEASURED_VALUE_COLUMNS if c in df.columns]
    any_other = functools.reduce(lambda a, b: a | b, others) if others else lit(False)
    return (
        when(col("lat").isNotNull() & col("lon").isNotNull(), lit(0))
        .when(any_other, lit(1))
        .otherwise(lit(2))
    )


def wide_search_roles(detection) -> tuple:
    """The track ends whose 110 NM candidate airports step 04 searches too.

    With :attr:`DetectionConfig.alternatives_within_endpoint_radius`, the
    lists ``ADEP_P`` / ``ADES_P`` of the ``endpoint`` method hold only the
    airports within the detection radius. The runway and layout events are
    still looked for at every airport within 110 NM, as before, so step 04
    reads them from ``opdi_endpoint_candidates`` for the ends named by the
    ``endpoint`` method. Empty when the setting is off.
    """
    if detection is None or not getattr(detection, "alternatives_within_endpoint_radius", False):
        return ()
    return tuple(
        role
        for role, mode in (("adep", detection.adep_mode), ("ades", detection.ades_mode))
        if mode == "endpoint"
    )


def with_endpoint_candidates(flights: DataFrame, storage, roles) -> DataFrame:
    """Add to ``apt`` the candidate airports of the given track ends.

    Args:
        flights: ``id`` and ``apt`` (array of airport codes).
        storage: Storage that holds ``opdi_endpoint_candidates``.
        roles: Ends of the track (``"adep"``, ``"ades"``) whose candidates
            are added. Empty, or without the table, ``flights`` is returned
            unchanged.

    Returns:
        ``id`` and ``apt``, the array holding each airport once.
    """
    if not roles or not storage.table_exists("opdi_endpoint_candidates"):
        return flights
    cand = (
        storage.read_table("opdi_endpoint_candidates")
        .filter(col("role").isin(*roles))
        .groupBy(col("track_id").alias("_cand_id"))
        .agg(F.collect_set("apt_ident").alias("_cand_apt"))
    )
    return (
        flights.join(cand, flights["id"] == cand["_cand_id"], "left")
        .withColumn(
            "apt",
            F.array_distinct(
                F.concat(col("apt"), F.coalesce(col("_cand_apt"), F.array().cast("array<string>")))
            ),
        )
        .select("id", "apt")
    )


def _releases_zones(method):
    """Let go of the zones kept on the executors when the outermost call returns.

    :meth:`FlightListProcessor._load_airports_hex` keeps the zone table on
    the executors for the rest of a step. The public methods that can read
    it carry this decorator: the first one entered lets the zones go when it
    returns or fails, and the calls nested inside it keep them.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        depth = getattr(self, "_zone_users", 0)
        self._zone_users = depth + 1
        try:
            return method(self, *args, **kwargs)
        finally:
            self._zone_users = depth
            if depth == 0:
                zones = getattr(self, "_zones", None)
                if zones is not None:
                    zones.unpersist()
                    self._zones = None

    return wrapper


class FlightListProcessor:
    """Step 03: build the flight list from the tracks.

    The flight list is built in two passes:

    * **Departures and arrivals** (:meth:`process_dai`): the departure and
      destination airports of every track that can be placed near an
      airport, by the methods of :class:`~opdi.config.DetectionConfig`.
    * **Overflights** (:meth:`process_overflights`): every other track of at
      least 5 minutes, with no airport.

    Each row gets the aircraft's registration, model, type code and operator
    from the OpenSky aircraft database.

    Args:
        spark: Active Spark session.
        config: OPDI configuration.
        log_dir: Folder of the progress logs.

    Example:
        >>> processor = FlightListProcessor(spark, config)
        >>> processor.process_day(date(2024, 6, 12))
    """

    # The v0.0.2 values, as in DetectionConfig.legacy(); the pipeline reads
    # config.detection instead.
    MAX_FL = 40  # highest flight level matched to the airport zones
    # Radius, in NM, within which reports are matched to an airport zone.
    DETECTION_RADIUS_NM = 30

    #: The algorithms that can name an aerodrome (``adep_mode``, ``ades_mode``).
    DETECTION_MODES = ("trend", "endpoint")

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        log_dir: str = "OPDI_live/logs",
        tracks_table: Optional[str] = None,
    ):
        """Prepare the processor.

        Args:
            spark: Active Spark session.
            config: OPDI configuration.
            log_dir: Folder of the progress logs.
            tracks_table: Track table to read instead of the configured one.
        """
        self.spark = spark
        self.config = config
        # Track table to read instead of the configured one. The pipeline does
        # not pass it; a study can run this step over its own copy of the
        # tracks.
        self._tracks_table_override = tracks_table
        self.storage = StorageManager(spark, config)
        self.project = config.project.project_name
        self.resolution = config.h3.airport_detection_resolution
        # A configuration object without `detection` gets the v0.0.2 settings.
        self.detection = getattr(config, "detection", None)
        if self.detection is None:
            from opdi.config import DetectionConfig

            self.detection = DetectionConfig.legacy()
        # Highest elevation among the airports that can match, computed once on
        # first use; it sets the width of the trend pre-filter.
        self._max_elev_ft = None
        self.log_dir = log_dir

        self._dai_log = os.path.join(log_dir, "03_osn-flight_table-etl-log-v2.parquet")
        # Progress log of the endpoint candidate table, kept like the others.
        self._endpoint_log = os.path.join(log_dir, "03_osn-endpoint_candidates-etl-log.parquet")
        self._overflight_log = os.path.join(
            log_dir, "03_osn-flight_table-overflights-etl-log-v2.parquet"
        )

        os.makedirs(log_dir, exist_ok=True)

    @property
    def tracks_table(self) -> str:
        """The track table this step reads: cleaned (step 02a) or not (step 02).

        Decided in one place, so both methods read the same table. When
        cleaning is off, the uncleaned table is read, as the cleaned one is
        not written.
        """
        if getattr(self, "_tracks_table_override", None):
            return self._tracks_table_override
        cleaning = getattr(self.config, "cleaning", None)
        if cleaning is None:
            return "osn_tracks"
        if getattr(cleaning, "enabled", False) and getattr(cleaning, "feeds_flight_list", False):
            return "osn_tracks_clean"
        return "osn_tracks"

    def _seen_from_measured_reports(self) -> bool:
        """Whether first and last seen come from measured reports.

        Read from :attr:`EventConfig.seen_from_measured_reports`, so that the
        flight list and the events use one definition.
        """
        events = getattr(getattr(self, "config", None), "events", None)
        return bool(getattr(events, "seen_from_measured_reports", False))

    def _version_for(self, path: str) -> str:
        """The ``version`` written on a row produced by method ``path``.

        A run with the settings of release v0.0.2 (thresholds, ranking,
        uncleaned tracks) writes the version that release wrote, so a day
        processed again reproduces its version column as well. Any other run
        writes :data:`FLIGHT_LIST_VERSION`.
        """
        from opdi.config import DetectionConfig

        if self.detection == DetectionConfig.legacy() and self.tracks_table == "osn_tracks":
            return LEGACY_TREND_VERSION if path == "trend" else LEGACY_ENDPOINT_VERSION
        return FLIGHT_LIST_VERSION

    # ------------------------------------------------------------------
    # Progress tracking
    # ------------------------------------------------------------------

    def _load_processed_months(self, log_path: str) -> List[date]:
        """The months the log at ``log_path`` records as processed."""
        if os.path.isfile(log_path):
            return pd.read_parquet(log_path).months.to_list()
        return []

    def _mark_month_processed(self, month: date, log_path: str) -> None:
        """Record ``month`` as processed in the log at ``log_path``."""
        processed = self._load_processed_months(log_path)
        if month not in processed:
            processed.append(month)
            pd.DataFrame({"months": processed}).to_parquet(log_path)

    # ------------------------------------------------------------------
    # Data retrieval
    # ------------------------------------------------------------------

    def _get_data_within_timeframe(
        self,
        table_name: str,
        month: date,
        time_col: str = "event_time",
        day: date = None,
    ) -> DataFrame:
        """Rows of ``table_name`` within the batch's time window.

        The window is the month of ``month``, or the day being processed when
        :meth:`process_day` is running.

        Reads of the track table also rename ``callsign`` to ``flight_id``
        and give every report of a track the track's call sign
        (:func:`resolve_flight_id`), here rather than in each method, so that
        every method sees the same call sign whatever reports it later keeps.
        The call sign is resolved after the time filter, which lets Spark
        skip the other days' files.

        A run that writes a v0.0.2 version string leaves the call sign as it
        is, which reproduces that release (its tracks carry one call sign
        each anyway, and its call signs keep their padding spaces).
        """
        from opdi.utils.batching import filter_batch

        # The batch day is held on the processor rather than passed to every
        # method that reads tracks, so that no read can forget it and quietly
        # read the whole month. Only `process_day` sets it, while it runs.
        day = day if day is not None else getattr(self, "_batch_day", None)

        df = self.storage.read_table(table_name)
        if day is not None and "dof" in df.columns:
            df = filter_batch(df, day=day)
        else:
            df = filter_batch(df, month=month, time_col=time_col)

        if table_name != self.tracks_table:
            return df

        df = df.withColumnRenamed("callsign", "flight_id")
        if self._version_for("trend") != FLIGHT_LIST_VERSION:
            return df
        trim = bool(getattr(getattr(self, "detection", None), "call_sign_trimmed", False))
        return resolve_flight_id(df, trim=trim)

    def _load_airports_hex(self, airports_hex_path: Optional[str] = None) -> DataFrame:
        """The airport detection zones (``h3_airport_detection_zones``).

        Read from the warehouse table when it exists, otherwise from a local
        Parquet file.

        Args:
            airports_hex_path: Local Parquet file to use when the table does
                not exist.

        Returns:
            The zones: H3 cells around each airport with their distance band.
        """
        if self.storage.table_exists("h3_airport_detection_zones"):
            # Read once per processor and kept on the executors: step 03
            # reads the zones three times (highest airport, endpoint
            # candidates, trend), and each read of the table opens all of its
            # files again. Kept data that an executor loses are read again
            # from the table, so a lost executor never changes the result.
            # The public method that started the work lets them go when it
            # returns (``_releases_zones``).
            if getattr(self, "_zones", None) is None:
                zones = self.storage.read_table("h3_airport_detection_zones")
                if getattr(self.detection, "skip_duplicate_airports", False):
                    zones = self._without_duplicate_airports(zones)
                self._zones = zones.cache()
            return self._zones
        if airports_hex_path:
            df_apt = pd.read_parquet(airports_hex_path)
            return self.spark.createDataFrame(df_apt.to_dict(orient="records"))
        raise FileNotFoundError(
            "No airport hex zones found. Run "
            "AirportDetectionZoneGenerator.save_prepared_to_table() first, "
            "or provide airports_hex_path."
        )

    def _without_duplicate_airports(self, zones: DataFrame) -> DataFrame:
        """The zones less those of OurAirports records marked as duplicates.

        The codes are read from ``oa_airports`` (a few records in the world)
        and the zones of those codes are left out
        (:attr:`DetectionConfig.skip_duplicate_airports`). Without the table
        the zones are returned as they are.
        """
        if not self.storage.table_exists("oa_airports"):
            return zones
        from opdi.ingestion.ourairports import is_duplicate_airport

        oa = self.storage.read_table("oa_airports")
        idents = [
            r["ident"]
            for r in oa.filter(is_duplicate_airport(col("name"))).select("ident").collect()
            if r["ident"] is not None
        ]
        if not idents:
            return zones
        return zones.filter(~col("apt_ident").isin(idents))

    # ------------------------------------------------------------------
    # DAI processing (Departures / Arrivals / Internal)
    # ------------------------------------------------------------------

    def _candidates(
        self,
        sv: DataFrame,
        sdf_apt: DataFrame,
        max_radius_nm: float,
        sched_penalty_nm: float = 0.0,
        broadcast_reports: bool = False,
    ) -> DataFrame:
        """Candidate (report, airport) pairs, with their exact distance.

        H3 cells find the candidates and the great-circle distance compares
        them. The zone table reaches further than any detection radius, so
        the radius is applied here.

        ``sched_penalty_nm`` is added to the distance of an airport without
        scheduled service, so that a small or military field near the
        departure track does not win on distance alone. 0 means no penalty.

        ``broadcast_reports`` sends the reports in ``sv`` to every executor
        instead of moving both sides by cell. Only for the end reports of one
        day (two per track, about 350,000 rows worldwide): over a month they
        are some 10 million rows, too many to copy to every executor and to
        the driver.
        """
        radius_col = next(
            (c for c in ("apt_max_c_radius_nm", "max_c_radius_nm") if c in sdf_apt.columns),
            None,
        )
        if radius_col:
            sdf_apt = sdf_apt.filter(col(radius_col) <= float(max_radius_nm))

        # The end reports of one day are few beside the zones, so they are
        # sent to every executor and the zones are not moved. Moving both
        # sides by cell put the cells of busy airports, where many tracks end
        # and many zones overlap, into a few long tasks.
        reports = F.broadcast(sv) if broadcast_reports else sv
        j = sdf_apt.join(reports, sv.h3_res_7 == sdf_apt.apt_hex_id, "inner")
        j = j.withColumn(
            "dist_nm",
            haversine_nm(col("lat"), col("lon"), col("apt_latitude_deg"), col("apt_longitude_deg")),
        )
        # The H3 cell is a coarse index: a cell can match while the exact
        # distance is outside the radius.
        j = j.filter(col("dist_nm") <= float(max_radius_nm))

        scheduled = next(
            (c for c in ("apt_scheduled", "scheduled_service") if c in j.columns), None
        )
        penalty = (
            when(col(scheduled) == "yes", lit(0.0)).otherwise(lit(float(sched_penalty_nm)))
            if scheduled and sched_penalty_nm
            else lit(0.0)
        )
        return j.withColumn("eff_nm", col("dist_nm") + penalty)

    def _trend_sources(self, t: DataFrame, month: date) -> DataFrame:
        """Add ``adep_source`` and ``ades_source`` to the ``trend`` table.

        ``trend`` names an airport or nothing, so each end is ``aerodrome``
        or ``undetermined``. ``month`` is not used; it is part of the
        signature so a study variant can read the month's tracks.
        """
        return t.withColumn(
            "adep_source",
            when(col("ADEP").isNotNull(), lit("aerodrome")).otherwise(lit("undetermined")),
        ).withColumn(
            "ades_source",
            when(col("ADES").isNotNull(), lit("aerodrome")).otherwise(lit("undetermined")),
        )

    def _attach_field_elevation(self, df: DataFrame, ident_col: str = "apt_ident") -> DataFrame:
        """Attach ``apt_elevation_ft`` from OurAirports, matched on ``ident_col``.

        Used by both methods, so they read the same elevation for the same
        airport. Always a left join: rows with a NULL ``apt_ident`` (reports
        near no airport) keep a NULL elevation, which
        :func:`height_above_field` reads as 0 ft.
        """
        if "apt_elevation_ft" in df.columns:
            return df
        if not self.storage.table_exists("oa_airports"):
            # Without OurAirports there is no elevation to subtract, so the
            # height becomes the pressure altitude; the step still runs.
            return df.withColumn("apt_elevation_ft", lit(None).cast("double"))

        elev_ref = self.storage.read_table("oa_airports").select(
            col("ident").alias("_elev_ident"),
            col("elevation_ft").cast("double").alias("apt_elevation_ft"),
        )
        return df.join(broadcast(elev_ref), col(ident_col) == col("_elev_ident"), "left").drop(
            "_elev_ident"
        )

    def _max_field_elevation_ft(self, sdf_apt: DataFrame) -> float:
        """Highest elevation, in feet, of the airports in the detection zones.

        Taken over the zone table rather than all of OurAirports, which
        lists airports no report can reach. Cached on the instance.
        """
        if getattr(self, "_max_elev_ft", None) is not None:
            return self._max_elev_ft

        idents = sdf_apt.select(col("apt_ident")).distinct()
        row = self._attach_field_elevation(idents).select(f_max("apt_elevation_ft")).first()
        self._max_elev_ft = float(row[0]) if row and row[0] is not None else 0.0
        return self._max_elev_ft

    @_releases_zones
    def build_endpoint_candidates(
        self,
        month: date,
        max_radius_nm: float = 110.0,
        rebuild: bool = False,
    ) -> None:
        """Write the candidate airports of each track's first and last report.

        The table ``opdi_endpoint_candidates`` holds, for the two end reports
        of every track, every airport within ``max_radius_nm`` (110 NM by
        default) with its distance and height. The ``endpoint`` method then
        only filters and ranks this small table, so its thresholds can be
        changed without reading the tracks again.
        """
        if not rebuild and month in self._load_processed_months(self._endpoint_log):
            print(f"  endpoint candidates for {month:%Y-%m} already built, skipping")
            return

        sv = self._get_data_within_timeframe(self.tracks_table, month)
        sv = sv.dropna(subset=["lat", "lon", "track_id"])
        sv = sv.fillna({"flight_id": ""})

        d = getattr(self, "detection", None)
        needs_climb = getattr(d, "endpoint_airborne_needs_climb", False)
        both_ends = getattr(d, "endpoint_ids_from_both_ends", False)

        w = Window.partitionBy("track_id").orderBy("event_time")
        extra = []
        if needs_climb:
            # The climb after the first report (DetectionConfig.
            # endpoint_airborne_needs_climb): the first and the last known
            # altitude within the next `endpoint_climb_window_s` seconds. The
            # reports are ordered by their time in seconds, which orders them
            # as `event_time` does, so the row numbers below and the time
            # window share one sort.
            sv = sv.withColumn("_t", col("event_time").cast("double"))
            w = Window.partitionBy("track_id").orderBy("_t")
            w_climb = w.rangeBetween(0, int(round(float(d.endpoint_climb_window_s))))
            sv = (
                sv.withColumn("_alt_n", F.count("baro_altitude").over(w_climb))
                .withColumn("_alt_0", F.first("baro_altitude", ignorenulls=True).over(w_climb))
                .withColumn("_alt_1", F.last("baro_altitude", ignorenulls=True).over(w_climb))
            )
        ends = (
            sv.withColumn("_rn", row_number().over(w))
            .withColumn(
                "_rr",
                row_number().over(w.orderBy(col("_t" if needs_climb else "event_time").desc())),
            )
            .filter((col("_rn") == 1) | (col("_rr") == 1))
            .withColumn("role", when(col("_rn") == 1, lit("adep")).otherwise(lit("ades")))
            .withColumn(
                "at_border",
                at_border(col("lat"), col("lon"), bbox=self.config.coverage.bbox),
            )
        )
        if needs_climb:
            # In feet; NULL on the last report, and where fewer than two
            # altitudes are known (the climb is then unknown).
            ends = ends.withColumn(
                "climb_ft",
                when(
                    (col("role") == "adep") & (col("_alt_n") >= 2),
                    (col("_alt_1") - col("_alt_0")) * FT_PER_M,
                ),
            )
            extra.append("climb_ft")
        if both_ends:
            # The track's first and last report with a position, on both end
            # rows, so a track whose other end has no airport nearby still
            # knows them (DetectionConfig.endpoint_ids_from_both_ends).
            w_track = Window.partitionBy("track_id")
            ends = ends.withColumn(
                "track_first_seen", f_min("event_time").over(w_track)
            ).withColumn("track_last_seen", f_max("event_time").over(w_track))
            extra += ["track_first_seen", "track_last_seen"]
        ends = ends.select(
            "track_id",
            "icao24",
            "flight_id",
            "role",
            "event_time",
            "lat",
            "lon",
            "baro_altitude",
            "on_ground",
            "at_border",
            "h3_res_7",
            *extra,
        )

        sdf_apt = self._load_airports_hex()
        # Broadcast only for one day (see ``_candidates``).
        cand = self._candidates(
            ends,
            sdf_apt,
            max_radius_nm=max_radius_nm,
            broadcast_reports=self._batch_day is not None,
        )

        # The zones carry no airport data, so the elevation comes from
        # OurAirports. Without it the height test could only be met by
        # reports flagged on the ground.
        cand = self._attach_field_elevation(cand)

        elev = next((c for c in ("apt_elevation_ft", "elevation_ft") if c in cand.columns), None)
        if elev:
            # Height above the airport, which means the same at every airport.
            cand = cand.withColumn("agl_ft", col("baro_altitude") * 3.28084 - col(elev)).withColumn(
                "elev_known", col(elev).isNotNull()
            )
        else:
            cand = cand.withColumn("agl_ft", lit(None).cast("double")).withColumn(
                "elev_known", lit(False)
            )

        keep = [
            "track_id",
            "icao24",
            "flight_id",
            "role",
            "event_time",
            "apt_ident",
            "dist_nm",
            "eff_nm",
            "agl_ft",
            "elev_known",
            "on_ground",
            "at_border",
            "lat",
            "lon",
        ]
        for name in (
            "apt_min_c_radius_nm",
            "apt_max_c_radius_nm",
            "apt_scheduled",
            "apt_type",
            *extra,
        ):
            if name in cand.columns:
                keep.append(name)

        # Overwrite, not append: building the same month again must replace it.
        self.storage.write_table(cand.select(*keep), "opdi_endpoint_candidates", mode="overwrite")
        self._mark_month_processed(month, self._endpoint_log)
        print(f"  endpoint candidates for {month:%Y-%m} written")

    def _fetch_and_label_sv(self, month: date, sdf_apt: DataFrame) -> DataFrame:
        """Reports near airports, for the ``trend`` method.

        Steps:

        1. read the tracks of the batch;
        2. add the flight level from ``baro_altitude``;
        3. add the first and last time seen and the date of flight per track;
        4. keep the reports below a ceiling that no report passing the exact
           test can exceed (:func:`trend_prefilter_ceiling_ft`);
        5. join with the airport zones within the detection radius;
        6. attach each candidate airport's elevation;
        7. keep the reports below the ceiling above that airport, or below the
           flight level when ``trend_max_datum`` is ``"msl"``.

        The exact test comes after the join because it needs to know which
        airport a report is a candidate for.

        Args:
            month: Month to process.
            sdf_apt: Airport detection zones.

        Returns:
            Reports near airports, one row per (report, candidate airport).
        """
        sv = self._get_data_within_timeframe(self.tracks_table, month)

        measured = FlightListProcessor._seen_from_measured_reports(self)
        if measured:
            # First and last seen over the reports with a position, before the
            # reports without an altitude are left out
            # (EventConfig.seen_from_measured_reports).
            sv = sv.dropna(subset=["lat", "lon", "track_id"])
            # Only the columns read below, so the reports shuffled for the
            # window are narrow.
            sv = sv.select(
                *[
                    c
                    for c in (
                        "track_id",
                        "icao24",
                        "flight_id",
                        "event_time",
                        "lat",
                        "lon",
                        "baro_altitude",
                        "heading",
                        "vert_rate",
                        "on_ground",
                        "h3_res_7",
                    )
                    if c in sv.columns
                ]
            )
            w_seen = Window.partitionBy("track_id")
            t = F.to_timestamp(col("event_time"))
            sv = sv.withColumn("first_seen", f_min(t).over(w_seen)).withColumn(
                "last_seen", f_max(t).over(w_seen)
            )

        sv_f = sv.dropna(subset=["lat", "lon", "baro_altitude", "track_id"])
        # Everything below groups and joins on flight_id, which holds one value
        # per track (see _get_data_within_timeframe).
        sv_f = sv_f.fillna({"flight_id": ""})
        sv_f = sv_f.withColumn("event_time", F.to_timestamp(col("event_time")))
        sv_f = sv_f.withColumn("flight_level", (col("baro_altitude") * 3.28084 / 100).cast("int"))

        columns_of_interest = [
            "track_id",
            "icao24",
            "flight_id",
            "event_time",
            "lat",
            "lon",
            "flight_level",
            "baro_altitude",
            "heading",
            "vert_rate",
            "on_ground",
            "h3_res_7",
        ]
        if measured:
            columns_of_interest += ["first_seen", "last_seen"]
        sv_f = sv_f.select(columns_of_interest)

        # Per-track first/last seen and DOF
        if not measured:
            window_track = Window.partitionBy("track_id")
            sv_f = sv_f.withColumn("first_seen", f_min("event_time").over(window_track))
            sv_f = sv_f.withColumn("last_seen", f_max("event_time").over(window_track))
        sv_f = sv_f.withColumn("DOF", to_date("first_seen"))

        # Smooth before the altitude cut, so the moving average at the ceiling
        # is a mean of five reports rather than of whatever lies below it.
        #
        # Grouped by track alone: the altitude series is the same for every
        # candidate airport, so it is smoothed once.
        if getattr(self.detection, "trend_smooth_before_cut", False):
            half = self.detection.trend_smooth_half_window
            w_smooth = Window.partitionBy("track_id").orderBy("event_time").rowsBetween(-half, half)
            sv_f = sv_f.withColumn("smoothed_altitude", F.avg("baro_altitude").over(w_smooth))

        # Quick pre-filter before the join; the exact test per airport follows
        # it. The ceiling is the cap plus the highest airport, so no report
        # that passes the exact test is removed here. With the flight level
        # as the ceiling, the two tests are the same.
        ceiling_ft = trend_prefilter_ceiling_ft(
            self.detection, self._max_field_elevation_ft(sdf_apt)
        )
        sv_low_alt = sv_f.filter(col("baro_altitude") * FT_PER_M <= lit(ceiling_ft))

        # The zone table reaches further than the detection radius, so the
        # radius is applied here.
        exact = getattr(self.detection, "trend_radius_exact", False)
        radius_nm = self.detection.trend_radius_nm
        # A band is a ring of cells with an inner and an outer radius. Selecting
        # on the inner radius keeps every band that can hold a report within
        # the radius; the exact distance test then narrows it. Selecting on
        # the outer radius (release v0.0.2) drops the band that crosses the
        # radius.
        band_col = next(
            (
                c
                for c in (
                    ("apt_min_c_radius_nm", "min_c_radius_nm")
                    if exact
                    else ("apt_max_c_radius_nm", "max_c_radius_nm")
                )
                if c in sdf_apt.columns
            ),
            None,
        )
        if band_col:
            sdf_apt = sdf_apt.filter(col(band_col) <= radius_nm)
        else:
            # An older zone table without bands was already cut to the radius
            # when it was built, so no filter is needed.
            print(
                "  NOTE: airport zones carry no radius band; using the table as "
                "generated. Regenerate step 00a to filter by radius at read time."
            )

        sv_nearby_apt = sv_low_alt.join(sdf_apt, sv_low_alt.h3_res_7 == sdf_apt.apt_hex_id, "left")

        if exact:
            # The exact test, in NM. Reports near no airport (a NULL from the
            # left join) are kept, so their tracks stay in the flight list
            # without an airport.
            sv_nearby_apt = sv_nearby_apt.filter(
                col("apt_ident").isNull()
                | (
                    haversine_nm(
                        col("lat"),
                        col("lon"),
                        col("apt_latitude_deg"),
                        col("apt_longitude_deg"),
                    )
                    <= lit(float(radius_nm))
                )
            )

        # Elevation, then the exact altitude test; both need the candidate
        # airport. Reports near no airport get a NULL elevation, read as 0 ft
        # by `height_above_field`.
        sv_nearby_apt = self._attach_field_elevation(sv_nearby_apt)
        return sv_nearby_apt.filter(trend_altitude_cut(self.detection))

    @staticmethod
    def _ranked_distance(
        cand: DataFrame, sched_penalty_nm: float, waiver: Optional[tuple] = None
    ) -> DataFrame:
        """Add ``_eff``: the distance by which the candidates are ranked.

        The distance plus ``sched_penalty_nm`` for an airport without
        scheduled service. ``waiver``, a pair (radius in NM, height in feet),
        leaves the penalty out where the end report is at that airport: flagged
        on the ground, or within the radius and no higher than the height above
        it (:attr:`DetectionConfig.sched_penalty_waived_at_field`). The penalty
        stays when an airport with scheduled service of the same end also lies
        within the radius: an aircraft on the ground there is at that airport.
        """
        scheduled = "apt_scheduled" if "apt_scheduled" in cand.columns else None
        penalty = (
            when(col(scheduled) == "yes", lit(0.0)).otherwise(lit(float(sched_penalty_nm)))
            if scheduled and sched_penalty_nm
            else lit(0.0)
        )
        if waiver is not None and scheduled and sched_penalty_nm:
            radius_nm, height_ft = waiver
            at_field = F.coalesce(col("on_ground"), lit(False)) | F.coalesce(
                (col("dist_nm") <= float(radius_nm)) & (col("agl_ft") <= float(height_ft)),
                lit(False),
            )
            end = Window.partitionBy("track_id", "role")
            scheduled_near = F.coalesce(
                F.max((col(scheduled) == "yes") & (col("dist_nm") <= float(radius_nm))).over(end),
                lit(False),
            )
            penalty = when(at_field & ~scheduled_near, lit(0.0)).otherwise(penalty)
        return cand.withColumn("_eff", col("dist_nm") + penalty)

    @staticmethod
    def _best_candidates(
        cand: DataFrame, sched_penalty_nm: float, waiver: Optional[tuple] = None
    ) -> DataFrame:
        """The best candidate airport per (track, end).

        Ranked by distance plus ``sched_penalty_nm`` for an airport without
        scheduled service (see :meth:`_ranked_distance`), computed here so the
        penalty can be changed without rebuilding the candidate table. The
        airport code breaks an exact tie, so the same candidates always give
        the same airport.
        """
        cand = FlightListProcessor._ranked_distance(cand, sched_penalty_nm, waiver)
        w = Window.partitionBy("track_id", "role").orderBy(
            col("_eff").asc_nulls_last(), col("apt_ident").asc()
        )
        return cand.withColumn("_r", row_number().over(w)).filter(col("_r") == 1)

    @staticmethod
    def classify_endpoints(
        cand: DataFrame,
        mode: str = "endpoint",
        abstention_radius_nm: float = 40.0,
        abstention_height_ft: float = 15000.0,
        sched_penalty_nm: float = 10.0,
        ooa: bool = True,
        waiver: Optional[tuple] = None,
        min_climb_ft: Optional[float] = None,
        min_climb_above_ft: float = 1000.0,
    ) -> DataFrame:
        """Name one airport per track end from the candidate table.

        ``endpoint`` takes the best candidate (see :meth:`_best_candidates`)
        and accepts it when the end report is within ``abstention_radius_nm``
        of it and no more than ``abstention_height_ft`` above its elevation,
        or flagged on the ground. Otherwise, when the report is at the edge of
        the covered area (``ooa``), it gives the code ``OOA``; else NULL.

        ``waiver`` is passed to :meth:`_ranked_distance`. With
        ``min_climb_ft``, a first report in the air names the departure
        airport only when the altitude changed by at least that many feet
        after it (column ``climb_ft`` of the candidate table; a negative
        value allows that much descent); an unknown change (NULL, or a table
        without the column) leaves the test as before.

        The airport test comes before the edge test: an airport close to the
        edge of the box (Ponta Delgada, about 8 NM inside it) must still be
        named for an aircraft on its runway.

        Returns:
            One row per (track, end) with ``apt`` and ``source``
            (``aerodrome``, ``out_of_area`` or ``undetermined``).
        """
        if mode != "endpoint":
            raise ValueError(f"classify_endpoints: unsupported mode {mode!r}")

        best = FlightListProcessor._best_candidates(cand, sched_penalty_nm, waiver)

        ok = col("dist_nm") <= float(abstention_radius_nm)
        # A report on the ground passes the height test by itself: surface
        # reports often have no barometric altitude. Where the elevation is
        # unknown, agl_ft is really altitude above sea level, so only the
        # ground flag is trusted.
        ok = ok & (
            col("on_ground") | (col("elev_known") & (col("agl_ft") <= float(abstention_height_ft)))
        )
        if min_climb_ft is not None and "climb_ft" in best.columns:
            # A first report in the air, followed by a descent, is an aircraft
            # arriving, not a departure.
            #
            # Only a report clearly in the air is tested: an aircraft seen first
            # at the stand, not flagged on the ground, does not climb while it
            # taxis.
            not_climbing = (
                (col("role") == "adep")
                & ~F.coalesce(col("on_ground"), lit(False))
                & (col("agl_ft") > float(min_climb_above_ft))
                & (col("climb_ft") < float(min_climb_ft))
            )
            ok = ok & ~F.coalesce(not_climbing, lit(False))

        apt = when(ok, col("apt_ident"))
        source = when(ok, lit("aerodrome"))
        if ooa:
            apt = apt.otherwise(when(col("at_border"), lit(OOA)))
            source = source.otherwise(
                when(col("at_border"), lit("out_of_area")).otherwise(lit("undetermined"))
            )
        else:
            source = source.otherwise(lit("undetermined"))

        return best.withColumn("apt", apt).withColumn("source", source)

    @staticmethod
    def _categorize_landing_take_off(
        df: DataFrame,
        smooth_half_window: int = 2,
        vote_margin: int = 4,
    ) -> DataFrame:
        """Classify each (track, airport) pair as take-off, landing or ambiguous.

        The altitude is smoothed and the reports near the airport vote: a
        rising altitude is a take-off, a falling one a landing. Climbing must
        outnumber descending (or the reverse) by ``vote_margin`` reports.

        Args:
            df: Reports from :meth:`_fetch_and_label_sv`.
            smooth_half_window: Half-width of the centred moving average of
                ``baro_altitude``, in reports; 2 gives a five-report window.
            vote_margin: Number of reports by which one direction must win, or
                the pair is ``ambiguous``. At one report per 5 s, 4 is about
                20 s.

        Returns:
            One row per (track, airport) with a ``status`` column
            (``take-off``, ``landing`` or ``ambiguous``).
        """
        window_spec = Window.partitionBy(["icao24", "flight_id", "track_id", "apt_ident"]).orderBy(
            "event_time"
        )

        # Smoothed altitude. Already present when the caller smoothed before
        # the altitude cut (see `_fetch_and_label_sv`); smoothing again would
        # average the averages.
        if "smoothed_altitude" in df.columns:
            df_m = df
        else:
            window_avg = (
                Window.partitionBy(["icao24", "flight_id", "track_id", "apt_ident"])
                .orderBy("event_time")
                .rowsBetween(-smooth_half_window, smooth_half_window)
            )
            df_m = df.withColumn("smoothed_altitude", F.avg("baro_altitude").over(window_avg))
        df_m = df_m.withColumn(
            "altitude_change",
            col("smoothed_altitude") - F.lag("smoothed_altitude").over(window_spec),
        )
        df_m = df_m.withColumn(
            "trajectory_type",
            when(col("altitude_change") > 0, "take-off")
            .when(col("altitude_change") < 0, "landing")
            .otherwise("constant altitude"),
        )

        # Aggregate per track-airport
        flight_type_df = df_m.groupBy(["icao24", "flight_id", "track_id", "apt_ident"]).agg(
            F.sum(when(col("trajectory_type") == "take-off", 1).otherwise(0)).alias(
                "take_off_count"
            ),
            F.sum(when(col("trajectory_type") == "landing", 1).otherwise(0)).alias("landing_count"),
        )

        # A margin of N samples is about 5N seconds in one state.
        flight_type_df = flight_type_df.withColumn(
            "status",
            when(col("take_off_count") > (col("landing_count") + vote_margin), "take-off")
            .when(col("landing_count") > (col("take_off_count") + vote_margin), "landing")
            .otherwise("ambiguous"),
        )

        return df.join(
            flight_type_df,
            on=["icao24", "flight_id", "track_id", "apt_ident"],
            how="left",
        )

    @staticmethod
    def _endpoint_courses(df: DataFrame, window_s: float = COURSE_WINDOW_S) -> DataFrame:
        """Course of each track near its departure or arrival point.

        The course is the bearing from the far end of a time window to the
        end report itself. The window lies after the first report for a
        departure and before the last report for an arrival, so for both the
        course points towards the airport that should be named.

        The course is NULL when the window holds fewer than
        :data:`COURSE_MIN_SAMPLES` distinct reports or spans less than
        :data:`COURSE_MIN_BASELINE_S` seconds; a NULL course sorts last, so
        distance then decides alone.
        """
        w = Window.partitionBy("track_id", "status")
        d = (
            df.withColumn(
                "_t_end",
                when(col("status") == "take-off", f_min("event_time").over(w)).otherwise(
                    f_max("event_time").over(w)
                ),
            )
            .withColumn(
                "_off",
                F.abs(unix_timestamp("event_time") - unix_timestamp("_t_end")),
            )
            .filter(col("_off") <= float(window_s))
        )

        return (
            d.groupBy("track_id", "status")
            .agg(
                # Distinct times, not rows: the zone join repeats each report
                # once per candidate airport.
                F.countDistinct("event_time").alias("_n"),
                F.max("_off").alias("_span"),
                F.min_by("lat", "_off").alias("_near_lat"),
                F.min_by("lon", "_off").alias("_near_lon"),
                F.max_by("lat", "_off").alias("_far_lat"),
                F.max_by("lon", "_off").alias("_far_lon"),
            )
            .withColumn(
                "course",
                when(
                    (col("_n") >= COURSE_MIN_SAMPLES) & (col("_span") >= COURSE_MIN_BASELINE_S),
                    bearing_deg(
                        col("_far_lat"), col("_far_lon"), col("_near_lat"), col("_near_lon")
                    ),
                ),
            )
            .select("track_id", "status", "course", "_near_lat", "_near_lon")
        )

    @staticmethod
    def _compute_flight_table(
        df: DataFrame,
        sched_penalty_nm: float = 0.0,
        rank_by: str = "ring",
        bearing_tiebreak_nm: float = 0.0,
        version: str = FLIGHT_LIST_VERSION,
        last_landing_window_s: Optional[float] = None,
        waiver: Optional[tuple] = None,
    ) -> DataFrame:
        """The flight table of the ``trend`` method.

        For each track with a take-off or landing:

        1. find each candidate airport's closest report;
        2. rank the candidates (see ``rank_by``) and keep the best;
        3. join the departure and the arrival into one row.

        ``sched_penalty_nm`` is added to the great-circle distance of an
        airport without scheduled service. 0 adds nothing.

        Args:
            df: Pairs from :meth:`_categorize_landing_take_off`.
            sched_penalty_nm: Distance, in NM, added to an airport without
                scheduled service.
            rank_by: ``"haversine"`` ranks by great-circle distance;
                ``"ring"`` (release v0.0.2) keeps only the candidates at the
                smallest H3 ring count and ranks those by distance.
            bearing_tiebreak_nm: Distance band within which the course
                decides between candidates; 0 turns it off.
            version: Version string to write.
            last_landing_window_s: When given, the landing airports whose
                last nearby report lies within this many seconds of the
                latest one are ranked before the others
                (:attr:`DetectionConfig.trend_last_landing`).
            waiver: A pair (radius in NM, height in feet): no penalty for an
                airport without scheduled service when the closest report
                is flagged on the ground, or within the radius and no higher
                than the height above it
                (:attr:`DetectionConfig.sched_penalty_waived_at_field`).

        Returns:
            The flight table with ADEP, ADES and the track's columns.
        """
        df = df.filter(df.status != "ambiguous")

        if last_landing_window_s is not None:
            # The time of each candidate's last report near it, before only
            # the closest report is kept (DetectionConfig.trend_last_landing).
            per_apt_all = Window.partitionBy(
                ["icao24", "flight_id", "track_id", "status", "apt_ident"]
            )
            df = df.withColumn("_last_near", f_max("event_time").over(per_apt_all))

        # Candidate selection, by one of two rules.
        #
        # `ring` (release v0.0.2): keep only the reports at the smallest H3
        # ring count across all candidates of the track and status, then
        # break ties on distance. A candidate one ring further out (about
        # 5 km at resolution 7) is dropped before its distance is measured.
        #
        # `haversine`: measure the distance to every candidate first and
        # choose afterwards.
        if rank_by == "ring":
            window_spec = Window.partitionBy(["icao24", "flight_id", "track_id", "status"])
            df = df.withColumn("min_distance", f_min("distance_from_center").over(window_spec))
            df_min = df.filter(df.distance_from_center == df.min_distance)
        elif rank_by == "haversine":
            # Distance from every sample to every candidate aerodrome, then the
            # closest approach per aerodrome. Ranking happens further down, on
            # `distance_km`, which this makes exact rather than ring-limited.
            df = df.withColumn(
                "_closest_nm",
                haversine_nm(
                    col("lat"),
                    col("lon"),
                    col("apt_latitude_deg"),
                    col("apt_longitude_deg"),
                ),
            )
            # Partitioned by aerodrome as well, so this is each candidate's own
            # closest approach rather than a cut across all of them. Nothing is
            # eliminated here -- every candidate survives to be ranked.
            per_apt = Window.partitionBy(["icao24", "flight_id", "track_id", "status", "apt_ident"])
            df = df.withColumn("_min_nm", f_min("_closest_nm").over(per_apt))
            # `min_distance` keeps its original meaning -- an H3 ring count --
            # because it is carried into the output projection and downstream
            # readers expect that. Selection is on exact distance; only the
            # ranking rule changes, not the column's units.
            df_min = (
                df.filter(col("_closest_nm") == col("_min_nm"))
                .drop("_closest_nm", "_min_nm")
                .withColumn("min_distance", col("distance_from_center"))
            )
        else:
            raise ValueError(f"unknown trend_rank_by: {rank_by!r}; expected 'haversine' or 'ring'")

        # The scheduled-service flag is kept only when the penalty uses it.
        # The ranking below looks it up on this frame.
        keep = [
            "icao24",
            "flight_id",
            "track_id",
            "apt_ident",
            "apt_longitude_deg",
            "apt_latitude_deg",
            "DOF",
            "first_seen",
            "last_seen",
            "status",
            "event_time",
            "lat",
            "lon",
            "min_distance",
            "take_off_count",
            "landing_count",
        ]
        if sched_penalty_nm:
            keep += [c for c in ("apt_scheduled", "scheduled_service") if c in df_min.columns]
        if last_landing_window_s is not None:
            keep.append("_last_near")
        if waiver is not None and sched_penalty_nm:
            keep += [
                c for c in ("on_ground", "baro_altitude", "apt_elevation_ft") if c in df_min.columns
            ]
        df_min = df_min.select(*keep)

        # Get time range per track-airport-status
        window_spec2 = Window.partitionBy(
            ["icao24", "flight_id", "track_id", "apt_ident", "status"]
        )
        df_min = df_min.withColumn("min_time", f_min("event_time").over(window_spec2))
        df_min = df_min.withColumn("max_time", f_max("event_time").over(window_spec2))

        df_take_off = df_min.filter(
            (col("status") == "take-off") & (col("event_time") == col("min_time"))
        )
        df_landing = df_min.filter(
            (col("status") == "landing") & (col("event_time") == col("max_time"))
        )

        flight_table = df_take_off.union(df_landing)

        # Haversine distance to airport for multi-airport disambiguation
        R = 6371.0
        flight_table = (
            flight_table.withColumn("lat1", radians(col("lat")))
            .withColumn("lon1", radians(col("lon")))
            .withColumn("lat2", radians(col("apt_latitude_deg")))
            .withColumn("lon2", radians(col("apt_longitude_deg")))
            .withColumn("dlat", col("lat2") - col("lat1"))
            .withColumn("dlon", col("lon2") - col("lon1"))
            .withColumn(
                "a",
                sin(col("dlat") / 2) ** 2
                + cos(col("lat1")) * cos(col("lat2")) * sin(col("dlon") / 2) ** 2,
            )
            .withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a"))))
            .withColumn("distance_km", R * col("c"))
        )

        # Select closest airport per flight
        key_columns = ["icao24", "flight_id", "track_id", "status", "first_seen", "last_seen"]

        order_col = col("distance_km")
        if sched_penalty_nm:
            sched_col = next(
                (c for c in ("apt_scheduled", "scheduled_service") if c in flight_table.columns),
                None,
            )
            if sched_col is None:
                # Fail loudly: a missing column would look like a penalty of 0.
                print(
                    "  NOTE: airport zones carry no scheduled-service column; "
                    "trend_sched_penalty_nm has no effect. Regenerate step 00a."
                )
            else:
                penalty_km = when(col(sched_col) == "yes", lit(0.0)).otherwise(
                    lit(float(sched_penalty_nm) * 1.852)
                )
                if waiver is not None:
                    # No penalty for the airport the aircraft is at.
                    radius_nm, height_ft = waiver
                    near = col("distance_km") <= lit(float(radius_nm) * 1.852)
                    if "baro_altitude" in flight_table.columns:
                        elev = (
                            col("apt_elevation_ft")
                            if "apt_elevation_ft" in flight_table.columns
                            else lit(None).cast("double")
                        )
                        near = near & (
                            height_above_field(col("baro_altitude"), elev) <= lit(float(height_ft))
                        )
                    else:
                        near = lit(False)
                    ground = (
                        F.coalesce(col("on_ground"), lit(False))
                        if "on_ground" in flight_table.columns
                        else lit(False)
                    )
                    at_field = ground | F.coalesce(near, lit(False))
                    # Not where an airport with scheduled service is as near.
                    scheduled_near = F.coalesce(
                        F.max(
                            (col(sched_col) == "yes")
                            & (col("distance_km") <= lit(float(radius_nm) * 1.852))
                        ).over(Window.partitionBy(key_columns)),
                        lit(False),
                    )
                    penalty_km = when(at_field & ~scheduled_near, lit(0.0)).otherwise(penalty_km)
                flight_table = flight_table.withColumn("_eff_km", col("distance_km") + penalty_km)
                order_col = col("_eff_km")

        # The ordering key. Distance decides, except among candidates that
        # distance cannot separate.
        order_by = [order_col.asc_nulls_last()]

        recent = None
        if last_landing_window_s is not None:
            # Landings ending more than the window before the latest landing of
            # the track (a stop in the middle of it) rank after the others.
            w_track = Window.partitionBy(key_columns)
            flight_table = flight_table.withColumn(
                "_recent",
                (col("status") != "landing")
                | (
                    unix_timestamp("_last_near")
                    >= unix_timestamp(f_max("_last_near").over(w_track))
                    - lit(float(last_landing_window_s))
                ),
            )
            recent = col("_recent")
            order_by = [recent.desc()] + order_by

        if bearing_tiebreak_nm:
            # The course cannot name an airport on its own: every airport on
            # the same line behind the right one is as well aligned. It only
            # decides between candidates that are about equally distant,
            # and the band limits it to those.
            crs = FlightListProcessor._endpoint_courses(df)
            flight_table = flight_table.join(crs, ["track_id", "status"], "left")
            flight_table = flight_table.withColumn(
                "align_deg",
                angle_between(
                    col("course"),
                    bearing_deg(
                        col("_near_lat"),
                        col("_near_lon"),
                        col("apt_latitude_deg"),
                        col("apt_longitude_deg"),
                    ),
                ),
            )
            band_km = float(bearing_tiebreak_nm) * 1.852
            w_best = Window.partitionBy(key_columns)
            best = f_min(order_col if recent is None else when(recent, order_col)).over(w_best)
            flight_table = flight_table.withColumn("_best_km", best).withColumn(
                "_in_band", order_col <= col("_best_km") + lit(band_km)
            )
            order_by = ([recent.desc()] if recent is not None else []) + [
                # Inside the band first, then best-aligned within it, and only
                # then distance. A track with no usable course has a null
                # alignment, which sorts last inside the band and leaves the
                # distance ordering untouched.
                col("_in_band").desc(),
                when(col("_in_band"), col("align_deg")).asc_nulls_last(),
                order_col.asc_nulls_last(),
            ]

        # Last key: the airport code, so that two candidates the keys above
        # cannot separate come out the same way in every run.
        window_closest = Window.partitionBy(key_columns).orderBy(*order_by, col("apt_ident").asc())

        df_numbered = flight_table.withColumn("row_number", row_number().over(window_closest))
        df_numbered = df_numbered.withColumn("is_most_likely", col("row_number") == 1)

        # The airport is read from the row ranked first; `first()` would
        # take whichever row the aggregation meets first, which is not always
        # the ranked one.
        # The other candidates in rank order: a list collected as the rows come
        # would follow the order Spark meets them in, which changes between runs.
        others = F.sort_array(
            collect_list(
                when(
                    ~col("is_most_likely"),
                    F.struct(col("row_number").alias("r"), col("apt_ident").alias("a")),
                )
            )
        )
        result_df = df_numbered.groupBy(key_columns).agg(
            f_max(when(col("is_most_likely"), col("apt_ident"))).alias("most_likely_airport"),
            others.alias("_others"),
        )
        result_df = result_df.withColumn(
            "potential_airports", F.transform(col("_others"), lambda x: x["a"])
        ).drop("_others")

        result_df = result_df.select(
            *key_columns, col("most_likely_airport"), col("potential_airports")
        )

        # Split into departures and arrivals, then merge
        take_offs = (
            result_df.filter(col("status") == "take-off")
            .withColumnRenamed("most_likely_airport", "ADEP")
            .withColumnRenamed("potential_airports", "ADEP_P")
        )
        landings = (
            result_df.filter(col("status") == "landing")
            .withColumnRenamed("most_likely_airport", "ADES")
            .withColumnRenamed("potential_airports", "ADES_P")
        )

        key_cols = ["icao24", "flight_id", "track_id", "first_seen", "last_seen"]
        flight_table = take_offs.drop("status").join(
            landings.drop("status"), on=key_cols, how="outer"
        )

        flight_table = flight_table.withColumn("DOF", to_date(col("first_seen")))
        flight_table = (
            flight_table.withColumnRenamed("track_id", "id")
            .withColumnRenamed("icao24", "ICAO24")
            .withColumnRenamed("flight_id", "FLT_ID")
        )

        flight_table = flight_table.withColumn("version", lit(version))
        flight_table = flight_table.withColumn("ADEP_P", concat_ws(", ", col("ADEP_P")))
        flight_table = flight_table.withColumn("ADES_P", concat_ws(", ", col("ADES_P")))

        return flight_table.select(
            "id",
            "ADEP",
            "ADES",
            "ADEP_P",
            "ADES_P",
            "ICAO24",
            "FLT_ID",
            "first_seen",
            "last_seen",
            "DOF",
            "version",
        )

    @staticmethod
    def _flight_table_from_endpoints(
        classified: DataFrame,
        cand: DataFrame,
        version: str = FLIGHT_LIST_VERSION,
        alternatives_radius_nm: Optional[float] = None,
        sched_penalty_nm: float = 0.0,
        waiver: Optional[tuple] = None,
        both_ends: bool = False,
    ) -> DataFrame:
        """The flight table of the ``endpoint`` method.

        Has the same columns as :meth:`_compute_flight_table`, so either
        method can serve either end. ``adep_source`` and ``ades_source`` say
        how each airport was decided: ``aerodrome``, ``out_of_area`` or
        ``undetermined``.

        With ``alternatives_radius_nm``, ``ADEP_P`` and ``ADES_P`` list only
        the other candidates within that distance, in the order of the choice
        (:meth:`_ranked_distance` with ``sched_penalty_nm`` and ``waiver``);
        otherwise every candidate, nearest first. With ``both_ends``, the
        address, call sign, first and last time seen come from whichever end
        has a row (columns ``track_first_seen`` / ``track_last_seen`` of the
        candidate table), so no row is left without them.
        """
        both_ends = both_ends and "track_first_seen" in classified.columns
        chosen_cols = ["track_id", "icao24", "flight_id", "role", "apt", "source", "event_time"]
        if both_ends:
            chosen_cols += ["track_first_seen", "track_last_seen"]
        chosen = classified.select(*chosen_cols)

        # The other candidate airports, nearest first (then by code), for
        # ADEP_P and ADES_P. Sorted, not collected as the rows come, so that
        # every run lists them in the same order.
        if alternatives_radius_nm is not None:
            listed = FlightListProcessor._ranked_distance(cand, sched_penalty_nm, waiver).filter(
                col("dist_nm") <= float(alternatives_radius_nm)
            )
            rank_key = col("_eff")
        else:
            listed = cand
            rank_key = col("dist_nm")
        others = (
            listed.join(
                chosen.select("track_id", "role", col("apt").alias("_chosen")),
                ["track_id", "role"],
                "left",
            )
            .filter(col("apt_ident") != col("_chosen"))
            .groupBy("track_id", "role")
            .agg(
                F.sort_array(
                    collect_list(F.struct(rank_key.alias("d"), col("apt_ident").alias("a")))
                ).alias("_others")
            )
            .withColumn("potential", F.transform(col("_others"), lambda x: x["a"]))
            .drop("_others")
        )

        j = chosen.join(others, ["track_id", "role"], "left")

        def side(role: str, prefix: str) -> DataFrame:
            d = j.filter(col("role") == role)
            cols = [
                col("track_id"),
                col("icao24"),
                col("flight_id"),
                col("apt").alias(prefix),
                col("source").alias(f"{prefix.lower()}_source"),
                col("potential").alias(f"{prefix}_P"),
                col("event_time").alias("first_seen" if role == "adep" else "last_seen"),
            ]
            if both_ends:
                tag = prefix.lower()
                cols += [
                    col("track_first_seen").alias(f"_fs_{tag}"),
                    col("track_last_seen").alias(f"_ls_{tag}"),
                ]
            return d.select(*cols)

        dep = side("adep", "ADEP")
        if both_ends:
            # The address and the call sign of the track, from either end
            # (DetectionConfig.endpoint_ids_from_both_ends).
            arr = (
                side("ades", "ADES")
                .withColumnRenamed("icao24", "_icao24_ades")
                .withColumnRenamed("flight_id", "_flight_id_ades")
            )
            ft = dep.join(arr, "track_id", "outer")
            ft = (
                ft.withColumn("icao24", F.coalesce(col("icao24"), col("_icao24_ades")))
                .withColumn("flight_id", F.coalesce(col("flight_id"), col("_flight_id_ades")))
                .withColumn("first_seen", F.coalesce(col("_fs_adep"), col("_fs_ades")))
                .withColumn("last_seen", F.coalesce(col("_ls_adep"), col("_ls_ades")))
            )
        else:
            arr = side("ades", "ADES").drop("icao24", "flight_id")
            ft = dep.join(arr, "track_id", "outer")
        ft = ft.withColumn("DOF", to_date(col("first_seen")))
        ft = ft.withColumn("version", lit(version))
        ft = (
            ft.withColumnRenamed("track_id", "id")
            .withColumnRenamed("icao24", "ICAO24")
            .withColumnRenamed("flight_id", "FLT_ID")
        )
        ft = ft.withColumn("ADEP_P", concat_ws(", ", col("ADEP_P")))
        ft = ft.withColumn("ADES_P", concat_ws(", ", col("ADES_P")))

        return ft.select(
            "id",
            "ADEP",
            "ADES",
            "ADEP_P",
            "ADES_P",
            "adep_source",
            "ades_source",
            "ICAO24",
            "FLT_ID",
            "first_seen",
            "last_seen",
            "DOF",
            "version",
        )

    def _add_osn_aircraft_db_data(self, flight_table: DataFrame) -> DataFrame:
        """Add the aircraft data of the OpenSky aircraft database.

        Args:
            flight_table: The flight table.

        Returns:
            The flight table with upper-case column names, the registration,
            model, type code, ICAO aircraft class and operator added, and the
            columns in the fixed order of ``opdi_flight_list``.
        """
        osn_aircraft_db = self.storage.read_table("osn_aircraft_db")

        merged = flight_table.alias("ft").join(
            osn_aircraft_db.alias("adb"),
            col("ft.ICAO24") == col("adb.icao24"),
            "left",
        )

        merged_upper = merged.select(
            *[col(f"ft.{c}").alias(c.upper()) for c in flight_table.columns],
            *[col(f"adb.{c}").alias(c.upper()) for c in osn_aircraft_db.columns if c != "icao24"],
        )

        columns = [
            "ID",
            "ICAO24",
            "FLT_ID",
            "DOF",
            "ADEP",
            "ADES",
            "ADEP_P",
            "ADES_P",
            "REGISTRATION",
            "MODEL",
            "TYPECODE",
            "ICAO_AIRCRAFT_CLASS",
            "ICAO_OPERATOR",
            "FIRST_SEEN",
            "LAST_SEEN",
            "VERSION",
        ]
        # Both passes of step 03 write into the same day partition, so they
        # must write the same columns in the same order. A frame without the
        # two source columns gets them as NULL.
        for name in ("ADEP_SOURCE", "ADES_SOURCE"):
            if name not in merged_upper.columns:
                merged_upper = merged_upper.withColumn(name, lit(None).cast("string"))
        columns += ["ADEP_SOURCE", "ADES_SOURCE"]
        return merged_upper.select(*columns)

    # ------------------------------------------------------------------
    # Overflight processing
    # ------------------------------------------------------------------

    def _fetch_overflights(self, month: date) -> DataFrame:
        """Overflights: tracks of at least 5 minutes not yet in the flight list.

        Args:
            month: Month to process.

        Returns:
            One row per overflight, without airports, and with both source
            columns ``undetermined``.
        """
        sv = self._get_data_within_timeframe(self.tracks_table, month)
        fl = self._get_data_within_timeframe(
            "opdi_flight_list",
            month,
            time_col="first_seen",
        ).select("id")

        # No fillna here: FLT_ID is written as it comes. In a default run it
        # is already the track's call sign, blank-filled; in a v0.0.2 run it
        # stays as that release carried it, NULLs and padding included.
        #
        # Only each track's first report is kept below, so without the call
        # sign of the whole track an overflight would be named after whatever
        # the aircraft sent first, often nothing.

        window_track = Window.partitionBy("track_id")
        sv = sv.withColumn("event_time", F.to_timestamp(col("event_time")))
        if FlightListProcessor._seen_from_measured_reports(self):
            # The first and last report with a measured position, or with any
            # measured value when the track has no position
            # (EventConfig.seen_from_measured_reports): the smallest
            # (rank, time) and (rank, minus time) of the track. One
            # aggregation by track, which sends one row per track and part
            # across the cluster rather than every report.
            sv = sv.withColumn("_seen_rank", seen_rank(sv))
            first = F.struct(
                col("_seen_rank").alias("r"),
                col("event_time").alias("t"),
                col("icao24").alias("icao24"),
                col("flight_id").alias("flight_id"),
            )
            last = F.struct(
                col("_seen_rank").alias("r"),
                (-col("event_time").cast("double")).alias("t"),
                col("event_time").alias("ts"),
            )
            per_track = sv.groupBy("track_id").agg(
                f_min(first).alias("_first"),
                f_min(last).alias("_last"),
                f_min("event_time").alias("_t0"),
                f_max("event_time").alias("_t1"),
            )
            # The five-minute rule reads the time between the first and last
            # report of any kind, as without the setting, so the same tracks
            # become overflight rows.
            sv_f = (
                per_track.filter(unix_timestamp("_t1") - unix_timestamp("_t0") >= 300)
                .select(
                    col("track_id"),
                    col("_first.icao24").alias("icao24"),
                    col("_first.flight_id").alias("flight_id"),
                    col("_first.t").alias("first_seen"),
                    col("_last.ts").alias("last_seen"),
                )
                .withColumn("DOF", to_date("first_seen"))
            )
        else:
            sv = sv.withColumn("first_seen", f_min("event_time").over(window_track))
            sv = sv.withColumn("last_seen", f_max("event_time").over(window_track))

            # Keep only the first row per track
            sv_f = sv.filter(col("first_seen") == col("event_time"))

            sv_f = sv_f.withColumn("event_date", to_date("event_time"))
            sv_f = sv_f.withColumn("DOF", f_min("event_date").over(window_track))
        sv_f = sv_f.withColumnRenamed("track_id", "id")
        sv_f = sv_f.withColumnRenamed("icao24", "ICAO24")
        sv_f = sv_f.withColumnRenamed("flight_id", "FLT_ID")

        for col_name in ["ADEP", "ADES", "ADEP_P", "ADES_P"]:
            sv_f = sv_f.withColumn(col_name, lit(None).cast("string"))
        # Neither end of an overflight was placed at an aerodrome.
        for col_name in ["adep_source", "ades_source"]:
            sv_f = sv_f.withColumn(col_name, lit("undetermined"))
        sv_f = sv_f.withColumn("version", lit(self._version_for("trend")))

        sv_f = sv_f.select(
            "id",
            "ADEP",
            "ADES",
            "ADEP_P",
            "ADES_P",
            "adep_source",
            "ades_source",
            "ICAO24",
            "FLT_ID",
            "first_seen",
            "last_seen",
            "DOF",
            "version",
        )

        # Anti-join to exclude flights already in the flight list
        fl_broadcast = broadcast(fl)
        sv_f = sv_f.join(fl_broadcast, sv_f.id == fl.id, "left_anti")

        # Filter out short ADS-B signals (< 5 min); with measured first and last
        # seen this was done above, on all reports.
        if not FlightListProcessor._seen_from_measured_reports(self):
            sv_f = sv_f.filter((unix_timestamp("last_seen") - unix_timestamp("first_seen")) >= 300)

        return sv_f

    # ------------------------------------------------------------------
    # Main processing entry points
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_roles(
        adep_table: DataFrame,
        ades_table: DataFrame,
        adep_mode: str,
        ades_mode: str,
    ) -> DataFrame:
        """The departure side from one flight table, the arrival side from another.

        Both tables have the same columns, keyed on ``id``. When both ends use
        the same method, the two arguments are the same table, which is
        returned unchanged.
        """
        if adep_mode == ades_mode:
            return adep_table

        dep = adep_table.select(
            "id",
            "ADEP",
            "ADEP_P",
            "adep_source",
            "ICAO24",
            "FLT_ID",
            "first_seen",
            "last_seen",
            "DOF",
            "version",
        )
        arr = ades_table.select(
            col("id").alias("_id"),
            "ADES",
            "ADES_P",
            "ades_source",
            col("ICAO24").alias("_ICAO24"),
            col("FLT_ID").alias("_FLT_ID"),
            col("first_seen").alias("_first_seen"),
            col("last_seen").alias("_last_seen"),
            col("DOF").alias("_DOF"),
            col("version").alias("_version"),
        )

        # Outer join: a track that only one method can place keeps the half
        # that worked.
        j = dep.join(arr, dep.id == arr._id, "outer")

        # The track's own columns are the same on both sides, but only one
        # side exists for a track that one method alone saw.
        return j.select(
            F.coalesce(col("id"), col("_id")).alias("id"),
            "ADEP",
            "ADES",
            "ADEP_P",
            "ADES_P",
            F.coalesce(col("ICAO24"), col("_ICAO24")).alias("ICAO24"),
            F.coalesce(col("FLT_ID"), col("_FLT_ID")).alias("FLT_ID"),
            F.coalesce(col("first_seen"), col("_first_seen")).alias("first_seen"),
            F.coalesce(col("last_seen"), col("_last_seen")).alias("last_seen"),
            F.coalesce(col("DOF"), col("_DOF")).alias("DOF"),
            F.coalesce(col("version"), col("_version")).alias("version"),
            F.coalesce(col("adep_source"), lit("undetermined")).alias("adep_source"),
            F.coalesce(col("ades_source"), lit("undetermined")).alias("ades_source"),
        )

    #: Set only by :meth:`process_day`, and only while it runs. Read by
    #: :meth:`_get_data_within_timeframe` as the default batch selector.
    _batch_day = None

    @_releases_zones
    def process_day(
        self,
        day: date,
        airports_hex_path: Optional[str] = None,
        skip_if_processed: bool = True,
        adep_mode: Optional[str] = None,
        ades_mode: Optional[str] = None,
    ) -> None:
        """Build the flight list of the tracks that started on ``day``.

        Runs the departure-and-arrival pass, then the overflight pass. The
        first replaces the day's folder and the second adds to it, so the
        order matters.

        The two passes read through :meth:`_get_data_within_timeframe`, which
        takes the day from the processor; it is cleared afterwards, even on
        failure, so a later monthly call is not limited to one day.

        The endpoint candidates are always built again, because a day is run
        again when its tracks were rebuilt, and old candidates would refer to
        tracks that no longer exist.
        """
        self._batch_day = day
        try:
            self.process_dai(
                month=day,
                airports_hex_path=airports_hex_path,
                skip_if_processed=skip_if_processed,
                adep_mode=adep_mode,
                ades_mode=ades_mode,
                rebuild_candidates=True,
            )
            self.process_overflights(day, skip_if_processed)
        finally:
            self._batch_day = None

    @_releases_zones
    def process_dai(
        self,
        month: date,
        airports_hex_path: Optional[str] = None,
        skip_if_processed: bool = True,
        mode: Optional[str] = None,
        adep_mode: Optional[str] = None,
        ades_mode: Optional[str] = None,
        abstention_radius_nm: Optional[float] = None,
        abstention_height_ft: Optional[float] = None,
        sched_penalty_nm: Optional[float] = None,
        table_name: str = "opdi_flight_list",
        write_mode: str = "overwrite",
        rebuild_candidates: bool = False,
    ) -> None:
        """The departure-and-arrival pass of step 03 for a month (or a day).

        Args:
            month: Month to process (any day of it), or the day when
                :meth:`process_day` runs.
            airports_hex_path: Local Parquet file of the airport zones, used
                when the warehouse table does not exist.
            skip_if_processed: Skip a month the log records as processed.
            mode: Method for both ends; ``adep_mode``/``ades_mode`` take
                precedence.
            adep_mode: Method for the departure: ``"trend"`` or
                ``"endpoint"``. Default: ``config.detection.adep_mode``.
            ades_mode: Method for the destination. Default:
                ``config.detection.ades_mode``.
            abstention_radius_nm: ``endpoint`` radius, in NM. Default:
                ``config.detection.endpoint_radius_nm``.
            abstention_height_ft: ``endpoint`` height limit, in feet. Default:
                ``config.detection.endpoint_height_ft``.
            sched_penalty_nm: ``endpoint`` penalty for an airport without
                scheduled service, in NM.
            table_name: Table to write.
            write_mode: Write mode; ``overwrite`` replaces only the days
                written.
            rebuild_candidates: Build the endpoint candidates again even where
                the log says they exist. :meth:`process_day` sets it.

        Raises:
            ValueError: If a method name is unknown.
        """
        if skip_if_processed and month in self._load_processed_months(self._dai_log):
            print(f"Month DAI {month} already processed. Skipping.")
            return

        d = self.detection

        # Each end has its own method; `mode` sets both. The default comes
        # from the configuration (DetectionConfig.legacy() sets "trend" for
        # both, as in release v0.0.2).
        if adep_mode is None:
            adep_mode = mode if mode is not None else getattr(d, "adep_mode", "trend")
        if ades_mode is None:
            ades_mode = mode if mode is not None else getattr(d, "ades_mode", "trend")

        if abstention_radius_nm is None:
            abstention_radius_nm = d.endpoint_radius_nm
        if abstention_height_ft is None:
            abstention_height_ft = d.endpoint_height_ft
        if sched_penalty_nm is None:
            sched_penalty_nm = d.endpoint_sched_penalty_nm

        needed = {adep_mode, ades_mode}
        unknown = needed - set(self.DETECTION_MODES)
        if unknown:
            raise ValueError(f"unknown detection mode(s): {sorted(unknown)}")

        print(f"Processing DAI for {month} (ADEP={adep_mode}, ADES={ades_mode})...")

        tables: dict = {}

        # The penalty for an airport without scheduled service is not added
        # for the airport the aircraft is at (both methods).
        waiver = (
            (d.sched_penalty_waiver_radius_nm, d.sched_penalty_waiver_height_ft)
            if getattr(d, "sched_penalty_waived_at_field", False)
            else None
        )

        if "trend" in needed:
            sdf_apt = self._load_airports_hex(airports_hex_path)
            sv_nearby = self._fetch_and_label_sv(month, sdf_apt)
            sv_classified = self._categorize_landing_take_off(
                sv_nearby,
                smooth_half_window=d.trend_smooth_half_window,
                vote_margin=d.trend_vote_margin,
            )
            t = self._compute_flight_table(
                sv_classified,
                sched_penalty_nm=d.trend_sched_penalty_nm,
                rank_by=getattr(d, "trend_rank_by", "ring"),
                bearing_tiebreak_nm=getattr(d, "trend_bearing_tiebreak_nm", 0.0),
                version=self._version_for("trend"),
                last_landing_window_s=(
                    d.trend_last_landing_window_s
                    if getattr(d, "trend_last_landing", False)
                    else None
                ),
                waiver=waiver,
            )
            tables["trend"] = self._trend_sources(t, month)

        for m in sorted(needed - {"trend"}):
            # Not tied to skip_if_processed: new thresholds do not make the
            # candidates out of date.
            self.build_endpoint_candidates(
                month,
                max_radius_nm=d.endpoint_candidate_radius_nm,
                rebuild=rebuild_candidates,
            )
            cand = self._get_data_within_timeframe("opdi_endpoint_candidates", month)
            classified = self.classify_endpoints(
                cand,
                mode=m,
                abstention_radius_nm=abstention_radius_nm,
                abstention_height_ft=abstention_height_ft,
                sched_penalty_nm=sched_penalty_nm,
                waiver=waiver,
                min_climb_ft=(
                    d.endpoint_min_altitude_change_ft
                    if getattr(d, "endpoint_airborne_needs_climb", False)
                    else None
                ),
                min_climb_above_ft=getattr(d, "endpoint_climb_above_ft", 1000.0),
            )
            tables[m] = self._flight_table_from_endpoints(
                classified,
                cand,
                version=self._version_for(m),
                alternatives_radius_nm=(
                    abstention_radius_nm
                    if getattr(d, "alternatives_within_endpoint_radius", False)
                    else None
                ),
                sched_penalty_nm=sched_penalty_nm,
                waiver=waiver,
                both_ends=getattr(d, "endpoint_ids_from_both_ends", False),
            )

        flight_table = self._merge_roles(tables[adep_mode], tables[ades_mode], adep_mode, ades_mode)

        flight_table = self._add_osn_aircraft_db_data(flight_table)

        # Write.
        #
        # ``DOF`` is the day the track starts. The write replaces only the
        # days present in this frame (the table is partitioned by DOF), so
        # processing a day again replaces it rather than adding to it.
        # ``process_overflights`` then appends into the same days, so it must
        # run after this pass and must not overwrite.
        flight_table = flight_table.repartition("DOF").orderBy("DOF")
        self.storage.write_table(flight_table, table_name, mode=write_mode, partition_by=["DOF"])

        self._mark_month_processed(month, self._dai_log)
        print(f"DAI processing complete for {month}.")

    @_releases_zones
    def process_overflights(
        self,
        month: date,
        skip_if_processed: bool = True,
    ) -> None:
        """The overflight pass of step 03 for a month (or a day).

        Appends the overflights to ``opdi_flight_list``, after
        :meth:`process_dai`.

        Args:
            month: Month to process.
            skip_if_processed: Skip a month the log records as processed.
        """
        if skip_if_processed and month in self._load_processed_months(self._overflight_log):
            print(f"Month overflights {month} already processed. Skipping.")
            return

        print(f"Processing overflights for {month}...")
        flight_table = self._fetch_overflights(month)
        flight_table = self._add_osn_aircraft_db_data(flight_table)

        flight_table = flight_table.repartition("DOF").orderBy("DOF")
        # Append, never overwrite: this lands in partitions ``process_dai``
        # has already filled for the same days, and replacing them here would
        # delete every departure and arrival it just wrote.
        self.storage.write_table(
            flight_table, "opdi_flight_list", mode="append", partition_by=["DOF"]
        )

        self._mark_month_processed(month, self._overflight_log)
        print(f"Overflight processing complete for {month}.")

    @_releases_zones
    def process_date_range(
        self,
        start_month: date,
        end_month: date,
        airports_hex_path: Optional[str] = None,
        skip_if_processed: bool = True,
        adep_mode: Optional[str] = None,
        ades_mode: Optional[str] = None,
    ) -> None:
        """Build the flight list of a range of months.

        Runs the departure-and-arrival pass for every month, then the
        overflight pass for every month.

        Args:
            start_month: First month to process.
            end_month: Last month to process.
            airports_hex_path: Local Parquet file of the airport zones.
            skip_if_processed: Skip months already processed.
            adep_mode: Method for the departure; see :meth:`process_dai`.
            ades_mode: Method for the destination; see :meth:`process_dai`.

        Example:
            >>> processor = FlightListProcessor(spark, config)
            >>> processor.process_date_range(date(2024, 1, 1), date(2024, 6, 1))
        """
        months = generate_months(start_month, end_month)
        print(f"Processing flight list for {len(months)} months...")

        for month in months:
            # By keyword, so no setting is passed to the wrong parameter.
            self.process_dai(
                month,
                airports_hex_path=airports_hex_path,
                skip_if_processed=skip_if_processed,
                adep_mode=adep_mode,
                ades_mode=ades_mode,
            )

        for month in months:
            self.process_overflights(month, skip_if_processed)

        print(f"Flight list processing complete for {start_month} to {end_month}.")

    def create_table_if_not_exists(self) -> None:
        """Create the ``opdi_flight_list`` Iceberg table if it does not exist."""
        today = datetime.today().strftime("%d %B %Y")
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`opdi_flight_list` (
            ID STRING COMMENT 'Unique flight identifier (track_id)',
            ICAO24 STRING COMMENT '24-bit ICAO transponder address',
            FLT_ID STRING COMMENT 'Flight callsign',
            DOF DATE COMMENT 'Date of flight',
            ADEP STRING COMMENT 'Departure airport (ICAO code), OOA out of area, NULL unknown',
            ADES STRING COMMENT 'Destination airport (ICAO code), OOA out of area, NULL unknown',
            ADEP_P STRING COMMENT 'Alternative departure airports',
            ADES_P STRING COMMENT 'Alternative destination airports',
            REGISTRATION STRING COMMENT 'Aircraft registration',
            MODEL STRING COMMENT 'Aircraft model',
            TYPECODE STRING COMMENT 'ICAO type designator',
            ICAO_AIRCRAFT_CLASS STRING COMMENT 'ICAO aircraft class',
            ICAO_OPERATOR STRING COMMENT 'ICAO operator code',
            FIRST_SEEN TIMESTAMP COMMENT 'First ADS-B reception time',
            LAST_SEEN TIMESTAMP COMMENT 'Last ADS-B reception time',
            ADEP_SOURCE STRING COMMENT 'aerodrome, runway_event, out_of_area or undetermined',
            ADES_SOURCE STRING COMMENT 'aerodrome, runway_event, out_of_area or undetermined',
            VERSION STRING COMMENT 'Version of the flight list method'
        )
        USING iceberg
        PARTITIONED BY (days(FIRST_SEEN))
        COMMENT 'OPDI flight list. Last updated: {today}.'
        """
        self.storage.create_table(create_sql)
        print(f"Table {self.project}.opdi_flight_list created/verified.")
