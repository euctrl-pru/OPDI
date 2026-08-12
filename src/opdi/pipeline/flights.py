"""
Flight list generation module.

Creates flight-level summaries from processed tracks by:
1. Detecting departures and arrivals using H3 airport zones
2. Classifying flights as take-off, landing, or overflight
3. Enriching with aircraft metadata from the OSN aircraft database
4. Producing the OPDI flight list table

Ported from: OPDI-live/python/v2.0.0/03_opdi_flight_list.py
"""

import os
from datetime import date, datetime
from typing import List, Optional

import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    broadcast,
    col,
    collect_list,
    concat_ws,
    expr,
    lit,
    max as f_max,
    min as f_min,
    radians,
    row_number,
    sin,
    cos,
    sqrt,
    atan2,
    to_date,
    to_timestamp,
    unix_timestamp,
    when,
)
from pyspark.sql.window import Window

from opdi.config import OPDIConfig
from opdi.utils.datetime_helpers import (
    generate_months,
    get_start_end_of_month,
)
from opdi.utils.storage import StorageManager


#: Ingestion bounding box, from ``StateVectorIngestion.DEFAULT_BBOX``. Used to
#: decide whether a track endpoint has left the observed area.
BBOX = (-25.86653, 26.74617, 49.65699, 70.25976)  # min_lon, min_lat, max_lon, max_lat

#: How close to the bbox edge an endpoint must be to read as "left the area"
#: rather than "stopped being seen". Reception falls off before the nominal
#: edge, so this is deliberately generous.
BORDER_MARGIN_NM = 30.0

#: Marker for an aerodrome outside the observed area. A flight that entered
#: European airspace already airborne has an origin no ADS-B feed here can
#: name, and saying so is a different -- and correct -- answer from silence.
OOA = "OOA"

NM_PER_DEG = 60.0
EARTH_R_NM = 3440.065


def haversine_nm(lat1, lon1, lat2, lon2):
    """Great-circle distance in nautical miles between two column pairs."""
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = (
        sin(dlat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    )
    return lit(2 * EARTH_R_NM) * F.asin(F.sqrt(F.least(a, lit(1.0))))


def at_border(lat, lon, margin_nm: float = BORDER_MARGIN_NM):
    """True where a position sits within *margin_nm* of the ingestion bbox edge.

    The longitude margin is scaled by 1/cos(latitude): a degree of longitude
    shrinks toward the pole, so an unscaled margin would be twice as strict in
    northern Norway as in the Canaries.
    """
    min_lon, min_lat, max_lon, max_lat = BBOX
    dlat = margin_nm / NM_PER_DEG
    dlon = dlat / F.greatest(cos(radians(lat)), lit(0.1))
    return (
        (lat <= lit(min_lat) + dlat)
        | (lat >= lit(max_lat) - dlat)
        | (lon <= lit(min_lon) + dlon)
        | (lon >= lit(max_lon) - dlon)
    )


class FlightListProcessor:
    """
    Generates the OPDI flight list from processed track data.

    The flight list is produced in two phases:

    * **DAI (Departures/Arrivals/Internal)** -- Identifies flights with
      known departure and/or arrival airports by matching track points
      to H3 airport detection zones within 30 NM and below FL40.
    * **Overflights** -- Captures remaining tracks (no airport match)
      that have ADS-B signals lasting at least 5 minutes.

    Each flight is enriched with aircraft metadata from the OSN aircraft
    database (registration, model, typecode, operator).

    Args:
        spark: Active SparkSession.
        config: OPDI configuration object.
        log_dir: Directory for processing progress logs.

    Example:
        >>> processor = FlightListProcessor(spark, config)
        >>> processor.process_date_range(date(2024, 1, 1), date(2024, 3, 1))
    """

    # Kept so anything importing them still resolves, but the pipeline reads
    # ``config.detection``. These are the pre-V6 values, identical to
    # :meth:`DetectionConfig.legacy`.
    MAX_FL = 40  # Maximum flight level for airport zone matching
    # Detection radius applied to the airport zone table. The table is
    # generated wider than this so one reference can serve both the flight list
    # and the ASMA ring crossings; this is what keeps flight-list behaviour
    # unchanged when the reference is regenerated with a longer reach.
    DETECTION_RADIUS_NM = 30

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        log_dir: str = "OPDI_live/logs",
    ):
        self.spark = spark
        self.config = config
        self.storage = StorageManager(spark, config)
        self.project = config.project.project_name
        self.resolution = config.h3.airport_detection_resolution
        # A config built before DetectionConfig existed still works, and gets
        # the pre-V6 constants rather than the tuned ones -- silently upgrading
        # an old caller's behaviour is exactly what must not happen here.
        self.detection = getattr(config, "detection", None)
        if self.detection is None:
            from opdi.config import DetectionConfig

            self.detection = DetectionConfig.legacy()
        self.log_dir = log_dir

        self._dai_log = os.path.join(log_dir, "03_osn-flight_table-etl-log-v2.parquet")
        # Same processed-month convention as the flight list and overflights,
        # so the candidate cache is invalidated and resumed the same way.
        self._endpoint_log = os.path.join(
            log_dir, "03_osn-endpoint_candidates-etl-log.parquet"
        )
        self._overflight_log = os.path.join(
            log_dir, "03_osn-flight_table-overflights-etl-log-v2.parquet"
        )

        os.makedirs(log_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Progress tracking
    # ------------------------------------------------------------------

    def _load_processed_months(self, log_path: str) -> List[date]:
        """Load list of already processed months from log file."""
        if os.path.isfile(log_path):
            return pd.read_parquet(log_path).months.to_list()
        return []

    def _mark_month_processed(self, month: date, log_path: str) -> None:
        """Mark a month as processed."""
        processed = self._load_processed_months(log_path)
        if month not in processed:
            processed.append(month)
            pd.DataFrame({"months": processed}).to_parquet(log_path)

    # ------------------------------------------------------------------
    # Data retrieval
    # ------------------------------------------------------------------

    def _get_data_within_timeframe(
        self, table_name: str, month: date, time_col: str = "event_time"
    ) -> DataFrame:
        """Retrieve records from a table within a monthly timeframe."""
        start_ts, end_ts = get_start_end_of_month(month)
        start_lit = to_timestamp(lit(start_ts))
        end_lit = to_timestamp(lit(end_ts))

        df = self.storage.read_table(table_name)
        return df.filter((col(time_col) >= start_lit) & (col(time_col) < end_lit))

    def _load_airports_hex(self, airports_hex_path: Optional[str] = None) -> DataFrame:
        """
        Load the airport hex detection zone reference data.

        Reads from the StorageManager table ``h3_airport_detection_zones``
        if available, otherwise falls back to a local parquet file.

        Args:
            airports_hex_path: Path to local parquet file (fallback).

        Returns:
            Spark DataFrame with airport hex zones.
        """
        if self.storage.table_exists("h3_airport_detection_zones"):
            return self.storage.read_table("h3_airport_detection_zones")
        if airports_hex_path:
            df_apt = pd.read_parquet(airports_hex_path)
            return self.spark.createDataFrame(df_apt.to_dict(orient="records"))
        raise FileNotFoundError(
            "No airport hex zones found. Run AirportDetectionZoneGenerator.save_prepared_to_table() first, "
            "or provide airports_hex_path."
        )

    # ------------------------------------------------------------------
    # DAI processing (Departures / Arrivals / Internal)
    # ------------------------------------------------------------------

    def _candidates(
        self,
        sv: DataFrame,
        sdf_apt: DataFrame,
        max_radius_nm: float,
        sched_penalty_nm: float = 0.0,
    ) -> DataFrame:
        """Candidate (sample, aerodrome) pairs, with exact distance.

        H3 is the index and haversine is the comparison. The zone table is
        generated wide and carries ``apt_max_c_radius_nm``, so the reach is
        chosen here rather than baked into the reference -- which is what makes
        a radius sweep possible without regenerating anything.

        ``sched_penalty_nm`` pushes back aerodromes without scheduled service.
        A departing aircraft's first recorded fix is often already a few miles
        into the climb, and where a military field sits between the airport and
        the departure track it wins on raw distance. A penalty of 0 reproduces
        the unbiased ranking exactly.
        """
        radius_col = next(
            (c for c in ("apt_max_c_radius_nm", "max_c_radius_nm") if c in sdf_apt.columns),
            None,
        )
        if radius_col:
            sdf_apt = sdf_apt.filter(col(radius_col) <= float(max_radius_nm))

        j = sv.join(sdf_apt, sv.h3_res_7 == sdf_apt.apt_hex_id, "inner")
        j = j.withColumn(
            "dist_nm",
            haversine_nm(col("lat"), col("lon"),
                         col("apt_latitude_deg"), col("apt_longitude_deg")),
        )
        # Beyond the ring reach the H3 cell can still match while the exact
        # distance does not; the band is a coarse index, not the answer.
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

    def build_endpoint_candidates(
        self,
        month: date,
        max_radius_nm: float = 110.0,
        rebuild: bool = False,
    ) -> None:
        """Materialise candidate aerodromes for each track's first/last sample.

        This is the cache the threshold sweeps run over. Deriving candidates
        from state vectors is the expensive part; once materialised, any
        (radius, height, penalty) combination is a filter and a re-rank over a
        few million rows rather than a pass over hundreds of millions.

        Only the two endpoint samples per track are kept, which is what makes
        the full 110 NM reach affordable -- roughly a million rows to join
        instead of the whole month of state vectors.
        """
        if not rebuild and month in self._load_processed_months(self._endpoint_log):
            print(f"  endpoint candidates for {month:%Y-%m} already built, skipping")
            return

        sv = self._get_data_within_timeframe("osn_tracks", month)
        sv = sv.dropna(subset=["lat", "lon", "track_id"])
        sv = sv.withColumnRenamed("callsign", "flight_id").fillna({"flight_id": ""})

        w = Window.partitionBy("track_id").orderBy("event_time")
        ends = (
            sv.withColumn("_rn", row_number().over(w))
            .withColumn("_rr", row_number().over(w.orderBy(col("event_time").desc())))
            .filter((col("_rn") == 1) | (col("_rr") == 1))
            .withColumn("role", when(col("_rn") == 1, lit("adep")).otherwise(lit("ades")))
            .withColumn("at_border", at_border(col("lat"), col("lon")))
            .select(
                "track_id", "icao24", "flight_id", "role", "event_time",
                "lat", "lon", "baro_altitude", "on_ground", "at_border", "h3_res_7",
            )
        )

        sdf_apt = self._load_airports_hex()
        cand = self._candidates(ends, sdf_apt, max_radius_nm=max_radius_nm)

        # The detection zones carry geometry, not aerodrome metadata, so
        # elevation comes from OurAirports. Without it the height test can only
        # be satisfied by on_ground and the endpoint mode collapses into a
        # surface-samples-only rule.
        if self.storage.table_exists("oa_airports"):
            elev_ref = self.storage.read_table("oa_airports").select(
                col("ident").alias("_elev_ident"),
                col("elevation_ft").cast("double").alias("apt_elevation_ft"),
            )
            cand = cand.join(
                broadcast(elev_ref), cand.apt_ident == col("_elev_ident"), "left"
            ).drop("_elev_ident")

        elev = next(
            (c for c in ("apt_elevation_ft", "elevation_ft") if c in cand.columns), None
        )
        if elev:
            # Height above the aerodrome, not above the ellipsoid: a fixed
            # cut-off means nothing at a field sitting at 5,000 ft.
            cand = cand.withColumn(
                "agl_ft", col("baro_altitude") * 3.28084 - col(elev)
            ).withColumn("elev_known", col(elev).isNotNull())
        else:
            cand = cand.withColumn("agl_ft", lit(None).cast("double")).withColumn(
                "elev_known", lit(False)
            )

        keep = [
            "track_id", "icao24", "flight_id", "role", "event_time",
            "apt_ident", "dist_nm", "eff_nm", "agl_ft", "elev_known",
            "on_ground", "at_border", "lat", "lon",
        ]
        for extra in ("apt_min_c_radius_nm", "apt_max_c_radius_nm",
                      "apt_scheduled", "apt_type"):
            if extra in cand.columns:
                keep.append(extra)

        # overwrite, not append: a rebuild of the same month must replace it.
        # Appending silently doubled the cache when the month was rebuilt, and
        # a doubled cache does not fail -- it just weights every aggregate.
        self.storage.write_table(
            cand.select(*keep), "opdi_endpoint_candidates", mode="overwrite"
        )
        self._mark_month_processed(month, self._endpoint_log)
        print(f"  endpoint candidates for {month:%Y-%m} written")

    def _fetch_and_label_sv(
        self, month: date, sdf_apt: DataFrame
    ) -> DataFrame:
        """
        Fetch track data and label state vectors near airports.

        Steps:
        1. Fetch tracks for the month
        2. Add flight level from baro_altitude
        3. Add first_seen, last_seen, DOF per track
        4. Filter to below FL40
        5. Join with airport hex zones within 30 NM

        Args:
            month: Month to process.
            sdf_apt: Airport hex zone reference DataFrame.

        Returns:
            DataFrame of state vectors near airports.
        """
        sv = self._get_data_within_timeframe("osn_tracks", month)

        sv_f = sv.dropna(subset=["lat", "lon", "baro_altitude", "track_id"])
        sv_f = sv_f.withColumnRenamed("callsign", "flight_id")
        sv_f = sv_f.fillna({"flight_id": ""})
        sv_f = sv_f.withColumn("event_time", F.to_timestamp(col("event_time")))
        sv_f = sv_f.withColumn(
            "flight_level", (col("baro_altitude") * 3.28084 / 100).cast("int")
        )

        columns_of_interest = [
            "track_id", "icao24", "flight_id", "event_time",
            "lat", "lon", "flight_level", "baro_altitude",
            "heading", "vert_rate", "on_ground", "h3_res_7",
        ]
        sv_f = sv_f.select(columns_of_interest)

        # Per-track first/last seen and DOF
        window_track = Window.partitionBy("track_id")
        sv_f = sv_f.withColumn("first_seen", f_min("event_time").over(window_track))
        sv_f = sv_f.withColumn("last_seen", f_max("event_time").over(window_track))
        sv_f = sv_f.withColumn("DOF", to_date("first_seen"))

        # Filter to low altitude (below FL40) and join with airport zones
        sv_low_alt = sv_f.filter(col("flight_level") <= self.detection.trend_max_fl)
        # The zone table is generated out to its full ring reach, so the
        # detection radius has to be applied here. Without this filter the join
        # would match aerodromes as far out as the rings go -- a large, silent
        # change in behaviour disguised as a reference-data update.
        radius_col = next(
            (c for c in ("apt_max_c_radius_nm", "max_c_radius_nm") if c in sdf_apt.columns),
            None,
        )
        if radius_col:
            sdf_apt = sdf_apt.filter(
                col(radius_col) <= self.detection.trend_radius_nm
            )
        else:
            # Pre-dates the banded reference, which was clipped at generation
            # time -- so no filter is the correct behaviour. Said out loud
            # because the alternative reading (a banded table whose column was
            # renamed) would silently widen detection to the full ring reach.
            print(
                "  NOTE: airport zones carry no radius band; using the table as "
                "generated. Regenerate step 00a to filter by radius at read time."
            )

        sv_nearby_apt = sv_low_alt.join(
            sdf_apt, sv_low_alt.h3_res_7 == sdf_apt.apt_hex_id, "left"
        )
        return sv_nearby_apt

    @staticmethod
    def classify_endpoints(
        cand: DataFrame,
        mode: str = "nearest",
        abstention_radius_nm: float = 40.0,
        abstention_height_ft: float = 15000.0,
        sched_penalty_nm: float = 10.0,
        ooa: bool = True,
    ) -> DataFrame:
        """Pick one aerodrome per (track, role) from cached endpoint candidates.

        Two of the three modes live here, because both read the same evidence --
        the track's first and last sample -- and differ only in whether they are
        willing to stay silent.

        ``nearest`` (M1)
            Faithful to ``traffic``'s rule: the closest aerodrome wins, with no
            altitude, ground-state or trend condition. It never abstains, so its
            only silence comes from having no candidate at all.

        ``endpoint`` (M7)
            The same naming rule plus an explicit test: emit only when the
            endpoint is within *abstention_radius_nm* and no more than
            *abstention_height_ft* above field elevation, or is flagged
            ``on_ground``. Otherwise fall back to the out-of-area marker when
            the endpoint sits at the ingestion boundary, and to null when it
            does not.

        Precedence in ``endpoint`` is aerodrome first, border second. The other
        order looks equivalent and is not: Ponta Delgada sits about 8 NM inside
        the western edge, and letting the border test win labelled its
        departures out-of-area with the aircraft still on the runway.
        """
        if mode not in ("nearest", "endpoint"):
            raise ValueError(f"classify_endpoints: unsupported mode {mode!r}")

        # Re-rank here rather than trusting a cached eff_nm, so the penalty can
        # be swept without rebuilding the cache.
        scheduled = "apt_scheduled" if "apt_scheduled" in cand.columns else None
        penalty = (
            when(col(scheduled) == "yes", lit(0.0)).otherwise(lit(float(sched_penalty_nm)))
            if scheduled and sched_penalty_nm
            else lit(0.0)
        )
        cand = cand.withColumn("_eff", col("dist_nm") + penalty)

        w = Window.partitionBy("track_id", "role").orderBy(col("_eff").asc_nulls_last())
        best = cand.withColumn("_r", row_number().over(w)).filter(col("_r") == 1)

        if mode == "nearest":
            return best.withColumn("apt", col("apt_ident")).withColumn(
                "source",
                when(col("apt_ident").isNotNull(), lit("aerodrome")).otherwise(
                    lit("undetermined")
                ),
            )

        ok = col("dist_nm") <= float(abstention_radius_nm)
        # on_ground satisfies the height test on its own: a missing barometric
        # altitude on the surface is normal. Where the aerodrome elevation is
        # unknown, agl_ft is really MSL, so fall back to the ground flag rather
        # than compare against the wrong datum.
        ok = ok & (
            col("on_ground")
            | (col("elev_known") & (col("agl_ft") <= float(abstention_height_ft)))
        )

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
        """
        Classify each track-airport pair as take-off, landing, or ambiguous.

        Uses a smoothed altitude change analysis: if the altitude is mostly
        increasing near the airport, it's a take-off; if decreasing, it's a
        landing. The margin prevents noise from flipping the classification.

        Both knobs were literals until they were measured. Kept as keyword
        arguments rather than read from ``self`` so this stays a static method,
        matching :meth:`classify_endpoints`, and so a sweep can call it
        directly without constructing a processor.

        Args:
            df: DataFrame from _fetch_and_label_sv.
            smooth_half_window: half-width of the centred rolling mean over
                ``baro_altitude``, in samples. 2 gives the original 5-sample
                window.
            vote_margin: one direction must beat the other by this many samples
                or the pair is ``ambiguous``. At 5 s sampling, 4 is about 20 s.

        Returns:
            DataFrame with 'status' column (take-off / landing / ambiguous).
        """
        window_spec = Window.partitionBy(
            ["icao24", "flight_id", "track_id", "apt_ident"]
        ).orderBy("event_time")

        # Smoothed altitude
        window_avg = Window.partitionBy(
            ["icao24", "flight_id", "track_id", "apt_ident"]
        ).orderBy("event_time").rowsBetween(-smooth_half_window, smooth_half_window)

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
        flight_type_df = df_m.groupBy(
            ["icao24", "flight_id", "track_id", "apt_ident"]
        ).agg(
            F.sum(when(col("trajectory_type") == "take-off", 1).otherwise(0)).alias("take_off_count"),
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
    def _compute_flight_table(
        df: DataFrame,
        sched_penalty_nm: float = 0.0,
        rank_by: str = "ring",
    ) -> DataFrame:
        """
        Create the flight table from classified tracks.

        For each track with a take-off or landing classification:
        1. Find the state vector closest to the airport center
        2. Use Haversine distance to resolve multi-airport ambiguity
        3. Merge departures and arrivals into a single flight record

        ``sched_penalty_nm`` biases step 2 toward aerodromes with scheduled
        service, so a military field must be *clearly* nearest rather than
        merely nearest. It is applied to the **haversine distance**, not to
        ``distance_from_center`` -- that column is an H3 ring count at the
        zone resolution, an integer step of several kilometres, and cannot
        carry nautical miles.

        Zero, the default, reproduces the original behaviour exactly: the
        penalty column is never added and the ordering key is untouched.

        Args:
            df: DataFrame from _categorize_landing_take_off.
            sched_penalty_nm: nautical miles added to the effective distance of
                an aerodrome without scheduled service.

        Returns:
            DataFrame with ADEP, ADES, and flight metadata.
        """
        df = df.filter(df.status != "ambiguous")

        # Candidate selection. Two rules, and the difference between them is
        # larger than any threshold in this class.
        #
        # `ring` is the original: keep only the samples at the minimum H3 ring
        # count for the whole track-status pair, then break ties on exact
        # distance among the survivors. The window below is partitioned by
        # track and status but *not* by aerodrome, so the minimum is taken
        # across every candidate at once -- and an aerodrome one ring further
        # out is gone before its distance is ever computed. At resolution 7 a
        # ring is about 5.2 km, so "one ring further out" routinely means "a
        # kilometre further away".
        #
        # `haversine` measures first and chooses afterwards, which is what the
        # sweep harness always did and what every tuned parameter was scored
        # against.
        if rank_by == "ring":
            window_spec = Window.partitionBy(
                ["icao24", "flight_id", "track_id", "status"]
            )
            df = df.withColumn(
                "min_distance", f_min("distance_from_center").over(window_spec)
            )
            df_min = df.filter(df.distance_from_center == df.min_distance)
        elif rank_by == "haversine":
            # Distance from every sample to every candidate aerodrome, then the
            # closest approach per aerodrome. Ranking happens further down, on
            # `distance_km`, which this makes exact rather than ring-limited.
            df = df.withColumn(
                "_closest_nm",
                haversine_nm(
                    col("lat"), col("lon"),
                    col("apt_latitude_deg"), col("apt_longitude_deg"),
                ),
            )
            # Partitioned by aerodrome as well, so this is each candidate's own
            # closest approach rather than a cut across all of them. Nothing is
            # eliminated here -- every candidate survives to be ranked.
            per_apt = Window.partitionBy(
                ["icao24", "flight_id", "track_id", "status", "apt_ident"]
            )
            df = df.withColumn(
                "_min_nm", f_min("_closest_nm").over(per_apt)
            )
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
            raise ValueError(
                f"unknown trend_rank_by: {rank_by!r}; expected 'haversine' or 'ring'"
            )

        # The scheduled-service flag is carried through only when it is going
        # to be used. It is not part of the original projection, and dropping
        # it here is what silently disabled the penalty: the ranking further
        # down looks the column up on this frame, does not find it, and falls
        # back to raw distance -- which is indistinguishable from a penalty of
        # zero unless something says so.
        keep = [
            "icao24", "flight_id", "track_id", "apt_ident",
            "apt_longitude_deg", "apt_latitude_deg", "DOF",
            "first_seen", "last_seen", "status", "event_time",
            "lat", "lon", "min_distance", "take_off_count", "landing_count",
        ]
        if sched_penalty_nm:
            keep += [
                c for c in ("apt_scheduled", "scheduled_service")
                if c in df_min.columns
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
            flight_table
            .withColumn("lat1", radians(col("lat")))
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
                (c for c in ("apt_scheduled", "scheduled_service")
                 if c in flight_table.columns),
                None,
            )
            if sched_col is None:
                # Said out loud rather than silently ranking on raw distance:
                # a missing column would look exactly like a penalty of zero.
                print(
                    "  NOTE: airport zones carry no scheduled-service column; "
                    "trend_sched_penalty_nm has no effect. Regenerate step 00a."
                )
            else:
                flight_table = flight_table.withColumn(
                    "_eff_km",
                    col("distance_km")
                    + when(col(sched_col) == "yes", lit(0.0)).otherwise(
                        lit(float(sched_penalty_nm) * 1.852)
                    ),
                )
                order_col = col("_eff_km")

        window_closest = Window.partitionBy(key_columns).orderBy(order_col)

        df_numbered = flight_table.withColumn("row_number", row_number().over(window_closest))
        df_numbered = df_numbered.withColumn("is_most_likely", col("row_number") == 1)

        result_df = df_numbered.groupBy(key_columns).agg(
            expr("first(apt_ident) as most_likely_airport"),
            collect_list(
                expr("case when not is_most_likely then apt_ident end")
            ).alias("potential_airports"),
        )

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
            flight_table
            .withColumnRenamed("track_id", "id")
            .withColumnRenamed("icao24", "ICAO24")
            .withColumnRenamed("flight_id", "FLT_ID")
        )

        flight_table = flight_table.withColumn("version", lit("v2.0.0"))
        flight_table = flight_table.withColumn("ADEP_P", concat_ws(", ", col("ADEP_P")))
        flight_table = flight_table.withColumn("ADES_P", concat_ws(", ", col("ADES_P")))

        return flight_table.select(
            "id", "ADEP", "ADES", "ADEP_P", "ADES_P",
            "ICAO24", "FLT_ID", "first_seen", "last_seen", "DOF", "version",
        )

    @staticmethod
    def _flight_table_from_endpoints(classified: DataFrame, cand: DataFrame) -> DataFrame:
        """Assemble the flight table from classified endpoints.

        Produces the same schema as :meth:`_compute_flight_table` so the two
        paths are interchangeable downstream, plus ``adep_source``/
        ``ades_source``. Those three states -- named an aerodrome, out of area,
        could not tell -- currently collapse into a single null in the published
        flight list, and they mean entirely different things to a consumer.
        """
        chosen = classified.select(
            "track_id", "icao24", "flight_id", "role", "apt", "source", "event_time"
        )

        # Runner-up aerodromes, kept for the ADEP_P/ADES_P semantics the portal
        # methodology already documents. Ranked by true distance.
        others = (
            cand.join(
                chosen.select("track_id", "role", col("apt").alias("_chosen")),
                ["track_id", "role"], "left",
            )
            .filter(col("apt_ident") != col("_chosen"))
            .groupBy("track_id", "role")
            .agg(collect_list("apt_ident").alias("potential"))
        )

        j = chosen.join(others, ["track_id", "role"], "left")

        def side(role: str, prefix: str) -> DataFrame:
            d = j.filter(col("role") == role)
            return d.select(
                col("track_id"),
                col("icao24"),
                col("flight_id"),
                col("apt").alias(prefix),
                col("source").alias(f"{prefix.lower()}_source"),
                col("potential").alias(f"{prefix}_P"),
                col("event_time").alias("first_seen" if role == "adep" else "last_seen"),
            )

        dep = side("adep", "ADEP")
        arr = side("ades", "ADES").drop("icao24", "flight_id")

        ft = dep.join(arr, "track_id", "outer")
        ft = ft.withColumn("DOF", to_date(col("first_seen")))
        ft = ft.withColumn("version", lit("v3.0.0"))
        ft = ft.withColumnRenamed("track_id", "id").withColumnRenamed(
            "icao24", "ICAO24"
        ).withColumnRenamed("flight_id", "FLT_ID")
        ft = ft.withColumn("ADEP_P", concat_ws(", ", col("ADEP_P")))
        ft = ft.withColumn("ADES_P", concat_ws(", ", col("ADES_P")))

        return ft.select(
            "id", "ADEP", "ADES", "ADEP_P", "ADES_P",
            "adep_source", "ades_source",
            "ICAO24", "FLT_ID", "first_seen", "last_seen", "DOF", "version",
        )

    def _add_osn_aircraft_db_data(self, flight_table: DataFrame) -> DataFrame:
        """
        Enrich the flight table with aircraft metadata from the OSN database.

        Args:
            flight_table: Flight table DataFrame.

        Returns:
            Enriched DataFrame with registration, model, typecode, etc.
        """
        osn_aircraft_db = self.storage.read_table("osn_aircraft_db")

        merged = flight_table.alias("ft").join(
            osn_aircraft_db.alias("adb"),
            col("ft.ICAO24") == col("adb.icao24"),
            "left",
        )

        merged_upper = merged.select(
            *[col(f"ft.{c}").alias(c.upper()) for c in flight_table.columns],
            *[
                col(f"adb.{c}").alias(c.upper())
                for c in osn_aircraft_db.columns
                if c != "icao24"
            ],
        )

        columns = [
            "ID", "ICAO24", "FLT_ID", "DOF",
            "ADEP", "ADES", "ADEP_P", "ADES_P",
            "REGISTRATION", "MODEL", "TYPECODE",
            "ICAO_AIRCRAFT_CLASS", "ICAO_OPERATOR",
            "FIRST_SEEN", "LAST_SEEN", "VERSION",
        ]
        # Carry the provenance columns when the caller produced them. A fixed
        # list here silently dropped them, so the flight list came out without
        # the one field that distinguishes "outside the observed area" from
        # "could not determine".
        columns += [c for c in ("ADEP_SOURCE", "ADES_SOURCE") if c in merged_upper.columns]
        return merged_upper.select(*columns)

    # ------------------------------------------------------------------
    # Overflight processing
    # ------------------------------------------------------------------

    def _fetch_overflights(self, month: date) -> DataFrame:
        """
        Identify overflights: tracks not in the DAI flight list.

        Overflights are tracks with ADS-B signals lasting >= 5 minutes
        that don't already appear in the flight list.

        Args:
            month: Month to process.

        Returns:
            DataFrame of overflight records.
        """
        sv = self._get_data_within_timeframe("osn_tracks", month)
        fl = self._get_data_within_timeframe(
            "opdi_flight_list",
            month,
            time_col="first_seen",
        ).select("id")

        window_track = Window.partitionBy("track_id")
        sv = sv.withColumn("event_time", F.to_timestamp(col("event_time")))
        sv = sv.withColumn("first_seen", f_min("event_time").over(window_track))
        sv = sv.withColumn("last_seen", f_max("event_time").over(window_track))

        # Keep only the first row per track
        sv_f = sv.filter(col("first_seen") == col("event_time"))

        sv_f = sv_f.withColumn("event_date", to_date("event_time"))
        sv_f = sv_f.withColumn("DOF", f_min("event_date").over(window_track))
        sv_f = sv_f.withColumnRenamed("track_id", "id")
        sv_f = sv_f.withColumnRenamed("icao24", "ICAO24")
        sv_f = sv_f.withColumnRenamed("callsign", "FLT_ID")

        for col_name in ["ADEP", "ADES", "ADEP_P", "ADES_P"]:
            sv_f = sv_f.withColumn(col_name, lit(None).cast("string"))
        sv_f = sv_f.withColumn("version", lit("v2.0.0"))

        sv_f = sv_f.select(
            "id", "ADEP", "ADES", "ADEP_P", "ADES_P",
            "ICAO24", "FLT_ID", "first_seen", "last_seen", "DOF", "version",
        )

        # Anti-join to exclude flights already in the flight list
        fl_broadcast = broadcast(fl)
        sv_f = sv_f.join(fl_broadcast, sv_f.id == fl.id, "left_anti")

        # Filter out short ADS-B signals (< 5 min)
        sv_f = sv_f.filter(
            (unix_timestamp("last_seen") - unix_timestamp("first_seen")) >= 300
        )

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
        """Take the departure half from one flight table and the arrival half
        from another.

        Both producers -- :meth:`_compute_flight_table` and
        :meth:`_flight_table_from_endpoints` -- emit the same schema keyed on
        ``id``, which is what makes this a join rather than a rewrite.

        When both roles use the same algorithm the two arguments are the *same
        DataFrame*, and this returns it untouched. That identity matters: it is
        what lets the per-role split be added without altering a single
        existing run, and it is the regression gate in the tests.
        """
        if adep_mode == ades_mode:
            return adep_table

        dep = adep_table.select(
            "id", "ADEP", "ADEP_P", "adep_source",
            "ICAO24", "FLT_ID", "first_seen", "last_seen", "DOF", "version",
        )
        arr = ades_table.select(
            col("id").alias("_id"), "ADES", "ADES_P", "ades_source",
            col("ICAO24").alias("_ICAO24"), col("FLT_ID").alias("_FLT_ID"),
            col("first_seen").alias("_first_seen"),
            col("last_seen").alias("_last_seen"),
            col("DOF").alias("_DOF"), col("version").alias("_version"),
        )

        # Outer, not inner: a track one algorithm can place and the other
        # cannot must survive with the half that worked. An inner join would
        # silently make the combination *less* covered than either input.
        j = dep.join(arr, dep.id == arr._id, "outer")

        # The identity columns are properties of the track, so either side will
        # do -- but only one side is populated for a track that just one
        # algorithm saw, hence the coalesce.
        return j.select(
            F.coalesce(col("id"), col("_id")).alias("id"),
            "ADEP", "ADES", "ADEP_P", "ADES_P",
            F.coalesce(col("ICAO24"), col("_ICAO24")).alias("ICAO24"),
            F.coalesce(col("FLT_ID"), col("_FLT_ID")).alias("FLT_ID"),
            F.coalesce(col("first_seen"), col("_first_seen")).alias("first_seen"),
            F.coalesce(col("last_seen"), col("_last_seen")).alias("last_seen"),
            F.coalesce(col("DOF"), col("_DOF")).alias("DOF"),
            F.coalesce(col("version"), col("_version")).alias("version"),
            F.coalesce(col("adep_source"), lit("undetermined")).alias("adep_source"),
            F.coalesce(col("ades_source"), lit("undetermined")).alias("ades_source"),
        )

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
        write_mode: str = "append",
    ) -> None:
        """
        Process Departures/Arrivals/Internal flights for a month.

        Args:
            month: Month to process.
            airports_hex_path: Path to local airport hex zones parquet (optional,
                falls back to StorageManager table).
            skip_if_processed: Skip if month already processed.
        """
        if skip_if_processed and month in self._load_processed_months(self._dai_log):
            print(f"Month DAI {month} already processed. Skipping.")
            return

        d = self.detection

        # Departures and arrivals are not equally hard, and the rules that suit
        # them differ, so each role gets its own. `mode` remains as an alias
        # that sets both, so every existing caller is unaffected.
        #
        # The fallback is the *config*, not a literal, so a caller that passes
        # nothing gets the recommended configuration rather than whichever
        # algorithm happened to be written here. `DetectionConfig.legacy()`
        # sets both to "trend", which is what published lists were built with.
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
        unknown = needed - {"trend", "endpoint", "nearest"}
        if unknown:
            raise ValueError(f"unknown detection mode(s): {sorted(unknown)}")

        print(f"Processing DAI for {month} (ADEP={adep_mode}, ADES={ades_mode})...")

        tables: dict = {}

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
            )
            # The trend path names an aerodrome or drops the flight; it has no
            # out-of-area concept, so every answer it gives is an aerodrome.
            tables["trend"] = t.withColumn(
                "adep_source",
                when(col("ADEP").isNotNull(), lit("aerodrome")).otherwise(lit("undetermined")),
            ).withColumn(
                "ades_source",
                when(col("ADES").isNotNull(), lit("aerodrome")).otherwise(lit("undetermined")),
            )

        for m in needed & {"endpoint", "nearest"}:
            # Deliberately not keyed to skip_if_processed: rebuilding the
            # flight list with different thresholds does not invalidate the
            # candidates, which is the entire point of caching them.
            self.build_endpoint_candidates(
                month, max_radius_nm=d.endpoint_candidate_radius_nm
            )
            cand = self._get_data_within_timeframe("opdi_endpoint_candidates", month)
            classified = self.classify_endpoints(
                cand,
                mode=m,
                abstention_radius_nm=abstention_radius_nm,
                abstention_height_ft=abstention_height_ft,
                sched_penalty_nm=sched_penalty_nm,
            )
            tables[m] = self._flight_table_from_endpoints(classified, cand)

        flight_table = self._merge_roles(
            tables[adep_mode], tables[ades_mode], adep_mode, ades_mode
        )

        flight_table = self._add_osn_aircraft_db_data(flight_table)

        # Prepare and write
        flight_table = flight_table.withColumn("DOF_day", to_date(col("DOF")))
        flight_table = flight_table.repartition("DOF_day").orderBy("DOF_day")
        flight_table = flight_table.drop("DOF_day")
        # append by default, because the production run accumulates month by
        # month; a comparison run wants one table per variant and overwrite.
        self.storage.write_table(flight_table, table_name, mode=write_mode)

        self._mark_month_processed(month, self._dai_log)
        print(f"DAI processing complete for {month}.")

    def process_overflights(
        self,
        month: date,
        skip_if_processed: bool = True,
    ) -> None:
        """
        Process overflight records for a month.

        Args:
            month: Month to process.
            skip_if_processed: Skip if month already processed.
        """
        if skip_if_processed and month in self._load_processed_months(self._overflight_log):
            print(f"Month overflights {month} already processed. Skipping.")
            return

        print(f"Processing overflights for {month}...")
        flight_table = self._fetch_overflights(month)
        flight_table = self._add_osn_aircraft_db_data(flight_table)

        flight_table = flight_table.withColumn("DOF_day", to_date(col("DOF")))
        flight_table = flight_table.repartition("DOF_day").orderBy("DOF_day")
        flight_table = flight_table.drop("DOF_day")
        self.storage.write_table(flight_table, "opdi_flight_list")

        self._mark_month_processed(month, self._overflight_log)
        print(f"Overflight processing complete for {month}.")

    def process_date_range(
        self,
        start_month: date,
        end_month: date,
        airports_hex_path: Optional[str] = None,
        skip_if_processed: bool = True,
        adep_mode: Optional[str] = None,
        ades_mode: Optional[str] = None,
    ) -> None:
        """
        Process the complete flight list for a range of months.

        Runs DAI processing first, then overflight processing for each month.

        Args:
            start_month: First month to process.
            end_month: Last month to process.
            airports_hex_path: Path to preprocessed airport hex zones parquet.
            skip_if_processed: Skip already processed months.

        Example:
            >>> processor = FlightListProcessor(spark, config)
            >>> processor.process_date_range(
            ...     date(2024, 1, 1),
            ...     date(2024, 6, 1),
            ...     "data/airport_hex/zones_res7_processed.parquet"
            ... )
        """
        months = generate_months(start_month, end_month)
        print(f"Processing flight list for {len(months)} months...")

        for month in months:
            # Keyword, not positional: called positionally this silently
            # dropped every threshold and ran the defaults, which is how the
            # tuning surface came to stop at process_dai.
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
        """Create the opdi_flight_list Iceberg table if it doesn't exist."""
        today = datetime.today().strftime("%d %B %Y")
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`opdi_flight_list` (
            ID STRING COMMENT 'Unique flight identifier (track_id)',
            ICAO24 STRING COMMENT '24-bit ICAO transponder address',
            FLT_ID STRING COMMENT 'Flight callsign',
            DOF DATE COMMENT 'Date of flight',
            ADEP STRING COMMENT 'Aerodrome of departure (ICAO code)',
            ADES STRING COMMENT 'Aerodrome of destination (ICAO code)',
            ADEP_P STRING COMMENT 'Alternative departure airports',
            ADES_P STRING COMMENT 'Alternative destination airports',
            REGISTRATION STRING COMMENT 'Aircraft registration',
            MODEL STRING COMMENT 'Aircraft model',
            TYPECODE STRING COMMENT 'ICAO type designator',
            ICAO_AIRCRAFT_CLASS STRING COMMENT 'ICAO aircraft class',
            ICAO_OPERATOR STRING COMMENT 'ICAO operator code',
            FIRST_SEEN TIMESTAMP COMMENT 'First ADS-B reception time',
            LAST_SEEN TIMESTAMP COMMENT 'Last ADS-B reception time',
            VERSION STRING COMMENT 'Processing version'
        )
        USING iceberg
        PARTITIONED BY (days(FIRST_SEEN))
        COMMENT 'OPDI flight list v2. Last updated: {today}.'
        """
        self.storage.create_table(create_sql)
        print(f"Table {self.project}.opdi_flight_list created/verified.")
