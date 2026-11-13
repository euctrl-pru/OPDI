"""Step 02: split the position reports into tracks.

Reads the state vectors (``osn_statevectors_v2``), gives every report the
``track_id`` of the track it belongs to, adds H3 cell indices, the distance
flown since the start of the track and a checked altitude, and writes
``osn_tracks``. A day's run keeps the tracks that start on that day, whole,
even when they end on the next day.

The track rule is chosen by ``config.segmentation.method``; the rules are in
:mod:`opdi.pipeline.segmentation`.
See https://www.opdi.aero/content/methods/track-identity.html.
"""

import math
import os
from datetime import date, datetime, timedelta
from typing import List

import h3
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    abs as f_abs,
)
from pyspark.sql.functions import (
    atan2,
    col,
    concat,
    concat_ws,
    cos,
    expr,
    lag,
    lit,
    month,
    radians,
    sha2,
    sin,
    sqrt,
    substring,
    to_date,
    to_timestamp,
    udf,
    unix_timestamp,
    when,
    year,
)
from pyspark.sql.functions import (
    max as f_max,
)
from pyspark.sql.functions import (
    min as f_min,
)
from pyspark.sql.functions import (
    sum as f_sum,
)
from pyspark.sql.types import StringType

from opdi.cleaning.native import AVIATION_UNIT_FACTOR
from opdi.config import OPDIConfig
from opdi.utils.datetime_helpers import get_start_end_of_month
from opdi.utils.h3_helpers import h3_list_prep
from opdi.utils.storage import StorageManager


@udf(returnType=StringType())
def _geo_to_h3_cell(lat, lon, resolution):
    """H3 cell of one position at one resolution, NULL for a missing coordinate.

    A plain user-defined function over ``h3.latlng_to_cell`` of h3 version 4.
    The ``h3_pyspark`` package calls the version 3 names, which do not exist
    in version 4.
    """
    if lat is None or lon is None or resolution is None:
        return None
    return h3.latlng_to_cell(float(lat), float(lon), int(resolution))


def keep_tracks_starting_in(df, claim_start, claim_end):
    """Keep whole tracks whose first sample falls in ``[claim_start, claim_end)``.

    The rule that makes day-by-day processing a partition rather than an
    overlap. A batch reads well past the day it is processing, so that a flight
    departing at 22:50 is complete rather than truncated at midnight; this then
    discards the tracks that reading wide dragged in. A track started
    yesterday is dropped *whole* -- keeping its tail would both duplicate
    samples yesterday already processed and invent a flight that never
    departed.

    Half-open at both ends, so midnight belongs to the day that begins. Two
    adjacent days therefore claim every track exactly once between them: no
    track is processed twice, and none falls between them.

    The filter is on the track's first sample, computed over the whole track,
    not on the row -- which is why a kept track keeps every sample it has,
    including the ones after ``claim_end``.

    Args:
        df: State vectors that already carry ``track_id``.
        claim_start: First instant of the day, inclusive.
        claim_end: First instant of the next day, exclusive.
    """
    # Materialised, not inlined: Spark rejects a window function inside a
    # WHERE clause, and the obvious one-liner fails with a message that names
    # the clause rather than the cause.
    tmp = "_track_first_sample"
    out = df.withColumn(tmp, f_min("event_time").over(Window.partitionBy("track_id")))
    return out.filter((col(tmp) >= lit(claim_start)) & (col(tmp) < lit(claim_end))).drop(tmp)


def tracks_touching_window_end(df, read_end, margin_seconds=60):
    """Tracks whose last sample sits within ``margin_seconds`` of ``read_end``.

    A track still transmitting when the read window closes was probably cut off
    by the window rather than by landing, so its tail was never processed and
    never will be -- the next day will not claim it, because it did not start
    there. This is the one way day-by-day processing can lose data, and it is
    invisible in the output: the track is present, plausible, and simply stops.

    Counting them is how the lookahead gets checked against reality instead of
    being trusted. A healthy run returns zero; a non-zero count means
    ``lookahead_hours`` is too short for this traffic.

    ``process_day`` widens the margin to ``link_max_gap_minutes`` plus the longer of
    ``link_callsign_lookahead_minutes`` and ``link_edge_position_minutes`` under
    ``cruise_linked``. There a hole can straddle ``read_end``, which leaves the last
    read sample far from the edge while the rest of the track waits beyond it.
    """
    per_track = df.groupBy("track_id").agg(f_max("event_time").alias("_track_last_sample"))
    cutoff = lit(read_end) - expr(f"INTERVAL {int(margin_seconds)} SECONDS")
    return per_track.filter(col("_track_last_sample") >= cutoff)


def _resolve_segmentation_method(method: str) -> str:
    """The rule a configured method names.

    ``standard`` is another name of the default rule, ``cruise_linked``; see
    ``SegmentationConfig.method``.
    """
    return "cruise_linked" if method == "standard" else method


def required_lookback_hours(method: str, p) -> int:
    """Hours ``process_day`` must read before the day under ``method``.

    Under most rules a new track starts at a local condition, so any silence
    longer than the gap threshold proves that a report starts a track, and
    ``DEFAULT_LOOKBACK_HOURS`` is enough. Under ``cruise_linked`` a track
    piece after midnight may continue a piece that ended up to
    ``link_max_gap_minutes`` earlier. If the read missed the earlier piece,
    the day would claim the later piece as a new track while the day before
    claims it as part of the linked track.

    The longest of ``link_callsign_lookback_minutes``,
    ``link_edge_position_minutes`` and the call sign persistence is added,
    because the link decision also reads the call sign and the position that
    far before the gap, and the call sign memory lasts for the longest
    linkable gap plus the persistence (``methods.callsign_memory_minutes``).
    Every day's read must hold that evidence, or two days could decide one
    gap differently.
    """
    if _resolve_segmentation_method(method) == "cruise_linked":
        behind_the_edge = max(
            p.link_callsign_lookback_minutes,
            p.link_edge_position_minutes,
            p.callsign_min_persistence_seconds / 60.0,
        )
        return max(
            TrackProcessor.DEFAULT_LOOKBACK_HOURS,
            math.ceil(p.link_max_gap_minutes / 60) + math.ceil(behind_the_edge / 60),
        )
    return TrackProcessor.DEFAULT_LOOKBACK_HOURS


class TrackProcessor:
    """Step 02: build tracks from the state vectors.

    For each report it adds:

    * ``track_id``, from the track rule ``config.segmentation.method``:
      ``standard`` (the default, which resolves to ``cruise_linked`` and
      keeps one track across a gap in en-route reception) or ``legacy``
      (the rule of release v0.0.2, which reproduces its published ids);
    * the H3 cells of the position (``h3_res_7``, ``h3_res_12``);
    * the distance from the previous report and the distance flown since
      the start of the track, in NM;
    * ``baro_altitude_c`` and ``geo_altitude_c``: the altitudes with
      implausible values set to NULL.

    Changing the track rule changes ``track_id`` for all data produced
    afterwards, and nothing in ``osn_tracks`` records which rule was used.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        log_file_path: str = "OPDI_live/logs/02_osn-tracks-etl-log.parquet",
    ):
        """Prepare the processor.

        Args:
            spark: Active Spark session.
            config: OPDI configuration.
            log_file_path: Parquet file that records the processed months.
        """
        self.spark = spark
        self.config = config
        self.storage = StorageManager(spark, config)
        self.project = config.project.project_name
        self.h3_resolutions = config.h3.track_resolutions
        self.log_file_path = log_file_path

        # Track splitting thresholds from config
        self.gap_threshold_minutes = config.ingestion.track_gap_threshold_minutes
        self.gap_low_alt_minutes = config.ingestion.track_gap_low_altitude_minutes
        self.low_altitude_meters = config.ingestion.track_gap_low_altitude_meters

        # Ensure log directory exists
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    def _load_processed_months(self) -> List[date]:
        """Load list of already processed months from log file."""
        if os.path.isfile(self.log_file_path):
            df = pd.read_parquet(self.log_file_path)
            return df.months.to_list()
        return []

    def _mark_month_processed(self, month: date) -> None:
        """Mark a month as processed in the log file."""
        processed_months = self._load_processed_months()
        if month not in processed_months:
            processed_months.append(month)
            processed_df = pd.DataFrame({"months": processed_months})
            processed_df.to_parquet(self.log_file_path)

    def _read_adsc(self, start_time_str: str, end_time_str: str) -> DataFrame:
        """The ADS-C state vectors for this batch's time window.

        Separate from :meth:`_merge_adsc` so that the merge can be tested
        without a table.
        """
        ref = self.storage.table_ref(self.config.adsc.table)
        return self.spark.sql(f"""
            SELECT * FROM {ref}
            WHERE (event_time >= TIMESTAMP('{start_time_str}'))
              AND (event_time < TIMESTAMP('{end_time_str}'));
        """)

    def _merge_adsc(self, df_sv: DataFrame, start_time_str: str, end_time_str: str) -> DataFrame:
        """Add the ADS-C reports to the state vectors, before track building.

        Merged before :meth:`_add_track_id` so that ADS-C reports can extend
        a track and bridge gaps in ADS-B reception.

        Returns ``df_sv`` unchanged, column for column, when
        ``config.adsc.enabled`` is false. In particular no ``source`` column
        is added then: a column present in new day folders and absent from
        older ones would appear and disappear depending on which file a
        Parquet reader takes the schema from.
        """
        cfg = self.config.adsc
        if not cfg.enabled:
            return df_sv

        from opdi.ingestion.osn_adsc import ADSB_SOURCE, shadow_filter

        if not self.storage.table_exists(cfg.table):
            raise RuntimeError(
                f"config.adsc.enabled is True but {cfg.table!r} is absent from "
                f"{self.config.project.warehouse_path!r}. Run step 01b for this "
                f"window first. Without it, these tracks would be built from "
                f"ADS-B alone but stored as if they held ADS-C as well, and the "
                f"two kinds of tables have the same columns, so nothing later "
                f"could tell them apart."
            )

        df_adsc = self._read_adsc(start_time_str, end_time_str)
        kept = shadow_filter(df_adsc, df_sv, cfg.shadow_before_seconds, cfg.shadow_after_seconds)

        # The day column `process_month` partitions by. ADS-C rows need their
        # own day: a NULL day lands in `__HIVE_DEFAULT_PARTITION__`, where no
        # day's read finds it.
        if "event_time_day" in df_sv.columns:
            kept = kept.withColumn("event_time_day", to_date(col("event_time")))

        return df_sv.withColumn("source", lit(ADSB_SOURCE)).unionByName(
            kept, allowMissingColumns=True
        )

    def _add_track_id(self, df: DataFrame) -> DataFrame:
        """Add ``track_id`` to every state vector.

        ``legacy``, the rule of release v0.0.2:

        1. group id: the first 16 characters of SHA-256(icao24 + callsign);
        2. a new track at a gap over 30 minutes, or over 15 minutes below
           1524 m (5,000 ft);
        3. offset: the number of new tracks before this report in its group;
        4. ``track_id`` = ``{group_id}_{offset}_{year}_{month}``.

        ``standard`` and any other rule of the registry are built by
        :mod:`opdi.pipeline.segmentation` (see
        :meth:`_add_track_id_segmentation`). The ``legacy`` code below is
        kept exactly as it produced the published ids.

        Args:
            df: State vectors.

        Returns:
            The state vectors with a ``track_id`` column.
        """
        method = self.config.segmentation.method
        if method != "legacy":
            return self._add_track_id_segmentation(df, method)

        # Add timestamp column
        df = df.withColumn("event_time_ts", to_timestamp("event_time"))

        # Create group ID: SHA2 hash of icao24 + callsign (first 16 characters)
        # This groups together flights with same aircraft and callsign
        df = df.withColumn(
            "group_id", substring(sha2(concat_ws("", "icao24", "callsign"), 256), 1, 16)
        )

        # Define window for time-based calculations
        window_spec = Window.partitionBy("group_id").orderBy("event_time_ts")

        # Calculate time gap from previous point in same group
        df = df.withColumn("prev_event_time_ts", lag("event_time_ts").over(window_spec))
        df = df.withColumn(
            "time_gap_minutes",
            (unix_timestamp("event_time_ts") - unix_timestamp("prev_event_time_ts")) / 60,
        )

        # Track split condition:
        # 1. Gap > 30 minutes (aircraft likely landed and took off again)
        # 2. Gap > 15 minutes AND low altitude (< 1524m / 5000ft)
        #    This catches cases where transponder stays on during ground operations
        track_split_condition = (col("time_gap_minutes") > self.gap_threshold_minutes) | (
            (col("time_gap_minutes") > self.gap_low_alt_minutes)
            & (col("baro_altitude") < self.low_altitude_meters)
        )

        df = df.withColumn("new_group_flag", when(track_split_condition, 1).otherwise(0))

        # Calculate cumulative offset (number of splits before this point)
        group_window_spec = (
            Window.partitionBy("group_id")
            .orderBy("event_time_ts")
            .rowsBetween(Window.unboundedPreceding, 0)
        )
        df = df.withColumn("offset", f_sum("new_group_flag").over(group_window_spec))

        # Generate final track ID
        # Format: {group_id}_{offset}_{year}_{month}
        # The year/month suffix prevents tracks spanning multiple months from being merged
        df = df.withColumn(
            "track_id",
            concat(
                col("group_id"),
                lit("_"),
                col("offset"),
                lit("_"),
                year("event_time_ts").cast("string"),
                lit("_"),
                month("event_time_ts").cast("string"),
            ),
        )

        # Clean up temporary columns
        df = df.drop(
            "event_time_ts",
            "group_id",
            "prev_event_time_ts",
            "time_gap_minutes",
            "new_group_flag",
            "offset",
        )

        return df

    def _add_track_id_segmentation(self, df: DataFrame, method: str) -> DataFrame:
        """``track_id`` from a rule of :mod:`opdi.pipeline.segmentation`.

        The thresholds come from ``config.segmentation`` through
        :meth:`SegmentationParams.from_config`.

        Args:
            df: State vectors.
            method: Name of the rule in the registry, or ``standard``.

        Returns:
            The state vectors with a ``track_id`` column.
        """
        from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
        from opdi.pipeline.segmentation.methods import ARMS, STUDY_ARMS, STUDY_HOME

        # "standard" names the default rule, "cruise_linked". Any rule in ARMS
        # is accepted, including the study rules a study script registered, so
        # a study drives the real pipeline rather than a copy of it.
        resolved = _resolve_segmentation_method(method)
        if resolved in STUDY_ARMS and resolved not in ARMS:
            raise ValueError(
                f"segmentation method {method!r} is a study rule and not part of opdi; "
                f"it lives in {STUDY_HOME}"
            )
        if resolved not in ARMS:
            known = sorted(set(ARMS) | {"standard"})
            raise ValueError(f"unknown segmentation method {method!r}; known: {', '.join(known)}")
        params = SegmentationParams.from_config(self.config.segmentation)
        return assign_track_id(df, ARMS[resolved](), params)

    def _add_h3_encoding(self, df: DataFrame) -> DataFrame:
        """Add H3 hexagonal grid indices at configured resolutions.

        Args:
            df: DataFrame with lat/lon columns

        Returns:
            DataFrame with h3_res_{resolution} columns added
        """
        for h3_resolution in self.h3_resolutions:
            df = df.withColumn("h3_resolution", lit(h3_resolution))
            df = df.withColumn(
                f"h3_res_{h3_resolution}",
                _geo_to_h3_cell("lat", "lon", "h3_resolution"),
            )

        # Drop temporary resolution column
        df = df.drop("h3_resolution")

        return df

    def _add_cumulative_distance(self, df: DataFrame) -> DataFrame:
        """Add the distance from the previous report and the distance flown.

        Great-circle (haversine) distance with an Earth radius of 6371 km,
        in nautical miles (1 NM = 1.852 km), in report order within each
        track.

        Args:
            df: Reports with ``lat``, ``lon``, ``track_id`` and ``event_time``.

        Returns:
            The reports with ``segment_distance_nm`` and
            ``cumulative_distance_nm``.
        """
        # Define window specifications
        window_lag = Window.partitionBy("track_id").orderBy("event_time")
        window_cumsum = (
            Window.partitionBy("track_id")
            .orderBy("event_time")
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        # Convert degrees to radians
        df = df.withColumn("lat_rad", radians(col("lat")))
        df = df.withColumn("lon_rad", radians(col("lon")))

        # Get previous point's coordinates
        df = df.withColumn("prev_lat_rad", lag("lat_rad").over(window_lag))
        df = df.withColumn("prev_lon_rad", lag("lon_rad").over(window_lag))

        # Haversine formula
        df = df.withColumn(
            "a",
            sin((col("lat_rad") - col("prev_lat_rad")) / 2) ** 2
            + cos(col("prev_lat_rad"))
            * cos(col("lat_rad"))
            * sin((col("lon_rad") - col("prev_lon_rad")) / 2) ** 2,
        )

        df = df.withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a"))))

        # Distance in kilometers (Earth radius = 6371 km)
        df = df.withColumn("distance_km", 6371 * col("c"))

        # Convert to nautical miles (1 NM = 1.852 km)
        df = df.withColumn("segment_distance_nm", col("distance_km") / 1.852)

        # Calculate cumulative distance
        df = df.withColumn(
            "cumulative_distance_nm", f_sum("segment_distance_nm").over(window_cumsum)
        )

        # Drop temporary columns
        df = df.drop("lat_rad", "lon_rad", "prev_lat_rad", "prev_lon_rad", "a", "c", "distance_km")

        return df

    def _add_clean_altitude(self, df: DataFrame, col_name: str) -> DataFrame:
        """Set to NULL the altitudes that change implausibly fast.

        ``{col_name}_c`` is the altitude with every report whose rate of
        change from the previous report exceeds the limit set to NULL. No
        value is replaced by an estimate, so a missing value stays
        distinguishable from a measured one.

        The limit is ``CleaningConfig.baro_altitude_d1_max_ft_s`` (or
        ``geo_altitude_d1_max_ft_s``), 200 ft/s, the first-difference limit
        of the trajectory cleaning. The stored altitude is in metres; it is
        scaled to feet only for the comparison.

        Args:
            df: Reports with the altitude column, ``track_id`` and
                ``event_time``.
            col_name: ``"baro_altitude"`` or ``"geo_altitude"``.

        Returns:
            The same rows, plus ``{col_name}_c``.
        """
        window_spec = Window.partitionBy("track_id").orderBy("event_time")

        seconds = unix_timestamp("event_time") - unix_timestamp(lag("event_time").over(window_spec))
        # A zero or missing time step gives a NULL rate, which does not fail
        # the test: the first report of a track has nothing to compare with.
        rate_ft_s = f_abs(
            (col(col_name) - lag(col_name).over(window_spec))
            * AVIATION_UNIT_FACTOR[col_name]
            / F.nullif(seconds.cast("double"), lit(0.0))
        )

        threshold = getattr(self.config.cleaning, f"{col_name}_d1_max_ft_s")
        return df.withColumn(
            f"{col_name}_c",
            when(rate_ft_s > lit(threshold), lit(None).cast("double")).otherwise(col(col_name)),
        )

    def process_month(
        self,
        month: date,
        skip_if_processed: bool = True,
        window: tuple = None,
        claim: tuple = None,
    ) -> None:
        """Build the tracks of one month, or of a time window inside it.

        Args:
            month: First day of the month to process.
            skip_if_processed: Skip a month the log records as processed.
            window: Optional ``(start, end)`` timestamp strings that narrow
                the reports read, end excluded. ``None`` reads the whole
                month. A study can use it to run this method over a few days.
            claim: Optional ``(start, end)`` instants; only the tracks whose
                first report falls in ``[start, end)`` are kept (see
                :func:`keep_tracks_starting_in`). :meth:`process_day` sets it
                to the day being processed.

        Example:
            >>> from datetime import date
            >>> processor = TrackProcessor(spark, config)
            >>> processor.process_month(date(2024, 1, 1))
        """
        # Check if already processed
        if skip_if_processed and month in self._load_processed_months():
            print(f"Month {month.strftime('%Y-%m')} already processed. Skipping.")
            return

        print(f"Processing tracks for month {month.strftime('%Y-%m')}... ({datetime.now()})")

        # Prepare H3 column names
        h3_columns = h3_list_prep(self.h3_resolutions)

        # Get month boundaries
        start_time, end_time = get_start_end_of_month(month)
        start_time_str = datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = datetime.utcfromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")
        if window is not None:
            start_time_str, end_time_str = window
            print(f"  window override: {start_time_str} <= event_time < {end_time_str}")

        # Read raw state vectors for the month
        sv_ref = self.storage.table_ref("osn_statevectors_v2")
        df_month = self.spark.sql(f"""
            SELECT * FROM {sv_ref}
            WHERE (event_time >= TIMESTAMP('{start_time_str}'))
              AND (event_time < TIMESTAMP('{end_time_str}'));
        """)

        # Before `original_columns`, so `source` survives the select below,
        # and before `_add_track_id`, so segmentation sees both feeds.
        df_month = self._merge_adsc(df_month, start_time_str, end_time_str)

        # Store original column names + new columns we'll add
        original_columns = df_month.columns + ["track_id"] + h3_columns

        # Step 1: Add track ID
        df_month = self._add_track_id(df_month)

        # Step 1b: keep only the tracks this batch owns.
        #
        # Day-by-day processing reads well past the day it is processing, so a
        # flight departing at 22:50 is complete rather than cut off at
        # midnight. That same wide read drags in tracks belonging to the
        # neighbouring days, and this is where they go back. Without it the
        # overlap would double-process every track that spans a boundary.
        if claim is not None:
            df_month = keep_tracks_starting_in(df_month, claim[0], claim[1])

        # Step 2: Add H3 encoding
        df_month = self._add_h3_encoding(df_month)

        # Select only the columns we want (drops temporary columns)
        df_month = df_month.select(original_columns)

        # Step 3: Add distance calculations
        df_month = self._add_cumulative_distance(df_month)

        # Step 4: Clean altitude data
        df_month = self._add_clean_altitude(df_month, col_name="geo_altitude")
        df_month = self._add_clean_altitude(df_month, col_name="baro_altitude")

        # Prepare for write.
        #
        # ``dof`` is the day the *track* starts, not the day of each report,
        # so a track that crosses midnight stays in one day folder and the
        # day that owns it reads it back whole.
        df_month = df_month.withColumn(
            "dof",
            to_date(f_min("event_time").over(Window.partitionBy("track_id"))),
        )
        # Spread over many tasks by track, so that the day is written as many
        # files in parallel; every report of a track stays in one file. (Grouping
        # by ``dof`` alone put the whole day into one task and one file.)
        df_month = df_month.repartition("dof", "track_id")

        # Overwrite, not append, and safe because it is partitioned: only the
        # days present are replaced, so re-running a day stops duplicating it
        # while a run over fresh days behaves exactly as append did.
        self.storage.write_table(
            df_month,
            "osn_tracks",
            mode="overwrite",
            partition_by=["dof"],
            # A claimed batch holds exactly the day it claimed, so the
            # partition is known without evaluating the frame to ask.
            partition_values=([{"dof": claim[0].date()}] if claim is not None else None),
        )

        # Clean up memory
        df_month.unpersist(blocking=True)
        self.spark.catalog.clearCache()

        # Mark as processed
        self._mark_month_processed(month)

        print(f"Month {month.strftime('%Y-%m')} processing complete. ({datetime.now()})")

    #: Hours read before the day, so a track overlapping midnight can be told
    #: from one that starts just after it. Only has to exceed the gap
    #: threshold (30 min): three hours of silence before a report already
    #: prove it starts a track. Under ``cruise_linked``,
    #: ``required_lookback_hours`` reads further.
    DEFAULT_LOOKBACK_HOURS = 3

    #: Hours read after the day, so a track starting inside it is complete.
    #:
    #: Too small a value cuts tracks short. A track is not always one flight:
    #: an aircraft received without interruption and without a call sign
    #: change can chain several flights into one track. ``process_day`` warns
    #: when a track reaches the end of the read, so a cut is never silent.
    DEFAULT_LOOKAHEAD_HOURS = 24

    def process_day(
        self,
        day: date,
        skip_if_processed: bool = True,
        lookback_hours: int = None,
        lookahead_hours: int = None,
    ) -> None:
        """Build the tracks that start on ``day``, each one whole.

        Reads the position reports (state vectors) from ``lookback_hours``
        before the start of ``day`` to ``lookahead_hours`` after its end, and
        keeps the tracks whose first report falls on ``day``. Every track has
        exactly one first report, so every track belongs to exactly one day:
        run day after day, no report is processed twice and none is left out.
        A track that runs past midnight is built in full by the day it
        started; that is what the reports after the end of the day are read
        for.

        The reports before the start of the day are read so that a track
        which began shortly before midnight is seen whole and not taken for a
        new one. By default this is :func:`required_lookback_hours`: 3 hours,
        or, under the segmentation method ``cruise_linked`` (which joins two
        parts of a track across a gap in reception), the longest gap it may
        join plus the time it looks back for a call sign or a position:
        ``max(3, ceil(link_max_gap_minutes / 60) +
        ceil(max(link_callsign_lookback_minutes, link_edge_position_minutes) / 60))``,
        11 hours with the default settings.

        Args:
            day: The day whose tracks are built.
            skip_if_processed: Skip the day when the progress log records it.
            lookback_hours: Hours read before the start of the day.
            lookahead_hours: Hours read after the end of the day
                (:attr:`DEFAULT_LOOKAHEAD_HOURS` by default).

        Raises:
            ValueError: If ``lookback_hours`` is below the required value under
                ``cruise_linked``. Two days could then decide one gap
                differently, and reports after midnight would be given to two
                days or to none. Under the other methods each split is decided
                from the reports next to it, so any value is accepted.
        """
        from opdi.pipeline.segmentation import SegmentationParams

        method = self.config.segmentation.method
        params = SegmentationParams.from_config(self.config.segmentation)
        links_across_holes = _resolve_segmentation_method(method) == "cruise_linked"
        required = required_lookback_hours(method, params)
        lookback = required if lookback_hours is None else lookback_hours
        if links_across_holes and lookback < required:
            raise ValueError(
                f"lookback_hours={lookback} under {method!r} is under the {required} "
                f"hours a linked track can reach back: samples after midnight could "
                f"be claimed by two days or by none"
            )
        lookahead = self.DEFAULT_LOOKAHEAD_HOURS if lookahead_hours is None else lookahead_hours

        claim_start = datetime(day.year, day.month, day.day)
        claim_end = claim_start + timedelta(days=1)
        read_start = claim_start - timedelta(hours=lookback)
        read_end = claim_end + timedelta(hours=lookahead)

        fmt = "%Y-%m-%d %H:%M:%S"
        print(
            f"Processing tracks starting on {day} "
            f"(reading {read_start:{fmt}} .. {read_end:{fmt}})"
        )

        self.process_month(
            month=day,
            skip_if_processed=skip_if_processed,
            window=(read_start.strftime(fmt), read_end.strftime(fmt)),
            claim=(claim_start, claim_end),
        )

        # Check the lookahead against the data: a track still transmitting when
        # the read ended was cut by the read, not by landing, and the next day
        # will not pick it up because it did not start then.
        written = self.storage.read_table("osn_tracks").filter(col("dof") == lit(day))
        # Under a link the last sample can be a whole hole short of the read end, and
        # the verdict on that hole also reads this far past its new edge.
        margin_seconds = (
            (
                params.link_max_gap_minutes
                + max(params.link_callsign_lookahead_minutes, params.link_edge_position_minutes)
            )
            * 60
            if links_across_holes
            else 60
        )
        n_truncated = tracks_touching_window_end(
            written, read_end, margin_seconds=margin_seconds
        ).count()
        if n_truncated:
            # The margin under a link is hours wide, so a healthy track ending inside
            # it is ordinary rather than a loss.
            consequence = (
                "Their tails may be missing, or the track is simply long"
                if links_across_holes
                else "Their tails may be missing and no other day will claim them"
            )
            print(
                f"  WARNING: {n_truncated} track(s) starting on {day} ended within "
                f"{margin_seconds:.0f} s of the end of the read window "
                f"({read_end:{fmt}}). {consequence}. Re-run this day with a larger "
                f"lookahead_hours (currently {lookahead})."
            )
        else:
            print(f"  lookahead of {lookahead}h covered every track starting on {day}")

    def process_date_range(
        self, start_month: date, end_month: date, skip_if_processed: bool = True
    ) -> None:
        """Build the tracks of a range of months.

        Args:
            start_month: First month to process (first day of the month).
            end_month: Last month to process (first day of the month).
            skip_if_processed: Skip months the log records as processed.

        Example:
            >>> from datetime import date
            >>> processor = TrackProcessor(spark, config)
            >>> processor.process_date_range(date(2024, 1, 1), date(2024, 3, 1))
        """
        from opdi.utils.datetime_helpers import generate_months

        months = generate_months(start_month, end_month)

        print(f"Processing {len(months)} months: {start_month} to {end_month}")

        for first_day in months:
            self.process_month(first_day, skip_if_processed=skip_if_processed)

        print(f"Completed processing {len(months)} months.")

    def create_table_if_not_exists(self) -> None:
        """Create the ``osn_tracks`` Iceberg table if it does not exist.

        Used in the ``local`` environment with Iceberg; with plain Parquet
        folders the table is created by its first write.
        """
        from datetime import date

        today = date.today().strftime("%d %B %Y")

        # `source` is not in this table definition. Only a run with
        # `config.adsc.enabled` writes it, and such runs use plain Parquet
        # folders, whose schema is whatever the files carry. If merged ADS-C
        # runs are ever written to an Iceberg table, this definition and the
        # one in `cleaning/cleaner.py` must gain the column together.
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`osn_tracks` (
          event_time TIMESTAMP COMMENT 'Timestamp of state vector',
          icao24 STRING COMMENT '24-bit ICAO transponder ID',
          lat DOUBLE COMMENT 'Latitude (degrees)',
          lon DOUBLE COMMENT 'Longitude (degrees)',
          velocity DOUBLE COMMENT 'Ground speed (m/s)',
          heading DOUBLE COMMENT 'Track angle from north (degrees)',
          vert_rate DOUBLE COMMENT 'Vertical rate (m/s)',
          callsign STRING COMMENT 'Aircraft callsign',
          on_ground BOOLEAN COMMENT 'On ground flag',
          alert BOOLEAN COMMENT 'ATC alert flag',
          spi BOOLEAN COMMENT 'ATC SPI flag',
          squawk STRING COMMENT 'Transponder code',
          baro_altitude DOUBLE COMMENT 'Barometric altitude (m)',
          geo_altitude DOUBLE COMMENT 'GNSS altitude (m)',
          last_pos_update DOUBLE COMMENT 'Position age (unix timestamp)',
          last_contact DOUBLE COMMENT 'Last contact (unix timestamp)',
          serials ARRAY<INT> COMMENT 'Receiver serials',
          track_id STRING COMMENT 'Unique track identifier',
          h3_res_7 STRING COMMENT 'H3 index at resolution 7',
          h3_res_12 STRING COMMENT 'H3 index at resolution 12',
          segment_distance_nm DOUBLE COMMENT 'Distance from previous point (NM)',
          cumulative_distance_nm DOUBLE COMMENT 'Total track distance (NM)',
          geo_altitude_c DOUBLE COMMENT 'Cleaned GNSS altitude (m)',
          baro_altitude_c DOUBLE COMMENT 'Cleaned barometric altitude (m)'
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        COMMENT 'Flight tracks derived from state vectors. Last updated: {today}.'
        """

        self.storage.create_table(create_table_sql)
        print(f"Table {self.project}.osn_tracks created/verified.")
