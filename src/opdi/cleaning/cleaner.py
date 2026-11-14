"""Step 02a: trajectory cleaning.

Reads the tracks (``osn_tracks``, step 02), sets the values that cannot be
trusted to NULL with :func:`opdi.cleaning.native.clean_tracks`, and writes
``osn_tracks_clean``. The uncleaned table is kept unchanged, because the
settings of release v0.0.2 build the flight list and events from it.
See https://www.opdi.aero/content/methods/trajectory-cleaning.html.
"""

import os
from datetime import date, datetime
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from opdi.cleaning.native import clean_tracks, null_rate_report
from opdi.config import OPDIConfig
from opdi.utils.datetime_helpers import get_start_end_of_month
from opdi.utils.storage import StorageManager

SOURCE_TABLE = "osn_tracks"
TARGET_TABLE = "osn_tracks_clean"


class TrackCleaner:
    """Step 02a: clean the tracks of a day or a month.

    Args:
        spark: Active Spark session.
        config: OPDI configuration; the thresholds are ``config.cleaning``.
        log_file_path: Parquet file that records the processed batches.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        log_file_path: str = "OPDI_live/logs/02a_track_cleaning.log",
    ):
        self.spark = spark
        self.config = config
        self.cleaning = config.cleaning
        self.storage = StorageManager(spark, config)
        self.project = config.project.project_name
        self.log_file_path = log_file_path

        os.makedirs(os.path.dirname(log_file_path) or ".", exist_ok=True)

    # -- processed-month bookkeeping ------------------------------------

    def _load_processed_months(self) -> List[date]:
        if not os.path.exists(self.log_file_path):
            return []
        with open(self.log_file_path) as handle:
            return [
                datetime.strptime(line.strip(), "%Y-%m-%d").date()
                for line in handle
                if line.strip()
            ]

    def _mark_month_processed(self, month: date) -> None:
        with open(self.log_file_path, "a") as handle:
            handle.write(f"{month.strftime('%Y-%m-%d')}\n")

    # -- processing ------------------------------------------------------

    def process_day(self, day: date, skip_if_processed: bool = True, report: bool = True) -> None:
        """Clean the tracks that started on ``day``, whole.

        Step 02 writes each track in the day folder of its first report, so
        this reads that folder and cleans what it holds.
        """
        self.process_month(month=day, skip_if_processed=skip_if_processed, report=report, day=day)

    def process_month(
        self,
        month: date,
        skip_if_processed: bool = True,
        report: bool = True,
        day: date = None,
    ) -> None:
        """Clean one batch of tracks -- a month, or a single day.

        Args:
            month: Any date within the month to process.
            skip_if_processed: Skip batches already in the log.
            report: Print the share of NULL values per column after
                cleaning; costs one extra Spark action per batch.
            day: Clean the tracks that *started* this day, whole, instead of a
                month. Selects on ``dof`` rather than on ``event_time``,
                because a track running past midnight belongs to the day it
                departed and must be cleaned in one piece -- cleaning half a
                trajectory would compute its derivative spikes and cumulative
                distances against a truncated series.
        """
        if not self.cleaning.enabled:
            print("Cleaning is disabled in config. Skipping step 02a.")
            return

        batch = day if day is not None else month
        if skip_if_processed and batch in self._load_processed_months():
            label = batch.strftime("%Y-%m-%d" if day is not None else "%Y-%m")
            print(f"Batch {label} already cleaned. Skipping.")
            return

        label = batch.strftime("%Y-%m-%d" if day is not None else "%Y-%m")
        print(f"Cleaning tracks for {label}... ({datetime.now()})")

        from opdi.utils.batching import filter_batch

        if day is not None:
            df = filter_batch(self.storage.read_table(SOURCE_TABLE), day=day)
        else:
            start_time, end_time = get_start_end_of_month(month)
            start_str = datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
            end_str = datetime.utcfromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")

            source = self.storage.table_ref(SOURCE_TABLE)
            df = self.spark.sql(f"""
                SELECT * FROM {source}
                WHERE (event_time >= TIMESTAMP('{start_str}'))
                  AND (event_time < TIMESTAMP('{end_str}'));
                """)

        cleaned = clean_tracks(df, self.cleaning)

        cleaned = cleaned.repartition("track_id")
        if day is not None:
            # `dof` comes from osn_tracks, so overwrite replaces this day and
            # leaves every other day alone: cleaning a day again replaces it.
            self.storage.write_table(
                cleaned,
                TARGET_TABLE,
                mode="overwrite",
                partition_by=["dof"],
                # Known, so the partition does not have to be rediscovered by
                # evaluating the frame a second time.
                partition_values=[{"dof": day}],
            )
        else:
            # Append, because a monthly run adds a month this table does not
            # have. Building a month that is already present would double it,
            # so a rebuild must first overwrite.
            self.storage.write_table(cleaned, TARGET_TABLE, mode="append")

        if report:
            # The rows just written: the day (or the month), not the whole table.
            written = self.storage.read_table(TARGET_TABLE)
            if day is not None:
                written = filter_batch(written, day=day)
            else:
                written = written.filter(
                    (F.col("event_time") >= F.lit(start_str).cast("timestamp"))
                    & (F.col("event_time") < F.lit(end_str).cast("timestamp"))
                )
            rates = null_rate_report(written)
            print("  NULL rate after cleaning:")
            for name, rate in sorted(rates.items()):
                print(f"    {name:<16} {rate:6.2%}")

        cleaned.unpersist(blocking=True)
        self.spark.catalog.clearCache()
        # Log the batch, not its month: otherwise cleaning one day would mark
        # the whole month as done.
        self._mark_month_processed(batch)

        print(f"Month {month.strftime('%Y-%m')} cleaning complete. ({datetime.now()})")

    def process_date_range(
        self, start_month: date, end_month: date, skip_if_processed: bool = True
    ) -> None:
        """Clean every month in ``[start_month, end_month)``."""
        month = date(start_month.year, start_month.month, 1)
        end = date(end_month.year, end_month.month, 1)
        while month < end:
            self.process_month(month, skip_if_processed=skip_if_processed)
            month = (
                date(month.year + 1, 1, 1)
                if month.month == 12
                else date(month.year, month.month + 1, 1)
            )

    def create_table_if_not_exists(self) -> None:
        """Create ``osn_tracks_clean``: the ``osn_tracks`` schema plus ``segment_id``."""
        self.storage.create_table(f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`{TARGET_TABLE}` (
            event_time TIMESTAMP COMMENT 'Timestamp for which the state vector was valid.',
            icao24 STRING COMMENT '24-bit ICAO transponder ID.',
            lat DOUBLE COMMENT 'Latitude. NULL where cleaning could not verify it.',
            lon DOUBLE COMMENT 'Longitude. NULL where cleaning could not verify it.',
            velocity DOUBLE COMMENT 'Speed over ground in m/s.',
            heading DOUBLE COMMENT 'Track angle in degrees clockwise from north.',
            vert_rate DOUBLE COMMENT 'Vertical speed in m/s.',
            callsign STRING COMMENT 'Broadcast callsign.',
            on_ground BOOLEAN COMMENT 'Surface (true) vs airborne (false) positions.',
            alert BOOLEAN COMMENT 'ATC indicator.',
            spi BOOLEAN COMMENT 'ATC indicator.',
            squawk STRING COMMENT 'Transponder code.',
            baro_altitude DOUBLE COMMENT 'Barometric altitude in metres.',
            geo_altitude DOUBLE COMMENT 'GNSS altitude in metres.',
            last_pos_update DOUBLE COMMENT 'Unix timestamp: age of the position.',
            last_contact DOUBLE COMMENT 'Unix timestamp: last signal received.',
            serials ARRAY<INT> COMMENT 'Serials of the receiving ADS-B stations.',
            track_id STRING COMMENT 'Unique identifier for the flight track.',
            h3_res_7 STRING COMMENT 'H3 cell at resolution 7.',
            h3_res_12 STRING COMMENT 'H3 cell at resolution 12.',
            segment_distance_nm DOUBLE COMMENT 'Distance from the previous state vector, NM.',
            cumulative_distance_nm DOUBLE COMMENT 'Cumulative distance from track start, NM.',
            geo_altitude_c DOUBLE COMMENT 'Checked GNSS altitude, metres (step 02).',
            baro_altitude_c DOUBLE COMMENT 'Checked barometric altitude, metres (step 02).',
            segment_id STRING COMMENT 'Contiguous sub-track: splits track_id at coverage holes.'
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='SNAPPY',
            'write.distribution.mode'='hash'
        )
        COMMENT '{SOURCE_TABLE} after cleaning (step 02a): bad values are NULL, rows kept.';
        """)
        print(f"Table {self.project}.{TARGET_TABLE} created/verified.")
