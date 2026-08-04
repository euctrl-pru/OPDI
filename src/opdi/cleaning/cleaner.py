"""
Track cleaning processor -- pipeline step 02a.

Reads ``osn_tracks`` (step 02), applies :func:`opdi.cleaning.native.clean_tracks`,
and writes ``osn_tracks_clean``.

**Why a separate table rather than cleaning in place.** ``osn_tracks`` backs
published OPDI releases. Masking values inside it would silently change data
that events v0.0.2 was derived from, and there would be no way to reproduce a
published result. A second table costs storage but keeps the existing output
byte-for-byte reproducible, and lets the benchmark compare milestones built on
cleaned versus uncleaned tracks -- which is the measurement that justifies the
step existing at all. Once that benchmark exists, collapsing the two tables is
a decision that can be made on evidence.
"""

import os
from datetime import date, datetime
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opdi.cleaning.native import clean_tracks, null_rate_report
from opdi.config import OPDIConfig
from opdi.utils.datetime_helpers import get_start_end_of_month
from opdi.utils.storage import StorageManager

SOURCE_TABLE = "osn_tracks"
TARGET_TABLE = "osn_tracks_clean"


class TrackCleaner:
    """Applies native trajectory cleaning to ``osn_tracks``, month by month."""

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

    def process_month(
        self, month: date, skip_if_processed: bool = True, report: bool = True
    ) -> None:
        """Clean one month of tracks.

        Args:
            month: Any date within the month to process.
            skip_if_processed: Skip months already in the log.
            report: Print per-column NULL rates after cleaning. This is the
                measurement the pipeline verification asserts against; it costs
                one extra Spark action per month.
        """
        if not self.cleaning.enabled:
            print("Cleaning is disabled in config. Skipping step 02a.")
            return

        if skip_if_processed and month in self._load_processed_months():
            print(f"Month {month.strftime('%Y-%m')} already cleaned. Skipping.")
            return

        print(f"Cleaning tracks for {month.strftime('%Y-%m')}... ({datetime.now()})")

        start_time, end_time = get_start_end_of_month(month)
        start_str = datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
        end_str = datetime.utcfromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")

        source = self.storage.table_ref(SOURCE_TABLE)
        df = self.spark.sql(
            f"""
            SELECT * FROM {source}
            WHERE (event_time >= TIMESTAMP('{start_str}'))
              AND (event_time < TIMESTAMP('{end_str}'));
            """
        )

        cleaned = clean_tracks(df, self.cleaning)

        if self.cleaning.enable_pandas_stage:
            from opdi.cleaning.pandas_udf import apply_pandas_stages

            cleaned = apply_pandas_stages(cleaned, self.cleaning)

        cleaned = cleaned.repartition("track_id")
        self.storage.write_table(cleaned, TARGET_TABLE)

        if report:
            rates = null_rate_report(self.storage.read_table(TARGET_TABLE))
            print("  NULL rate after cleaning:")
            for name, rate in sorted(rates.items()):
                print(f"    {name:<16} {rate:6.2%}")

        cleaned.unpersist(blocking=True)
        self.spark.catalog.clearCache()
        self._mark_month_processed(month)

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
        self.storage.create_table(
            f"""
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
            geo_altitude_c DOUBLE COMMENT 'Rolling-mean-corrected GNSS altitude, metres (step 02).',
            baro_altitude_c DOUBLE COMMENT 'Rolling-mean-corrected barometric altitude, metres (step 02).',
            segment_id STRING COMMENT 'Contiguous sub-track: splits track_id at coverage holes.'
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='SNAPPY',
            'write.distribution.mode'='hash'
        )
        COMMENT '`{self.project}`.`{SOURCE_TABLE}` after native trajectory cleaning (step 02a). Bad values are masked to NULL, rows are preserved.';
        """
        )
        print(f"Table {self.project}.{TARGET_TABLE} created/verified.")
