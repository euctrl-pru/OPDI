"""
Configuration management for OPDI pipeline.

Provides centralized configuration using dataclasses for project settings,
Spark configurations, H3 parameters, and ingestion settings.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class ProjectConfig:
    """Project-level configuration."""

    project_name: str = "project_opdi"
    """Database/catalog name for Iceberg tables."""

    warehouse_path: str = "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged"
    """Warehouse path for Iceberg catalog."""

    hadoop_filesystem: str = "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged"
    """Hadoop filesystem path for Kerberos access."""


@dataclass
class SparkConfig:
    """Spark session configuration."""

    app_name: str = "OPDI Pipeline"
    """Spark application name."""

    # Driver settings
    driver_cores: str = "1"
    driver_memory: str = "8G"
    driver_max_result_size: str = "6g"

    # Executor settings
    executor_memory: str = "12G"
    executor_memory_overhead: str = "3G"
    executor_cores: str = "2"
    executor_instances: str = "3"

    # Dynamic allocation
    dynamic_allocation_max_executors: str = "10"

    # Network and timeouts
    network_timeout: str = "800s"
    executor_heartbeat_interval: str = "400s"

    # Compression
    shuffle_compress: str = "true"
    shuffle_spill_compress: str = "true"

    # UI settings
    ui_show_console_progress: str = "false"

    # Iceberg-specific settings
    iceberg_jar_path: str = "/opt/spark/optional-lib/iceberg-spark-runtime-3.5_2.12-1.5.2.1.23.17218.0-1.jar"
    """Path to Iceberg Spark runtime JAR."""

    handle_timestamp_without_timezone: str = "true"
    """Handle timestamps without timezone in Iceberg."""

    # Azure/Hadoop settings
    hadoop_group: str = "eur-app-opdi"
    """Required group for Azure filesystem access."""

    def to_spark_config(self, project_config: ProjectConfig) -> Dict[str, str]:
        """
        Convert to Spark configuration dictionary.

        Args:
            project_config: Project configuration for warehouse paths

        Returns:
            Dictionary of Spark configuration key-value pairs
        """
        return {
            "spark.ui.showConsoleProgress": self.ui_show_console_progress,
            "spark.hadoop.fs.azure.ext.cab.required.group": self.hadoop_group,
            "spark.kerberos.access.hadoopFileSystems": project_config.hadoop_filesystem,
            "spark.jars": self.iceberg_jar_path,
            "spark.executor.extraClassPath": self.iceberg_jar_path,
            "spark.driver.extraClassPath": self.iceberg_jar_path,
            "spark.sql.catalog.spark_catalog.type": "hive",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.iceberg.handle-timestamp-without-timezone": self.handle_timestamp_without_timezone,
            "spark.sql.catalog.spark_catalog.warehouse": project_config.warehouse_path,
            "spark.driver.cores": self.driver_cores,
            "spark.driver.memory": self.driver_memory,
            "spark.executor.memory": self.executor_memory,
            "spark.executor.memoryOverhead": self.executor_memory_overhead,
            "spark.executor.cores": self.executor_cores,
            "spark.executor.instances": self.executor_instances,
            "spark.dynamicAllocation.maxExecutors": self.dynamic_allocation_max_executors,
            "spark.network.timeout": self.network_timeout,
            "spark.executor.heartbeatInterval": self.executor_heartbeat_interval,
            "spark.driver.maxResultSize": self.driver_max_result_size,
            "spark.shuffle.compress": self.shuffle_compress,
            "spark.shuffle.spill.compress": self.shuffle_spill_compress,
        }


@dataclass
class H3Config:
    """H3 hexagonal indexing configuration."""

    airport_detection_resolution: int = 7
    """H3 resolution for airport detection zones (~5.2 km hexagons)."""

    airport_layout_resolution: int = 12
    """H3 resolution for airport ground layouts (~307 m hexagons)."""

    track_resolutions: List[int] = field(default_factory=lambda: [7, 12])
    """H3 resolutions for track encoding."""

    airspace_resolution: int = 7
    """H3 resolution for airspace encoding."""


@dataclass
class IngestionConfig:
    """Data ingestion configuration."""

    # MinIO / OpenSky Network
    minio_endpoint: str = "https://s3.opensky-network.org"
    """MinIO endpoint for OpenSky Network data."""

    osn_aircraft_db_url: str = "https://s3.opensky-network.org/data-samples/metadata/aircraft-database-complete-2024-10.csv"
    """URL for OpenSky Network aircraft database."""

    # OurAirports
    ourairports_base_url: str = "https://ourairports.com/data/"
    """Base URL for OurAirports CSV datasets."""

    ourairports_datasets: Dict[str, str] = field(default_factory=lambda: {
        'airports': 'airports.csv',
        'runways': 'runways.csv',
        'navaids': 'navaids.csv',
        'airport-frequencies': 'airport-frequencies.csv',
        'countries': 'countries.csv',
        'regions': 'regions.csv',
    })
    """OurAirports dataset filenames."""

    # Batch processing
    batch_size: int = 250
    """Number of files to process in a single batch (state vectors)."""

    # Track splitting parameters
    track_gap_threshold_minutes: int = 30
    """Time gap threshold for splitting tracks (minutes)."""

    track_gap_low_altitude_minutes: int = 15
    """Time gap threshold at low altitude for splitting tracks (minutes)."""

    track_gap_low_altitude_meters: float = 1524.0
    """Altitude threshold for low altitude track splitting (meters, ~5000 ft)."""

    # Altitude cleaning
    max_vertical_rate_mps: float = 25.4
    """Maximum realistic vertical rate in m/s (~5000 ft/min)."""

    altitude_smoothing_window_minutes: int = 5
    """Window size for altitude smoothing (minutes)."""


@dataclass
class OPDIConfig:
    """Main OPDI configuration container."""

    project: ProjectConfig = field(default_factory=ProjectConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    h3: H3Config = field(default_factory=H3Config)
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)

    @classmethod
    def for_environment(cls, env: str = "dev") -> "OPDIConfig":
        """
        Create configuration for specific environment.

        Args:
            env: Environment name ("dev", "live", or "local")

        Returns:
            OPDIConfig instance with environment-specific settings
        """
        if env == "live":
            # Production environment settings
            return cls(
                project=ProjectConfig(
                    project_name="project_opdi",
                    warehouse_path="abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged",
                    hadoop_filesystem="abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged",
                ),
                spark=SparkConfig(
                    app_name="OPDI Pipeline - Live",
                    driver_memory="14G",
                    executor_memory="16G",
                    executor_instances="3",
                    dynamic_allocation_max_executors="20",
                ),
            )
        elif env == "dev":
            # Development environment settings
            return cls(
                project=ProjectConfig(
                    project_name="project_opdi",
                    warehouse_path="abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/opdi.db/unmanaged",
                    hadoop_filesystem="abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/opdi.db/unmanaged",
                ),
                spark=SparkConfig(
                    app_name="OPDI Pipeline - Dev",
                    driver_memory="8G",
                    executor_memory="12G",
                    executor_instances="2",
                    dynamic_allocation_max_executors="10",
                    hadoop_group="eur-app-opdi-dev",
                ),
            )
        elif env == "local":
            # Local testing environment
            return cls(
                project=ProjectConfig(
                    project_name="opdi_local",
                    warehouse_path="./data/warehouse",
                    hadoop_filesystem="file:///",
                ),
                spark=SparkConfig(
                    app_name="OPDI Pipeline - Local",
                    driver_memory="4G",
                    executor_memory="4G",
                    executor_instances="1",
                    dynamic_allocation_max_executors="2",
                    hadoop_group="",  # No Azure auth for local
                    iceberg_jar_path="",  # May need to be set manually
                ),
            )
        else:
            raise ValueError(f"Unknown environment: {env}. Use 'dev', 'live', or 'local'.")

    @classmethod
    def default(cls) -> "OPDIConfig":
        """Create default configuration (dev environment)."""
        return cls.for_environment("dev")
