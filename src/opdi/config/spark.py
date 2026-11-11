"""Settings of the Spark session: :class:`SparkConfig`."""

from dataclasses import dataclass
from typing import Dict

from opdi.config.project import ProjectConfig

#: Time zone of every Spark session of the pipeline.
SESSION_TIME_ZONE = "UTC"


@dataclass
class SparkConfig:
    """Settings of the Spark session.

    Memory, cores, storage access and, for a run on Kubernetes, the
    executors. The values are passed to Spark unchanged, as text, so they use Spark's own
    notation (``"8G"``, ``"800s"``).
    """

    app_name: str = "OPDI Pipeline"
    """Spark application name."""

    # Driver settings
    driver_cores: str = "1"
    """Cores of the Spark driver (``spark.driver.cores``)."""
    driver_memory: str = "8G"
    """Java heap of the driver (``spark.driver.memory``). The period runner reduces it to what
    the container can hold."""
    driver_max_result_size: str = "6g"
    """Largest result the driver collects from the executors
    (``spark.driver.maxResultSize``)."""

    # Executor settings
    executor_memory: str = "12G"
    """Java heap of each executor (``spark.executor.memory``)."""
    executor_memory_overhead: str = "3G"
    """Memory of each executor outside the heap (``spark.executor.memoryOverhead``)."""
    executor_cores: str = "2"
    """Cores of each executor (``spark.executor.cores``)."""
    executor_instances: str = "3"
    """Number of executors (``spark.executor.instances``)."""

    # Dynamic allocation
    dynamic_allocation_max_executors: str = "10"
    """Largest number of executors with dynamic allocation."""

    # Network and timeouts
    network_timeout: str = "800s"
    """Spark network timeout (``spark.network.timeout``)."""
    executor_heartbeat_interval: str = "400s"
    """Interval of the executors' heartbeats to the driver."""

    # Compression
    shuffle_compress: str = "true"
    """Compress shuffle output (``spark.shuffle.compress``)."""
    shuffle_spill_compress: str = "true"
    """Compress data spilled to disk during shuffles."""

    # UI settings
    ui_show_console_progress: str = "false"
    """Show the progress bar of Spark stages in the console."""

    # Iceberg-specific settings
    iceberg_jar_path: str = (
        "/opt/spark/optional-lib/iceberg-spark-runtime-3.5_2.12-1.5.2.1.23.17218.0-1.jar"
    )
    """Path of the Iceberg Spark runtime library, used when
    :attr:`enable_iceberg` is set."""

    handle_timestamp_without_timezone: str = "true"
    """Let Iceberg tables hold timestamps without a time zone."""

    # Hadoop group settings
    hadoop_group: str = "eur-app-opdi"
    """Group required to access an Azure file system through Hadoop. Used
    only with a Hive catalog."""

    # S3 settings (for OpenSky environment)
    s3_endpoint: str = ""
    """S3 address of the storage (``opensky`` environment). Empty: no S3 access is
    configured."""
    s3_access_key: str = ""
    """S3 access key; normally read from ``AWS_ACCESS_KEY_ID`` instead."""
    s3_secret_key: str = ""
    """S3 secret key; normally read from ``AWS_SECRET_ACCESS_KEY`` instead."""
    spark_packages: str = ""
    """Maven packages Spark loads at start (``spark.jars.packages``), for S3 access."""

    # Kubernetes distributed settings
    k8s_master: str = ""
    """Address of the Kubernetes API that runs the executors. Empty: Spark runs on the local
    machine only."""
    k8s_namespace: str = ""
    """Kubernetes namespace of the executors."""
    k8s_container_image: str = ""
    """Container image of the executors; it must hold every Python dependency of the package."""
    k8s_driver_host: str = "jupyterlab"
    """Host name under which the executors reach the driver."""
    k8s_driver_bind_address: str = "0.0.0.0"
    """Address the driver listens on."""
    k8s_driver_port: str = "7078"
    """Port of the driver."""
    k8s_driver_block_manager_port: str = "7079"
    """Port of the driver's block manager."""
    k8s_executor_memory_limit: str = ""
    """Kubernetes memory limit of each executor."""
    k8s_executor_cores_limit: str = ""
    """Kubernetes CPU limit of each executor."""

    # Feature flags
    enable_hive: bool = True
    """Use a Hive metastore for the catalog."""
    enable_iceberg: bool = True
    """Store tables as Iceberg tables in a catalog. When ``False``, tables
    are plain Parquet folders under the release root (the ``opensky``
    environment)."""

    def to_spark_config(self, project_config: ProjectConfig) -> Dict[str, str]:
        """The Spark settings as a dictionary of Spark keys and values.

        Args:
            project_config: The storage settings, for the warehouse path.

        Returns:
            Spark configuration keys and values.
        """
        # The days of OPDI are UTC days, and Spark turns times into dates in
        # the session's time zone. Every session therefore works in UTC,
        # whatever the zone of the machine.
        configs: Dict[str, str] = {"spark.sql.session.timeZone": SESSION_TIME_ZONE}

        # S3 access (opensky environment)
        if self.s3_endpoint:
            configs.update(
                {
                    "spark.jars.packages": self.spark_packages,
                    "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    "spark.hadoop.fs.s3a.aws.credentials.provider": (
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                    ),
                    "spark.driver.memory": self.driver_memory,
                    "spark.driver.maxResultSize": self.driver_max_result_size,
                }
            )

        # Iceberg catalog settings (local environment with Iceberg)
        if self.enable_iceberg:
            configs.update(
                {
                    "spark.ui.showConsoleProgress": self.ui_show_console_progress,
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
                    "spark.hadoop.fs.azure.ext.cab.required.group": self.hadoop_group,
                    "spark.kerberos.access.hadoopFileSystems": project_config.hadoop_filesystem,
                    "spark.jars": self.iceberg_jar_path,
                    "spark.executor.extraClassPath": self.iceberg_jar_path,
                    "spark.driver.extraClassPath": self.iceberg_jar_path,
                    "spark.sql.catalog.spark_catalog.type": "hive",
                    "spark.sql.catalog.spark_catalog": (
                        "org.apache.iceberg.spark.SparkSessionCatalog"
                    ),
                    "spark.sql.iceberg.handle-timestamp-without-timezone": (
                        self.handle_timestamp_without_timezone
                    ),
                    "spark.sql.catalog.spark_catalog.warehouse": project_config.warehouse_path,
                }
            )

        return configs

    def to_distributed_config(self) -> Dict[str, str]:
        """Spark settings for executors on Kubernetes.

        Empty when :attr:`k8s_master` is not set, in which case Spark runs
        on the local machine only.
        """
        if not self.k8s_master:
            return {}
        return {
            "spark.submit.deployMode": "client",
            "spark.driver.host": self.k8s_driver_host,
            "spark.driver.bindAddress": self.k8s_driver_bind_address,
            "spark.driver.port": self.k8s_driver_port,
            "spark.driver.blockManager.port": self.k8s_driver_block_manager_port,
            "spark.executor.instances": self.executor_instances,
            "spark.executor.memory": self.executor_memory,
            "spark.executor.cores": self.executor_cores,
            "spark.kubernetes.executor.limit.memory": self.k8s_executor_memory_limit,
            "spark.kubernetes.executor.limit.cores": self.k8s_executor_cores_limit,
            "spark.kubernetes.container.image": self.k8s_container_image,
            "spark.kubernetes.namespace": self.k8s_namespace,
        }
