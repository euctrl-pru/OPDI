"""All settings together, and the two environments: :class:`OPDIConfig`."""

import os
from dataclasses import dataclass, field, replace
from typing import Optional

from opdi.config.adsc import AdscConfig
from opdi.config.cleaning import CleaningConfig
from opdi.config.detection import DetectionConfig
from opdi.config.events import EventConfig
from opdi.config.h3 import H3Config
from opdi.config.ingestion import IngestionConfig
from opdi.config.project import ProjectConfig
from opdi.config.segmentation import SegmentationConfig
from opdi.config.spark import SparkConfig
from opdi.coverage import CoverageConfig

#: The warehouse of the ``opensky`` environment when nothing else is named:
#: the storage prefix under which each release has its own folder.
OPENSKY_DEFAULT_WAREHOUSE = "s3a://eurocontrol/opdi"

#: Environment variable that names the warehouse of the ``opensky``
#: environment.
WAREHOUSE_VARIABLE = "OPDI_WAREHOUSE"


def _worldwide_warehouse(declared_warehouse_path: str) -> str:
    """Where a worldwide run writes in the ``local`` environment.

    That environment keeps its European tables at the declared path, so a
    worldwide run writes beside them, at the declared path followed by
    ``-world``. The ``opensky`` environment does not use this rule: there the
    coverage does not change the warehouse, and the coverage marker (a small
    table that records which coverage built a warehouse) refuses to mix the
    two.
    """
    return declared_warehouse_path.rstrip("/") + "-world"


@dataclass
class OPDIConfig:
    """Every setting of the pipeline, one group per field.

    Build it with :meth:`for_environment`, then change single fields, for
    example::

        config = OPDIConfig.for_environment("opensky")
        config.events = EventConfig.legacy()
    """

    project: ProjectConfig = field(default_factory=ProjectConfig)
    """Where the tables are kept."""
    spark: SparkConfig = field(default_factory=SparkConfig)
    """The Spark session."""
    h3: H3Config = field(default_factory=H3Config)
    """The H3 resolutions."""
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)
    """Sources of the reports and reference data (step 01, part of step 00)."""
    cleaning: CleaningConfig = field(default_factory=CleaningConfig)
    """Trajectory cleaning (step 02a)."""
    adsc: AdscConfig = field(default_factory=AdscConfig)
    """ADS-C as a second source (step 01b, off by default)."""
    detection: DetectionConfig = field(default_factory=DetectionConfig)
    """Departure and destination airports (step 03)."""
    events: EventConfig = field(default_factory=EventConfig)
    """Flight events and measurements (step 04)."""
    segmentation: SegmentationConfig = field(default_factory=SegmentationConfig)
    """Track building (step 02)."""
    coverage: CoverageConfig = field(default_factory=CoverageConfig)
    """Worldwide or a bounding box."""

    @classmethod
    def for_environment(
        cls,
        env: str = "opensky",
        worldwide: bool = True,
        warehouse: Optional[str] = None,
    ) -> "OPDIConfig":
        """Create the configuration of one environment.

        Where the ``opensky`` environment writes is decided by one setting,
        the warehouse: the ``warehouse`` argument if given, else the
        ``OPDI_WAREHOUSE`` environment variable if set, else
        ``s3a://eurocontrol/opdi``. The same rule holds for both coverages.
        The tables of the release then go to ``<warehouse>/v2.0.0`` (see
        :attr:`ProjectConfig.release_root`).

        Args:
            env: Environment name: ``"opensky"`` (the OpenSky cluster, the
                default) or ``"local"`` (one machine).
            worldwide: Process the whole world (the default). Pass ``False``
                to keep only positions inside the European box
                (``EUROPE_BBOX``). In ``local`` a worldwide run writes to the
                declared warehouse path followed by ``-world``.
            warehouse: The warehouse to use. It replaces the environment's
                declared path in every environment.

        Returns:
            The configuration.
        """
        if env == "local":
            # Local testing environment
            config = cls(
                project=ProjectConfig(
                    project_name="opdi_local",
                    warehouse_path="./data/warehouse",
                ),
                spark=SparkConfig(
                    app_name="OPDI Pipeline - Local",
                    driver_memory="4G",
                    executor_memory="4G",
                    executor_instances="1",
                    dynamic_allocation_max_executors="2",
                    iceberg_jar_path="",  # set by hand when Iceberg is used
                ),
            )
        elif env == "opensky":
            # The OpenSky cluster: Parquet folders on S3, executors on Kubernetes.
            config = cls(
                project=ProjectConfig(
                    project_name="opensky",
                    # Every table is read and written under this one prefix,
                    # so OPDI_WAREHOUSE runs the whole pipeline elsewhere
                    # without touching the production tables.
                    warehouse_path=os.environ.get(WAREHOUSE_VARIABLE) or OPENSKY_DEFAULT_WAREHOUSE,
                ),
                spark=SparkConfig(
                    app_name="OPDI - OpenSky",
                    driver_memory="10G",
                    executor_memory="12g",
                    executor_memory_overhead="2g",
                    executor_cores="2",
                    # 13 executors of 2 cores use 26 of the namespace's 30 CPUs;
                    # memory is the limit (13 x 14 GiB of a 192 GiB quota, with
                    # room left for the driver). The number of executors does
                    # not change the results.
                    executor_instances="13",
                    enable_hive=False,
                    enable_iceberg=False,
                    s3_endpoint="https://s3.opensky-network.org",
                    spark_packages="org.apache.spark:spark-hadoop-cloud_2.13:4.1.1",
                    k8s_master="k8s://https://192.168.60.102:6443",
                    k8s_namespace="eurocontrol",
                    # The executor image must carry h3 version 4, which this
                    # package's H3 calls need; tags before -6 carry version 3.
                    k8s_container_image="docker.io/quintengs/opdi-spark:v4.1.1-6",
                    k8s_executor_memory_limit="14g",
                    # Equal to executor_cores: the namespace quota counts CPU
                    # limits, so a higher limit would reserve CPUs no executor
                    # uses and allow fewer executors.
                    k8s_executor_cores_limit="2",
                ),
            )
        else:
            raise ValueError(f"Unknown environment: {env}. Use 'local' or 'opensky'.")

        if worldwide:
            config.coverage = CoverageConfig(bbox=None)
            if env != "opensky":
                config.project = replace(
                    config.project,
                    warehouse_path=_worldwide_warehouse(config.project.warehouse_path),
                )
        if warehouse:
            config.project = replace(config.project, warehouse_path=warehouse)
        return config

    def describe_target(self) -> str:
        """Where this configuration reads and writes, as lines to print at start-up.

        Names the warehouse, the release root and the coverage. When the
        ``OPDI_WAREHOUSE`` environment variable is set to another place than
        the warehouse in use, a further line says so, so that a wrong target
        is seen before anything is written.
        """
        lines = [
            f"  warehouse   : {self.project.warehouse_path}",
            f"  release root: {self.project.release_root}",
            f"  coverage    : {self.coverage.label}",
        ]
        variable = os.environ.get(WAREHOUSE_VARIABLE)
        if variable and variable.rstrip("/") != self.project.warehouse_path.rstrip("/"):
            lines.append(
                f"  note        : {WAREHOUSE_VARIABLE} is set to {variable}, but this run "
                f"uses {self.project.warehouse_path}"
            )
        return "\n".join(lines)

    @classmethod
    def default(cls) -> "OPDIConfig":
        """Create the default configuration (``opensky`` environment)."""
        return cls.for_environment("opensky")
