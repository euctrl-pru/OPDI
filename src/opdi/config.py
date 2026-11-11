"""Settings of the OPDI pipeline.

Every setting of the pipeline is a field of a dataclass in this module, with
its default value. :class:`OPDIConfig` holds one instance of each group
(storage, Spark, ingestion, cleaning, track building, airport detection,
events) and :meth:`OPDIConfig.for_environment` builds a ready-made
configuration for one of the two environments, ``local`` and ``opensky``.

Thresholds are written in aviation units, with the unit in the field name
(``endpoint_radius_nm``, ``baro_altitude_d1_max_ft_s``), because that is how
the methods are described and how the published data is expressed. The
position reports themselves are stored in SI units (metres, metres per
second), as OpenSky delivers them; where a threshold meets such a column, the
column is scaled to the threshold's unit at the point of comparison.

The presets :meth:`DetectionConfig.legacy` and :meth:`EventConfig.legacy`,
together with ``segmentation.method = "legacy"`` and
``ingestion.decimation = "modulo"``, reproduce the published release v0.0.2.
Each method is described on the OPDI website, ``https://www.opdi.aero``.
"""

import os
import sys
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from opdi.coverage import CoverageConfig
from opdi.output.schema import RELEASE


def _load_dotenv() -> None:
    """Read ``KEY=value`` lines of a ``.env`` file into the environment.

    The file is looked for in the working directory, then in the repository
    root. A variable already set in the environment is not replaced.
    """
    for candidate in [Path.cwd(), Path(__file__).resolve().parent.parent.parent]:
        env_file = candidate / ".env"
        if env_file.is_file():
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())
            return


_load_dotenv()


@dataclass
class ProjectConfig:
    """Where the pipeline's tables are kept."""

    project_name: str = "project_opdi"
    """Name of the catalog database that holds the tables (``local``
    environment), or ``"opensky"`` for plain Parquet folders."""

    warehouse_path: str = (
        "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged"
    )
    """Folder (local path or ``s3a://`` address) under which the tables are
    written. :meth:`OPDIConfig.for_environment` sets it for each environment."""

    hadoop_filesystem: str = (
        "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged"
    )
    """File system the Spark session may access with Kerberos. Used only with
    a Hive catalog; the two environments of this package do not use it."""

    release: str = RELEASE
    """Name of the data release, for example ``v2.0.0``.

    In the ``opensky`` environment every table, the progress records and the
    published files of a release are kept in a folder of this name inside the
    warehouse (see :attr:`release_root`), so two releases never share a
    table. Set it to an empty string to use the warehouse path itself, for
    example to read a folder that holds its tables directly."""

    @property
    def release_root(self) -> str:
        """The folder that holds this release's tables.

        In the ``opensky`` environment it is ``<warehouse_path>/<release>``,
        for example ``s3a://eurocontrol/opdi/v2.0.0``. In the other
        environments, and when :attr:`release` is empty, it is
        :attr:`warehouse_path` itself.
        """
        if self.project_name == "opensky" and self.release:
            return f"{self.warehouse_path.rstrip('/')}/{self.release}"
        return self.warehouse_path


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
        configs: Dict[str, str] = {}

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


@dataclass
class H3Config:
    """Resolutions of the H3 grid used to index positions and airports.

    H3 divides the Earth into hexagonal cells; a higher resolution means
    smaller cells. Resolution 7 cells are about 5 km across, resolution 12
    cells about 20 m.
    """

    airport_detection_resolution: int = 7
    """Resolution of the cells around each airport used to find departure and
    destination airports (about 5 km across)."""

    airport_layout_resolution: int = 12
    """Resolution of the cells that map runways, taxiways and stands (about
    20 m across)."""

    airport_layout_pbf_path: Optional[str] = None
    """Local OpenStreetMap extract (``.pbf`` file) that step 00b reads the
    airport layouts (runways, taxiways, stands) from.

    When ``None``, step 00b queries the public Overpass service instead,
    which is slow for many airports and can return nothing for an airport
    whose name it cannot resolve. The environment variable ``OPDI_OSM_PBF``
    replaces this setting.

    A file filtered to aeroway features is much smaller and faster to read
    than a raw regional extract, and a raw extract of a whole continent needs
    more than 16 GB of memory; see
    :func:`opdi.reference.pbf_filter.filter_aeroway_pbf`.
    """

    track_resolutions: List[int] = field(default_factory=lambda: [7, 12])
    """Resolutions at which every position of a track is indexed (columns
    ``h3_res_7`` and ``h3_res_12``)."""

    airspace_resolution: int = 7
    """Resolution of the cells that map airspaces (FIRs)."""


@dataclass
class IngestionConfig:
    """Sources of the position reports and reference data, and their thinning.

    Also holds the time gaps that split tracks under the v0.0.2 rule.
    """

    # OpenSky Network
    minio_endpoint: str = "https://s3.opensky-network.org"
    """S3 address of the OpenSky Network archive of position reports."""

    osn_aircraft_db_url: str = (
        "https://s3.opensky-network.org/data-samples/metadata/aircraft-database-complete-2024-10.csv"
    )
    """Address of the OpenSky aircraft database (registration, type,
    operator by transponder address)."""

    decimation: str = "bucket"
    """How the 1 Hz position reports are thinned to one per 5 seconds:
    ``"bucket"`` (default) or ``"modulo"``.

    ``"bucket"`` keeps the newest report in each 5-second window.
    ``"modulo"`` keeps a report only when its time in seconds is a multiple of
    5, which is the rule of release v0.0.2. The two give nearly the same rows
    on OpenSky data, which repeats the last position every second, but the
    bucket rule does not depend on that.

    Changing this changes which reports reach track building, and therefore
    the ``track_id`` values.
    See https://www.opdi.aero/content/methods/track-identity.html.
    """

    # OurAirports
    ourairports_base_url: str = "https://ourairports.com/data/"
    """Address of the OurAirports data files (airports, runways)."""

    ourairports_datasets: Dict[str, str] = field(
        default_factory=lambda: {
            "airports": "airports.csv",
            "runways": "runways.csv",
            "navaids": "navaids.csv",
            "airport-frequencies": "airport-frequencies.csv",
            "countries": "countries.csv",
            "regions": "regions.csv",
        }
    )
    """OurAirports files to download, by name."""

    # Batch processing
    batch_size: int = 250
    """Number of state-vector files downloaded and written in one batch."""

    # Track splitting thresholds of the v0.0.2 rule (segmentation method
    # "legacy"). The default rule reads SegmentationConfig instead.
    track_gap_threshold_minutes: int = 30
    """A gap in the reports longer than this, in minutes, starts a new track."""

    track_gap_low_altitude_minutes: int = 15
    """Below :attr:`track_gap_low_altitude_meters`, a gap longer than this,
    in minutes, starts a new track."""

    track_gap_low_altitude_meters: float = 1524.0
    """Altitude below which the shorter gap applies, in metres (1524 m is
    5000 ft). Kept in metres because it is compared with the stored
    barometric altitude and the published tracks depend on it."""


@dataclass
class CleaningConfig:
    """Thresholds of trajectory cleaning (pipeline step 02a).

    Cleaning reads the tracks (``osn_tracks``) and writes ``osn_tracks_clean``.
    A value that cannot be trusted is set to NULL and the row is kept, so
    "no data" stays distinct from "measured". The stages, in order:

    1. remove duplicate reports of the same track and second;
    2. set coordinates outside the possible range to NULL;
    3. set to NULL values that are only repeats of the previous report
       (ADS-B sends position and velocity in separate messages, so an
       unchanged value is often a repeat, not a new measurement);
    4. set to NULL the middle of spikes, found from the first and second
       differences of each column over time;
    5. set to NULL values with no other value of the same column within
       20 seconds on either side;
    6. start a new segment (``segment_id``) at every gap of 5 minutes or
       more, so later steps never interpolate across a hole in coverage.

    The method and thresholds follow the trajectory cleaning of Alligier and
    Gianazza (ENAC), winners of the 2024 PRC Data Challenge.
    See https://www.opdi.aero/content/methods/trajectory-cleaning.html.

    Thresholds are in aviation units, with the unit in the field name. The
    stored columns are in SI units (metres, metres per second), so each column
    is scaled to the threshold's unit only for the comparison, with the
    factors also used for the published events:

    ==============  ==================  =============
    Column          Aviation unit       Factor
    ==============  ==================  =============
    baro/geo alt    m -> ft             3.28084
    vert_rate       m/s -> ft/min       196.850394
    velocity        m/s -> kt           1.94384
    heading         deg (unchanged)     1.0
    lat / lon       deg (unchanged)     1.0
    ==============  ==================  =============

    The result is a NULL mask, which has no unit, so no converted value is
    ever stored.

    Both difference thresholds of a column have the same unit, column unit
    per second. The second difference is the change between two successive
    first differences divided by their mean time step, not a rate of a rate.
    """

    enabled: bool = True
    """Run the cleaning step. When ``False``, step 02a writes nothing and step
    03 reads the uncleaned tracks."""

    # -- Stage 1: duplicate removal -------------------------------------
    dedup_enabled: bool = True
    """Stage 1: keep one row per ``(track_id, event_time)``."""

    # -- Stage 2: range validity ----------------------------------------
    lat_min: float = -90.0
    """Lowest valid latitude, in degrees."""
    lat_max: float = 90.0
    """Highest valid latitude, in degrees."""
    lon_min: float = -180.0
    """Lowest valid longitude, in degrees."""
    lon_max: float = 180.0
    """Stage 2: possible latitude and longitude ranges, in degrees. A
    coordinate outside them is set to NULL."""

    # -- Stage 3: stale-broadcast removal --------------------------------
    stale_enabled: bool = True
    """Stage 3: set to NULL a value identical to the previous report's, as a
    repeat rather than a new measurement. Position (latitude and longitude
    together) and each of ``vert_rate``, ``heading`` and ``velocity`` are
    tested separately."""

    stale_position_uses_last_pos_update: bool = True
    """Use ``last_pos_update`` to tell a repeated position from an aircraft
    that is standing still.

    A parked aircraft reports exactly the same latitude and longitude every
    time, which the plain equality test of stage 3 would remove as repeats,
    leaving no position on the stand for the off-block and in-block
    detection. ``last_pos_update`` is the time at which ADS-B last measured
    the position. When it has advanced since the previous report, the
    position is a new measurement even if its value has not changed, and it
    is kept. Rows with no ``last_pos_update`` use the plain equality test.

    Applies to the position only; ``vert_rate``, ``heading`` and ``velocity``
    keep the plain equality test. Set to ``False`` for the plain test
    everywhere.
    """

    # -- Stage 4: derivative spike filter --------------------------------
    spike_enabled: bool = True
    """Stage 4: run the spike filter."""

    spike_min_votes: int = 2
    """Number of flagged difference windows a value must take part in before
    it is set to NULL.

    A difference is flagged when it exceeds the threshold of its column. The
    middle point of a spike takes part in the rise and in the fall, so it
    collects two flags, while a real step change collects only one and is
    kept. First and second differences are counted separately: a value is
    removed when it has at least this many first-difference flags, or at least
    this many second-difference flags."""

    baro_altitude_d1_max_ft_s: float = 200.0
    """Largest plausible first difference of barometric altitude, in ft/s."""
    baro_altitude_d2_max_ft_s: float = 50.0
    """Largest plausible second difference of barometric altitude, in ft/s."""

    geo_altitude_d1_max_ft_s: float = 200.0
    """Largest plausible first difference of geometric (GNSS) altitude, in
    ft/s."""
    geo_altitude_d2_max_ft_s: float = 150.0
    """Largest plausible second difference of geometric altitude, in ft/s.
    Looser than for barometric altitude, because GNSS altitude is noisier."""

    vert_rate_d1_max_ftmin_s: float = 1500.0
    """Largest plausible first difference of vertical rate, in ft/min per
    second."""
    vert_rate_d2_max_ftmin_s: float = 1000.0
    """Largest plausible second difference of vertical rate, in ft/min per
    second."""

    velocity_d1_max_kt_s: float = 12.0
    """Largest plausible first difference of ground speed, in kt/s."""
    velocity_d2_max_kt_s: float = 10.0
    """Largest plausible second difference of ground speed, in kt/s."""

    heading_d1_max_deg_s: float = 12.0
    """Largest plausible first difference of track angle, in degrees per
    second."""
    heading_d2_max_deg_s: float = 10.0
    """Largest plausible second difference of track angle, in degrees per
    second."""

    latlon_d1_max_deg_s: float = 0.01
    """Largest plausible first difference of latitude or longitude, in
    degrees per second (0.01 deg/s is about 1,100 m/s along a meridian)."""
    latlon_d2_max_deg_s: float = 0.06
    """Largest plausible second difference of latitude or longitude, in
    degrees per second."""

    # -- Stage 5: isolated-point removal ---------------------------------
    isolated_enabled: bool = True
    """Stage 5: run the isolated-value test."""
    isolated_max_gap_seconds: float = 20.0
    """A value with no other value of the same column within this many
    seconds on both sides cannot be checked against its neighbours and is set
    to NULL."""

    sparse_sources: Tuple[str, ...] = ("ADS-C",)
    """Values of the ``source`` column that skip stages 3 and 5.

    Both stages assume a dense feed: stage 5 removes a value with no
    neighbour within 20 seconds, and stage 3 reads an unchanged value as a
    repeat. ADS-C reports arrive about every 10 minutes and are each a new
    measurement, so without this exemption cleaning would remove every ADS-C
    value. The ``source`` column exists only when ADS-C is merged in
    (:attr:`AdscConfig.enabled`); otherwise this setting has no effect.
    """

    # -- Stage 6: gap segmentation ---------------------------------------
    segment_gap_seconds: float = 300.0
    """Stage 6: a gap of at least this many seconds between two reports of a
    track starts a new ``segment_id`` (default 300 s, 5 minutes)."""

    # -- Which tracks step 03 reads ---------------------------------------
    feeds_flight_list: bool = True
    """Build the flight list (step 03) from the cleaned tracks
    ``osn_tracks_clean`` (``True``) or from the uncleaned ``osn_tracks``
    (``False``, as in release v0.0.2).

    When cleaning is off (:attr:`enabled` is ``False``), step 03 reads the
    uncleaned tracks whatever this says. Cleaning keeps every row but sets
    untrusted values to NULL, and step 03 leaves out reports without a
    barometric altitude, so the cleaned tracks give step 03 fewer but more
    reliable reports."""


@dataclass
class AdscConfig:
    """ADS-C as a second source of positions. Off by default.

    Step 01b reads the ADS-C reports and step 02 merges them into the ADS-B
    reports before building the tracks.

    ADS-C (Automatic Dependent Surveillance - Contract) is a satellite and
    datalink position report sent mainly over oceans and remote areas. The
    OpenSky Network receives it as a separate feed. It is sparse (one report
    every few minutes), almost entirely at cruise altitude, and never on the
    ground, so it can extend a track's ends and fill gaps over the ocean but
    adds nothing near airports. Fields such as ground speed and vertical rate
    are empty in the older parts of the feed, so merged rows from those
    periods have NULL ``velocity`` and ``vert_rate``.

    Enabling it changes which reports form each track, and therefore the
    ``track_id`` values of every aircraft that has ADS-C reports; it adds a
    ``source`` column to the state vectors. Use it in a separate warehouse
    from the published data. The ADS-C ``flight`` field is the airline's
    IATA flight number, not the ICAO call sign that ADS-B carries, so it is
    not used as a call sign. See
    https://www.opdi.aero/content/methods/track-identity.html.
    """

    enabled: bool = False
    """Merge ADS-C reports into the state vectors before track building."""

    s3_base: str = "s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro"
    """Folder of the raw ADS-C feed, one folder per hour. The files are
    Parquet (compressed with snappy) even though the folder name ends in
    ``.avro``."""

    table: str = "osn_adsc_statevectors"
    """Table that step 01b writes the ADS-C reports to, in the state-vector
    schema, inside the run's own warehouse."""

    shadow_before_seconds: int = 60
    """Leave out an ADS-C report when an ADS-B report of the same aircraft
    lies at most this many seconds before it.

    Both feeds then describe the same position, and keeping both would put a
    second point a few hundred metres away at nearly the same time. The
    ADS-B report is kept: it is denser and carries the on-ground flag. The two
    sides of the window can be set separately because the clocks of the two
    feeds are not aligned (ADS-C reports are on average about 1.5 s ahead).
    """

    shadow_after_seconds: int = 60
    """Leave out an ADS-C report when an ADS-B report of the same aircraft
    lies at most this many seconds after it. See
    :attr:`shadow_before_seconds`."""

    time_offset_seconds: float = 0.0
    """Seconds added to the time of every ADS-C report. Zero by default.

    The time offset between the two feeds differs from one aircraft to the
    next (from about 0 to 2 seconds, with rare larger values), so one
    constant correction would make some aircraft worse. The setting exists
    for experiments with a correction. See
    ``benchmarks/adsc_time_sync.py``.
    """


@dataclass
class DetectionConfig:
    """Thresholds of departure and destination airport detection (step 03).

    Step 03 names, for every track, the airport it departed from (ADEP) and
    the airport it arrived at (ADES). Two methods are available, and each end
    of the flight can use either (:attr:`adep_mode`, :attr:`ades_mode`):

    ``trend``
        Looks at every report below a ceiling (:attr:`trend_max_height_ft`
        above the airport's elevation, or :attr:`trend_max_fl`) within
        :attr:`trend_radius_nm` of an airport, and decides from the smoothed
        altitude whether the aircraft was climbing away from it (a departure)
        or descending towards it (an arrival). It names no airport when the
        altitude neither clearly rises nor clearly falls.

    ``endpoint``
        Looks at the first report of the track (for the departure) or the
        last one (for the arrival) and accepts the nearest airport when that
        report is within :attr:`endpoint_radius_nm` and no more than
        :attr:`endpoint_height_ft` above the airport's elevation, or on the
        ground.

    Both rank candidate airports by distance plus a penalty for airports
    without scheduled service, so a small or military field is chosen only
    when it is clearly the nearest.

    Distances are in nautical miles (NM), heights in feet (ft). The defaults
    are those of release v2.0.0 (departures by ``endpoint``, arrivals by
    ``trend``); :meth:`legacy` gives the settings of release v0.0.2.
    See https://www.opdi.aero/content/methods/airports.html.
    """

    # -- trend -----------------------------------------------------------
    trend_max_fl: int = 60
    """Ceiling of the ``trend`` method as a flight level (60 means FL060,
    6,000 ft at standard pressure). Read only when :attr:`trend_max_datum`
    is ``"msl"``, as in release v0.0.2 (FL040)."""

    trend_max_height_ft: float = 6000.0
    """Ceiling of the ``trend`` method as a height above the airport's
    elevation, in feet. Read when :attr:`trend_max_datum` is ``"field"`` (the
    default).

    A ceiling measured from the airport rather than from sea level gives the
    method the same climb and descent to look at whatever the elevation of
    the airport."""

    trend_max_datum: str = "field"
    """What the ``trend`` ceiling is measured from.

    ``"field"``
        Height above the candidate airport's elevation, up to
        :attr:`trend_max_height_ft`. The default.
    ``"msl"``
        Flight level, up to :attr:`trend_max_fl`, as in release v0.0.2. The
        flight level is a whole number, so ``trend_max_fl = 60`` admits
        everything below 6,100 ft.
    """

    trend_radius_nm: float = 30.0
    """Radius around each airport, in NM, within which ``trend`` reads the
    reports. Default 30 NM."""

    trend_smooth_half_window: int = 2
    """Half-width, in reports, of the centred moving average applied to the
    barometric altitude before the climb or descent is decided. 2 means a
    window of five reports (about 25 seconds)."""

    trend_vote_margin: int = 0
    """Number of reports by which climbing must outnumber descending (or the
    reverse) for ``trend`` to decide. With fewer, the airport is not named.
    0 (the default) means a simple majority decides; release v0.0.2 used 4."""

    trend_rank_by: str = "haversine"
    """How ``trend`` ranks candidate airports: ``"haversine"`` (default) by
    the great-circle distance to each airport, or ``"ring"`` (release v0.0.2)
    by the number of H3 rings between the report and the airport, about 5 km
    each, with distance used only among airports at the same ring count."""

    trend_sched_penalty_nm: float = 10.0
    """Distance, in NM, added to an airport without scheduled service when
    ``trend`` ranks the candidates. 0 in release v0.0.2."""

    trend_smooth_before_cut: bool = True
    """Smooth the barometric altitude before the ceiling is applied (``True``)
    or after (``False``, release v0.0.2). Smoothing first keeps the moving
    average complete at the ceiling, where the altitude changes fastest."""

    trend_radius_exact: bool = True
    """Select the reports within :attr:`trend_radius_nm` by their exact
    great-circle distance (``True``), or by keeping only the H3 rings that lie
    wholly inside the radius (``False``, release v0.0.2), which leaves out
    reports in the rings that cross the radius."""

    trend_bearing_tiebreak_nm: float = 2.0
    """Distance band, in NM, within which ``trend`` chooses between candidate
    airports by the direction of flight rather than by distance. 0 turns it
    off.

    When two candidates are less than this much apart in ranked distance, the
    one the track's course points at (for an arrival) or comes from (for a
    departure) is chosen. Outside the band, distance decides alone."""

    # -- endpoint --------------------------------------------------------
    endpoint_radius_nm: float = 30.0
    """Largest distance, in NM, between the first or last report of a track
    and the airport ``endpoint`` names. Default 30 NM; 40 NM in release
    v0.0.2."""

    endpoint_height_ft: float = 15000.0
    """Largest height of the first or last report above the airport's
    elevation, in feet, for ``endpoint`` to name the airport. A report on the
    ground is accepted whatever its altitude."""

    endpoint_sched_penalty_nm: float = 10.0
    """Distance, in NM, added to an airport without scheduled service when
    ``endpoint`` ranks the candidates."""
    endpoint_candidate_radius_nm: float = 110.0
    """Radius, in NM, of the table of candidate airports kept for each track
    end (``opdi_endpoint_candidates``). Larger than any detection radius, so
    the radius can be changed without rebuilding the table."""

    # -- which method serves which end -----------------------------------
    adep_mode: str = "endpoint"
    """Method that names the departure airport: ``"endpoint"`` (default) or
    ``"trend"`` (release v0.0.2). The first report of a departing track is
    usually on or near the runway, so its position decides well."""

    ades_mode: str = "trend"
    """Method that names the destination airport: ``"trend"`` (default and
    release v0.0.2) or ``"endpoint"``. The last report of an arriving track
    is often where reception was lost rather than where it landed, so the
    altitude trend decides better."""

    def __post_init__(self) -> None:
        """Refuse an unknown :attr:`trend_max_datum`.

        Raises:
            ValueError: If it is neither ``"field"`` nor ``"msl"``.
        """
        if self.trend_max_datum not in ("field", "msl"):
            raise ValueError(
                f"trend_max_datum must be 'field' or 'msl', got "
                f"{self.trend_max_datum!r}. Falling back to a default here "
                f"would apply one altitude cut while reporting the other."
            )

    @classmethod
    def legacy(cls) -> "DetectionConfig":
        """The settings of release v0.0.2.

        Every flight list published in release v0.0.2 was built with these,
        so a day processed with them reproduces that release.
        """
        return cls(
            trend_max_fl=40,
            # Release v0.0.2 measured the ceiling as a flight level.
            trend_max_datum="msl",
            trend_radius_nm=30.0,
            trend_smooth_half_window=2,
            trend_vote_margin=4,
            trend_sched_penalty_nm=0.0,
            trend_rank_by="ring",
            # Not part of release v0.0.2.
            trend_smooth_before_cut=False,
            trend_radius_exact=False,
            trend_bearing_tiebreak_nm=0.0,
            endpoint_radius_nm=40.0,
            endpoint_height_ft=15000.0,
            endpoint_sched_penalty_nm=10.0,
            endpoint_candidate_radius_nm=110.0,
            # Release v0.0.2 used the trend method for both ends.
            adep_mode="trend",
            ades_mode="trend",
        )


@dataclass
class EventConfig:
    """Thresholds of flight event detection (step 04).

    Step 04 reads the tracks and the flight list and writes the flight events
    (``opdi_flight_events``) and their measurements (``opdi_measurements``).
    The fields are grouped by event family: flight phases and the events
    read from them, flight-level and distance-circle crossings, runway and
    off-block/in-block times, level segments, holding, point merge, FIR
    crossings and airport layout events. Each family has its page on the
    OPDI website, ``https://www.opdi.aero/content/methods/``.

    Units are in the field names: ``_ft`` feet, ``_fl`` flight level,
    ``_nm`` nautical miles, ``_kt`` knots, ``_ftmin`` feet per minute,
    ``_deg`` degrees, ``_seconds`` seconds, ``_pct`` percent.

    The defaults are those of release v2.0.0. :meth:`legacy` gives the
    behaviour of release v0.0.2 (event version ``events_v0.0.2``).
    """

    # -- flight phases (fuzzy logic) --------------------------------------
    #
    # Every report is given a flight phase (ground, climb, descent, level,
    # cruise) by fuzzy logic on altitude, vertical rate and speed, with the
    # membership functions of OpenAP. Take-off and landing, top of climb and
    # top of descent, and level-start/level-end are read from the changes of
    # phase. See https://www.opdi.aero/content/methods/vertical-events.html.
    phase_twindow_seconds: float = 60.0
    """Length, in seconds, of the window over which the phase labels are
    smoothed by majority vote before the changes of phase are read. A single
    misclassified report then no longer creates a spurious level segment or
    breaks a take-off. 0 turns smoothing off (release v0.0.2)."""

    phase_ground_ceiling_ft: float = 200.0
    """Height, in feet, at which the "ground" membership of a report falls to
    zero. Below it a report can be classified as on the ground. Measured from
    the airport's elevation when :attr:`phase_ground_above_field` is set."""

    phase_ground_above_field: bool = True
    """Measure the ground membership by height above the airport's elevation
    rather than by pressure altitude.

    Pressure altitude at an airport equals its elevation only on a standard
    day, so with a fixed 200 ft limit on pressure altitude no report at an
    airport higher than about 200 ft is ever classified as on the ground, and
    its flights get no take-off or landing. With this setting, the elevations
    of the flight's departure and destination airports are subtracted and the
    lower height of the two is used; an unknown elevation counts as 0 ft.
    Off in release v0.0.2."""

    phase_require_complete_rules: bool = True
    """A fuzzy rule can win only when all its inputs (altitude, vertical
    rate, speed) are present. Without this, a missing input is skipped and the
    rule is judged on fewer inputs, which raises its score. Off in release
    v0.0.2."""

    phase_cruise_speed_kt: float = 600.0
    """Centre of the "cruise" speed membership, in knots; see
    :attr:`phase_cruise_speed_sigma_kt`."""
    phase_cruise_speed_sigma_kt: float = 100.0
    """Centre and width, in knots, of the bell-shaped "cruise" speed
    membership (OpenAP values: 600 kt and 100 kt). An aircraft much slower
    than the centre, for example a turboprop at 250 kt, has a cruise
    membership close to zero and may get no top of climb or top of descent
    from the phase method."""

    # -- flight-level crossings and distance circles ----------------------
    crossing_levels_fl: tuple = (50, 70, 100, 245)
    """Flight levels whose crossings are detected (events
    ``first-xing-fl<level>`` and ``last-xing-fl<level>``). Default FL050,
    FL070, FL100 and FL245."""

    crossing_hysteresis_ft: float = 300.0
    """Half-width, in feet, of a band around each crossed level. A crossing
    counts only when the aircraft passes from below the band to above it (or
    the reverse), so an aircraft flying at the level does not create a
    crossing at every small altitude change. A flight that levels off at the
    level itself never leaves the band and has no crossing of it."""

    crossing_all_occurrences: bool = True
    """Keep every crossing of a level, not only the first and the last.
    Off in release v0.0.2."""

    crossing_interpolate: bool = True
    """Give the time and position at which the aircraft was exactly at the
    level, interpolated between the two reports around it, instead of the
    last report before it (up to 5 seconds early). Off in release v0.0.2."""

    ring_radii_nm: tuple = (40.0, 50.0, 60.0, 100.0, 110.0, 120.0)
    """Radii, in NM, of the circles around the departure and destination
    airports whose crossings are detected. 40 NM is the area used by the
    ICAO indicator of additional time in the arrival sequencing area (ASMA)
    and 100 NM its variant for airports that hold further out. Each radius
    also gives a column of the flight list (for example ``C40_ARR``). The
    distance is the great-circle distance to the airport, so it is not
    limited by the extent of the airport zones. Empty in release v0.0.2.
    See https://www.opdi.aero/content/methods/distance-rings.html."""

    ring_hysteresis_nm: float = 1.0
    """Half-width, in NM, of a band around each circle, playing the same role
    as :attr:`crossing_hysteresis_ft`: a track running along the circle does
    not cross it again and again."""

    # -- event families that release v0.0.2 did not publish ---------------
    emit_runway_events: bool = True
    """Publish the actual take-off and landing times (``ATOT``, ``ALDT``)
    detected on a runway. Off in release v0.0.2.
    See https://www.opdi.aero/content/methods/takeoff-landing.html."""

    emit_block_events: bool = True
    """Publish the off-block and in-block times (``AOBT``, ``AIBT``),
    detected from the first and last movement at the stand. Off in release
    v0.0.2. See https://www.opdi.aero/content/methods/block-times.html."""

    emit_level_offs: bool = True
    """Publish the level-off events (``level-off-start``, ``level-off-end``)
    of climbs and descents. Separate from ``level-start``/``level-end``,
    which are published in both releases. Off in release v0.0.2."""

    # -- ground movement (off-block and in-block) -------------------------
    ground_speed_threshold_kt: float = 2.0
    """Ground speed, in knots, above which an aircraft on the ground counts
    as moving (the value of the ``traffic`` library)."""

    ground_move_min_seconds: float = 30.0
    """Seconds the movement must last before it counts, which tells a real
    push-back from noise in the speed value."""

    ground_speed_derive_from_position: bool = True
    """Where the reported ground speed is missing, use the speed computed
    from two successive positions to detect ground movement.

    Ground speed is often missing on the airport surface, and cleaning
    removes repeated values, which is exactly what a parked aircraft that
    starts to move sends. The reported speed is used wherever it exists.
    Off in release v0.0.2."""

    # -- runway identification for take-off and landing times ------------
    runway_max_dist_nm: float = 5.0
    """Only reports within this distance of the airport, in NM, are used to
    identify the runway (the value of the ``traffic`` library)."""

    runway_max_height_ft: float = 1500.0
    """Only reports at most this high above the airport, in feet, are used
    to identify the runway."""

    runway_min_vert_rate_ftmin: float = 257.0
    """Vertical rate, in ft/min, that tells the initial climb or final
    descent from movement on the ground (1.3 m/s)."""

    runway_min_groundspeed_kt: float = 30.0
    """Lowest ground speed, in knots, of a report used to identify the
    runway, which leaves out taxiing."""

    runway_max_bearing_deg: float = 10.0
    """Largest difference, in degrees, between the median track of the
    reports and a runway's direction for that runway to be a candidate."""

    # -- level segments ---------------------------------------------------
    #
    # Values of the ICAO and EUROCONTROL definitions of level flight in climb
    # and descent, used by the level-off events.
    # See https://www.opdi.aero/content/methods/vertical-events.html.
    level_analysis_radius_nm: float = 200.0
    """Radius, in NM, around the departure airport within which the climb
    is analysed, and around the destination airport within which the descent
    is analysed. Measured as great-circle distance to the airport."""

    level_vertical_speed_limit_ftmin: float = 300.0
    """Largest vertical rate, in ft/min, of a report that belongs to a level
    segment."""

    level_band_limit_ft: float = 200.0
    """Height of the altitude band, in feet, within which the reports must
    stay to belong to one level segment."""

    level_min_duration_seconds: float = 20.0
    """Shortest level segment reported, in seconds."""

    level_exclusion_box_pct: float = 90.0
    """Lower edge of the cruise exclusion box, as a percentage of the top of
    climb (or top of descent) altitude. A level segment above this edge and
    longer than :attr:`level_exclusion_box_seconds` is cruise, not a
    level-off, and is left out."""

    level_exclusion_box_seconds: float = 300.0
    """Duration, in seconds, above which a level segment inside the
    exclusion box counts as cruise."""

    level_min_altitude_climb_ft: float = 3000.0
    """Height, in feet, from which level segments are looked for in the
    climb; the climb below it is not analysed."""

    level_min_altitude_descent_ft: float = 1800.0
    """Height, in feet, below which level segments are no longer looked for
    in the descent. Lower than for the climb, because an aircraft on final
    approach is close to level."""

    # -- runway milestones -------------------------------------------------
    emit_runway_milestones: bool = True
    """Publish the runway milestones of Airport Collaborative Decision
    Making (A-CDM): ``line-up``, ``take-off-roll``, ``airborne``,
    ``landing``, ``touchdown``, ``runway-vacated``, ``go-around`` and the
    two runway-crossing events. Off in release v0.0.2, which published
    ``take-off``, ``landing``, ``entry-runway`` and ``exit-runway`` instead.
    See https://www.opdi.aero/content/methods/runway-milestones.html."""

    runway_airborne_height_ft: float = 15.0
    """Height above the airport, in feet, at which the wheels are taken to
    leave or meet the runway. ``airborne`` and ``touchdown`` are the times,
    interpolated between reports, at which the height crosses this value.
    It is small rather than zero because barometric altitude near the
    ground is noisy by a few feet."""

    runway_roll_speed_kt: float = 50.0
    """Ground speed, in knots, above which a departing aircraft is rolling
    for take-off rather than taxiing (taxi speed is about 20 kt, rotation
    about 140 kt)."""

    runway_roll_min_seconds: float = 5.0
    """Seconds the roll speed must last before ``take-off-roll`` is set, so
    a fast turn-off is not read as a take-off roll."""

    runway_taxi_speed_kt: float = 40.0
    """Highest ground speed, in knots, of a passage over a runway that is
    read as a taxiing crossing. Take-offs and landings exceed it."""

    runway_crossing_max_seconds: float = 120.0
    """Longest passage over a runway, in seconds, that is read as a crossing."""

    runway_align_max_deg: float = 30.0
    """Largest difference, in degrees, between the median track of a passage
    and the runway direction for the passage to be a take-off or landing."""

    runway_cross_min_deg: float = 45.0
    """Smallest angle, in degrees, between a passage and the runway
    direction for the passage to be a crossing. A passage between
    :attr:`runway_align_max_deg` and this angle is classified as neither and
    gets no event."""

    goaround_trigger_height_ft: float = 500.0
    """Height above the airport, in feet, below which an arrival must
    descend for a later climb to be a go-around rather than a level-off."""

    goaround_recovery_height_ft: float = 1500.0
    """Height above the airport, in feet, that the aircraft must climb back
    to, without touching down, for ``go-around`` to be published."""

    goaround_min_roc_ftmin: float = 500.0
    """Rate of climb, in ft/min, that the climb back must reach."""

    goaround_radius_nm: float = 10.0
    """Largest distance from the airport, in NM, at which a go-around is
    looked for."""

    # -- counting movements ------------------------------------------------
    supersede_window_seconds: float = 10800.0
    """Two departures (or two arrivals) of one aircraft at one airport closer
    than this, in seconds, may be one movement. The earlier one is not
    counted as a movement when it has no take-off: a departure is often split
    into a short track that never leaves the stand and the flight itself.
    Three hours is longer than any such split and shorter than a real second
    flight. See https://www.opdi.aero/content/methods/airports.html."""

    # -- reception gaps inside a track -----------------------------------
    track_hole_seconds: float = 1800.0
    """A gap longer than this, in seconds, between two reports of one track
    is a hole. The default track rule keeps one ``track_id`` across a hole
    in reception when the two sides fit one continuous flight. No event
    detector interpolates across a hole: level segments and crossings are
    found in each piece between holes. Equal to
    :attr:`SegmentationConfig.gap_minutes`."""

    # -- holding -----------------------------------------------------------
    emit_holding: bool = True
    """Publish ``holding-start`` and ``holding-end`` (version
    ``holding_v0.1.0``).

    A hold is a period in which the aircraft turns through at least one
    nearly complete circle in one direction while staying in a small area at
    roughly one level, above circuit height and at flying speed. Racetrack
    holds and single orbits both count; ``info.pattern`` says which. The
    thresholds follow published holding detection methods and ICAO Doc 8168.
    See https://www.opdi.aero/content/methods/holding.html."""

    holding_window_seconds: float = 360.0
    """Length, in seconds, of the window over which the turn is summed. One
    holding circuit takes 4 to 7 minutes."""

    holding_sample_period_seconds: float = 5.0
    """Time between reports, in seconds, used to count the window in rows:
    the window spans ``holding_window_seconds / holding_sample_period_seconds``
    rows, which is much faster to compute than a window bounded in time. A
    window whose two ends are not 0.8 to 1.25 window lengths apart in time
    (because of a gap inside it) is not judged."""

    holding_max_sample_gap_seconds: float = 60.0
    """A heading change across a gap longer than this, in seconds, is not
    counted as a turn."""

    holding_turn_rate_spike_deg: float = 6.0
    """Turn rate, in degrees per second, above which a heading change is
    noise rather than a turn (twice the ICAO maximum of 3 deg/s)."""

    holding_turn_rate_min_deg: float = 0.4
    """Turn rate, in degrees per second, from which a report counts as
    turning. Airliners in a hold turn at 1 to 2 deg/s; track noise at the
    5-second report interval is about 0.2 deg/s."""

    holding_net_turn_min_deg: float = 300.0
    """Net turn, in degrees, a window must contain. A full circle is 360; 300
    allows for a window that includes a straight leg."""

    holding_same_sense_min_pct: float = 80.0
    """Smallest share, in percent, of the turn made in the window's main
    direction. Radar vectoring turns both ways and a figure of eight nets to
    zero; a hold turns one way."""

    holding_max_displacement_nm: float = 8.0
    """Largest distance, in NM, between the first and last positions of the
    window. A hold is 8 to 10 NM long; a go-around loop or a vectoring
    detour is wider."""

    holding_min_path_nm: float = 15.0
    """Smallest distance flown in the window, in NM (6 minutes at 150 kt),
    which leaves out slow traffic."""

    holding_max_altitude_range_ft: float = 2500.0
    """Largest total altitude change in one window, in feet (the sum of the
    changes between reports): two 1,000 ft steps down a holding stack. A
    climb or descent through the window exceeds it."""

    holding_min_height_ft: float = 2500.0
    """Height above the airport, in feet, below which nothing is a hold:
    circuits and go-arounds happen below it."""

    holding_min_speed_kt: float = 140.0
    """Lowest ground speed, in knots; helicopters and training circuits are
    slower."""

    holding_bridge_gap_seconds: float = 120.0
    """Two holding stretches closer than this, in seconds, are one hold."""

    holding_min_duration_seconds: float = 240.0
    """Shortest hold kept, in seconds (one standard circuit)."""

    holding_min_episode_turn_deg: float = 330.0
    """Smallest net turn over the whole hold, in degrees: at least one nearly
    complete circle."""

    holding_orbit_max_straight_pct: float = 15.0
    """Share of reports that are not turning, in percent, below which the
    hold is an orbit rather than a racetrack (a racetrack is 35 to 55 %
    straight)."""

    holding_orbit_max_turn_deg: float = 450.0
    """Total turn, in degrees, above which a hold is a racetrack even
    without straight legs (more than one and a quarter circles)."""

    holding_goaround_exclusion_seconds: float = 180.0
    """A hold that starts or ends within this many seconds of a
    ``go-around`` of the same track is left out: repeated approaches are not
    holds."""

    # -- point merge -------------------------------------------------------
    emit_point_merge: bool = True
    """Publish ``pms-leg-entry``, ``pms-leg-exit`` and ``pms-merge-point``
    (version ``pms_v0.1.0``) for arrivals at airports with a point merge
    system listed in ``reference/data/point_merge_systems.csv``.

    In a point merge system, arrivals fly an arc (a sequencing leg) at a
    constant distance from a merge point until they are sent direct to it.
    A leg is found as a stretch flown at constant distance from the merge
    point, inside the published band, moving along the arc, and followed by
    a direct to the merge point.
    See https://www.opdi.aero/content/methods/point-merge.html."""

    pms_candidate_radius_nm: float = 60.0
    """Reports farther than this, in NM, from every merge point of the
    destination are not looked at (the longest legs listed are 46 NM
    out)."""

    pms_candidate_max_altitude_ft: float = 25000.0
    """Reports above this altitude, in feet, are not looked at (legs are
    flown between FL070 and FL270)."""

    pms_leg_window_seconds: float = 45.0
    """Half-length, in seconds, of the window over which constant distance
    is tested (90 seconds in all)."""

    pms_leg_distance_band_nm: float = 1.5
    """Largest spread, in NM, of the distance to the merge point inside the
    window. A published leg is a chain of straight segments, not a perfect
    arc, and navigation has a tolerance."""

    pms_leg_distance_margin_nm: float = 2.0
    """Margin, in NM, around the listed band of leg distances."""

    pms_leg_min_bearing_sweep_deg: float = 8.0
    """Smallest change, in degrees, of the bearing from the merge point over
    the window. An aircraft on an 18 NM arc at 250 kt sweeps about 20 degrees
    in 90 seconds; an aircraft holding at the same distance sweeps none."""

    pms_leg_tangential_min_pct: float = 80.0
    """Smallest share, in percent, of reports in the window whose track is 60
    to 120 degrees off the bearing to the merge point, that is, moving along
    the arc."""

    pms_leg_min_duration_seconds: float = 90.0
    """Shortest leg kept, in seconds."""

    pms_direct_lookahead_seconds: float = 600.0
    """A leg counts only if the track reaches the merge point within this
    many seconds."""

    pms_direct_align_deg: float = 5.0
    """Largest difference, in degrees, between the track and the bearing to
    the merge point for the aircraft to be flying direct to it. Reported as
    ``info.direct_aligned``; it does not decide whether a leg is kept."""

    pms_direct_min_duration_seconds: float = 60.0
    """Shortest time, in seconds, flown direct that sets
    ``info.direct_aligned``."""

    pms_merge_point_radius_nm: float = 3.0
    """The track must come this close to the merge point, in NM, from a leg
    at least twice as far out, for the leg to count. ``pms-merge-point`` is
    set at the closest approach."""

    # -- FIR crossings -----------------------------------------------------
    emit_fir_crossings: bool = True
    """Publish ``xing-fir`` (version ``fir_v0.1.0``): one event at each
    change of flight information region (FIR) along a track, from the
    airspace table step 00c builds (``opdi_h3_airspace_ref``). The regions
    are those of the Network Manager airspace model, with lower FIRs and
    upper UIRs, so climbing through their boundary level is a crossing as
    well. Each event names the AIRAC cycle of the airspace definition.
    See https://www.opdi.aero/content/methods/fir-crossings.html."""

    fir_min_dwell_seconds: float = 60.0
    """A stay in one region shorter than this, in seconds, is counted as part
    of the region before it. A track along a boundary would otherwise cross
    it every few seconds."""

    distance_from_last_known_position: bool = True
    """Compute the distance flown from the last known position.

    Cleaning sets untrusted positions to NULL. With this setting (version
    ``distance_v0.0.3``), each step of the distance flown is measured from the
    last report that has a position, so a missing position no longer drops
    the distance before and after it; across a gap the straight line between
    its two sides is added, which is the least the aircraft can have flown.
    Off (version ``distance_v0.0.2``, release v0.0.2), a report next to a
    missing position adds no distance. The level segments' ``distance_nm``
    values depend on it as well.
    See https://www.opdi.aero/content/methods/measurements.html."""

    # -- one event type per question -----------------------------------
    merge_duplicate_milestones: Optional[bool] = None
    """Publish one event type per milestone, where two detectors give the
    same one.

    With the runway milestones on, ``airborne`` and the take-off time
    ``ATOT`` describe the same instant, as do ``touchdown`` and the landing
    time ``ALDT``. With this setting, one event is published::

        ATOT = airborne if detected, else the take-off time of the older method
        ALDT = touchdown if detected, else the landing time of the older method

    ``info.method`` of each such event says which detector gave it
    (``"acdm"`` or ``"legacy"``), because the two have different accuracy.
    The same setting publishes the stand times under the names ``AOBT`` and
    ``AIBT`` and gives the vertical profile tops (:attr:`emit_pru_tops`) the
    names ``top-of-climb`` and ``top-of-descent``.

    ``None`` (default) follows :attr:`emit_runway_milestones`. ``True`` with
    the runway milestones off is refused, as there is nothing to merge. Off
    in release v0.0.2.
    """

    # -- vertical profile tops and level segments --------------------------
    emit_pru_tops: bool = True
    """Publish the top of climb and top of descent of the vertical flight
    efficiency method of the EUROCONTROL Performance Review Unit (PRU), which
    are the first point of the cruise and the last. With
    :attr:`merge_duplicate_milestones` they are named ``top-of-climb`` and
    ``top-of-descent`` and are the only tops published; otherwise they are
    ``top-of-climb-cco`` and ``top-of-descent-cdo``, published beside the
    tops of the phase method.
    See https://www.opdi.aero/content/methods/vertical-events.html."""

    level_method: str = "pru"
    """Method of the level segments: ``"pru"`` (default), a rolling window of
    :attr:`level_window_seconds` whose height follows 300 ft per minute, or
    ``"icao"``, an altitude band anchored at the start of each segment
    (:attr:`level_band_limit_ft`)."""

    level_window_seconds: float = 10.0
    """Length, in seconds, of the rolling window of the ``"pru"`` method
    (10 s gives a window 50 ft high). The trajectory is interpolated to this
    time step first, as the method requires."""

    level_floors_above_field: bool = True
    """Measure :attr:`level_min_altitude_climb_ft` and
    :attr:`level_min_altitude_descent_ft` above the airport's elevation
    (default) rather than as pressure altitude, which would place them
    lower above the ground at a high airport."""

    level_anchor: str = "pru"
    """Tops that bound the cruise exclusion box of the level-off events:
    ``"pru"`` (default, the tops of :attr:`emit_pru_tops`) or ``"phase"``
    (the tops of the phase method)."""

    level_radius_enforced: bool = True
    """Look for level-offs only within :attr:`level_analysis_radius_nm` of
    the departure airport (climb) and the destination airport (descent).
    ``False`` looks at every level segment, whatever its distance."""

    # -- airport layout events -------------------------------------------
    #
    # Entry and exit events of runways, taxiways, aprons, stands and other
    # parts of an airport, matched on the H3 cells of the airport layout.
    # See https://www.opdi.aero/content/methods/surface-events.html.
    airport_gate_above_field: bool = True
    """Admit a report to the layout matching by its height above the
    airport (:attr:`airport_max_height_agl_ft`) rather than by flight level
    (:attr:`airport_max_fl`). A flight level is measured from standard
    pressure, so its height above the ground changes with the weather and
    the airport's elevation. Off in release v0.0.2."""

    airport_max_height_agl_ft: float = 2000.0
    """Highest height above the airport, in feet, of a report matched to the
    layout when :attr:`airport_gate_above_field` is set."""

    airport_admit_on_ground: bool = True
    """Admit every report flagged on the ground, whatever its altitude says.

    A surface position report carries no altitude, so a height test alone
    would leave out nearly every report on the airport surface. The height
    test still applies to airborne reports. It stops an aircraft flying over
    an airport from being matched to its layout. Off in release v0.0.2."""

    # -- airport events ---------------------------------------------------
    airport_max_fl: int = 20
    """Highest flight level of a report matched to the layout when
    :attr:`airport_gate_above_field` is off (release v0.0.2)."""

    airport_trace_gap_seconds: float = 300.0
    """A gap longer than this, in seconds, inside one passage through one
    part of the airport starts a new passage."""

    airport_events_ordered: bool = True
    """Take the position, altitude and measurements of an entry or exit event
    from the report at the entry or exit time. Without it (release v0.0.2)
    they come from whichever report Spark reads first, which need not be the
    one at that time."""

    # -- inputs and identifiers -------------------------------------------
    feeds_from_clean_tracks: bool = True
    """Detect events on the cleaned tracks ``osn_tracks_clean`` (default),
    which also carry the ``segment_id`` of each gap, rather than on the
    uncleaned ``osn_tracks`` (release v0.0.2)."""

    deterministic_event_ids: bool = True
    """Build each event id from the event's own content (track, type, time),
    so that processing a day again gives the same ids. Off (release v0.0.2),
    ids are row numbers counted over the day and change when the data
    change."""

    events_version: str = "events_v0.3.0"
    """Version string written on the events this configuration produces
    (column ``version``). A published version string is never reused for
    different results."""

    def __post_init__(self) -> None:
        """Resolve :attr:`merge_duplicate_milestones` and check the methods.

        Raises:
            ValueError: If ``merge_duplicate_milestones`` is ``True`` without
                the runway milestones, or ``level_method`` or
                ``level_anchor`` is not a known value.
        """
        if self.merge_duplicate_milestones is None:
            object.__setattr__(self, "merge_duplicate_milestones", self.emit_runway_milestones)
        elif self.merge_duplicate_milestones and not self.emit_runway_milestones:
            raise ValueError(
                "merge_duplicate_milestones=True requires emit_runway_milestones. "
                "The merge exists to reconcile the A-CDM family with the legacy "
                "one; with A-CDM off there is nothing to merge, and the fuzzy "
                "take-off/landing pair would be published beside a vocabulary "
                "that has no place for it. Leave it unset to follow "
                "emit_runway_milestones."
            )
        if self.level_method not in ("icao", "pru"):
            raise ValueError(
                f"level_method must be 'icao' or 'pru', got {self.level_method!r}. "
                "An unknown arm would silently fall through to no level segments "
                "at all, which reads as a coverage failure rather than a typo."
            )
        if self.level_anchor not in ("pru", "phase"):
            raise ValueError(f"level_anchor must be 'pru' or 'phase', got {self.level_anchor!r}.")

    def level_window_height_ft(self) -> float:
        """Height of the rolling window of the ``"pru"`` level method, in feet.

        The method fixes the window's height over its length at 300 ft per
        minute, so the height is computed from
        :attr:`level_vertical_speed_limit_ftmin` and
        :attr:`level_window_seconds` rather than set separately.
        """
        return self.level_vertical_speed_limit_ftmin * self.level_window_seconds / 60.0

    @classmethod
    def legacy(cls) -> "EventConfig":
        """The settings of release v0.0.2.

        Every event published in release v0.0.2 (version ``events_v0.0.2``)
        was built with these, so a day processed with them reproduces that
        release.
        """
        return cls(
            # Not part of release v0.0.2.
            phase_twindow_seconds=0.0,
            phase_ground_above_field=False,
            emit_runway_events=False,
            emit_block_events=False,
            emit_level_offs=False,
            ground_speed_derive_from_position=False,
            phase_require_complete_rules=False,
            crossing_all_occurrences=False,
            crossing_interpolate=False,
            airport_events_ordered=False,
            feeds_from_clean_tracks=False,
            deterministic_event_ids=False,
            # No distance circles in release v0.0.2.
            ring_radii_nm=(),
            emit_runway_milestones=False,
            merge_duplicate_milestones=False,
            emit_pru_tops=False,
            level_method="icao",
            level_floors_above_field=False,
            level_radius_enforced=False,
            level_anchor="phase",
            airport_gate_above_field=False,
            airport_admit_on_ground=False,
            distance_from_last_known_position=False,
            emit_holding=False,
            emit_point_merge=False,
            emit_fir_crossings=False,
            events_version="events_v0.0.2",
        )


#: Names of the track rules that are not part of the package. They are kept
#: with the study scripts, in ``benchmarks/study_variants/segmentation.py``,
#: which can register them for use (see :class:`SegmentationConfig`).
STUDY_SEGMENTATION_METHODS = (
    "no_month_suffix",
    "traffic_style",
    "airframe_only",
    "ground_anchored",
    "airport_anchored",
    "vertical_profile",
    "recommended",
    "debounced",
)


@dataclass
class SegmentationConfig:
    """How the position reports are split into tracks (step 02).

    A track is the sequence of reports of one aircraft that belong to one
    flight; its identifier is ``track_id``, which the flight list publishes
    as ``flight_id``. Two rules are available (:attr:`method`):

    * ``"standard"`` (default), the rule of release v2.0.0;
    * ``"legacy"``, the rule of release v0.0.2.

    Heights are in feet and times in minutes or seconds, with the unit in
    the field name. The thresholds reach the track builder through
    :meth:`opdi.pipeline.segmentation.base.SegmentationParams.from_config`.
    The ``legacy`` rule reads the gap thresholds of :class:`IngestionConfig`
    instead.
    See https://www.opdi.aero/content/methods/track-identity.html.
    """

    method: str = "standard"
    """The rule that builds ``track_id``: ``"standard"`` (default) or
    ``"legacy"``.

    ``"legacy"``, the rule of release v0.0.2, groups the reports by aircraft
    address and call sign, starts a new track at a gap longer than 30
    minutes (15 minutes below 5,000 ft), and ends each id with the year and
    month (``_2024_6``).

    ``"standard"`` (also reachable as ``"cruise_linked"``) groups the reports
    by aircraft address alone and starts a new track when:

    * the call sign changes to a new non-blank value that is then kept for at
      least :attr:`callsign_min_persistence_seconds`, so a call sign garbled
      for a few seconds does not split a flight; or
    * there is a gap longer than :attr:`gap_minutes` (or
      :attr:`low_alt_gap_minutes` at low altitude), unless the two sides of
      the gap fit one continuous flight at cruise level (the ``link_*``
      settings), as when a flight crosses an ocean out of reception.

    Because a track can then span a long gap, building one day reads the
    reports of up to about 11 hours before it (see
    :func:`opdi.pipeline.tracks.required_lookback_hours`), and the event
    detectors treat the gaps inside a track as holes
    (:attr:`EventConfig.track_hole_seconds`).

    The two rules give different ``track_id`` values for the same flight,
    and nothing in the data says which rule produced an id, so ids of
    different releases do not join. The study rules listed in
    :data:`STUDY_SEGMENTATION_METHODS` can be selected only after a study
    script has registered them; otherwise their names are refused.
    """

    gap_minutes: float = 30.0
    """A gap longer than this, in minutes, splits a track at any altitude,
    unless the two sides are linked as one flight."""

    low_alt_gap_minutes: float = 15.0
    """A gap longer than this, in minutes, splits a track when the aircraft
    is below :attr:`low_alt_ft`."""

    low_alt_ft: float = 5000.0
    """Altitude, in feet, below which the shorter gap applies."""

    callsign_lookback_minutes: float | None = None
    """How far back, in minutes, the last non-blank call sign is looked for
    when deciding whether the call sign changed. ``None`` uses
    :attr:`gap_minutes`."""

    callsign_min_persistence_seconds: float = 30.0
    """Seconds a new call sign must be kept before the change splits the
    track. A call sign that changes to a garbled value and back within this
    time does not split it."""

    link_max_gap_minutes: float = 600.0
    """Longest gap in reception, in minutes, that can be linked as one
    flight (10 hours, which covers the longest oceanic gaps). It also sets
    how far back step 02 reads (see
    :func:`opdi.pipeline.tracks.required_lookback_hours`)."""

    link_cruise_floor_ft: float = 18000.0
    """Lowest altitude, in feet, at each side of a gap that can be linked:
    above it the aircraft is en route rather than near an airport."""

    link_edge_vert_rate_fpm: float = 500.0
    """Vertical rate, in ft/min, beyond which the side of a gap counts as
    climbing or descending: a gap whose old side descends and whose new side
    climbs is read as a landing and a new flight."""

    link_callsign_lookahead_minutes: float = 10.0
    """How far after a gap, in minutes, the first non-blank call sign is
    looked for, to compare it with the call sign before the gap."""

    link_callsign_lookback_minutes: float = 60.0
    """How far before a gap, in minutes, the last call sign may have been
    seen and still count as the call sign before the gap; an older one counts
    as blank. This keeps the decision the same whichever day's run makes it.
    It adds to how far back step 02 reads (see
    :func:`opdi.pipeline.tracks.required_lookback_hours`)."""

    link_edge_position_minutes: float = 5.0
    """How far from each side of a gap, in minutes, reports without a
    position are skipped to find the side's position, altitude, vertical rate
    and speed (these arrive in separate messages). With no position or no
    altitude within this time, the gap splits the track; a missing vertical
    rate does not count either way at cruise level. Must be shorter than
    :attr:`low_alt_gap_minutes`."""

    link_min_implied_speed_kt: float = 400.0
    """Lowest speed, in knots, implied by the great-circle distance and the
    time across a gap, for the gap to be linked. A lower speed suggests the
    aircraft landed in between."""

    link_max_implied_speed_kt: float = 750.0
    """Highest implied speed across a gap, in knots, for it to be linked. A
    higher one means two aircraft share one address."""

    link_min_speed_ratio: float = 0.0
    """Lowest ratio of the implied speed across a gap to the ground speed at
    its sides for the gap to be linked. 0 turns the test off."""

    link_min_stop_minutes: float = 0.0
    """Time, in minutes, that would be left over when flying across the gap
    at :attr:`link_vfast_kt`, from which the gap counts as having room for a
    stop. 0 turns the test off."""

    link_vfast_kt: float = 650.0
    """Fastest plausible ground speed, in knots, used by
    :attr:`link_min_stop_minutes`."""

    link_old_edge_climb: bool = True
    """Allow a gap whose old side is below :attr:`link_cruise_floor_ft` to be
    linked when that side is climbing faster than
    :attr:`link_edge_vert_rate_fpm`: an aircraft lost while still climbing
    is outbound, so the next report is the same flight. The climb is judged
    on the mean of at least three vertical rates; missing ones do not
    count."""

    link_new_edge_descent: bool = True
    """Allow a gap whose new side is below :attr:`link_cruise_floor_ft` to be
    linked when that side is descending faster than
    :attr:`link_edge_vert_rate_fpm`, judged on at least three vertical
    rates. A gap that the low-altitude rule splits (below :attr:`low_alt_ft`)
    is still split. The implied speed test leaves out gaps that hide a
    landing."""

    def __post_init__(self) -> None:
        """Refuse a study rule that no study script has registered.

        Raises:
            ValueError: If :attr:`method` names a rule of
                :data:`STUDY_SEGMENTATION_METHODS` that is not registered.
        """
        methods = sys.modules.get("opdi.pipeline.segmentation.methods")
        registered = methods is not None and self.method in methods.ARMS
        if self.method in STUDY_SEGMENTATION_METHODS and not registered:
            raise ValueError(
                f"segmentation method {self.method!r} is a study rule and not part of "
                f"opdi; it lives in benchmarks/study_variants/segmentation.py, and a "
                f"study script calls register_study_arms() to select it"
            )


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
                    hadoop_filesystem="file:///",
                ),
                spark=SparkConfig(
                    app_name="OPDI Pipeline - Local",
                    driver_memory="4G",
                    executor_memory="4G",
                    executor_instances="1",
                    dynamic_allocation_max_executors="2",
                    hadoop_group="",  # no Azure access on a local machine
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
                    hadoop_filesystem="",
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
