"""
Configuration management for OPDI pipeline.

Provides centralized configuration using dataclasses for project settings,
Spark configurations, H3 parameters, and ingestion settings.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional
import os


def _load_dotenv() -> None:
    """Load .env file from the project root into os.environ (no external deps)."""
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

    # S3 settings (for OpenSky environment)
    s3_endpoint: str = ""
    s3_access_key: str = ""
    s3_secret_key: str = ""
    spark_packages: str = ""

    # Kubernetes distributed settings
    k8s_master: str = ""
    k8s_namespace: str = ""
    k8s_container_image: str = ""
    k8s_driver_host: str = "jupyterlab"
    k8s_driver_bind_address: str = "0.0.0.0"
    k8s_driver_port: str = "7078"
    k8s_driver_block_manager_port: str = "7079"
    k8s_executor_memory_limit: str = ""
    k8s_executor_cores_limit: str = ""

    # Feature flags
    enable_hive: bool = True
    enable_iceberg: bool = True

    def to_spark_config(self, project_config: ProjectConfig) -> Dict[str, str]:
        """
        Convert to Spark configuration dictionary.

        Args:
            project_config: Project configuration for warehouse paths

        Returns:
            Dictionary of Spark configuration key-value pairs
        """
        configs: Dict[str, str] = {}

        # S3 configs (opensky environment)
        if self.s3_endpoint:
            configs.update({
                "spark.jars.packages": self.spark_packages,
                "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.driver.memory": self.driver_memory,
                "spark.driver.maxResultSize": self.driver_max_result_size,
            })

        # Iceberg / Azure / Hive configs (dev, live environments)
        if self.enable_iceberg:
            configs.update({
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
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.iceberg.handle-timestamp-without-timezone": self.handle_timestamp_without_timezone,
                "spark.sql.catalog.spark_catalog.warehouse": project_config.warehouse_path,
            })

        return configs

    def to_distributed_config(self) -> Dict[str, str]:
        """
        Return Kubernetes distributed-mode Spark configuration.

        Includes executor resource settings that only apply when running
        with K8s executors.
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

    decimation: str = "bucket"
    """Which 5 s thinning rule the ingest applies: ``"bucket"`` or ``"modulo"``.

    **Changed from "modulo".** The modulo rule keeps a row only if one exists
    at the single second per window congruent to zero -- a fixed-phase sampler,
    which is arbitrary with respect to the data. The bucket rule keeps the
    newest row in each 5 s window, which is the sampler the thinning was always
    meant to be.

    The measured gain is small, because the OpenSky archive is a complete 1 Hz
    grid with carried-forward positions, so the modulo rule rarely misses:
    1.002x the rows, and +0.26 pp of arrival coverage end to end. The reason to
    prefer it is that it does not depend on that property. If OpenSky ever
    stops carrying positions forward, the modulo rule degrades badly and this
    one degrades gracefully.

    .. warning::

       Changing this changes which samples reach ``_add_track_id``, and the
       rescued rows sit at track boundaries -- so ``track_id`` values differ
       from those published under the modulo rule. That is a deliberate,
       accepted discontinuity, and it needs a new algorithm ``version``, not a
       mutation of an existing one.
    """

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
class CleaningConfig:
    """Thresholds for trajectory cleaning (pipeline step 02a).

    Ported from the 2024 PRC Data Challenge winning solution (Alligier &
    Gianazza, ENAC), whose ``filterclassic.py`` is the best available evidence
    on ADS-B cleaning. Every threshold is named here rather than inlined so the
    benchmark can sweep them.

    **Thresholds are in aviation units, matching Alligier verbatim.** OPDI is
    an ATM dataset and publishes in aviation units (``events.py`` emits
    ``altitude_ft``, ``roc_ft_min``, ``speed_kt``, ``FL``), so tuning knobs are
    stated the way the domain -- and the source paper -- states them. Every
    field below carries its unit in its name.

    The OSN *storage* schema stays SI (metres, m/s), mirroring OpenSky's own
    schema. It does not need converting, because **masking is unit-agnostic**:
    :mod:`opdi.cleaning.native` scales each column into its aviation unit only
    to evaluate the derivative comparison, then applies the resulting NULL mask
    to the untouched SI column. No converted value is ever stored, so there is
    no round-trip and no schema change.

    Conversion factors used, identical to those already in ``events.py`` so the
    two can never drift:

    ==============  ==================  =============
    Column          Aviation unit       Factor
    ==============  ==================  =============
    baro/geo alt    m -> ft             3.28084
    vert_rate       m/s -> ft/min       196.850394
    velocity        m/s -> kt           1.94384
    heading         deg (unchanged)     1.0
    lat / lon       deg (unchanged)     1.0
    ==============  ==================  =============

    **Both derivatives carry the same unit,** ``[column] / s``. This is not a
    typo. Alligier's second derivative is a difference of *raw* differences
    divided by a mean timestep (``deriv2 = 2 * |d(i+1) - d(i)| / (dt(i+1) +
    dt(i))``, ``filterclassic.py:181``), not a rate-of-a-rate, so it never
    acquires an ``s^2``.

    Note ``geo_altitude`` gets a **looser** second-derivative threshold than
    ``baro_altitude`` (150 vs 50 ft/s) -- GNSS altitude is noisier. Alligier
    sets these separately (``filterclassic.py:139-140``); do not collapse them.
    """

    enabled: bool = True
    """Master switch for the whole cleaning step."""

    # -- Stage 1: duplicate removal -------------------------------------
    dedup_enabled: bool = True
    """Drop duplicate ``(track_id, event_time)`` rows, keeping one."""

    # -- Stage 2: range validity ----------------------------------------
    lat_min: float = -90.0
    lat_max: float = 90.0
    lon_min: float = -180.0
    lon_max: float = 180.0
    """Physically possible coordinate ranges. Outside -> NULL."""

    # -- Stage 3: stale-broadcast removal --------------------------------
    stale_enabled: bool = True
    """ADS-B transmits position and velocity in separate message types, so
    identical consecutive values mean *repeated*, not *measured*."""

    # -- Stage 4: derivative spike filter --------------------------------
    spike_enabled: bool = True

    spike_min_votes: int = 2
    """A point dies only if it participates in >= this many flagged derivative
    windows. The middle of a spike takes part in both the rise and the fall and
    so collects two votes, while a legitimate step change takes part in only
    one and survives.

    Votes are tallied **separately per derivative order** and the kill is
    ``votes_d1 >= n OR votes_d2 >= n`` (``filterclassic.py:191``). They are not
    summed: summing would let one first-derivative vote plus one
    second-derivative vote kill a legitimate step change, which is exactly the
    behaviour the rule exists to prevent."""

    baro_altitude_d1_max_ft_s: float = 200.0
    """ft/s. Alligier ``altitude`` first."""
    baro_altitude_d2_max_ft_s: float = 50.0
    """ft/s. Alligier ``altitude`` second."""

    geo_altitude_d1_max_ft_s: float = 200.0
    """ft/s. Alligier ``geoaltitude`` first."""
    geo_altitude_d2_max_ft_s: float = 150.0
    """ft/s. Alligier ``geoaltitude`` second -- deliberately looser than baro,
    because GNSS altitude is noisier."""

    vert_rate_d1_max_ftmin_s: float = 1500.0
    """ft/min per second. Alligier ``vertical_rate`` first."""
    vert_rate_d2_max_ftmin_s: float = 1000.0
    """ft/min per second. Alligier ``vertical_rate`` second."""

    velocity_d1_max_kt_s: float = 12.0
    """kt/s. Alligier ``groundspeed`` first."""
    velocity_d2_max_kt_s: float = 10.0
    """kt/s. Alligier ``groundspeed`` second."""

    heading_d1_max_deg_s: float = 12.0
    """deg/s. Alligier ``track`` first."""
    heading_d2_max_deg_s: float = 10.0
    """deg/s. Alligier ``track`` second."""

    latlon_d1_max_deg_s: float = 0.01
    """deg/s. Alligier ``latitude``/``longitude`` first."""
    latlon_d2_max_deg_s: float = 0.06
    """deg/s. Alligier ``latitude``/``longitude`` second."""

    # -- Stage 5: isolated-point removal ---------------------------------
    isolated_enabled: bool = True
    isolated_max_gap_seconds: float = 20.0
    """A value further than this from any other valid value of the *same*
    column, on both sides, is unverifiable and is NULLed."""

    # -- Stage 6: gap segmentation ---------------------------------------
    segment_gap_seconds: float = 300.0
    """Coverage holes at or beyond this become a new ``segment_id``, so
    downstream detectors never interpolate across them. 5 minutes."""

    # -- Optional pandas stage -------------------------------------------
    enable_pandas_stage: bool = False
    """Opt-in ``applyInPandas`` escape hatch (csaps smoothing, 1 s resampling,
    ILS alignment). Off by default: it needs the fat executor image, so the
    default path stays dependency-free. See ``cleaning/pandas_udf.py``."""

    # -- Does anything downstream actually read the cleaned table? -------
    feeds_flight_list: bool = True
    """Whether step 03 reads ``osn_tracks_clean`` or the uncleaned
    ``osn_tracks``.

    **This was `False` in effect until 2026-08.** Step 02a wrote a cleaned
    table and ``pipeline/flights.py`` read the raw one, so the cleaning step
    existed, was tested, and fed nothing. Naming the choice makes it a decision
    rather than an oversight, and makes "clean versus raw" a measurable
    comparison rather than a code edit.

    Turning cleaning off (:attr:`enabled`) falls back to the raw table
    automatically, so this can never select a table that was never built.

    Note that cleaning **masks bad values to NULL and keeps the row**, and the
    detection path drops samples with no barometric altitude -- so every masked
    altitude is one fewer candidate sample. Expect this to cost coverage and
    buy accuracy; which way the exchange falls is a measurement, not an
    assumption."""


@dataclass
class DetectionConfig:
    """Thresholds for ADEP/ADES detection (pipeline step 03).

    Named here rather than inlined in ``pipeline/flights.py`` for the same
    reason as :class:`CleaningConfig`: so the benchmark can sweep them. Until
    this existed, ``MAX_FL`` and ``DETECTION_RADIUS_NM`` were class constants
    and the vote margin and smoothing window were literals inside a static
    method, so tuning them meant editing the pipeline.

    **Units are aviation and carried in the field names**, matching the
    convention the rest of the package follows.

    Two detection algorithms share this config, and they take *different*
    thresholds because they read different evidence:

    ``trend``
        Votes on the sign of a smoothed altitude change across every sample
        below ``trend_max_height_ft`` **above field elevation** inside
        ``trend_radius_nm`` -- or below ``trend_max_fl``, when
        ``trend_max_datum`` is ``"msl"``. Its abstention is evidence-based --
        "the altitude trace does not clearly rise or fall".

    ``endpoint``
        Looks at one fix, the track's first or last, and accepts it if it is
        within ``endpoint_radius_nm`` and below ``endpoint_height_ft`` above
        field elevation. Its abstention is geometric.

    Both rank candidate aerodromes on distance plus a penalty for aerodromes
    without scheduled service, so a military field must be *clearly* nearest
    rather than merely nearest.

    .. warning::

       The defaults are the **tuned** values, not the ones every dataset
       published before 2026-08 was built with. Re-running an old month with
       these will not reproduce what was released. Use :meth:`legacy` for that.
    """

    # -- trend -----------------------------------------------------------
    trend_max_fl: int = 60
    """Only samples below this flight level are considered. This is the
    binding constraint on trend's coverage: an aircraft never seen below it
    near an aerodrome has no candidate at all, whatever the radius.

    **Changed from 40**, which was never swept. Measured through
    ``process_dai`` itself over FL 40/60/80/100/120, arrivals peak at FL60 and
    fall away on both sides -- a genuine interior optimum, worth +1,408 against
    the production constants.

    This value was rejected once, on evidence that turned out to be measuring
    the wrong thing: under the old H3 ring-count selection, raising the cap
    lost ground, because it admits samples from higher up where more aerodromes
    share a ring and a ring-count filter cannot separate them. With
    ``trend_rank_by="haversine"`` the constraint is gone and the gain appears.
    The two settings must move together.

    **Superseded as the shipped cut by ``trend_max_height_ft``**, but retained:
    it is what ``trend_max_datum="msl"`` reads, and so what ``legacy()`` needs
    to reproduce released data with."""

    trend_max_height_ft: float = 6000.0
    """Ceiling for the trend vote, as a height **above field elevation**.

    Read when ``trend_max_datum == "field"``, which is the shipped setting.

    **New in v6.1.** ``trend`` was the last of the three altitude tests still
    measured against sea level. ``endpoint`` has used ``endpoint_height_ft``
    above field elevation since V6, and step 04's ground membership moved to the
    same datum under ``phase_ground_above_field`` -- for the reason recorded
    there, that a fixed cut-off measured from sea level is not the same test at
    every aerodrome.

    The failure mode is silence, not error. At FL60 an aerodrome at sea level
    gets 6,000 ft of climb and descent to vote on; Madrid at 1,998 ft gets about
    4,100 and Ankara at 3,125 ft about 2,975. The trace has less room to rise or
    fall clearly, so the method abstains more often, and it abstains in
    proportion to how high the field sits -- the same bias
    ``phase_ground_above_field`` documents for step 04.

    Note this is a *height*, not a flight level. "FL60 above field elevation"
    would be a contradiction: a flight level is by definition referenced to the
    standard pressure datum."""

    trend_max_datum: str = "field"
    """Which datum ``trend``'s altitude cut is measured against.

    ``"field"``
        Height above the candidate aerodrome's own elevation, capped by
        ``trend_max_height_ft``. The shipped setting.
    ``"msl"``
        Flight level above the standard pressure datum, capped by
        ``trend_max_fl``. What every published flight list was built with, and
        what :meth:`legacy` pins.

    A switch rather than a replacement, because the two cuts are not
    interchangeable even at the same nominal ceiling: ``flight_level`` is an
    integer cast, so ``<= 60`` admits everything below 6,100 ft rather than
    below 6,000. The ``msl`` branch keeps the original expression verbatim for
    that reason, and a study comparing the two datums must use 6,100 ft if it
    wants the ceiling held constant."""

    trend_radius_nm: float = 30.0
    """Zone radius for the sample-to-aerodrome join.

    **Reverted to 30 in V7**, the value the algorithm always had. V6 moved it to
    20 on a single period, and it did not hold: measured through ``process_dai``
    the change gains 214 on the 2025 sample -- noise on 95,116 flights -- and
    loses 79 on 2024, and it is the *only* step of the V7 ladder whose sign
    differs between the two samples. Ranked over both periods together, the
    research sweep puts 30 NM at an interior optimum, beating both 20 and 40.

    Four independent measurements agree, which is why this is a revert rather
    than an open question. It is also the clearest illustration in the study of
    why a second period is worth its cost: 20 NM looked consistent across ten
    cells of a one-period grid, and one independent sample was enough to show
    that consistency was the sample's and not the algorithm's."""

    trend_smooth_half_window: int = 2
    """Half-width of the centred rolling mean over ``baro_altitude``, in
    samples: 2 gives the five-sample window ``rowsBetween(-2, 2)``. **Never
    swept** -- inherited, not chosen."""

    trend_vote_margin: int = 0
    """One direction must beat the other by this many samples or the pair is
    ``ambiguous`` and dropped. Zero means a simple majority decides.

    **Changed from 4, then from 2.** V6 moved it to 2 on sweep evidence alone,
    and flagged it as the weakest value in its recommendation. V7 measured it
    through ``process_dai`` on both periods, applied to the shipped
    configuration on its own: **+395 and +296** for zero against two.

    The exchange rate is what settles it. Dropping the margin gains 465 correct
    arrivals for 35 extra wrong ones -- **13 correct per wrong, against a bar of
    two** -- so coverage rises half a point while accuracy moves by four
    hundredths.

    **Cleaning is why the margin stopped earning its keep.** It exists to stop
    noise flipping a climb-or-descent call, and step 02a now masks about a fifth
    of barometric altitudes and nearly half the velocity columns *before* the
    vote is counted. The margin was defending against something the cleaner has
    already removed, and all it did was refuse flights it could have answered.

    That interaction is the reason this value could not have been settled
    before: it depends on an input treatment that did not previously reach the
    flight list at all."""

    trend_rank_by: str = "haversine"
    """How ``trend`` chooses among candidate aerodromes: ``"haversine"`` or
    ``"ring"``.

    **Changed from "ring".** The original rule kept only candidates at the
    minimum H3 *ring count* -- an integer stepping about 5.2 km at resolution 7
    -- and computed exact distance only among those. An aerodrome one ring
    further out was eliminated before distance was ever measured, so the
    tie-break could not see it.

    That coarseness is what made every tuned trend parameter fail to transfer:
    raising the flight-level cap admits samples from higher up, where more
    aerodromes fall in the same ring, and a ring-count filter cannot separate
    them. Ranking on exact distance removes the constraint rather than tuning
    around it."""

    trend_sched_penalty_nm: float = 10.0
    """Scheduled-service penalty applied to trend's aerodrome choice.

    **Changed from 0.** This is the one tuned trend value the pipeline confirms:
    worth +234 score, and across the hundred busiest aerodromes it improves 4
    departures and 5 arrivals while worsening none. It re-ranks candidates
    without changing how many are answered, so coverage is untouched and only
    accuracy moves."""

    trend_smooth_before_cut: bool = True
    """Smooth barometric altitude **before** the flight-level cut, not after.

    **Changed from False in effect.** The cut used to be applied first, so the
    rolling mean at the boundary averaged only the samples that survived it --
    a truncated window exactly where the altitude is changing fastest, which is
    the worst place to lose half of it. The research sweep never had this
    problem, and it is one of the two differences that kept the sweep and the
    pipeline from agreeing on ``trend``."""

    trend_radius_exact: bool = True
    """Cut the detection radius on exact distance rather than on H3 band.

    **Changed from False in effect.** The zone table is a set of hexagon rings,
    each carrying an inner and outer radius. Selecting bands whose *outer*
    radius is within the detection radius keeps only rings lying wholly inside
    it, and so discards hexagons that straddle the boundary -- along with
    samples that are genuinely within the radius. Selecting on the *inner*
    radius and then testing exact great-circle distance keeps precisely the
    samples the radius names.

    The same reasoning as ``trend_rank_by``: a hexagon index is a coarse
    approximation of a distance, and where a distance is what is meant, the
    distance should be measured."""

    trend_bearing_tiebreak_nm: float = 2.0
    """Distance band, in nautical miles, within which ``trend`` breaks ties on
    alignment rather than on distance. Zero disables it.

    **New.** Among candidates whose effective distances differ by less than
    this, distance is not informative -- they are equidistant to within the
    measurement -- so the one the track's course actually points at wins.
    Outside the band, distance decides alone and alignment is never consulted.

    The band is the whole mechanism, and the sweep shows why: at 5 NM the rule
    turns negative, and ranking by alignment *without* a band is catastrophic.
    Alignment cannot name an aerodrome, because every field on the same radial
    behind the right one is equally well aligned. It can only separate two
    candidates that distance has already declared equal.

    2 NM is an interior optimum: the research sweep measured +1,140 score and
    +0.58 pp of arrival accuracy at **zero** coverage cost -- the same flights
    answered, 380 more of them correctly."""

    trend_ooa: bool = False
    """Whether ``trend`` may emit the out-of-area marker.

    Implemented in V7, measured, and **left off**. The capability is real and
    the mechanism is sound; the geometry does not support it for the role that
    matters.

    ``trend`` named an aerodrome or said nothing, and those are not the only two
    possibilities: a flight that entered the observed area already airborne has
    an origin no feed here can name. Roughly one flight in twelve is in that
    position, so the gap was worth closing.

    **Why it is off.** The shipped configuration takes departures from
    ``endpoint``, so this switch only ever reaches *arrivals* in practice -- and
    an arrival label is right barely half the time: 50.35% precision, against
    89.20% for the same test applied to departures by ``endpoint``. Every label
    replaces a *silence*, so at the study's exchange rate of two, converting
    nulls into coin flips loses about 205 points per three-day sample. It costs
    arrivals on both periods measured, -827 and -245.

    The asymmetry is geometric rather than a tuning accident. A track that
    *begins* at the edge of the observed area almost certainly entered from
    outside. A track that *ends* near the edge is ambiguous: it may be leaving,
    or it may be a flight still bound somewhere inside whose reception was lost.
    The border test ports cleanly to departures because the geometry supports
    it, and does not port to arrivals.

    **What would make it shippable.** A direction requirement -- the track must
    be heading *out* of the area, not merely near its edge. The course is
    already computed for ``trend_bearing_tiebreak_nm``, so the machinery exists;
    it has not been built or measured. Until then arrivals get their out-of-area
    labels from ``endpoint``, where precision is 89%.

    The test and its precedence, when enabled, are ``endpoint``'s deliberately:
    a fix within the border margin of the ingestion bbox, and aerodrome first,
    border second. Reversing that order labels departures from aerodromes near
    the edge as out-of-area with the aircraft still on the runway."""

    # -- endpoint --------------------------------------------------------
    endpoint_radius_nm: float = 30.0
    """**Changed from 40.** An interior optimum of the radius x height sweep,
    and one the pipeline reproduces exactly -- the endpoint sweep filters the
    same cached candidate table the pipeline reads, so harness and pipeline
    agree to the flight."""

    endpoint_height_ft: float = 15000.0
    """Height above *field elevation*, not above sea level -- a fixed altitude
    cut-off means nothing at an aerodrome sitting at 5,000 ft.

    Confirmed at the value it already had: the sweep's argmax lands here."""

    endpoint_sched_penalty_nm: float = 10.0
    endpoint_candidate_radius_nm: float = 110.0
    """How far the cached candidate table reaches. Wider than any detection
    radius on purpose, so the radius stays sweepable without a rebuild."""

    # -- which algorithm serves which role -------------------------------
    adep_mode: str = "endpoint"
    """Algorithm naming the departure aerodrome: ``trend``, ``endpoint`` or
    ``nearest``.

    **Changed from ``trend``.** The two roles are not equally hard and the
    rules that suit them differ, which is the study's second result after the
    ranking rule. Departures are decided by geometry -- the first fix of a
    departing track is on or near the runway -- so ``endpoint`` wins outright.

    This lives in the config rather than only in ``process_dai``'s arguments so
    that the recommended configuration is what a caller gets by default. Before
    it did, every entry point that did not pass the modes explicitly -- the CLI
    among them -- silently ran ``trend`` for both roles, which is a
    configuration this study recommends for neither."""

    ades_mode: str = "trend"
    """Algorithm naming the destination aerodrome.

    Unchanged, and unchanged for a reason: an arriving track's last fix is
    often the point reception was lost rather than the point it landed, so
    geometry alone is weaker here than the altitude trend."""

    def __post_init__(self) -> None:
        """Reject a datum this class does not implement.

        Deliberately loud. Both branches of the cut are reachable and neither
        is obviously wrong from its output, so a typo that fell through to a
        default would apply one cut while every log line and version string
        claimed the other -- the failure this whole change exists to remove.
        """
        if self.trend_max_datum not in ("field", "msl"):
            raise ValueError(
                f"trend_max_datum must be 'field' or 'msl', got "
                f"{self.trend_max_datum!r}. Falling back to a default here "
                f"would apply one altitude cut while reporting the other."
            )

    @classmethod
    def legacy(cls) -> "DetectionConfig":
        """The constants in force before the V6 tuning.

        Every OPDI flight list published up to 2026-08 was built with these.
        They are kept reachable so released data stays reproducible -- the same
        principle as the frozen ``version`` strings on published events.
        """
        return cls(
            trend_max_fl=40,
            # The published cut was a flight level, so the preset has to name
            # the datum as well as the number. Leaving this at the shipped
            # default would keep FL40's value while measuring it from the
            # field -- a different algorithm wearing the legacy constants.
            trend_max_datum="msl",
            trend_radius_nm=30.0,
            trend_smooth_half_window=2,
            trend_vote_margin=4,
            trend_sched_penalty_nm=0.0,
            trend_rank_by="ring",
            # None of these existed before; all must be off for a legacy run.
            trend_smooth_before_cut=False,
            trend_radius_exact=False,
            trend_bearing_tiebreak_nm=0.0,
            trend_ooa=False,
            endpoint_radius_nm=40.0,
            endpoint_height_ft=15000.0,
            endpoint_sched_penalty_nm=10.0,
            endpoint_candidate_radius_nm=110.0,
            # Both roles from `trend`: the published lists never used
            # `endpoint` for anything.
            adep_mode="trend",
            ades_mode="trend",
        )


@dataclass
class EventConfig:
    """Thresholds for flight event detection (pipeline step 04).

    Named here for the same reason as :class:`CleaningConfig` and
    :class:`DetectionConfig`: so the benchmark can sweep them. Until this
    existed, **every** number in ``pipeline/events.py`` was an inline literal --
    the nine fuzzy membership parameters, the flight levels, the 2000 ft
    airport-event gate, the 300 s trace gap -- so tuning any of them meant
    editing the pipeline, and none of them was ever measured.

    **Units are aviation and carried in the field names**, matching the
    convention the rest of the package follows.

    .. warning::

       The defaults change what step 04 emits. Every event OPDI published under
       ``events_v0.0.2`` was built with :meth:`legacy`, which is kept reachable
       so released data stays reproducible -- the same principle as the frozen
       ``version`` strings themselves.
    """

    # -- phase classification (the fuzzy detector) ------------------------
    phase_twindow_seconds: float = 60.0
    """Width of the majority-smoothing window applied to per-sample phase
    labels before transitions are read off.

    **New, and the largest divergence from the reference implementation.**
    OPDI's membership functions are a faithful port of OpenAP's published
    constants, but OpenAP applies ``phaselabel(twindow=60)`` -- a 60 second
    majority vote over the per-sample labels -- and the port dropped it. Phases
    were being decided per state vector at 5 s spacing with no temporal
    aggregation at all, so a single misclassified sample injects a spurious
    ``level-start``/``level-end`` pair or destroys a ``take-off`` by breaking
    the GND->CL adjacency.

    Set to 0 to disable smoothing and reproduce the published behaviour."""

    phase_ground_ceiling_ft: float = 200.0
    """Upper edge of the ground membership ramp, ``zmf(altitude, 0, ceiling)``.

    Unchanged in value. What changes is *what it is measured against* -- see
    ``phase_ground_above_field``."""

    phase_ground_above_field: bool = True
    """Measure the ground membership against **height above field elevation**
    rather than raw pressure altitude.

    Implemented by ``events.attach_field_elevation``, which attaches the field
    elevation of **both** the track's ADEP and ADES; the membership takes the
    more permissive of the two. A track is only ever on the ground at one of
    its ends and cruise sits far above both, so this is correct without a
    per-sample distance deciding which end applies. A missing elevation
    coalesces to zero, i.e. to the published behaviour, rather than removing
    the flight's phases.

    **New.** ``baro_altitude_c`` is uncorrected pressure altitude, so the
    published detector's ``zmf(alt_ft, 0, 200)`` reaches zero at 200 ft
    *pressure* altitude. At any aerodrome above ~200 ft AMSL, or on a
    low-pressure day, no sample is ever classified GND -- and since
    ``take-off`` requires ``prev_phase == "GND"`` and ``landing`` requires
    ``flight_phase == "GND"``, **neither event is ever emitted for that
    flight**. The loss is not random: it is biased against high-elevation
    aerodromes.

    ``DetectionConfig.endpoint_height_ft`` already measures height above field
    elevation for exactly this reason; this makes step 04 consistent with
    step 03."""

    phase_require_complete_rules: bool = True
    """Require every input of a fuzzy rule to be non-NULL before the rule can
    win.

    **New.** Spark's ``least``/``greatest`` skip NULLs, so a NULL ``velocity``
    silently reduces a three-input rule to a two-input minimum and *raises* its
    activation. Rules with missing inputs were out-competing complete ones,
    which is precisely backwards."""

    phase_cruise_speed_kt: float = 600.0
    phase_cruise_speed_sigma_kt: float = 100.0
    """Centre and width of the cruise speed membership, ``gaussmf(spd, 600, 100)``.

    **Unchanged, deliberately.** These are OpenAP's own published values, so
    the port is faithful and this is an inherited limitation rather than an
    OPDI defect. It is real, though: a turboprop at 250 kt scores
    ``exp(-6.125) = 0.002``, so ``rule_cruise`` never wins and the aircraft
    gets no ``top-of-climb`` or ``top-of-descent`` at all. Exposed here so the
    affected population can be *measured* by typecode before anyone argues for
    deviating from the reference implementation."""

    # -- threshold crossings ---------------------------------------------
    crossing_levels_fl: tuple = (50, 70, 100, 245)
    """Flight levels at which crossings are detected. The default is the
    hard-coded set the published detector used, so the vocabulary does not
    shift unless someone deliberately changes it."""

    crossing_hysteresis_ft: float = 300.0
    """Half-width of the dead band around each level, in feet.

    **New, and the reason capturing every crossing is usable at all.** A
    crossing is registered only when the aircraft passes from below
    ``L - hysteresis`` to above ``L + hysteresis``. An aircraft cruising *at*
    FL100 oscillates across the bare boundary on barometric noise and would
    otherwise emit hundreds of meaningless events; inside the dead band it
    emits none.

    Consequence worth knowing: a flight that climbs and levels off exactly at
    FL100 never clears the upper edge, so it registers no FL100 crossing. That
    case is described by the level-segment and top-of-climb events instead."""

    crossing_all_occurrences: bool = True
    """Emit every crossing of a level, not only the first and last.

    **New.** The published detector keeps ``row_number`` 1 ascending and 1
    descending per ``(track_id, level)`` and discards everything between, so a
    flight that levels off and re-climbs through FL100 loses the middle
    crossings entirely -- which are exactly the crossings a vertical-efficiency
    indicator is about."""

    crossing_interpolate: bool = True
    """Interpolate the crossing time and position to the exact level, instead
    of reporting the last sample before it.

    **New.** The published detector reports the bracketing sample, biasing
    every crossing timestamp by up to one sample interval (5 s) always in the
    same direction. APDF's own ring crossings are interpolated to the second,
    so an uninterpolated comparison would measure our snapping rather than the
    algorithm."""

    ring_radii_nm: tuple = (40.0, 100.0)
    """Distance rings around an aerodrome at which crossings are detected.
    40 NM is ICAO's ASMA cylinder for KPI08 and the reference area for KPI05;
    100 NM is the documented variant for aerodromes whose holding sits outside
    40 NM. ``h3_airport_detection_zones`` already reaches 110 NM, so both are
    available without regenerating reference data.

    Wired by ``events.calculate_ring_crossing_events``, which builds the
    distance frame from the flight's **own** ADEP and ADES rather than from the
    zone table: both indicators are defined against the flight's origin and
    destination, and APDF records one crossing per movement for the same
    reason."""

    ring_hysteresis_nm: float = 1.0
    """Half-width of the dead band around each ring, in nautical miles. Same
    role as ``crossing_hysteresis_ft``: it suppresses the repeated crossings a
    track flying tangentially along the ring would otherwise produce."""

    # -- which families are emitted at all --------------------------------
    #
    # Each of these families is new in v0.1.0. They need explicit switches
    # because `legacy()` has to reproduce what `events_v0.0.2` *emitted*, not
    # merely run with its thresholds -- and the first ladder run proved the
    # difference: rung 0 emitted 48,134 ATOT and 69,528 ALDT events that no
    # published dataset contains, so the baseline every gain was measured
    # against was not the baseline.
    emit_runway_events: bool = True
    """Emit ATOT and ALDT. Off under `legacy()`: `events_v0.0.2` has neither."""

    emit_block_events: bool = True
    """Emit AOBT and AIBT. Off under `legacy()`, same reason."""

    emit_level_offs: bool = True
    """Emit the ICAO `level-off-*` family. Off under `legacy()`, same reason.
    Note this is separate from `level-start`/`level-end`, which are published
    and stay on in both configurations."""

    # -- ground movement (T04 off-block / T21 on-block) -------------------
    ground_speed_threshold_kt: float = 2.0
    """Groundspeed above which an aircraft counts as moving. traffic's
    ``StartMoving`` value."""

    ground_move_min_seconds: float = 30.0
    """How long movement must be sustained before it counts. traffic's value.
    The sustained part is what separates a push from a jitter in the speed
    field, which at 5 s sampling is not rare."""

    # -- runway identification and ATOT/ALDT (T08 / T17) ------------------
    runway_max_dist_nm: float = 5.0
    """Only samples this close to the aerodrome are considered. traffic's
    ``TrackBasedRunwayDetection`` value."""

    runway_max_height_ft: float = 1500.0
    """Ceiling above field elevation for the runway-detection window.
    traffic's value; note its polygon-based sibling uses 2000 and its own
    deprecated wrapper passes 5000, an inconsistency in the source."""

    runway_min_vert_rate_ftmin: float = 257.0
    """Vertical rate that separates the initial climb (or final descent) from
    ground movement. traffic's value, which is 1.3 m/s expressed in ft/min."""

    runway_min_groundspeed_kt: float = 30.0
    """Groundspeed floor, to exclude taxiing. traffic's value."""

    runway_max_bearing_deg: float = 10.0
    """How far the median track may differ from a runway's bearing before that
    runway stops being a candidate. traffic's value."""

    # -- ICAO level segments (KPI17 / KPI19) ------------------------------
    #
    # .. warning::
    #
    #    **Declared but not yet implemented.** No detector reads any of the
    #    eight ``level_*`` fields yet. They are recorded here because they are
    #    *ICAO's* published parameter values, not ours to choose, and pinning
    #    them is what makes the eventual conformance claim checkable -- no data
    #    source holds level-segment truth to score against. Until the detector
    #    exists these values describe an intention, not a behaviour.
    level_analysis_radius_nm: float = 200.0
    """Radius around the aerodrome within which the climb or descent trajectory
    is analysed. ICAO's example value.

    Beyond the 110 NM reach of ``h3_airport_detection_zones``, so this is
    evaluated as a haversine distance to the aerodrome reference point taken
    from the flight list, not through H3 bands."""

    level_vertical_speed_limit_ftmin: float = 300.0
    """Maximum vertical speed for a sample to belong to a level segment.
    ICAO's example value."""

    level_band_limit_ft: float = 200.0
    """Altitude band within which samples must stay to remain in one level
    segment. ICAO's example value."""

    level_min_duration_seconds: float = 20.0
    """Minimum duration for a level segment to be reported. ICAO's example
    value."""

    level_exclusion_box_pct: float = 90.0
    """Percentage of the top-of-climb (or top-of-descent) altitude defining the
    lower edge of the exclusion box. A level segment above that edge and longer
    than ``level_exclusion_box_seconds`` is cruise, not a level-off, and is
    excluded. ICAO's example value."""

    level_exclusion_box_seconds: float = 300.0
    """Duration above which a segment inside the exclusion box is treated as
    cruise. ICAO's example value."""

    level_min_altitude_climb_ft: float = 3000.0
    """Altitude at which level-segment detection starts during climb; the
    trajectory below it is not analysed. ICAO's example value."""

    level_min_altitude_descent_ft: float = 1800.0
    """Altitude at which level-segment detection stops during descent. ICAO's
    example value, and lower than the climb equivalent because an aircraft on
    final is legitimately close to level."""

    # -- airport events ---------------------------------------------------
    airport_max_fl: int = 20
    """Only samples below this flight level are matched against airport layout
    hexagons. Unchanged in value; previously an inline literal."""

    airport_trace_gap_seconds: float = 300.0
    """A gap longer than this within one ``(track, osm_id)`` pair starts a new
    traversal. Unchanged in value; previously an inline literal."""

    airport_events_ordered: bool = True
    """Take entry and exit attributes from the samples at the entry and exit
    *times*.

    **New, and a correctness fix rather than a tuning knob.** The published
    detector uses ``F.first``/``F.last`` inside a ``groupBy``, which take
    partition order, not ``event_time`` order -- so the reported entry latitude,
    longitude, altitude and cumulative measures are not guaranteed to be the
    values at the reported entry time."""

    # -- plumbing ----------------------------------------------------------
    feeds_from_clean_tracks: bool = True
    """Read ``osn_tracks_clean`` rather than the raw ``osn_tracks``.

    **New.** Step 02a's cleaning reached the flight list (step 03) and nothing
    else; step 04 read the raw table, so no published event has ever been
    derived from a cleaned trajectory. The cleaned table also carries
    ``segment_id``, which lets the transition logic stop stepping across
    coverage holes."""

    deterministic_event_ids: bool = True
    """Derive event IDs from the event's own identity rather than
    ``monotonically_increasing_id()``.

    **New.** Partition-dependent IDs are not reproducible across runs, and
    because ``StorageManager.write_table`` defaults to append, re-processing a
    month duplicates its events instead of replacing them."""

    enable_pandas_stage: bool = False
    """Enable the ``applyInPandas`` escape hatch for algorithms with no Spark
    equivalent (holding-pattern detection, and anything else needing the
    ``traffic`` library at runtime).

    **Off by default and expected to stay off.** It requires the fatter
    ``docker/Dockerfile.traffic`` executor image; nothing in the current event
    vocabulary needs it, because the algorithms worth porting -- ILS alignment,
    track-based runway detection, start-of-movement -- are all closed-form and
    express natively as column and window expressions."""

    events_version: str = "events_v0.1.0"
    """Version stamped on events this configuration produces.

    A single string covered every event type before this, so a new detector
    added to the existing function would silently inherit ``events_v0.0.2`` and
    change what a published version means. Never mutate a released value."""

    @classmethod
    def legacy(cls) -> "EventConfig":
        """The behaviour in force before this configuration existed.

        Every OPDI flight event published up to 2026-08 was built this way.
        Kept reachable so released data stays reproducible -- the same
        principle as the frozen ``version`` strings on the events themselves.
        """
        return cls(
            # None of these existed before; all must be off for a legacy run.
            phase_twindow_seconds=0.0,
            phase_ground_above_field=False,
            emit_runway_events=False,
            emit_block_events=False,
            emit_level_offs=False,
            phase_require_complete_rules=False,
            crossing_all_occurrences=False,
            crossing_interpolate=False,
            airport_events_ordered=False,
            feeds_from_clean_tracks=False,
            deterministic_event_ids=False,
            enable_pandas_stage=False,
            # Rings did not exist at all.
            ring_radii_nm=(),
            events_version="events_v0.0.2",
        )


@dataclass
class OPDIConfig:
    """Main OPDI configuration container."""

    project: ProjectConfig = field(default_factory=ProjectConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    h3: H3Config = field(default_factory=H3Config)
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)
    cleaning: CleaningConfig = field(default_factory=CleaningConfig)
    detection: DetectionConfig = field(default_factory=DetectionConfig)
    events: EventConfig = field(default_factory=EventConfig)

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
                    driver_memory="14G",
                    executor_memory="16G",
                    executor_instances="3",
                    dynamic_allocation_max_executors="20",
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
        elif env == "opensky":
            # OpenSky Network S3 environment (read-only access to state vectors)
            return cls(
                project=ProjectConfig(
                    project_name="opensky",
                    # One knob isolates every table this environment reads or
                    # writes: StorageManager resolves each name against
                    # `warehouse_path`. Setting OPDI_WAREHOUSE therefore runs
                    # the whole pipeline into a parallel prefix without
                    # touching a production table, which is what makes a
                    # from-scratch rebuild safe to attempt.
                    warehouse_path=os.environ.get(
                        "OPDI_WAREHOUSE", "s3a://eurocontrol/opdi"),
                    hadoop_filesystem="",
                ),
                spark=SparkConfig(
                    app_name="OPDI - OpenSky",
                    driver_memory="10G",
                    executor_memory="12g",
                    executor_memory_overhead="2g",
                    executor_cores="2",
                    executor_instances="4",
                    enable_hive=False,
                    enable_iceberg=False,
                    s3_endpoint="https://s3.opensky-network.org",
                    spark_packages="org.apache.spark:spark-hadoop-cloud_2.13:4.1.1",
                    k8s_master="k8s://https://192.168.60.102:6443",
                    k8s_namespace="eurocontrol",
                    k8s_container_image="docker.io/quintengs/opdi-spark:v4.1.1-5",
                    k8s_executor_memory_limit="14g",
                    # Match executor_cores. The namespace ResourceQuota counts
                    # limits.cpu, not requests.cpu, so a limit above the actual
                    # core count bills the quota for burst headroom the executor
                    # cannot use: at limit=4 with cores=2, every executor spent
                    # 4 CPU of a 30 CPU quota to run 2, capping the namespace at
                    # 6 executors instead of 13.
                    k8s_executor_cores_limit="2",
                ),
            )
        else:
            raise ValueError(f"Unknown environment: {env}. Use 'dev', 'live', 'local', or 'opensky'.")

    @classmethod
    def default(cls) -> "OPDIConfig":
        """Create default configuration (dev environment)."""
        return cls.for_environment("dev")
