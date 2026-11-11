"""Sources of the position reports and reference data: :class:`IngestionConfig`."""

from dataclasses import dataclass, field
from typing import Dict


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
