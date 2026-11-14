"""ADS-C as a second source of positions (step 01b): :class:`AdscConfig`."""

from dataclasses import dataclass


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
