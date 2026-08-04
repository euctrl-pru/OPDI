"""
Native Spark trajectory cleaning (pipeline step 02a).

Ported from the 2024 PRC Data Challenge winning solution (Alligier & Gianazza,
ENAC), ``filterclassic.py``. Every stage here is a pure ``DataFrame ->
DataFrame`` function, composable via ``DataFrame.transform``, expressed purely
as column and window expressions partitioned by ``track_id`` -- the same idiom
the rest of the pipeline uses. No ``applyInPandas``, no ``pandas_udf``, no
``traffic`` dependency.

**Design: mask to NULL, keep the row.** The 2024 solution masks bad values and
retains the row; the 2025 solution drops rows and resamples to 1 s. The 2024
approach is the Spark-natural one -- no row explosion, pure column expressions,
and it preserves the distinction between "no data" and "interpolated data".

**Derivatives are computed over valid values only.** Alligier compacts the NaNs
out of the array before differencing (``filterclassic.py:170-173``), so a
derivative always spans two *measured* points, using their real time
difference. A naive ``lag()`` would instead span whatever row happens to sit
behind, including rows this module has already NULLed, and would silently
produce derivatives of NULL. Every stage below therefore uses
"previous/next valid value" windows rather than plain ``lag``/``lead``.

**Units: thresholds are aviation, storage stays SI.** OPDI publishes in
aviation units, and the source thresholds are stated that way, so every tuning
knob in :class:`~opdi.config.CleaningConfig` is in ft/s, ft/min/s, kt/s or
deg/s. The OSN storage schema is left in metres and m/s, mirroring OpenSky.
Nothing has to be converted on disk to reconcile the two, because this module's
only output is a **NULL mask, and a mask carries no unit**: each column is
scaled by :data:`AVIATION_UNIT_FACTOR` solely to evaluate the derivative
comparison, and the resulting mask is applied to the untouched SI column.

Stage order matches ``filter_trajs.py:23``
(``FilterCstLatLon | FilterCstPosition | FilterCstSpeed | MyFilterDerivative |
FilterIsolated``), with dedup and range validity prepended and gap
segmentation appended.

Deliberately **not** ported -- these are documented negative results from the
PRC challenges, not oversights:

* Synthetic gap filling. "Attempts to complete the trajectory always lead to
  worse result" (2025 report).
* GPS-jamming removal. Leaving jamming untouched scored better; the winning
  solution's jamming filter is present but commented out (``resqu/data.py:126``).
* Wind/ERA5 enrichment. Removing wind *improved* RMSE; the winning solution's
  ``get_wind`` call is commented out (``resqu/preprocessor.py:616-617``).
"""

from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from opdi.config import CleaningConfig

__all__ = [
    "AVIATION_UNIT_FACTOR",
    "CLEANED_COLUMNS",
    "clean_tracks",
    "drop_duplicate_statevectors",
    "mask_out_of_range_positions",
    "mask_stale_broadcasts",
    "mask_derivative_spikes",
    "mask_isolated_points",
    "add_segment_id",
    "null_rate_report",
]


#: Columns this module may mask to NULL. ``track_id``, ``event_time``,
#: ``icao24`` and ``callsign`` are identity and are never touched.
CLEANED_COLUMNS: Tuple[str, ...] = (
    "lat",
    "lon",
    "baro_altitude",
    "geo_altitude",
    "velocity",
    "heading",
    "vert_rate",
)

#: Internal epoch-seconds column. Added and dropped within each stage.
_T = "_clean_t"

#: Sentinel for "no neighbour on this side". Larger than any real track gap.
_NO_NEIGHBOUR = 1.0e9


def _track_window() -> Window:
    """Ordering window used by every stage. Track-local, chronological."""
    return Window.partitionBy("track_id").orderBy(_T)


def _with_epoch(df: DataFrame) -> DataFrame:
    return df.withColumn(_T, F.unix_timestamp("event_time").cast("double"))


def _prev_valid(value: Column, target: Column, window: Window) -> Column:
    """Value of *target* at the previous row where *value* is non-NULL."""
    return F.last(F.when(value.isNotNull(), target), ignorenulls=True).over(
        window.rowsBetween(Window.unboundedPreceding, -1)
    )


def _next_valid(value: Column, target: Column, window: Window) -> Column:
    """Value of *target* at the next row where *value* is non-NULL."""
    return F.first(F.when(value.isNotNull(), target), ignorenulls=True).over(
        window.rowsBetween(1, Window.unboundedFollowing)
    )


def _angular_delta(current: Column, previous: Column) -> Column:
    """Shortest signed angular difference in degrees, in (-180, 180].

    Equivalent, for differencing purposes, to ``np.unwrap(period=360)``
    followed by ``diff`` (``filterclassic.py:175``): unwrapping exists solely
    so that 359 deg -> 1 deg reads as +2 rather than -358, which is exactly
    what this expression yields, without needing a cumulative pass.
    """
    return F.pmod(current - previous + F.lit(180.0), F.lit(360.0)) - F.lit(180.0)


# ---------------------------------------------------------------------------
# Stage 1 -- duplicate removal
# ---------------------------------------------------------------------------

def drop_duplicate_statevectors(df: DataFrame, cfg: CleaningConfig) -> DataFrame:
    """Drop duplicate ``(track_id, event_time)`` rows.

    Both PRC winners deduplicate first; the 2025 solution does it on
    ``timestamp`` alone (``resqu/data.py:109``). Ties are broken on
    ``last_contact`` descending so the freshest observation wins, which makes
    the choice deterministic across re-runs rather than dependent on partition
    order.
    """
    if not cfg.dedup_enabled:
        return df

    ordering = Window.partitionBy("track_id", "event_time").orderBy(
        F.col("last_contact").desc_nulls_last()
    )
    return (
        df.withColumn("_clean_rn", F.row_number().over(ordering))
        .where(F.col("_clean_rn") == 1)
        .drop("_clean_rn")
    )


# ---------------------------------------------------------------------------
# Stage 2 -- range validity
# ---------------------------------------------------------------------------

def mask_out_of_range_positions(df: DataFrame, cfg: CleaningConfig) -> DataFrame:
    """NULL coordinates outside the physically possible range."""
    return df.withColumn(
        "lat",
        F.when(F.col("lat").between(cfg.lat_min, cfg.lat_max), F.col("lat")),
    ).withColumn(
        "lon",
        F.when(F.col("lon").between(cfg.lon_min, cfg.lon_max), F.col("lon")),
    )


# ---------------------------------------------------------------------------
# Stage 3 -- stale-broadcast removal
# ---------------------------------------------------------------------------

def mask_stale_broadcasts(df: DataFrame, cfg: CleaningConfig) -> DataFrame:
    """NULL values that were repeated rather than re-measured.

    ADS-B carries position and velocity in *separate* message types. A state
    vector is the receiver's latest-known snapshot, so a field that is byte
    identical to the previous sample was not measured again -- it was carried
    forward. Treating it as a measurement invents information.

    Three rules, from ``FilterCstLatLon``, ``FilterCstPosition`` and
    ``FilterCstSpeed``:

    * neither ``lat`` nor ``lon`` changed -> NULL both
    * none of ``lat``/``lon``/``baro_altitude`` changed -> NULL all three
    * none of ``vert_rate``/``heading``/``velocity`` changed -> NULL all three

    All "changed" flags are derived from the *original* values before any
    masking is applied, so the rules cannot cascade into each other.
    """
    if not cfg.stale_enabled:
        return df

    df = _with_epoch(df)
    w = _track_window()

    def changed(name: str) -> Column:
        """True only if both this and the previous value exist and differ.

        Mirrors ``isvar`` (``filterclassic.py:27-32``): a comparison against a
        missing neighbour is not evidence of an update.
        """
        prev = F.lag(F.col(name)).over(w)
        return F.col(name).isNotNull() & prev.isNotNull() & (F.col(name) != prev)

    lat_or_lon = changed("lat") | changed("lon")
    position = lat_or_lon | changed("baro_altitude")
    speed = changed("vert_rate") | changed("heading") | changed("velocity")

    df = (
        df.withColumn("_stale_latlon", ~F.coalesce(lat_or_lon, F.lit(False)))
        .withColumn("_stale_position", ~F.coalesce(position, F.lit(False)))
        .withColumn("_stale_speed", ~F.coalesce(speed, F.lit(False)))
    )

    for name in ("lat", "lon"):
        df = df.withColumn(
            name, F.when(~F.col("_stale_latlon"), F.col(name))
        )
    df = df.withColumn(
        "baro_altitude",
        F.when(~F.col("_stale_position"), F.col("baro_altitude")),
    )
    for name in ("vert_rate", "heading", "velocity"):
        df = df.withColumn(name, F.when(~F.col("_stale_speed"), F.col(name)))

    return df.drop("_stale_latlon", "_stale_position", "_stale_speed", _T)


# ---------------------------------------------------------------------------
# Stage 4 -- derivative spike filter
# ---------------------------------------------------------------------------

#: Factor converting each SI storage column into the aviation unit its
#: threshold is expressed in. Identical to the constants already used in
#: ``events.py`` so the two cannot drift.
#:
#: These scale the derivative *comparison* only. Nothing converted here is ever
#: written: the filter's output is a NULL mask, and a mask carries no unit, so
#: it applies unchanged to the untouched SI column.
AVIATION_UNIT_FACTOR: Dict[str, float] = {
    "baro_altitude": 3.28084,  # m -> ft
    "geo_altitude": 3.28084,  # m -> ft
    "vert_rate": 196.850394,  # m/s -> ft/min
    "velocity": 1.94384,  # m/s -> kt
    "heading": 1.0,  # already degrees
    "lat": 1.0,  # already degrees
    "lon": 1.0,  # already degrees
}


def _spike_thresholds(cfg: CleaningConfig) -> Dict[str, Tuple[float, float]]:
    """Per-column ``(first, second)`` derivative thresholds, in aviation units.

    Paired with :data:`AVIATION_UNIT_FACTOR`, which lifts each SI column into
    the unit its threshold is stated in. Keeping both halves adjacent is
    deliberate -- a threshold and its unit factor are only correct together.
    """
    return {
        "baro_altitude": (
            cfg.baro_altitude_d1_max_ft_s,
            cfg.baro_altitude_d2_max_ft_s,
        ),
        "geo_altitude": (
            cfg.geo_altitude_d1_max_ft_s,
            cfg.geo_altitude_d2_max_ft_s,
        ),
        "vert_rate": (cfg.vert_rate_d1_max_ftmin_s, cfg.vert_rate_d2_max_ftmin_s),
        "velocity": (cfg.velocity_d1_max_kt_s, cfg.velocity_d2_max_kt_s),
        "heading": (cfg.heading_d1_max_deg_s, cfg.heading_d2_max_deg_s),
        "lat": (cfg.latlon_d1_max_deg_s, cfg.latlon_d2_max_deg_s),
        "lon": (cfg.latlon_d1_max_deg_s, cfg.latlon_d2_max_deg_s),
    }


def _mask_spikes_one_column(
    df: DataFrame,
    name: str,
    first_max: float,
    second_max: float,
    min_votes: int,
    unit_factor: float = 1.0,
) -> DataFrame:
    """Apply the vote-based derivative filter to a single column.

    Both derivatives are indexed at their *right-hand* endpoint here, whereas
    Alligier indexes them at the left. The vote spans below compensate, so the
    set of killed points is identical:

    * ``d1`` at point *i* spans measured points ``{i-1, i}``, so point *i*
      collects votes from ``d1(i)`` and ``d1(next valid after i)``.
    * ``d2`` at point *i* spans ``{i-2, i-1, i}``, so point *i* collects votes
      from ``d2(i)``, ``d2(next)`` and ``d2(next-next)``.

    A spike sits in the middle of both a rise and a fall and so collects two
    first-derivative votes; a genuine step change participates in only one and
    survives. That asymmetry is the entire point of the filter.

    ``unit_factor`` scales the column into the aviation unit its thresholds are
    stated in (see :data:`AVIATION_UNIT_FACTOR`). Because both derivatives are
    linear in the value, scaling the value is equivalent to scaling both
    thresholds, and it keeps the stored column untouched: the only thing this
    function emits is a NULL mask, which has no unit.
    """
    w = _track_window()
    value = F.col(name)
    # Scaled purely for the comparison below. Never written.
    scaled = value * F.lit(unit_factor) if unit_factor != 1.0 else value

    prev_value = _prev_valid(value, scaled, w)
    prev_time = _prev_valid(value, F.col(_T), w)

    delta = (
        _angular_delta(scaled, prev_value)
        if name == "heading"
        else scaled - prev_value
    )

    df = df.withColumn("_d_dt", F.col(_T) - prev_time).withColumn("_d_dv", delta)

    # Second derivative needs the *previous* raw difference and timestep.
    prev_dv = _prev_valid(value, F.col("_d_dv"), w)
    prev_dt = _prev_valid(value, F.col("_d_dt"), w)

    # deriv2 = 2 * |dv(i) - dv(i-1)| / (dt(i) + dt(i-1))  -- filterclassic.py:181.
    # Note this is a difference of *raw* differences over a mean timestep, so
    # it carries the same unit as deriv1. It is not a rate of a rate.
    df = df.withColumn(
        "_d_f1",
        F.when(
            (F.abs(F.col("_d_dv")) / F.col("_d_dt")) >= F.lit(first_max), 1
        ).otherwise(0),
    ).withColumn(
        "_d_f2",
        F.when(
            (
                F.lit(2.0)
                * F.abs(F.col("_d_dv") - prev_dv)
                / (F.col("_d_dt") + prev_dt)
            )
            >= F.lit(second_max),
            1,
        ).otherwise(0),
    )

    # Votes travel backwards along the chain of *valid* points.
    df = df.withColumn("_d_f1_next", _next_valid(value, F.col("_d_f1"), w))
    df = df.withColumn("_d_f2_next", _next_valid(value, F.col("_d_f2"), w))
    df = df.withColumn("_d_f2_next2", _next_valid(value, F.col("_d_f2_next"), w))

    votes_d1 = F.col("_d_f1") + F.coalesce(F.col("_d_f1_next"), F.lit(0))
    votes_d2 = (
        F.col("_d_f2")
        + F.coalesce(F.col("_d_f2_next"), F.lit(0))
        + F.coalesce(F.col("_d_f2_next2"), F.lit(0))
    )

    # Tallied per order and OR-ed, never summed -- see CleaningConfig.
    kill = F.coalesce(votes_d1 >= F.lit(min_votes), F.lit(False)) | F.coalesce(
        votes_d2 >= F.lit(min_votes), F.lit(False)
    )

    return df.withColumn(name, F.when(~kill, value)).drop(
        "_d_dt",
        "_d_dv",
        "_d_f1",
        "_d_f2",
        "_d_f1_next",
        "_d_f2_next",
        "_d_f2_next2",
    )


def mask_derivative_spikes(df: DataFrame, cfg: CleaningConfig) -> DataFrame:
    """NULL points whose 1st or 2nd derivative marks them as a spike."""
    if not cfg.spike_enabled:
        return df

    df = _with_epoch(df)
    for name, (first_max, second_max) in _spike_thresholds(cfg).items():
        if name in df.columns:
            df = _mask_spikes_one_column(
                df,
                name,
                first_max,
                second_max,
                cfg.spike_min_votes,
                AVIATION_UNIT_FACTOR.get(name, 1.0),
            )
    return df.drop(_T)


# ---------------------------------------------------------------------------
# Stage 5 -- isolated-point removal
# ---------------------------------------------------------------------------

def mask_isolated_points(df: DataFrame, cfg: CleaningConfig) -> DataFrame:
    """NULL values too far in time from any other measurement of that column.

    Per column, ``gap = min(t - t_prev_valid, t_next_valid - t)``. A value
    whose nearest same-column neighbour is more than
    ``isolated_max_gap_seconds`` away on *both* sides cannot be corroborated by
    anything, so it is discarded (``FilterIsolated``).

    Deviation from Alligier, deliberate: a missing neighbour is treated as
    infinitely distant rather than propagating NaN. A track's first and last
    valid samples are therefore judged on the side that exists, instead of
    being discarded merely for sitting at the boundary. A column with exactly
    one valid sample in the whole track has no neighbour either side and is
    still discarded, which is the intended behaviour.
    """
    if not cfg.isolated_enabled:
        return df

    df = _with_epoch(df)
    w = _track_window()
    limit = F.lit(cfg.isolated_max_gap_seconds)
    sentinel = F.lit(_NO_NEIGHBOUR)

    for name in CLEANED_COLUMNS:
        if name not in df.columns:
            continue
        value = F.col(name)
        backward = F.col(_T) - _prev_valid(value, F.col(_T), w)
        forward = _next_valid(value, F.col(_T), w) - F.col(_T)
        gap = F.least(
            F.coalesce(backward, sentinel), F.coalesce(forward, sentinel)
        )
        df = df.withColumn(name, F.when(gap <= limit, value))

    return df.drop(_T)


# ---------------------------------------------------------------------------
# Stage 6 -- gap segmentation
# ---------------------------------------------------------------------------

def add_segment_id(df: DataFrame, cfg: CleaningConfig) -> DataFrame:
    """Add ``segment_id``, splitting a track at coverage holes.

    Downstream detectors must never interpolate across a hole -- a 40 NM ring
    crossing "detected" inside a 20 minute gap is fiction. ``segment_id`` gives
    them a key to group by that is guaranteed contiguous.

    The track itself is left intact: this adds a column, it does not split
    rows. ``track_id`` continuity with published data is preserved.
    """
    df = _with_epoch(df)
    w = _track_window()

    gap = F.col(_T) - F.lag(F.col(_T)).over(w)
    # A NULL gap is the track's first row, which always opens a segment.
    starts = F.when(F.coalesce(gap >= F.lit(cfg.segment_gap_seconds), F.lit(True)), 1).otherwise(0)
    index = F.sum(starts).over(w.rowsBetween(Window.unboundedPreceding, 0))

    return df.withColumn(
        "segment_id", F.concat_ws("_", F.col("track_id"), index.cast("string"))
    ).drop(_T)


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------

def clean_tracks(df: DataFrame, cfg: Optional[CleaningConfig] = None) -> DataFrame:
    """Run the full native cleaning pipeline over an ``osn_tracks`` frame.

    Args:
        df: Track-level state vectors. Must carry ``track_id``, ``event_time``
            and the columns in :data:`CLEANED_COLUMNS`.
        cfg: Thresholds. Defaults to :class:`CleaningConfig` defaults.

    Returns:
        The same rows, with bad values masked to NULL and a ``segment_id``
        column added. Row count changes only via stage 1 (dedup).
    """
    cfg = cfg or CleaningConfig()
    if not cfg.enabled:
        return df

    return (
        df.transform(lambda d: drop_duplicate_statevectors(d, cfg))
        .transform(lambda d: mask_out_of_range_positions(d, cfg))
        .transform(lambda d: mask_stale_broadcasts(d, cfg))
        .transform(lambda d: mask_derivative_spikes(d, cfg))
        .transform(lambda d: mask_isolated_points(d, cfg))
        .transform(lambda d: add_segment_id(d, cfg))
    )


def null_rate_report(
    df: DataFrame, columns: Sequence[str] = CLEANED_COLUMNS
) -> Dict[str, float]:
    """Fraction of NULLs per column, for verifying a cleaning run.

    The pipeline verification asserts that null rates per stage stay within
    configured bounds; this is the measurement behind that assertion. One
    Spark action.
    """
    present = [c for c in columns if c in df.columns]
    if not present:
        return {}

    row = df.select(
        F.count(F.lit(1)).alias("_n"),
        *[
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in present
        ],
    ).first()

    total = row["_n"] or 0
    if total == 0:
        return {c: 0.0 for c in present}
    return {c: (row[c] or 0) / total for c in present}
