"""
Optional ``applyInPandas`` cleaning stages -- **off by default**.

Guarded by :attr:`opdi.config.CleaningConfig.enable_pandas_stage`. Reserved for
algorithms with no Spark equivalent:

* csaps spline smoothing
* 1 s resampling with an LCC projection
* ILS alignment for T15 (traffic's ``LandingAlignedOnILS``)

**Why this is opt-in rather than the default path.** Everything in
:mod:`opdi.cleaning.native` is column and window expressions, so it runs on the
stock executor image with no extra dependencies. Anything here needs a fat
image -- ``docker/Dockerfile`` currently installs only ``h3<4``, ``h3-pyspark``
and ``h3pandas`` onto ``idlefella/spark:v4.1.1``, so a ``traffic`` or ``csaps``
import would fail at executor start, not at submit time. Introducing an
``applyInPandas`` into the default path is a deliberate architectural step, not
a convenience.

A second ``docker/Dockerfile.traffic`` is needed before anything here is
enabled in production: ``traffic`` + ``openap`` + ``pyproj``, with a pre-warmed
``traffic`` cache baked in (airports/runways parquet), because ``traffic.data``
lazily downloads from ourairports.com and Overpass on first access and OSN
executors may be offline. ``onnxruntime`` is only needed for holding-pattern
detection and should be excluded.

No stages are implemented yet. This module exists so that the switch, the
contract and the deployment prerequisite are recorded in one place rather than
discovered at runtime.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

    from opdi.config import CleaningConfig

__all__ = ["apply_pandas_stages"]


def apply_pandas_stages(df: "DataFrame", cfg: "CleaningConfig") -> "DataFrame":
    """Apply the enabled pandas stages in order.

    Args:
        df: Cleaned track frame from :func:`opdi.cleaning.native.clean_tracks`.
        cfg: Cleaning configuration.

    Returns:
        The frame, unchanged, while no stages are implemented.

    Raises:
        NotImplementedError: If ``enable_pandas_stage`` is set. Failing loudly
            here is deliberate: silently returning the input would let a run
            believe smoothing had been applied when it had not, and the result
            would look plausible.
    """
    if not cfg.enable_pandas_stage:
        return df

    raise NotImplementedError(
        "CleaningConfig.enable_pandas_stage is True but no pandas stages are "
        "implemented yet. Implement the stage here and build "
        "docker/Dockerfile.traffic before enabling this on a cluster -- the "
        "default executor image cannot import traffic/csaps."
    )
