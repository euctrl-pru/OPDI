"""Field elevation, and the bands the v6.1 study is read along.

`trend`'s altitude cut was measured against sea level, which costs an aerodrome
its detection in proportion to how high it sits. The claim under test is
therefore not "the method improves" but "the method improves *at elevated
aerodromes and not elsewhere*", and that is only checkable against a banding
fixed in advance.

The bands are chosen before any result is seen, and are not to be re-cut
afterwards to make a boundary fall more favourably.
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from adep_ades import AIRPORTS

#: (label, lo_ft inclusive, hi_ft exclusive). Contiguous and total.
#:
#: `<500` is the control band: there the datum change is a no-op to within
#: 500 ft, so any movement is noise or a second-order effect, and it bounds how
#: much of the headline gain can honestly be attributed to the datum.
BANDS = (
    ("<500", float("-inf"), 500.0),
    ("500-1500", 500.0, 1500.0),
    ("1500-3000", 1500.0, 3000.0),
    (">3000", 3000.0, float("inf")),
)


def elevation_band(elev_col: Column) -> Column:
    """Label an elevation in feet with its band, or ``unknown`` if NULL.

    NULL is its own label rather than being folded into the lowest band. An
    aerodrome the reference cannot place would otherwise inflate exactly the
    band the study uses as its control.
    """
    out = F.when(elev_col.isNull(), F.lit("unknown"))
    for label, lo, hi in BANDS:
        if lo == float("-inf"):
            cond = elev_col < F.lit(hi)
        elif hi == float("inf"):
            cond = elev_col >= F.lit(lo)
        else:
            cond = (elev_col >= F.lit(lo)) & (elev_col < F.lit(hi))
        out = out.when(cond, F.lit(label))
    return out.otherwise(F.lit("unknown"))


def airport_elevations(spark: SparkSession) -> DataFrame:
    """Every aerodrome's field elevation in feet, from OurAirports.

    The same table ``flights.py`` reads elevations from, so the study and the
    pipeline band aerodromes by the same number rather than by two references
    that agree until they do not.
    """
    return (
        spark.read.parquet(AIRPORTS)
        .select(
            F.col("ident").alias("_apt"),
            F.col("elevation_ft").cast("double").alias("_elev_ft"),
        )
        .filter(F.col("_apt").isNotNull())
    )
