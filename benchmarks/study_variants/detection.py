"""Flight-list (step 03) options that the package no longer ships.

Two choices were measured in the flight-list studies and are not part of the
default or of the v0.0.2 configuration:

* the ``nearest`` mode: the aerodrome closest to the first or last position of
  the track, with no height or ground test, so it never declines to answer;
* the ``trend_ooa`` option: the ``trend`` mode also labels a flight that
  starts or ends at the edge of the covered area as coming from or going to
  outside it (``OOA``).

A study script runs them with :class:`StudyFlightListProcessor`, and turns the
option on with :class:`StudyDetectionConfig`::

    from study_variants.detection import StudyDetectionConfig, StudyFlightListProcessor

    config.detection = StudyDetectionConfig(trend_ooa=True)
    StudyFlightListProcessor(spark, config).process_dai(month, adep_mode="nearest",
                                                        ades_mode="nearest")
"""

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number, to_date, when
from pyspark.sql.window import Window

from opdi.config import DetectionConfig
from opdi.pipeline.flights import (
    FLIGHT_LIST_VERSION,
    OOA,
    FlightListProcessor,
    at_border,
)

__all__ = ["StudyDetectionConfig", "StudyFlightListProcessor"]


@dataclass
class StudyDetectionConfig(DetectionConfig):
    """The package's flight-list thresholds plus the ``trend_ooa`` option."""

    trend_ooa: bool = False
    """Label ``trend`` endpoints at the coverage edge as out of area (``OOA``)."""


class StudyFlightListProcessor(FlightListProcessor):
    """Step 03 with the ``nearest`` mode and the ``trend_ooa`` option."""

    DETECTION_MODES = FlightListProcessor.DETECTION_MODES + ("nearest",)

    def _trend_sources(self, t: DataFrame, month: date) -> DataFrame:
        """With ``trend_ooa`` on, also name the flights that leave or enter the area."""
        if getattr(self.detection, "trend_ooa", False):
            # Three states, not two: named an aerodrome, came from outside
            # the observed area, could not tell.
            return self._label_trend_out_of_area(t, month)
        return super()._trend_sources(t, month)

    @staticmethod
    def classify_endpoints(
        cand: DataFrame,
        mode: str = "nearest",
        abstention_radius_nm: float = 40.0,
        abstention_height_ft: float = 15000.0,
        sched_penalty_nm: float = 10.0,
        ooa: bool = True,
    ) -> DataFrame:
        """Like :meth:`FlightListProcessor.classify_endpoints`, plus ``nearest``.

        ``nearest`` follows the rule of the ``traffic`` library: the closest
        aerodrome wins, with no altitude, ground-state or trend condition. It
        never abstains, so it is silent only when there is no candidate at all.
        """
        if mode != "nearest":
            return FlightListProcessor.classify_endpoints(
                cand,
                mode=mode,
                abstention_radius_nm=abstention_radius_nm,
                abstention_height_ft=abstention_height_ft,
                sched_penalty_nm=sched_penalty_nm,
                ooa=ooa,
            )
        best = FlightListProcessor._best_candidates(cand, sched_penalty_nm)
        return best.withColumn("apt", col("apt_ident")).withColumn(
            "source",
            when(col("apt_ident").isNotNull(), lit("aerodrome")).otherwise(lit("undetermined")),
        )

    def _track_border_flags(self, month: date) -> DataFrame:
        """Per track: whether its first and last fix sit at the bbox edge.

        The evidence behind an out-of-area label, reduced to two booleans per
        track. ``endpoint`` gets this for free -- it is already looking at the
        endpoints -- but ``trend`` never looks at them, so it needs its own
        pass. That pass is two window functions over ``track_id`` rather than a
        join, which is why it is affordable to add to a path that does not
        otherwise need the endpoints at all.

        Identity columns come along because this frame is outer-joined into the
        trend table: a track whose altitude trace showed nothing near any
        aerodrome has no row there at all, and a flight that entered the area
        already airborne and left it still airborne is exactly such a track.
        Without them it could be labelled but not named.
        """
        sv = self._get_data_within_timeframe(self.tracks_table, month)
        sv = sv.dropna(subset=["lat", "lon", "track_id"])
        # flight_id arrives renamed and resolved to one value per track; see
        # _get_data_within_timeframe. The fillna still matters on a legacy run,
        # which is not resolved.
        sv = sv.fillna({"flight_id": ""})
        sv = sv.withColumn("event_time", F.to_timestamp(col("event_time")))
        sv = sv.withColumn(
            "_border",
            at_border(col("lat"), col("lon"), bbox=self.config.coverage.bbox),
        )

        w = Window.partitionBy("track_id").orderBy("event_time")
        w_desc = Window.partitionBy("track_id").orderBy(col("event_time").desc())
        return (
            sv.withColumn("_rn", row_number().over(w))
            .withColumn("_rr", row_number().over(w_desc))
            .filter((col("_rn") == 1) | (col("_rr") == 1))
            .groupBy("track_id")
            .agg(
                # min_by/max_by would skip nulls; the border flag is never null
                # here because lat/lon were dropped above.
                F.max(when(col("_rn") == 1, col("_border"))).alias("adep_at_border"),
                F.max(when(col("_rr") == 1, col("_border"))).alias("ades_at_border"),
                F.min("event_time").alias("_first_seen"),
                F.max("event_time").alias("_last_seen"),
                F.min("icao24").alias("_ICAO24"),
                # Every row of the track carries the same value by the time it
                # gets here -- resolved on a v5.0.0 run, homogeneous by
                # construction on a legacy one -- so the choice of aggregate no
                # longer decides anything. ``first`` says that; ``min`` implied
                # a decision, and made the wrong one, because "" sorts before
                # every real callsign.
                F.first("flight_id").alias("_FLT_ID"),
            )
            .withColumn("_DOF", to_date(col("_first_seen")))
        )

    def _label_trend_out_of_area(self, t: DataFrame, month: date) -> DataFrame:
        """Give ``trend`` the out-of-area label that ``endpoint`` already has.

        ``trend`` names an aerodrome or says nothing, and those are not the only
        two possibilities: a flight that entered the observed area already
        airborne has an origin no feed here can name, and reporting that is a
        different -- and correct -- answer from silence. Roughly one arrival in
        twelve is in that position, and until this existed every one of them was
        a null indistinguishable from a detection failure.

        The test is the one ``classify_endpoints`` uses, deliberately: a fix
        within :data:`BORDER_MARGIN_NM` of the configured coverage edge --
        the published European box by default, inert worldwide since there is
        no edge to be near. Precedence
        is the same too -- **aerodrome first, border second**. The other order
        looks equivalent and is not: Ponta Delgada sits about 8 NM inside the
        western edge, and letting the border test win labels its departures
        out-of-area with the aircraft still on the runway.

        The join is an outer one because a track can be missing from the trend
        table entirely: a flight that crossed the area without ever descending
        near an aerodrome has no row there, and it is precisely the flight this
        label exists for.

        That would also admit every track the border test does *not* fire on --
        thousands of rows naming nothing at either end -- so the result is
        filtered back to rows that say something. The population of the flight
        list therefore grows by exactly the flights that gained a label, and by
        nothing else. Getting this wrong would not fail: it would quietly add
        empty rows and change the denominator of every per-aerodrome count.
        """
        flags = self._track_border_flags(month)
        j = t.join(flags, t.id == flags.track_id, "outer")

        named = {"ADEP": "adep_source", "ADES": "ades_source"}
        for apt_col, src_col in named.items():
            border = col(f"{apt_col.lower()}_at_border")
            j = j.withColumn(
                src_col,
                when(col(apt_col).isNotNull(), lit("aerodrome"))
                .when(border, lit("out_of_area"))
                .otherwise(lit("undetermined")),
            )
            j = j.withColumn(
                apt_col,
                when(col(apt_col).isNotNull(), col(apt_col)).when(border, lit(OOA)),
            )

        # Identity from whichever side has it: a track present only in the
        # border frame has none of the trend table's columns populated.
        out = j.select(
            F.coalesce(col("id"), col("track_id")).alias("id"),
            "ADEP",
            "ADES",
            "ADEP_P",
            "ADES_P",
            F.coalesce(col("ICAO24"), col("_ICAO24")).alias("ICAO24"),
            F.coalesce(col("FLT_ID"), col("_FLT_ID")).alias("FLT_ID"),
            F.coalesce(col("first_seen"), col("_first_seen")).alias("first_seen"),
            F.coalesce(col("last_seen"), col("_last_seen")).alias("last_seen"),
            F.coalesce(col("DOF"), col("_DOF")).alias("DOF"),
            F.coalesce(col("version"), lit(FLIGHT_LIST_VERSION)).alias("version"),
            "adep_source",
            "ades_source",
        )
        # Only rows that say something about at least one end. See above: the
        # outer join is what lets a border-only track appear at all, and this is
        # what stops it bringing every other unmatched track with it.
        return out.filter(col("ADEP").isNotNull() | col("ADES").isNotNull())
