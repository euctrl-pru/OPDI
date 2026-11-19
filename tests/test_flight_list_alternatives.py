"""The other candidate airports (ADEP_P, ADES_P) are listed nearest first, the same on every run.

Two runs of one day gave the same departure airports but listed the other
candidates in a different order for 121,957 of 175,615 flights: the list was
collected in the order Spark met the rows.
"""

import datetime as dt

from test_flight_detection import _EPOCH, CLASSIFIED

from opdi.pipeline.flights import FlightListProcessor

CAND = "track_id string, role string, apt_ident string, dist_nm double"
CHOSEN = (
    "track_id string, icao24 string, flight_id string, role string, apt string, "
    "source string, event_time timestamp"
)


def _endpoint_alternatives(spark, order):
    classified = spark.createDataFrame(
        [("t1", "abc123", "TEST01", "adep", "EBBR", "aerodrome", _EPOCH)], CHOSEN
    )
    rows = {
        "EBBR": ("t1", "adep", "EBBR", 1.0),
        "LFPG": ("t1", "adep", "LFPG", 30.0),
        "EHAM": ("t1", "adep", "EHAM", 10.0),
        "EDDF": ("t1", "adep", "EDDF", 10.0),
    }
    cand = spark.createDataFrame([rows[k] for k in order], CAND).coalesce(1)
    out = FlightListProcessor._flight_table_from_endpoints(classified, cand).collect()
    return out[0]["ADEP_P"]


def test_the_endpoint_alternatives_are_nearest_first_whatever_the_row_order(spark):
    for order in (["LFPG", "EHAM", "EDDF", "EBBR"], ["EDDF", "EBBR", "LFPG", "EHAM"]):
        # EDDF and EHAM are equally far: the code decides.
        assert _endpoint_alternatives(spark, order) == "EDDF, EHAM, LFPG", order


def _trend_frame(spark, order):
    # A landing at 50 N 4 E; three aerodromes due west at 3, 6 and 9 NM.
    aerodromes = {"AAA3": 3.0, "BBB6": 6.0, "CCC9": 9.0}
    rows = []
    for i in range(6):
        t = _EPOCH + dt.timedelta(seconds=30 * i)
        for apt in order:
            nm = aerodromes[apt]
            rows.append(
                (
                    "abc123",
                    "TEST01",
                    "trk-1",
                    apt,
                    50.0,
                    4.0 - nm / 60.0 / 0.6428,
                    t,
                    50.0,
                    4.0,
                    "landing",
                    1,
                    _EPOCH,
                    _EPOCH + dt.timedelta(seconds=150),
                    _EPOCH.date(),
                    0,
                    1,
                )
            )
    return spark.createDataFrame(rows, schema=CLASSIFIED).coalesce(1)


def test_the_trend_alternatives_are_in_rank_order_whatever_the_row_order(spark):
    got = set()
    for order in (["CCC9", "BBB6", "AAA3"], ["BBB6", "AAA3", "CCC9"]):
        out = FlightListProcessor._compute_flight_table(
            _trend_frame(spark, order), rank_by="haversine", bearing_tiebreak_nm=0.0
        ).collect()
        assert out[0]["ADES"] == "AAA3"
        got.add(out[0]["ADES_P"])
    assert got == {"BBB6, CCC9"}
