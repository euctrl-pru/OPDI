"""Which aerodrome the flight list names, and that it names it the same way twice.

Two defects, found by comparing two runs over tracks that were identical in
both and still came out with different airports:

* the flight table ranked the candidates and then took ``first(apt_ident)``
  over a group, which reads whichever row the aggregation happened to meet
  first -- not the row ranked first. About 1% of named airports were in fact one
  of the alternatives, and the named airport then also appeared in its own
  alternatives list;
* two candidates at the same distance (an aerodrome listed twice under two
  identifiers) were ordered by nothing at all.

Neither changes the method. The row ranked first is the answer the method
always intended; these tests pin that it is the one reported.
"""

import datetime as dt
import random

from test_flight_detection import _EPOCH, CLASSIFIED

from opdi.pipeline.flights import FlightListProcessor


def _landings(spark, aerodromes, tracks=1, n=2, seed=None):
    """``tracks`` landings ending at 50 N 4.000 E, each seen near every
    aerodrome in ``aerodromes`` (ident -> (lat, lon))."""
    rows = []
    last_seen = _EPOCH + dt.timedelta(seconds=(n - 1) * 30)
    for k in range(tracks):
        for i in range(n):
            t = _EPOCH + dt.timedelta(seconds=i * 30)
            lon = 4.5 - 0.5 * i / max(n - 1, 1)
            for apt, (alat, alon) in aerodromes.items():
                rows.append(
                    (
                        f"ac{k:04d}",
                        "TEST01  ",
                        f"trk-{k:04d}",
                        apt,
                        alat,
                        alon,
                        t,
                        50.0,
                        lon,
                        "landing",
                        1,
                        _EPOCH,
                        last_seen,
                        _EPOCH.date(),
                        0,
                        1,
                    )
                )
    if seed is not None:
        random.Random(seed).shuffle(rows)
    return spark.createDataFrame(rows, schema=CLASSIFIED)


#: Three aerodromes at clearly different distances from the final fix.
THREE = {
    "FAR1": (50.30, 4.0),
    "NEAR": (50.02, 4.0),
    "MID1": (50.10, 4.0),
}


def test_the_named_airport_is_the_one_ranked_first(spark):
    """Enough flights in one task that the aggregation falls back to sorting,
    which is where ``first()`` stopped meaning "ranked first"."""
    out = FlightListProcessor._compute_flight_table(
        _landings(spark, THREE, tracks=400, seed=7), rank_by="haversine"
    ).collect()
    assert len(out) == 400
    wrong = [r["id"] for r in out if r["ADES"] != "NEAR"]
    assert wrong == []


def test_the_named_airport_is_not_among_its_own_alternatives(spark):
    out = FlightListProcessor._compute_flight_table(
        _landings(spark, THREE, tracks=400, seed=7), rank_by="haversine"
    ).collect()
    listed_twice = [r["id"] for r in out if r["ADES"] in r["ADES_P"].split(", ")]
    assert listed_twice == []
    assert {tuple(sorted(r["ADES_P"].split(", "))) for r in out} == {("FAR1", "MID1")}


#: One aerodrome under two identifiers, as OurAirports lists CYEG and CA-1291.
TWINS = {"CYEG": (50.05, 4.0), "CA-1291": (50.05, 4.0)}


def test_equal_distance_is_settled_by_the_identifier(spark):
    """Whatever order the rows arrive in, the same one of the two is named."""
    named = set()
    for seed in (1, 2, 3):
        out = FlightListProcessor._compute_flight_table(
            _landings(spark, TWINS, tracks=50, n=8, seed=seed),
            rank_by="haversine",
            bearing_tiebreak_nm=2.0,
        )
        named |= {r["ADES"] for r in out.collect()}
    assert named == {"CA-1291"}


def _candidates(spark, order):
    rows = [("trk-1", "ades", ident, 3.0) for ident in order]
    return spark.createDataFrame(
        rows, "track_id string, role string, apt_ident string, dist_nm double"
    )


def test_classify_endpoints_settles_equal_distance_by_the_identifier(spark):
    got = {
        FlightListProcessor.classify_endpoints(_candidates(spark, order), mode="nearest").first()[
            "apt"
        ]
        for order in (("CYEG", "CA-1291"), ("CA-1291", "CYEG"))
    }
    assert got == {"CA-1291"}


# ---------------------------------------------------------------------------
# A day run builds its candidates from the tracks it was given
# ---------------------------------------------------------------------------


class _Stop(Exception):
    pass


def test_a_day_run_rebuilds_its_endpoint_candidates():
    """The candidate table is a cache of the two end samples of each track.
    Left in place after the day's tracks were rebuilt, it lists tracks that no
    longer exist, and the flight list then has rows without a track. A day run
    therefore never trusts the "already built" log."""
    from opdi.config import DetectionConfig

    seen = {}
    proc = FlightListProcessor.__new__(FlightListProcessor)
    proc.detection = DetectionConfig()

    def build(month, max_radius_nm=110.0, rebuild=False):
        seen["rebuild"] = rebuild
        raise _Stop()

    proc.build_endpoint_candidates = build
    try:
        proc.process_day(
            dt.date(2026, 6, 14),
            skip_if_processed=False,
            adep_mode="endpoint",
            ades_mode="endpoint",
        )
    except _Stop:
        pass
    assert seen == {"rebuild": True}
    assert proc._batch_day is None


def test_a_month_run_keeps_its_candidate_cache():
    """The cache exists so thresholds can be swept without rebuilding it; a
    month run that did not rebuild its tracks keeps using it."""
    from opdi.config import DetectionConfig

    seen = {}
    proc = FlightListProcessor.__new__(FlightListProcessor)
    proc.detection = DetectionConfig()

    def build(month, max_radius_nm=110.0, rebuild=False):
        seen["rebuild"] = rebuild
        raise _Stop()

    proc.build_endpoint_candidates = build
    try:
        proc.process_dai(
            dt.date(2026, 6, 1), skip_if_processed=False, adep_mode="endpoint", ades_mode="endpoint"
        )
    except _Stop:
        pass
    assert seen == {"rebuild": False}
