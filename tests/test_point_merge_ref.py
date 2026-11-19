"""The curated point merge table ships with the package and loads cleanly."""

import csv
import datetime as dt
import math

from opdi.reference.point_merge import (
    PMS_CSV,
    load_point_merge_systems,
    point_merge_systems_for_day,
)

EIDW = (53.4213, -6.2701)


def _nm(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (
        math.sin(math.radians(lat2 - lat1) / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return 2 * 3440.065 * math.asin(math.sqrt(a))


def test_every_catalogue_airport_has_a_row():
    with open(PMS_CSV) as fh:
        rows = list(csv.DictReader(fh))
    assert len({r["apt_icao"] for r in rows}) == 61
    assert {r["confidence"] for r in rows} <= {"high", "medium", "missing"}


def test_placed_rows_have_coordinates_and_geometry(spark):
    ref = load_point_merge_systems(spark)
    rows = ref.collect()
    assert rows and all(r.confidence in ("high", "medium") for r in rows)
    for r in rows:
        assert r.lat is not None and r.lon is not None
        assert r.legs_mp_min_nm is not None and r.legs_mp_max_nm >= r.legs_mp_min_nm
        assert isinstance(r.valid_from, dt.date)


def test_rows_for_a_day_respect_validity(spark, tmp_path):
    p = tmp_path / "pms.csv"
    p.write_text(
        "apt_icao,pms_id,merge_point,lat,lon,legs_mp_min_nm,legs_mp_max_nm,leg_level_min_ft,"
        "leg_level_max_ft,valid_from,valid_to,source,confidence\n"
        "EDDP,EDDP-ALL,XXXXX,51.4,12.2,15,17,,,2015-12-18,2020-01-30,test,high\n"
        "EIDW,EIDW-28,LAPMO,53.4031,-5.9456,15,17,7000,8000,2012-12-01,,test,high\n"
        "ZZZZ,ZZZZ-ALL,,,,10,12,,,2024-01-01,,test,missing\n"
    )
    on_day = point_merge_systems_for_day(spark, dt.date(2026, 6, 14), path=str(p)).collect()
    assert [r.merge_point for r in on_day] == ["LAPMO"]
    before = point_merge_systems_for_day(spark, dt.date(2010, 1, 1), path=str(p)).collect()
    assert before == []


def test_eidw_lapmo_is_about_12_nm_from_the_airport(spark):
    ref = load_point_merge_systems(spark)
    lapmo = ref.filter("merge_point = 'LAPMO'").first()
    assert 9.0 <= _nm(lapmo.lat, lapmo.lon, *EIDW) <= 15.0
