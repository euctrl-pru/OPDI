"""The segmentation rules that moved to benchmarks/study_variants/segmentation.py.

These run them through the real pipeline (``TrackProcessor``) after
``register_study_arms()``, as a study script does.
"""

import datetime as dt

import pytest
from conftest import make_track
from test_track_claim import D_MINUS_1, _every, _shipped
from test_tracks_method import BLANKING, FLICKER, _partition, _proc

from opdi.pipeline.segmentation import methods


@pytest.fixture(autouse=True)
def study_rules(monkeypatch):
    """The study rules registered in a copy of the rule table, for one test."""
    from study_variants.segmentation import register_study_arms

    monkeypatch.setattr(methods, "ARMS", methods._Rules(methods.ARMS))
    register_study_arms()


def test_registering_adds_every_study_rule():
    from study_variants.segmentation import STUDY_ARMS

    assert set(STUDY_ARMS) <= set(methods.ARMS)
    assert set(STUDY_ARMS) == set(methods.STUDY_ARMS)


def test_standard_now_debounces_where_recommended_would_split(spark, tmp_path):
    """The flip is real, not cosmetic: ``standard`` no longer equals A8.

    On a sub-30 s callsign flicker A8 (``recommended``) splits one flight into
    three tracks; the shipped ``standard`` (A10, which keeps A9's guard) keeps it
    whole. If ``standard`` ever silently reverted to A8 this partition would
    jump from one group to three.
    """
    df = make_track(spark, FLICKER)
    standard = _partition(_proc(spark, tmp_path, "standard")._add_track_id(df))
    a8 = _partition(_proc(spark, tmp_path, "recommended")._add_track_id(df))
    assert len(standard) == 1
    assert len(a8) == 3
    assert standard != a8


def test_any_study_arm_can_be_selected(spark, tmp_path):
    """The benchmark drives the real pipeline by naming an arm here.

    Without this the v2 study would have to re-implement each arm outside the
    pipeline, which is the arrangement v1 had and the reason its numbers
    described a harness rather than production.
    """
    df = make_track(spark, BLANKING)
    out = _proc(spark, tmp_path, "airframe_only")._add_track_id(df)
    assert out.count() == df.count()
    assert out.filter(out.track_id.isNull()).count() == 0


def test_debounced_by_name_keeps_its_published_memory(spark):
    """A9 is a published rule and stays as it was: it remembers a callsign without limit.

    Eleven hours of samples with no callsign, then a garble and the old callsign again.
    A9 still calls the garble a flicker; A10 has forgotten the old callsign and splits.
    """
    from study_variants.segmentation import debounced

    from opdi.pipeline.segmentation import assign_track_id
    from opdi.pipeline.segmentation.methods import cruise_linked

    h = lambda hours, minutes=0: dt.timedelta(hours=hours, minutes=minutes)  # noqa: E731
    samples = _every(D_MINUS_1 + h(1), D_MINUS_1 + h(1, 30), 10, lon=0.0, callsign="DLH1")
    samples += _every(D_MINUS_1 + h(1, 40), D_MINUS_1 + h(12, 30), 10, lon=0.0, callsign="")
    samples += _every(D_MINUS_1 + h(12, 31), D_MINUS_1 + h(12, 31), 1, lon=0.0, callsign="DLH7X")
    samples += _every(
        D_MINUS_1 + h(12, 31) + dt.timedelta(seconds=5),
        D_MINUS_1 + h(12, 40),
        1,
        lon=0.0,
        callsign="DLH1",
    )
    sv = make_track(spark, samples)

    def tracks(rule):
        return assign_track_id(sv, rule(), _shipped()).select("track_id").distinct().count()

    assert tracks(debounced) == 1
    assert tracks(cruise_linked) == 2
