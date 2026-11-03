"""Step 01b: registered everywhere, and a no-op when ADS-C is off."""

import datetime as dt

from opdi import periodrun, runner
from opdi.config import OPDIConfig


def test_step_01b_is_registered():
    assert "01b" in runner.STEPS


def test_step_01b_runs_between_01_and_02():
    # `run_pipeline` with no --step iterates `sorted(STEPS)`, so the ordering
    # is lexical and not declared anywhere. ADS-C has to be ingested after the
    # state vectors it is merged into and before the tracks that merge them.
    order = sorted(runner.STEPS)
    assert order.index("01") < order.index("01b") < order.index("02")


def test_step_01b_is_a_per_day_step():
    assert "01b" in periodrun.DAY_STEPS


def test_step_01b_does_nothing_when_adsc_is_off(capsys):
    # No Spark session is passed, and none may be touched: the gate has to be
    # checked before anything is read. A session argument of None that reached
    # the ingestion would raise AttributeError and fail this test.
    cfg = OPDIConfig.for_environment("local")
    assert cfg.adsc.enabled is False
    runner.STEPS["01b"](None, cfg, dt.date(2026, 6, 5), dt.date(2026, 6, 6))
    assert "disabled" in capsys.readouterr().out


def test_run_day_step_01b_does_nothing_when_adsc_is_off(capsys):
    cfg = OPDIConfig.for_environment("local")
    periodrun.run_day_step(None, cfg, "01b", dt.date(2026, 6, 5), {})
    assert "disabled" in capsys.readouterr().out


def test_the_per_day_window_is_the_one_step_01_uses(capsys, monkeypatch):
    # Day D is segmented over [D-1, D+2). ADS-C has to cover the same window,
    # or a track crossing midnight sees two sources on one side of it and one
    # on the other -- which would make the merge's effect depend on the time of
    # day a flight departed.
    seen = {}

    class _Fake:
        def __init__(self, spark, config):
            pass

        def ingest_from_s3(self, start_date, end_date):
            seen["window"] = (start_date, end_date)
            return 0

    import opdi.ingestion.osn_adsc as mod
    monkeypatch.setattr(mod, "AdscIngestion", _Fake)

    cfg = OPDIConfig.for_environment("local")
    cfg.adsc.enabled = True
    periodrun.run_day_step(None, cfg, "01b", dt.date(2026, 6, 5), {})

    assert seen["window"] == (dt.date(2026, 6, 4), dt.date(2026, 6, 7))
