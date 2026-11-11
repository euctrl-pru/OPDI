"""ADS-C configuration: off everywhere, in every environment."""

import pytest

from opdi.config import AdscConfig, OPDIConfig


@pytest.mark.parametrize("env", ["dev", "live", "local", "opensky"])
def test_adsc_is_off_in_every_environment(env):
    # The whole safety argument for this feature is that no existing caller
    # changes behaviour. An environment preset that quietly enabled it would
    # change `track_id` for every airframe with an ADS-C sample.
    assert OPDIConfig.for_environment(env).adsc.enabled is False


def test_defaults_name_the_readsb_topic():
    cfg = AdscConfig()
    assert cfg.s3_base == ("s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro")
    assert cfg.table == "osn_adsc_statevectors"


def test_shadow_window_is_positive_on_both_sides():
    # Zero would mean "drop an ADS-C row only on an exact timestamp
    # collision", which no satellite report will ever produce.
    cfg = AdscConfig()
    assert cfg.shadow_before_seconds > 0 and cfg.shadow_after_seconds > 0


def test_no_global_time_correction_by_default():
    # The measured skew is per-airframe (+0.01 s to +2.09 s, one at -12.9 s).
    # A constant would be wrong for nearly every aircraft and would damage the
    # ones already synchronised, so the knob ships at zero.
    assert AdscConfig().time_offset_seconds == 0.0


def test_adsc_is_exempt_from_the_density_stages_by_default():
    # 600 s cadence against a 20 s isolated-point rule: without this, step 02a
    # NULLs every ADS-C lat/lon/altitude and the merge silently does nothing.
    cfg = OPDIConfig.for_environment("opensky")
    assert "ADS-C" in cfg.cleaning.sparse_sources


def test_worldwide_does_not_change_the_adsc_default():
    assert OPDIConfig.for_environment("opensky", worldwide=True).adsc.enabled is False
