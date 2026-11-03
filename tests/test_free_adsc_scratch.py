"""The delete script's scope. Nothing here touches S3."""

import pytest

import free_adsc_scratch as f


@pytest.mark.parametrize("bad", [
    "", "opdi-prod/", "opdi-world/research/adsc/", "opdi/research/adsc",
    "opdi/research/adsc_airframes/", "opdi/", "opdi/research/",
])
def test_a_prefix_outside_the_study_is_refused(bad):
    with pytest.raises(SystemExit, match="refusing"):
        f.validate_prefix(bad)


def test_the_study_prefix_and_anything_beneath_it_pass():
    assert f.validate_prefix(f.ROOT) == f.ROOT
    narrow = f.ROOT + "input/osn_statevectors_v2/"
    assert f.validate_prefix(narrow) == narrow


def test_a_stray_key_is_refused():
    with pytest.raises(SystemExit, match="stray"):
        f.assert_in_scope("opdi-prod/osn_tracks/part-0", f.ROOT)
    f.assert_in_scope(f.ROOT + "merged/x", f.ROOT)


def test_the_checks_are_not_asserts():
    # `python -O` strips asserts, and with them the only thing between a typo
    # and deleting production.
    from pathlib import Path
    src = Path(f.__file__).read_text()
    assert "assert " not in src.replace("assert_in_scope", "")
