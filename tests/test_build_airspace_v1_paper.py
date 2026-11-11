"""The airspace-events-v1 paper builds from its CSVs and names its own scripts."""

import os
from pathlib import Path

import pytest

import build_airspace_v1_paper as b

PAPER = Path(os.environ.get(
    "OPDI_PAPER_DIR_AIRSPACE",
    Path(__file__).resolve().parents[2] / "opdi-portal" / "papers" / "airspace-events-v1"))


def test_the_paper_names_its_own_builder_regenerate_and_pdf():
    assert b.V1.builder == "build_airspace_v1_paper.py"
    assert b.V1.regenerate == "regenerate_airspace_v1.py"
    assert b.V1.pdf_name == "airspace-events-v1.pdf"
    qmd = b.v4.to_qmd("body", b.V1)
    assert "regenerate_airspace_v1.py" in qmd and "build_airspace_v1_paper.py" in qmd
    assert "regenerate_track_v4.py" not in qmd


def test_shipped_context_reads_the_defaults_from_the_source():
    ctx = b.shipped_context()
    assert ctx["holding_default"] in ("on", "off")
    assert int(ctx["pms_sites"].replace(",", "")) == 61
    assert ctx["holding_net_turn"] == "300"


@pytest.mark.skipif(not (PAPER / "data" / "holding").is_dir(), reason="paper data not checked out")
def test_the_staged_data_builds_and_matches_index_qmd():
    qmd, page, md = b.v4.build(PAPER, b.V1)
    assert "## 5. Holding against hand-corrected labels" in md
    assert (PAPER / "index.qmd").read_text() == qmd
