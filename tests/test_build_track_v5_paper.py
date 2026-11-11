"""The track-construction V5 report builder: v4's generic parts with v5's context."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import build_track_v4_paper as V4  # noqa: E402
import build_track_v5_paper as B  # noqa: E402

#: The paper directory: the builder's own, which ``OPDI_PAPER_DIR_V5`` overrides.
PAPER = B.PAPER


def _template():
    path = PAPER / "report_template.md"
    if not path.is_file():
        pytest.skip(f"no paper at {PAPER}: set OPDI_PAPER_DIR_V5")
    return path.read_text()


def _needs_data():
    _template()
    if not (PAPER / "data" / "two_days").is_dir() or not (PAPER / "data" / "pipeline_day").is_dir():
        pytest.skip(f"no staged data under {PAPER / 'data'}")


def test_the_paper_is_v5():
    assert B.V5.version == "v5"
    assert B.V5.builder == "build_track_v5_paper.py"
    assert B.V5.pdf_name == "track-construction-v5.pdf"
    assert B.PAPER.name == "track-construction-v5"
    assert B.V5.render_default == "run"


def test_the_qmd_runs_the_v5_entrypoint_by_default():
    qmd = V4.to_qmd("## Hello\n", B.V5)
    assert "regenerate_track_v5.py" in qmd
    assert "build_track_v5_paper.py" in qmd
    assert "regenerate_track_v4" not in qmd
    assert 'Sys.getenv("OPDI_RENDER", "run")' in qmd
    assert qmd.startswith(f'---\ntitle: "{B.TITLE}"')


def test_the_html_names_the_v5_builder():
    page = V4.to_html("text", B.V5)
    assert "build_track_v5_paper.py" in page
    assert B.TITLE in page and B.SUBTITLE in page


def test_the_template_states_no_number_without_a_source():
    text = _template()
    assert V4.literal_numbers(text, B.ALLOWED_LITERALS) == []


def test_every_term_is_defined_once_before_use():
    text = _template()
    words = text.find("## 3. Words used in this report")
    assert words > 0
    for term in (
        "**track**",
        "**hole**",
        "**edges**",
        "**piece**",
        "**linked hole**",
        "**false join**",
    ):
        assert text.count(term) == 1, term
        assert text.find(term) > words


def test_the_audit_figure_is_the_reports():
    # The number of decisions the audit compared, from its report.
    assert B.AUDIT_DECISIONS == 691_524


def test_the_report_builds_from_the_staged_data():
    _needs_data()
    qmd, page, md = V4.build(PAPER, B.V5)
    assert "{{" not in md
    assert "## 1. Summary" in md
    for required in ("distance_v0.0.3", "`debounced`", "Running time"):
        assert required in md


def test_every_context_value_has_a_placeholder():
    _needs_data()
    text = _template()
    # A value is used as ``{{ name }}`` or inside a ``{% if name %}`` block.
    unused = [k for k in B.build_context(PAPER / "data") if k not in text]
    assert unused == []


def test_check_passes_on_the_committed_qmd():
    _needs_data()
    assert B.main(["--paper-dir", str(PAPER), "--check"]) == 0
