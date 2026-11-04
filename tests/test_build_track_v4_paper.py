"""The track-construction V4 report: every number from a CSV, and a gap stops the build."""

import os
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import build_track_v4_paper as B  # noqa: E402

PAPER = Path(
    os.environ.get(
        "OPDI_PAPER_DIR",
        REPO.parents[3] / "opdi-portal" / ".claude" / "worktrees" / "track-construction-v4"
        / "papers" / "track-construction-v4",
    )
)
needs_paper = pytest.mark.skipif(
    not (PAPER / "report_template.md").is_file() or not (PAPER / "data" / "stage2").is_dir(),
    reason="the paper checkout is not beside this one",
)


def test_percentages_are_computed_from_counts():
    assert B.pct(4439, 6450) == "68.8%"
    assert B.pct(2, 4670, 2) == "0.04%"


def test_a_percentage_of_nothing_stops_the_build():
    with pytest.raises(B.MissingValue):
        B.pct(1, 0)


def test_a_missing_csv_stops_the_build_and_names_the_file(tmp_path):
    with pytest.raises(B.MissingValue, match="link_truth_all_flights.csv"):
        B.read_csv(tmp_path, "stage2/link_truth_all_flights.csv")


def test_a_row_must_be_found_exactly_once():
    rows = [{"a": "1.0"}, {"a": "2"}, {"a": "2"}]
    assert B.one(rows, a=1) == {"a": "1.0"}
    with pytest.raises(B.MissingValue):
        B.one(rows, a=2)
    with pytest.raises(B.MissingValue):
        B.one(rows, a=3)


def test_a_blank_cell_stops_the_build():
    with pytest.raises(B.MissingValue):
        B.num({"x": ""}, "x")
    with pytest.raises(B.MissingValue):
        B.num({}, "x")


def test_an_unknown_placeholder_stops_the_build():
    with pytest.raises(B.MissingValue, match="nope"):
        B.render("{{ nope }}", {})


def test_numbers_typed_into_the_prose_are_found():
    text = "We saw 1,204 flights, {{ x }} tests, `code 99`, a 30 min gap, [a](b/77)."
    assert B.literal_numbers(text) == ["1,204"]


def test_the_converter_handles_each_block_it_claims():
    page = B.markdown_to_html(
        "## Head\n\nA **bold** `a<b` [l](http://x) para\ncontinued.\n\n"
        "- one\n- two\n  more\n\n1. first\n\n| A | B |\n|---|---|\n| 1 | **2** |\n\n"
        "```\nx < y\n```\n"
    )
    assert "<h2>Head</h2>" in page
    assert "<strong>bold</strong>" in page
    assert "<code>a&lt;b</code>" in page
    assert '<a href="http://x">l</a>' in page
    assert "para continued." in page
    assert "<li>two more</li>" in page
    assert "<ol><li>first</li></ol>" in page
    assert "<th>A</th>" in page and "<td><strong>2</strong></td>" in page
    assert "<pre><code>x &lt; y</code></pre>" in page


def test_the_converter_escapes_plain_text():
    assert "&lt;script&gt;" in B.markdown_to_html("a <script> b")


def test_the_page_is_safe_in_dark_mode_and_on_a_phone():
    page = B.to_html("| A |\n|---|\n| 1 |\n")
    assert "prefers-color-scheme: dark" in page
    assert 'name="viewport"' in page
    assert "table-wrap" in page and "overflow-x: auto" in page


def test_the_qmd_carries_the_regeneration_step_for_this_paper():
    qmd = B.to_qmd("## Hello\n")
    assert qmd.startswith("---\ntitle:")
    assert "regenerate_track_v4.py" in qmd
    assert "regenerate_track_v3" not in qmd
    assert qmd.endswith("## Hello\n")


@needs_paper
def test_the_template_states_no_number_without_a_source():
    assert B.literal_numbers((PAPER / "report_template.md").read_text()) == []


@needs_paper
def test_the_report_builds_from_the_staged_csvs():
    qmd, page = B.build(PAPER)
    for text in (qmd, page):
        assert "{{" not in text and "}}" not in text
        assert "nan" not in text.lower().split()
    assert "68.8%" in page and "25.3%" in page


@needs_paper
def test_the_words_the_owner_ruled_out_stay_out():
    text = re.sub(r"\{\{.*?\}\}", " ", (PAPER / "report_template.md").read_text()).lower()
    text = re.sub(r"`[^`]*`", " ", text)
    for word in ("gates", "grid point", "stage 1", "stage 2", " arms", "controls"):
        assert word not in text, word


@needs_paper
def test_every_value_the_builder_computes_is_used():
    text = (PAPER / "report_template.md").read_text()
    assert [k for k in B.build_context(PAPER / "data") if "{{ " + k + " }}" not in text] == []
