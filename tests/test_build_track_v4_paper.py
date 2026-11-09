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


def _recommended():
    return B.one(B.read_csv(PAPER / "data", "final_grid/link_recommended.csv"))


def _skip_until_the_grid_is_rerun():
    """The read-through count over all holes is staged only by a run after it was added,
    and ``final_edges`` only by a run after the flags were split."""
    if "real_holes_edge_read_through_blank" not in _recommended():
        pytest.skip("the staged final grid predates real_holes_edge_read_through_blank")
    if not (PAPER / "data" / "final_edges" / "link_grid_pooled.csv").is_file():
        pytest.skip("final_edges is not staged yet")


@needs_paper
def test_the_report_builds_from_the_staged_csvs():
    _skip_until_the_grid_is_rerun()
    qmd, page, _md = B.build(PAPER)
    for text in (qmd, page):
        assert "{{" not in text and "}}" not in text
        assert "nan" not in text.lower().split()
    rec = _recommended()
    flights = B.num(rec, "crossing_flights")
    assert B.pct(B.num(rec, "crossing_flights_in_one_track"), flights) in page
    assert B.pct(B.num(rec, "base_crossing_flights_in_one_track"), flights) in page


def test_a_grid_without_the_read_through_count_stops_the_build_and_names_it():
    """Until the re-run this is what the staged CSVs give, and a gap must not read as a result."""
    with pytest.raises(B.MissingValue, match="real_holes_edge_read_through_blank"):
        B.blank_edge_values({"real_holes": "228655"})


def test_the_share_of_holes_read_through_a_blank_is_of_all_holes():
    values = B.blank_edge_values(
        {"real_holes_edge_read_through_blank": "25", "real_holes": "100"}
    )
    assert values == {"blank_edge_holes": "25", "blank_edge_share": "25.0%"}


@needs_paper
def test_the_low_edge_count_is_of_tests_the_setting_allows():
    _skip_until_the_grid_is_rerun()
    rec = _recommended()
    assert B.build_context(PAPER / "data")["low_edge_tests"] == B.n(
        B.num(rec, "negative_low_edge_untested_controls")
    )


def _edges(**verdicts):
    base = {"off": "True", "climb": "True", "descent": "False", "both": "False", **verdicts}
    rows = {k: {"within_limits": v, "crossing_flights_in_one_track": "1", "linked_false_join": "1",
                "negative_limit_false_joins": "1", "real_by_climb_status": "insufficient",
                "real_by_descent_status": "over_limit"} for k, v in base.items()}
    return rows


_PICK = {"status": "recommended_provisional", "provisional": "True", "link_old_edge_climb": "True",
         "link_new_edge_descent": "False", "crossing_flights_in_one_track": "1",
         "linked_false_join": "1", "negative_limit_false_joins": "1"}


def test_the_edge_decision_holds_on_what_the_prose_says():
    B.check_edge_decision(_PICK, _edges())


@pytest.mark.parametrize("name", ["descent", "both"])
def test_a_rejected_edge_setting_passing_stops_the_build(name):
    with pytest.raises(B.MissingValue, match="rewriting"):
        B.check_edge_decision(_PICK, _edges(**{name: "True"}))


@pytest.mark.parametrize("change", [
    {"status": "recommended"}, {"provisional": "False"}, {"link_old_edge_climb": "False"},
    {"link_new_edge_descent": "True"},
])
def test_a_different_pick_stops_the_build(change):
    with pytest.raises(B.MissingValue, match="rewriting"):
        B.check_edge_decision({**_PICK, **change}, _edges())


def test_each_edge_setting_must_have_its_own_row():
    rows = [{"link_old_edge_climb": a, "link_new_edge_descent": b, "id": a + b}
            for a in ("True", "False") for b in ("True", "False")]
    assert B.edge_rows(rows)["climb"]["id"] == "TrueFalse"
    with pytest.raises(B.MissingValue):
        B.edge_rows(rows[:3])


def test_the_bound_on_none_of_n_has_its_closed_form():
    assert B.upper_95(0, 185) == pytest.approx(1 - 0.05 ** (1 / 185), rel=1e-9)
    with pytest.raises(B.MissingValue):
        B.upper_95(0, 0)


@needs_paper
def test_the_bound_is_the_one_the_study_computes():
    _skip_until_the_grid_is_rerun()
    rec = _recommended()
    assert B.upper_95(num := B.num(rec, "negative_limit_false_joins"), B.num(rec, "negative_limit_controls")) \
        == pytest.approx(B.num(rec, "negative_false_join_upper_95"), rel=1e-6)
    assert num > 0
    assert B.upper_95(B.num(rec, "same_callsign_low_edge_false_joins"),
                      B.num(rec, "same_callsign_low_edge_controls")) \
        == pytest.approx(B.num(rec, "same_callsign_low_edge_upper_95"), rel=1e-6)


def test_the_chances_of_all_falling_on_one_side():
    assert f"{B.chance_all_in_one_drawn(3, 119, 304):.3f}" == "0.059"
    assert f"{B.chance_all_in_one_repeated(6, 82974, 162317):.3f}" == "0.018"
    with pytest.raises(B.MissingValue):
        B.chance_all_in_one_drawn(3, 1, 0)
    with pytest.raises(B.MissingValue):
        B.chance_all_in_one_repeated(3, 1, 0)


def test_the_constants_read_from_source_are_the_real_ones():
    from opdi.pipeline.segmentation import methods

    for name, relative in B.CODE_CONSTANTS.values():
        if "segmentation" in relative:
            assert B.code_constant(name, relative) == getattr(methods, name)
    import track_link_controls

    assert B.code_constant("EVENT_NEAR_EDGE_MIN", "benchmarks/track_link_controls.py") \
        == track_link_controls.EVENT_NEAR_EDGE_MIN


def test_a_constant_gone_from_the_source_stops_the_build():
    with pytest.raises(B.MissingValue, match="NOPE"):
        B.code_constant("NOPE", "src/opdi/pipeline/segmentation/methods.py")


@needs_paper
def test_the_chances_come_from_the_staged_counts():
    _skip_until_the_grid_is_rerun()
    c = B.build_context(PAPER / "data")
    assert (c["chance_real"], c["chance_hidden"]) == ("0.059", "0.018")
    assert (c["low_labelled"], c["low_hidden_tests"]) == ("304", "162,317")


@needs_paper
def test_the_literals_three_and_five_keep_one_meaning_each():
    text = re.sub(r"\{\{.*?\}\}", " ", (PAPER / "report_template.md").read_text())
    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    text = re.sub(r"`[^`]*`", " ", text)
    assert re.findall(r"(?<![\w.,])3(?![\d,.]*\d)", text) == ["3"]
    assert "rather than 3," in text
    fives = re.findall(r"(?<![\w.,])5(?![\d,.]*\d)(.?)", text)
    assert fives and set(fives) == {"%"}


@needs_paper
def test_the_words_the_owner_ruled_out_stay_out():
    text = re.sub(r"\{\{.*?\}\}", " ", (PAPER / "report_template.md").read_text()).lower()
    text = re.sub(r"`[^`]*`", " ", text)
    for word in ("gates", "grid point", "stage 1", "stage 2", " arms", "controls"):
        assert word not in text, word


@needs_paper
def test_every_value_the_builder_computes_is_used():
    _skip_until_the_grid_is_rerun()
    text = (PAPER / "report_template.md").read_text()
    assert [k for k in B.build_context(PAPER / "data") if "{{ " + k + " }}" not in text] == []


def test_col_widths_fit_the_page_and_keep_words_whole():
    measure = len  # one point per character
    rows = [["Number", "Description of the thing"], ["1", "a much longer cell " * 10]]
    w = B.col_widths(rows, 100, measure)
    assert sum(w) == pytest.approx(100)
    assert w[0] >= len("Number")
    assert B.col_widths(rows, 20, measure) == pytest.approx([20 * f / 80 for f in (28, 52)], abs=30)
    assert sum(B.col_widths(rows, 20, measure)) == pytest.approx(20)
    assert sum(B.col_widths([["a", "b"]], 500, measure)) == pytest.approx(500)


def test_pdf_without_reportlab_says_so(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "reportlab", None)  # makes the import fail
    with pytest.raises(SystemExit, match="needs reportlab"):
        B.to_pdf("# x\n\ntext", tmp_path / "x.pdf")


def test_version_4_is_frozen():
    """Superseded by version 5: a render does not re-run the analysis by default."""
    assert B.V4.render_default == "skip"
    assert 'Sys.getenv("OPDI_RENDER", "skip")' in B.to_qmd("x\n")
