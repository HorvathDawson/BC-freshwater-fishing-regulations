"""T667 slice 8: what the corpus claims (`pipeline.deliver.export.claims`). With no `--claims` the export, the answers
step and the highlights read exactly the book's claims — every declared example, every case mechanism, both duties,
the repo synopsis and its row boxes — pinned whole here, so dropping one item from the default fails. A claims file
is validated whole when it is read."""
from __future__ import annotations

import json

import pytest

from pipeline.deliver.export import claims as CL


def test_with_no_claims_file_the_export_holds_the_corpus_to_the_whole_book():
    from pipeline.common.synopsis_pages import default_pdf
    from pipeline.deliver.export.cases import CASE_MECHANISMS
    from pipeline.deliver.export.examples import EXAMPLES
    from pipeline.regs.extraction.row_boxes import default_out
    b = CL.of(None)
    assert b.examples == tuple(EXAMPLES) and len(b.examples) == 64
    assert b.mechanisms == tuple(CASE_MECHANISMS) and len(b.mechanisms) == 75
    assert b.duties == CL.DUTIES == CL.duties_of(None) == ("paper_licence", "protected_fish")
    assert (b.synopsis, b.row_boxes, b.source) == (default_pdf(), default_out(), None)


def test_the_export_cli_and_the_answers_cli_default_to_the_book(monkeypatch, tmp_path):
    from pipeline.deliver import __main__ as D
    from pipeline.deliver.export import __main__ as X
    seen = []
    monkeypatch.setattr(X, "main", lambda argv: seen.append(argv) or 0)
    D.main(["export", "--bundle", str(tmp_path / "b.sqlite"), "--out", str(tmp_path / "e.json")])
    assert "--claims" not in seen[0]
    D.main(["export", "--bundle", str(tmp_path / "b.sqlite"), "--out", str(tmp_path / "e.json"),
            "--claims", str(tmp_path / "c.json")])
    assert seen[1][-2:] == ["--claims", str(tmp_path / "c.json")]


def test_the_duties_are_required_unless_the_claims_leave_them_out():
    from pipeline.deliver.export.read import paper_licence_problems, protected_problems
    doc = {"rules": {}, "licensing": {}, "species": {}}
    assert paper_licence_problems(doc) == ["no 'carry your paper licence' duty (doing.act retaining_recorded)"]
    assert protected_problems(doc) == ["no rule names a protected fish"]
    assert paper_licence_problems(doc, required=False) == [] and protected_problems(doc, required=False) == []


def _claims(tmp_path, **over):
    (tmp_path / "s.pdf").write_bytes(b"%PDF-1.4\n")
    (tmp_path / "rb.json").write_text("{}")
    doc = {"format": CL.FORMAT, "synopsis": "s.pdf", "row_boxes": "rb.json", "examples": [], "mechanisms": [],
           "duties": []} | over
    p = tmp_path / "claims.json"
    p.write_text(json.dumps(doc))
    return p


def test_a_claims_file_is_validated_whole(tmp_path):
    c = CL.load(_claims(tmp_path, mechanisms=["water_release"], duties=["paper_licence"], examples=[
        {"id": "x", "cited_in": ["mini"], "water": "gnis:1", "date": "06-01", "fish": "RB", "speaks": ["e::r"]}]))
    assert c.mechanisms == ("water_release",) and c.duties == ("paper_licence",)
    assert c.examples[0].speaks == ("e::r",) and c.synopsis == (tmp_path / "s.pdf").resolve()
    for over, msg in (({"mechanisms": ["no_such_mechanism"]}, "no case mechanism"),
                      ({"duties": ["carry_a_net"]}, "the duties are"),
                      ({"examples": [{"id": "x", "cited_in": [], "water": "w", "date": "01-01", "fish": "RB",
                                      "colour": "red"}]}, "unknown fields"),
                      ({"examples": [{"id": "x", "cited_in": [], "water": "w", "date": "01-01", "fish": "RB"}] * 2},
                       "example id twice"),
                      ({"synopsis": "missing.pdf"}, "not found"),
                      ({"format": "export-claims/0"}, "not 'export-claims/1'")):
        with pytest.raises(CL.ClaimsError, match=msg):
            CL.load(_claims(tmp_path, **over))
    with pytest.raises(CL.ClaimsError, match="not found"):
        CL.load(tmp_path / "nope.json")


def test_the_highlights_read_the_book_unless_the_claims_name_another(tmp_path):
    from pipeline.common.synopsis_pages import default_pdf
    from pipeline.deliver.answers.display import decided
    from pipeline.regs.extraction.row_boxes import default_out
    decided.use_book()
    assert decided.book_files() == (default_pdf(), default_out())
    decided.use_book(tmp_path / "s.pdf", tmp_path / "rb.json")
    try:
        assert decided.book_files() == (tmp_path / "s.pdf", tmp_path / "rb.json")
    finally:
        decided.use_book()
