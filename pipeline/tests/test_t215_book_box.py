"""T215 — the synopsis PDF opened with the entry's passage highlighted: each rule ships its row's box
(`rules[].book_box`, from `row_boxes.json`, only when cut from THE synopsis PDF) and the release ships our copy of the PDF."""
from __future__ import annotations

import hashlib
import json

from pipeline.deliver.answers.display import decided as DEC
from pipeline.deliver.release import publish
from pipeline.deliver.release.manifest import manifest


def _doc(name="ADAM RIVER (except Eve River)", pages=(14,)):
    return {"entries": {"e1": {"full_name": name, "name": "Adam River", "pages": list(pages)}}}


def test_a_rule_gets_its_entrys_row_box_by_printed_page_and_name(monkeypatch):
    row = {"pdf": 16, "box": [36.14, 86.88, 539.94, 96.48], "size": [576.0, 756.0]}
    monkeypatch.setattr(DEC, "_row_boxes", lambda: {(14, "ADAM RIVER (except Eve River)"): [row]})
    assert DEC.book_boxes({"entry_id": "e1"}, _doc()) == [row]
    assert DEC.book_boxes({"entry_id": "e1"}, _doc(name="OTHER")) == []          # no row: opens unhighlighted
    assert DEC.book_boxes({"entry_id": "e1"}, _doc(pages=(15,))) == []


def test_a_sidecar_cut_from_another_pdf_gives_no_boxes(monkeypatch, tmp_path):
    from pipeline.regs.extraction import row_boxes as RB
    from pipeline.common import synopsis_pages as SP
    pdf = tmp_path / "s.pdf"
    pdf.write_bytes(b"%PDF one")
    side = tmp_path / "row_boxes.json"
    rows = {"page_016_row_000.png": {"pdf_page": 16, "box": [1, 2, 3, 4], "page_size": [576, 756], "printed": 14, "water": "A"}}
    side.write_text(json.dumps({"pdf_sha256": hashlib.sha256(b"%PDF one").hexdigest(), "rows": rows}))
    monkeypatch.setattr(RB, "default_out", lambda: side)
    monkeypatch.setattr(SP, "default_pdf", lambda: pdf)
    monkeypatch.setattr(DEC, "_BOXES", {})
    assert DEC._row_boxes() == {(14, "A"): [{"pdf": 16, "box": [1.0, 2.0, 3.0, 4.0], "size": [576.0, 756.0]}]}
    pdf.write_bytes(b"%PDF two")                                   # the PDF moved on: the boxes are stale
    monkeypatch.setattr(DEC, "_BOXES", {})
    assert DEC._row_boxes() == {}


def test_the_manifest_names_our_copy_and_publish_uploads_it_raw(tmp_path):
    syn = {"id": "abc", "url": "r-1/synopsis-abc.pdf", "bytes": 3, "sha256": "x"}
    assert manifest("r-1", {}, "d768d6e135e62b99", synopsis=syn)["synopsis"] == syn
    assert "synopsis" not in manifest("r-1", {}, "d768d6e135e62b99")
    root = tmp_path / "release"
    r = root / "r-1"
    r.mkdir(parents=True)
    (r / "synopsis-abc.pdf").write_bytes(b"pdf")
    (r / "manifest.json").write_text(json.dumps({**manifest("r-1", {}, "d768d6e135e62b99", synopsis=syn)}))
    puts = publish.plan(r, root)
    assert [(p.key, p.content_type, p.encoding) for p in puts] == [
        ("r-1/synopsis-abc.pdf", "application/pdf", None), ("manifest.json", "application/json", None)]
