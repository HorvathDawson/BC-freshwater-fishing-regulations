"""T415 (in T239): a cell's lines are grouped by height, and a glyph stacked under another is a new line.

No PDF: the chars are the ones pdfplumber reads on PDF p.18 (Heber River), so this runs in every tier.
The page itself is pinned in `test_extract_synopsis.py::test_heber_river_tight_last_line_is_its_own_line`.
"""
from types import SimpleNamespace

from pipeline.regs.extraction.extract_synopsis import FishingSynopsisParser, _stacked_under


def _chars(text, x0, top, size=7.0, w=3.0, font="ArialNovaCond-Regular"):
    return [{"text": t, "x0": x0 + i * w, "x1": x0 + (i + 1) * w, "top": top, "bottom": top + size,
             "size": size, "width": w, "fontname": font} for i, t in enumerate(text)]


def _text(chars, tmp_path):
    return FishingSynopsisParser(output_dir=str(tmp_path)).extract_text_with_bold(SimpleNamespace(chars=chars))


def test_a_last_line_5pt_under_is_its_own_line(tmp_path):
    # p.18: "Fly fishing ... when" at top 381.7924, "open" at 386.7924 — exactly 5 pt, which `> 5` kept on one line.
    chars = _chars("Fly fishing when", 177.66, 381.7924) + _chars("open", 177.66, 386.7924)
    assert _text(chars, tmp_path) == "Fly fishing when\nopen"


def test_jitter_and_faux_bold_stay_on_their_line(tmp_path):
    bold = _chars("No", 177.7, 362.2, font="ArialNovaCond-Bold")
    rest = _chars("upstream", 208.3, 362.09)            # bold vs regular tops differ by 0.11 pt
    double = _chars("X", 300.0, 362.09)                 # faux bold: the same glyph twice, same top
    sym = _chars("", 142.3, 361.67)                # a Wingdings symbol sits 0.42 pt higher
    assert _text(bold + rest + double + double + sym, tmp_path) == "**No**upstream XX"  # one line; spacing across a bold switch is the builder's own


def test_lower_but_beside_is_not_stacked():
    line = _chars("when", 100.0, 381.79)
    assert not _stacked_under(_chars("o", 200.0, 386.79)[0], line)   # no glyph above it
    assert _stacked_under(_chars("o", 101.0, 386.79)[0], line)
    assert not _stacked_under(_chars("o", 101.0, 383.0)[0], line)    # under half its size lower: same line
