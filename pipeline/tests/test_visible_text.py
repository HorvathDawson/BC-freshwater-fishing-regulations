"""T1190: the visible-text method (`pipeline/regs/extraction/visible.py`), one tiny hand-written PDF per rule.

Each PDF is written here byte by byte (no PDF library writes it), so these tests stand whatever library
`visible.py` reads with. Page 200 x 100 pt, Helvetica 12 pt, text at x 20, baseline y 50 (PDF coordinates: y up).
The real book's pages are pinned in `test_visible_text_book.py`.
"""
import re

import pdfplumber
import pytest

from pipeline.regs.extraction import visible as V
from pipeline.regs.extraction.extract_synopsis import FishingSynopsisParser, strike_items, strike_report
from pipeline.regs.extraction.models import WaterbodyRow


def _pdf(content: str, *, crop=None, width: int = 200) -> bytes:
    """One page; every resource a test may use: font F1, graphics states (GS0 opacity 0, GH opacity 0.5, GM Multiply),
    layers /on and /off (the off one is OFF in the default view), image /Im (1x1 grey, no mask)."""
    stream = content.encode("latin-1")
    page = (f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {width} 100] "
            + (f"/CropBox [{' '.join(map(str, crop))}] " if crop else "")
            + "/Resources << /Font << /F1 7 0 R >> "
              "/ExtGState << /GS0 << /ca 0 /CA 0 >> /GH << /ca 0.5 /CA 0.5 >> /GM << /BM /Multiply >> >> "
              "/Properties << /on 5 0 R /off 6 0 R >> /XObject << /Im 8 0 R >> >> /Contents 4 0 R >>")
    objs = [
        "<< /Type /Catalog /Pages 2 0 R /OCProperties << /OCGs [5 0 R 6 0 R] /D << /ON [5 0 R] /OFF [6 0 R] >> >> >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        page,
        None,                                                   # the content stream
        "<< /Type /OCG /Name (on) >>",
        "<< /Type /OCG /Name (off) >>",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
        None,                                                   # the image
    ]
    out, offsets = bytearray(b"%PDF-1.7\n"), []
    for n, o in enumerate(objs, 1):
        offsets.append(len(out))
        if n == 4:
            body = b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream"
        elif n == 8:
            body = (b"<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB "
                    b"/BitsPerComponent 8 /Length 3 >>\nstream\n\x80\x80\x80\nendstream")
        else:
            body = o.encode("latin-1")
        out += b"%d 0 obj\n" % n + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objs) + 1)
    out += b"".join(b"%010d 00000 n \n" % o for o in offsets)
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objs) + 1, xref)
    return bytes(out)


def _text(s: str, x: float = 20, y: float = 50, pre: str = "") -> str:
    return f"BT /F1 12 Tf {pre} {x} {y} Td ({s}) Tj ET\n"


def _verdict(tmp_path, content: str, **kw):
    """[(char, visible, struck, hidden reasons)] for the page's pdfplumber chars, in content order."""
    return _judged(tmp_path, content, **kw)[0]


def _notes(tmp_path, content: str, **kw):
    """The page's notes: what the method kept because it could not be sure it is hidden."""
    return _judged(tmp_path, content, **kw)[1]


def _judged(tmp_path, content: str, **kw):
    p = tmp_path / "t.pdf"
    p.write_bytes(_pdf(content, **kw))
    with pdfplumber.open(p) as pdf:
        page = pdf.pages[0]
        v = V.page_verdict(page)
        return ([(c["text"], i in v.visible, i in v.struck, v.hidden_reasons.get(i, []))
                 for i, c in enumerate(page.chars)], v.notes)


def _seen(rows) -> str:
    return "".join(t for t, vis, _s, _r in rows if vis)


def test_plain_text_is_visible(tmp_path):
    assert _seen(_verdict(tmp_path, _text("AB"))) == "AB"


def test_invisible_render_mode_hides(tmp_path):
    rows = _verdict(tmp_path, "q " + _text("AB", pre="3 Tr") + "Q " + _text("C", x=60))
    assert _seen(rows) == "C" and rows[0][3] == ["mode"]


def test_zero_opacity_hides(tmp_path):
    rows = _verdict(tmp_path, "q /GS0 gs " + _text("AB") + "Q " + _text("C", x=60))
    assert _seen(rows) == "C" and "alpha" in rows[0][3]


def test_a_layer_that_is_off_hides(tmp_path):
    rows = _verdict(tmp_path, "/OC /off BDC " + _text("AB") + "EMC /OC /on BDC " + _text("C", x=60) + "EMC")
    assert _seen(rows) == "C" and rows[0][3] == ["layer"]
    # a fill in a layer that is off covers nothing
    assert _seen(_verdict(tmp_path, _text("AB") + "/OC /off BDC 1 1 1 rg 0 30 100 40 re f EMC")) == "AB"


def test_outside_the_crop_box_hides(tmp_path):
    rows = _verdict(tmp_path, _text("AB") + _text("C", x=150), crop=(0, 0, 100, 100))
    assert _seen(rows) == "AB" and rows[2][3] == ["crop"]


def test_outside_the_clip_hides(tmp_path):
    rows = _verdict(tmp_path, "q 0 0 100 100 re W n " + _text("AB") + _text("C", x=150) + "Q")
    assert _seen(rows) == "AB" and rows[2][3] == ["clip"]


def test_covered_by_a_later_opaque_fill_hides(tmp_path):
    rows = _verdict(tmp_path, _text("AB") + "1 1 1 rg 0 30 100 40 re f 0 g " + _text("C", x=150))
    assert _seen(rows) == "C" and rows[0][3] == ["covered"]


def test_a_fill_painted_before_the_text_does_not_hide(tmp_path):
    assert _seen(_verdict(tmp_path, "1 1 0 rg 0 30 100 40 re f 0 g " + _text("AB"))) == "AB"


def test_covered_by_a_later_opaque_image_hides(tmp_path):
    rows = _verdict(tmp_path, _text("AB") + "q 100 0 0 100 0 0 cm /Im Do Q " + _text("C", x=150))
    assert _seen(rows) == "C" and rows[0][3] == ["image"]


def test_a_multiply_fill_over_text_does_not_hide(tmp_path):
    assert _seen(_verdict(tmp_path, _text("AB") + "q /GM gs 1 1 0 rg 0 30 100 40 re f Q")) == "AB"


def test_a_translucent_fill_over_text_does_not_hide(tmp_path):
    assert _seen(_verdict(tmp_path, _text("AB") + "q /GH gs 1 1 1 rg 0 30 100 40 re f Q")) == "AB"


def test_two_fills_meeting_under_one_glyph_cover_it_together(tmp_path):
    # 'W' spans about x 20-31: the two fills meet at x 26, under it; each alone covers only half of it
    rows = _verdict(tmp_path, _text("W") + "1 1 1 rg 0 30 26 40 re f 26 30 74 40 re f 0 g " + _text("C", x=150))
    assert _seen(rows) == "C" and rows[0][3] == ["covered"]
    assert _seen(_verdict(tmp_path, _text("W") + "1 1 1 rg 26 30 74 40 re f")) == "W"


def test_the_same_glyph_drawn_again_shows_once(tmp_path):
    rows = _verdict(tmp_path, "1 g " + _text("AB") + "0 g " + _text("AB"))   # white copy first, black on top
    assert _seen(rows) == "AB" and [r[3] for r in rows[:2]] == [["overdrawn"], ["overdrawn"]]


def test_a_doubled_space_shows_once(tmp_path):
    rows = _verdict(tmp_path, _text("A B") + _text("A B"))
    assert _seen(rows) == "A B" and rows[1] == (" ", False, False, ["overdrawn"])


def test_the_same_colour_as_the_fill_under_it_hides(tmp_path):
    rows = _verdict(tmp_path, "0 0 1 rg 0 30 200 40 re f " + _text("AB") + "1 0 0 rg " + _text("C", x=150))
    assert _seen(rows) == "C" and rows[0][3] == ["same-colour"]


def test_white_text_on_the_bare_page_hides(tmp_path):
    rows = _verdict(tmp_path, "1 g " + _text("AB") + "0 g " + _text("C", x=150))
    assert _seen(rows) == "C" and rows[0][3] == ["same-colour"]


def test_a_strike_through_marks_only_the_glyphs_it_crosses(tmp_path):
    # 'Oct 15': '1' and '5' lie at about x 41.4-54.7; the line runs through their middle, 3.5 pt over the baseline
    rows = _verdict(tmp_path, _text("Oct 15") + "0.14 w 0 G 41.5 53.5 m 54.5 53.5 l S")
    assert [t for t, vis, struck, _ in rows if struck] == ["1", "5"] and _seen(rows) == "Oct 15"


def test_a_line_drawn_before_the_text_is_no_strike_but_is_named(tmp_path):
    """(review of #323) a line through the text's middle painted BEFORE the text lies under it: maybe a strike the text
    covers, maybe a rule. Not marked either way, and not silent: the page names it as a possible strike."""
    content = "0.14 w 0 G 41.5 53.5 m 54.5 53.5 l S " + _text("Oct 15")
    assert not any(s for _, _, s, _ in _verdict(tmp_path, content))
    assert _notes(tmp_path, content) == ["2 char(s) kept, possible-strike: '15'"]


def test_an_underline_is_no_strike(tmp_path):
    content = _text("Oct 15") + "0.14 w 0 G 20 48.5 m 54.5 48.5 l S"
    assert not any(s for _, _, s, _ in _verdict(tmp_path, content)) and _notes(tmp_path, content) == []


def test_a_long_strike_through_a_sentence_is_found(tmp_path):
    """No length cap (the 200 pt one missed a struck sentence): a rule across a column is kept out because most of
    its length does not lie on text, not by its length."""
    sentence = "No Fishing from the bridge upstream to the signs"
    rows = _verdict(tmp_path, _text(sentence, x=5) + "0.5 w 0 G 5 53.5 m 260 53.5 l S", width=300)
    struck = "".join(t for t, _, s, _ in rows if s)
    assert len(struck.replace(" ", "")) >= 35 and struck.startswith("NoFishing")
    rule = _verdict(tmp_path, _text("AB") + "0.5 w 0 G 0 53.5 m 199 53.5 l S")         # a rule through two letters
    assert not any(s for _, _, s, _ in rule)


# ---------------------------------------------------------------- what the method cannot be sure of: kept, named
# (review of #323) a rule may hide a glyph only when it is sure; otherwise the glyph stays and the page says why.

def test_an_image_clipped_away_from_the_text_hides_nothing(tmp_path):
    content = _text("AB") + "q 100 0 100 100 re W n q 200 0 0 100 0 0 cm /Im Do Q Q"     # image clipped to the right
    assert _seen(_verdict(tmp_path, content)) == "AB" and _notes(tmp_path, content) == []


def test_an_image_clipped_by_a_rectangle_over_the_text_still_hides_it(tmp_path):
    rows = _verdict(tmp_path, _text("AB") + "q 0 0 100 100 re W n q 200 0 0 100 0 0 cm /Im Do Q Q 0 g " + _text("C", x=150))
    assert _seen(rows) == "C" and rows[0][3] == ["image"]


def test_an_image_under_a_triangle_clip_hides_nothing_and_says_so(tmp_path):
    content = _text("AB") + "q 0 0 m 200 0 l 200 100 l h W n q 200 0 0 100 0 0 cm /Im Do Q Q"
    assert _seen(_verdict(tmp_path, content)) == "AB"
    assert _notes(tmp_path, content) == ["2 char(s) kept, image-under-a-clip-path: 'AB'"]


def test_a_fill_under_a_triangle_clip_does_not_hide_text_outside_it(tmp_path):
    # the triangle (0,0)-(200,0)-(200,100) lies below the text at x 20, y 50: the red fill paints nothing there
    content = _text("AB") + "q 0 0 m 200 0 l 200 100 l h W n 1 0 0 rg 0 0 200 100 re f Q"
    assert _seen(_verdict(tmp_path, content)) == "AB"
    assert _notes(tmp_path, content) == ["2 char(s) kept, fill-under-a-clip-path: 'AB'"]


def test_an_even_odd_clip_with_a_hole_over_the_text_does_not_hide_it(tmp_path):
    content = _text("AB") + "q 0 0 200 100 re 15 40 30 25 re W* n 1 0 0 rg 0 0 200 100 re f Q"
    assert _seen(_verdict(tmp_path, content)) == "AB"
    assert _notes(tmp_path, content) == ["2 char(s) kept, fill-under-a-clip-path: 'AB'"]


def test_a_fill_under_a_rectangle_clip_still_hides_what_it_covers(tmp_path):
    rows = _verdict(tmp_path, _text("AB") + "q 0 0 100 100 re W n 1 1 1 rg 0 0 200 100 re f Q 0 g " + _text("C", x=150))
    assert _seen(rows) == "C" and rows[0][3] == ["covered"]


@pytest.mark.parametrize("name,content", [
    ("black text on a 50 % black fill", "q /GH gs 0 g 0 30 200 40 re f Q " + _text("AB", pre="0 g")),
    ("0.5 grey text on a Multiply 0.5 fill over 0.5", ".5 g 0 30 200 40 re f q /GM gs .5 g 0 30 200 40 re f Q "
                                                       + _text("AB", pre=".5 g")),
    ("white text on a 50 % white fill over black", "0 g 0 30 200 40 re f q /GH gs 1 g 0 30 200 40 re f Q "
                                                   + _text("AB", pre="1 g")),
])
def test_the_colour_of_a_translucent_or_blended_fill_is_no_background(tmp_path, name, content):
    """Only an opaque Normal fill is its own colour on the page; text of the colour of a translucent or blended
    one may well be seen (a mix shows under it), so it stays and is named."""
    assert _seen(_verdict(tmp_path, content)) == "AB", name
    assert _notes(tmp_path, content) == ["2 char(s) kept, same-colour-as-a-translucent-or-blended-fill: 'AB'"], name


def test_two_text_streams_that_differ_refuse_the_page():
    g = [V.Glyph(0, "A", 1, (20, 50), (20, 40, 28, 52), 12, "Helvetica", (0,), 0, 1.0, "")]
    with pytest.raises(V.PageNotJudged, match="PDF page 7"):
        V.pair(g, [{"text": "B"}], 100, 7)
    with pytest.raises(V.PageNotJudged, match="PDF page 7"):
        V.pair(g, [{"text": "A"}, {"text": "B"}], 100, 7)


# ---------------------------------------------------------------- how the extractor carries a strike

def _chars(text: str, struck: str, x: float = 20.0):
    out = []
    for i, (t, s) in enumerate(zip(text, struck)):
        out.append({"text": t, "x0": x + 6 * i, "x1": x + 6 * i + 5, "width": 5, "top": 40, "bottom": 52,
                    "size": 12, "fontname": "Helvetica", "struck": s == "x"})
    return out


@pytest.mark.parametrize("text,struck,want", [
    ("Oct 15", "....xx", "Oct ~~15~~"),
    ("Oct 15 x", "....xx..", "Oct ~~15~~ x"),
    ("1 2 3", "xxxxx", "~~1 2 3~~"),
    ("Oct 15", "......", "Oct 15"),
])
def test_a_struck_run_is_marked_in_the_marked_text_only(tmp_path, text, struck, want):
    p = FishingSynopsisParser(output_dir=str(tmp_path))
    line = _chars(text, struck)
    assert p._process_line_with_bold(line, mark_struck=True) == want
    assert p._process_line_with_bold(line) == text


def test_the_marked_text_is_raw_regs_plus_the_marks_or_refused(tmp_path):
    p = FishingSynopsisParser(output_dir=str(tmp_path))
    assert p._marked_regs(19, "A RIVER 1-5", "Sept 1-Oct ~~15~~", [], "Sept 1-Oct 15") == "Sept 1-Oct ~~15~~"
    assert p._marked_regs(19, "A RIVER 1-5", None, [], "Sept 1-Oct 15") is None
    with pytest.raises(ValueError, match="PDF page 19"):
        p._marked_regs(19, "A RIVER 1-5", "Sept 1-Oct ~~16~~", [], "Sept 1-Oct 15")


def test_a_row_without_a_strike_serialises_as_before():
    r = WaterbodyRow(water="A", mu=["1-1"], raw_regs="x", symbols=[], page=1, image="i.png")
    assert "raw_regs_marked" not in r.to_dict()
    assert WaterbodyRow.from_dict(r.to_dict()) == r
    m = WaterbodyRow(water="A", mu=["1-1"], raw_regs="Oct 15", symbols=[], page=1, image="i.png",
                     raw_regs_marked="Oct ~~15~~")
    assert m.to_dict()["raw_regs_marked"] == "Oct ~~15~~" and WaterbodyRow.from_dict(m.to_dict()) == m


def test_one_warning_line_names_every_strike_and_flags_only_new_ones():
    rows = [{"water": "NANAIMO RIVER", "pdf_page": 19, "page": 17, "raw_regs_marked": "Sept 1-Oct ~~15~~"},
            {"water": "B CREEK", "pdf_page": 30, "page": 28, "raw_regs_marked": "~~Bait ban~~, Jan 1"},
            {"water": "C LAKE", "pdf_page": 30, "page": 28}]
    assert [i["text"] for i in strike_items(rows)] == ["Oct ~~15~~", "~~Bait ban~~"]
    line, new = strike_report(rows, [{"water": "NANAIMO RIVER", "text": "Oct ~~15~~"}])
    assert "\n" not in line and line.startswith("[struck] 2 struck-through item(s)")
    assert re.search(r"NANAIMO RIVER PDF p\.19 'Oct ~~15~~'(;|$)", line)          # known: not flagged
    assert "B CREEK PDF p.30 '~~Bait ban~~' NEW" in line and [n["water"] for n in new] == ["B CREEK"]
    assert strike_report([rows[2]], []) == (None, [])


def test_the_extractor_command_starts():
    """T1190: a local import of get_config inside main() made main()'s first line raise UnboundLocalError, so
    `python -m pipeline.regs.extraction.extract_synopsis` (and its --check-against gate) could not start at all."""
    from pipeline.regs.extraction.extract_synopsis import main
    with pytest.raises(SystemExit) as e:
        main(["--help"])
    assert e.value.code == 0
