"""T354 — the shipped synopsis: big JPEGs downsampled (nothing else touched, deterministic, render-gated) and the page
index the app's append needs (the append itself: app/packages/ui-native/src/regs/appendPdf.test.ts)."""
import io

import pikepdf
import pytest
from PIL import Image

from pipeline.deliver.release import synopsis as SYN


def _book(path, px=3200):
    pdf = pikepdf.new()
    pdf.add_blank_page(page_size=(576, 756))
    b = io.BytesIO()
    Image.new("RGB", (px, px // 2), (40, 120, 200)).save(b, "JPEG", quality=90)
    im = pikepdf.Stream(pdf, b.getvalue(), Type=pikepdf.Name.XObject, Subtype=pikepdf.Name.Image, Width=px,
                        Height=px // 2, ColorSpace=pikepdf.Name.DeviceRGB, BitsPerComponent=8, Filter=pikepdf.Name.DCTDecode)
    page = pdf.pages[0]
    page.obj.Resources = pikepdf.Dictionary(XObject=pikepdf.Dictionary(Im0=im))
    page.obj.Contents = pdf.make_stream(b"q 576 0 0 288 0 400 cm /Im0 Do Q BT /F1 12 Tf ET")
    pdf.add_blank_page(page_size=(576, 756))
    pdf.save(path)
    return path


def test_a_big_jpeg_is_downsampled_and_the_copy_is_deterministic(tmp_path):
    src = _book(tmp_path / "book.pdf")
    a = SYN.shipped(src, cache=tmp_path / "a")
    b = SYN.shipped(src, cache=tmp_path / "b")
    assert a.read_bytes() == b.read_bytes()
    assert a.stat().st_size < src.stat().st_size
    shipped = pikepdf.open(a)
    im = next(o for o in shipped.objects if isinstance(o, pikepdf.Stream) and o.get("/Subtype") == "/Image")
    assert (int(im.Width), int(im.Height)) == (SYN.CAP, SYN.CAP // 2)
    assert SYN.render_gate(src, a)["ok"]


def test_the_index_names_what_the_append_needs(tmp_path):
    out = SYN.shipped(_book(tmp_path / "book.pdf"), cache=tmp_path / "c")
    ix = SYN.append_index(out)
    pdf = pikepdf.open(out)
    assert ix["v"] == SYN.INDEX_V and ix["count"] == 2 and len(ix["page"]) == 2
    assert ix["pages"] == list(pdf.Root.Pages.objgen) and ix["root"] == list(pdf.trailer.Root.objgen)
    assert out.read_bytes()[ix["startxref"]:].lstrip()[:1].isdigit()           # an xref stream object starts there
    # shared boxes said once; every page's resources a ref (made indirect), so each entry is a few refs
    assert "/MediaBox" in ix["box"] and all("/Resources" in p and " R" in p for p in ix["page"])


def test_a_copy_that_renders_differently_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(SYN, "GATE_MEAN", -1.0)
    with pytest.raises(ValueError, match="render gate"):
        SYN.shipped(_book(tmp_path / "book.pdf"), cache=tmp_path / "d")
    assert not list((tmp_path / "d").glob("*.pdf"))
