"""T354 — the synopsis PDF the release ships, made cheap to open on a phone.

Two things, both done once here so the phone does neither:

1. **Smaller.** 18 of the book's 24 MB are a handful of photos scanned at ~330 dpi. Every JPEG whose long side passes
   `CAP` px (150 dpi across a full 10.5 in page) is downsampled to it; text, vector art, content streams and the page
   boxes are untouched, so the printed page numbers and every `book_box` still hold. Deterministic (the release id is a
   content digest): the same source gives the same bytes. `render_gate` compares every page in pdfium, old vs new.
2. **Appendable.** The app's viewer opens the book at the cited page with the row highlighted IN PLACE, by APPENDING a
   few hundred bytes (a PDF incremental update: the highlight annotations, the page's /Annots array again with them
   added, an xref stream with /Prev) to a native copy of the held file, never reading it into JS (the T351/T360 heap
   spike). Every page's /Annots is made an indirect array here (an empty one where it had none), so the append
   overrides that one array and never a page. What the append needs is `append_index`, in the manifest's
   `synopsis.append`.

    python -m pipeline.deliver.release.synopsis [SRC]    # build the shipped copy, print its size + index + the gate
"""
from __future__ import annotations

import hashlib
import io
import re
import sys
from pathlib import Path
from typing import List, Optional

#: long side, px, past which a JPEG is downsampled (150 dpi over the 10.5 in page)
CAP = 1600
#: JPEG quality of a downsampled image
QUALITY = 80
#: the page index's format; the app refuses one it does not know and opens the book the old way
INDEX_V = 2
#: render gate: worst page's mean per-channel difference (0-255), pdfium at 72 dpi
GATE_MEAN = 4.0


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def shipped(src: Path, cache: Optional[Path] = None) -> Path:
    """The release's copy of `src`: downsampled + every /Annots indirect, render-gated, cached by the source's digest."""
    from pipeline.common.curated import GENERATED
    cache = cache or (GENERATED.base / "synopsis")
    cache.mkdir(parents=True, exist_ok=True)
    out = cache / f"synopsis-{_sha(src)[:12]}-c{CAP}q{QUALITY}v{INDEX_V}-{_libs()}.pdf"
    if not out.exists():
        part = out.with_suffix(".part")
        _shrink(src, part)
        gate = render_gate(src, part)
        if not gate["ok"]:
            part.unlink()
            raise ValueError(f"T354 render gate: the shipped synopsis differs from {src}: {gate}")
        part.replace(out)
    return out


def _libs() -> str:
    """(T416) The libraries that write the copy's bytes (Pillow + its JPEG codec, pikepdf + qpdf), in the cache name: a
    copy cached under other versions is a different file. A fresh clone once kept one made before its versions were
    pinned and shipped other bytes than the Mac's from the same source."""
    import PIL
    import pikepdf
    from PIL import features
    said = f"{PIL.__version__}|{features.version('jpg')}|{pikepdf.__version__}|{pikepdf.__libqpdf_version__}"
    return hashlib.sha256(said.encode()).hexdigest()[:8]


def _shrink(src: Path, dst: Path) -> None:
    import pikepdf
    from PIL import Image
    pdf = pikepdf.open(src)
    for o in list(pdf.objects):
        if not (isinstance(o, pikepdf.Stream) and o.get("/Subtype") == "/Image"):
            continue
        f = o.get("/Filter")
        if isinstance(f, pikepdf.Array):
            f = f[0] if len(f) == 1 else None
        w, h = int(o.Width), int(o.Height)
        if f != "/DCTDecode" or max(w, h) <= CAP:
            continue
        im = Image.open(io.BytesIO(o.read_raw_bytes()))
        s = CAP / max(w, h)
        nw, nh = max(1, round(w * s)), max(1, round(h * s))
        b = io.BytesIO()
        im.resize((nw, nh), Image.LANCZOS).save(b, "JPEG", quality=QUALITY, optimize=True)
        o.write(b.getvalue(), filter=pikepdf.Name.DCTDecode)
        o.Width, o.Height = nw, nh
        if "/DecodeParms" in o:
            del o["/DecodeParms"]
    for page in pdf.pages:
        a = page.obj.get("/Annots")
        if a is None or not a.is_indirect:
            page.obj.Annots = pdf.make_indirect(a if a is not None else pikepdf.Array())
    pdf.save(dst, compress_streams=True, object_stream_mode=pikepdf.ObjectStreamMode.generate,
             deterministic_id=True, linearize=False)


def _ref(o) -> List[int]:
    n, g = o.objgen
    return [n, g]


def append_index(pdf_path: Path) -> dict:
    """What the app's append needs from `pdf_path`, so it reads nothing from the file:

        startxref  offset of the last xref (the append's /Prev)    size  the trailer /Size (first free number)
        root/info  the trailer's /Root and /Info refs               id    the trailer /ID, hex
        page       per page [page ref, its /Annots array ref, the refs already in it (PDF syntax, no brackets)]
    """
    import pikepdf
    raw = pdf_path.read_bytes()
    m = re.findall(rb"startxref\s+(\d+)\s+%%EOF\s*$", raw[-1024:])
    if not m:
        raise ValueError(f"{pdf_path}: no startxref at the end")
    pdf = pikepdf.open(pdf_path)
    tr = pdf.trailer
    per = []
    for i, page in enumerate(pdf.pages):
        a = page.obj.get("/Annots")
        if a is None or not a.is_indirect:
            raise ValueError(f"{pdf_path}: page {i + 1} has no indirect /Annots (not a `shipped` copy)")
        refs = " ".join(x.unparse(resolved=False).decode("latin-1") for x in a)
        per.append([*_ref(page.obj), *_ref(a), refs])
    ids = [bytes(x).hex() for x in tr.ID] if "/ID" in tr else None
    return {"v": INDEX_V, "startxref": int(m[-1]), "size": int(tr.Size),
            "root": _ref(tr.Root), "info": _ref(tr.Info) if "/Info" in tr else None, "id": ids, "page": per}


def render_gate(old: Path, new: Path, *, scale: float = 1.0) -> dict:
    """Every page of `new` against `old` in pdfium: same page count, same text, worst mean difference under GATE_MEAN."""
    import pypdfium2 as pdfium
    from PIL import ImageChops, ImageStat
    a, b = pdfium.PdfDocument(str(old)), pdfium.PdfDocument(str(new))
    if len(a) != len(b):
        return {"ok": False, "why": f"pages {len(a)} -> {len(b)}"}
    worst, text = (0.0, 0), []
    for i in range(len(a)):
        ia = a[i].render(scale=scale).to_pil().convert("RGB")
        ib = b[i].render(scale=scale).to_pil().convert("RGB")
        mean = sum(ImageStat.Stat(ImageChops.difference(ia, ib)).mean) / 3
        worst = max(worst, (mean, i + 1))
        if a[i].get_textpage().get_text_range() != b[i].get_textpage().get_text_range():
            text.append(i + 1)
    ok = not text and worst[0] <= GATE_MEAN
    return {"ok": ok, "pages": len(a), "worst_mean": round(worst[0], 2), "worst_page": worst[1], "text_differs": text}


def main(argv=None) -> int:
    import json
    from pipeline.common.synopsis_pages import default_pdf
    args = sys.argv[1:] if argv is None else argv
    src = Path(args[0]) if args else default_pdf()
    out = shipped(src)
    idx = append_index(out)
    gate = render_gate(src, out)
    print(json.dumps({"src": str(src), "src_bytes": src.stat().st_size, "out": str(out), "out_bytes": out.stat().st_size,
                      "index_bytes": len(json.dumps(idx, separators=(",", ":"))), "gate": gate}, indent=1))
    return 0 if gate["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
