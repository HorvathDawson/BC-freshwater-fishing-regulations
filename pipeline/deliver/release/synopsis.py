"""T354 — the synopsis PDF the release ships, made cheap to open on a phone.

Two things, both done once here so the phone does neither:

1. **Smaller.** 18 of the book's 24 MB are a handful of photos scanned at ~330 dpi. Every JPEG whose long side passes
   `CAP` px (150 dpi across a full 10.5 in page) is downsampled to it; text, vector art, content streams and the page
   boxes are untouched, so the printed page numbers and every `book_box` still hold. Deterministic (the release id is a
   content digest): the same source gives the same bytes. `render_gate` compares every page in pdfium, old vs new.
2. **Appendable.** The app opens the book with the cited page first and highlighted by APPENDING a few KB (a PDF
   incremental update: new objects + an xref stream with /Prev) to a native copy of the held file, never reading the
   24 MB into JS (the T351/T360 heap spike). What that append needs from the file is `append_index`, shipped in the
   manifest's `synopsis.append`. Page /Resources and /Group are made indirect here so each page's entry is a few refs.

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
#: the page keys the front copy repeats (Parent/Annots are the append's own; structure/thumbs stay with the original)
PAGE_KEYS = ("/MediaBox", "/CropBox", "/Rotate", "/Resources", "/Contents", "/Group")
#: the page index's format; the app refuses one it does not know and opens the book the old way
INDEX_V = 1
#: render gate: worst page's mean per-channel difference (0-255), pdfium at 72 dpi
GATE_MEAN = 4.0


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def shipped(src: Path, cache: Optional[Path] = None) -> Path:
    """The release's copy of `src`: downsampled + resources made indirect, cached by the source's digest."""
    from pipeline.common.curated import GENERATED
    cache = cache or (GENERATED.base / "synopsis")
    cache.mkdir(parents=True, exist_ok=True)
    out = cache / f"synopsis-{_sha(src)[:12]}-c{CAP}q{QUALITY}v{INDEX_V}.pdf"
    if not out.exists():
        part = out.with_suffix(".part")
        _shrink(src, part)
        gate = render_gate(src, part)
        if not gate["ok"]:
            part.unlink()
            raise ValueError(f"T354 render gate: the shipped synopsis differs from {src}: {gate}")
        part.replace(out)
    return out


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
        for k in ("/Resources", "/Group"):
            v = page.obj.get(k)
            if v is not None and not v.is_indirect:
                page.obj[k] = pdf.make_indirect(v)
    pdf.save(dst, compress_streams=True, object_stream_mode=pikepdf.ObjectStreamMode.generate,
             deterministic_id=True, linearize=False)


def _syntax(v) -> str:
    import pikepdf
    return v.unparse(resolved=False).decode("latin-1") if isinstance(v, pikepdf.Object) else str(v)


def _ref(o) -> List[int]:
    n, g = o.objgen
    return [n, g]


def append_index(pdf_path: Path) -> dict:
    """What the app's append needs from `pdf_path`, so it reads nothing from the file:

        startxref  offset of the last xref (the append's /Prev)      size   the trailer /Size (first free number)
        root/info  the trailer's /Root and /Info refs                 id     the trailer /ID, hex
        pages      the root /Pages ref (the append overrides it)      kids   its /Kids, PDF syntax; count its /Count
        box        page keys every page shares, PDF syntax            page   per page, the rest of its PAGE_KEYS
    """
    import pikepdf
    raw = pdf_path.read_bytes()
    m = re.findall(rb"startxref\s+(\d+)\s+%%EOF\s*$", raw[-1024:])
    if not m:
        raise ValueError(f"{pdf_path}: no startxref at the end")
    pdf = pikepdf.open(pdf_path)
    tr = pdf.trailer
    root_pages = pdf.Root.Pages
    if set(root_pages.keys()) - {"/Type", "/Kids", "/Count", "/Parent"}:
        raise ValueError(f"{pdf_path}: the root /Pages carries inheritable keys {sorted(root_pages.keys())}")
    per = []
    for page in pdf.pages:
        o = page.obj
        if "/Parent" in o and o.Parent.objgen != root_pages.objgen:
            # inherited keys could hide on an intermediate node: the copy must state them itself
            for k in ("/Resources", "/MediaBox", "/CropBox", "/Rotate"):
                if k not in o and k in o.Parent:
                    raise ValueError(f"{pdf_path}: page {len(per) + 1} inherits {k}")
        per.append({k: _syntax(o[k]) for k in PAGE_KEYS if k in o})
    shared = {k: per[0][k] for k in per[0] if all(p.get(k) == per[0][k] for p in per)}
    ids = [bytes(x).hex() for x in tr.ID] if "/ID" in tr else None
    return {
        "v": INDEX_V, "startxref": int(m[-1]), "size": int(tr.Size),
        "root": _ref(tr.Root), "info": _ref(tr.Info) if "/Info" in tr else None, "id": ids,
        "pages": _ref(root_pages), "kids": _syntax(root_pages.Kids), "count": int(root_pages.Count),
        "box": "".join(f"{k} {v}" for k, v in shared.items()),
        "page": ["".join(f"{k} {v}" for k, v in p.items() if k not in shared) for p in per],
    }


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
