"""Chart PDFs -> 200 dpi 1-bit PDFs (T634, owner 2026-11-11: "Yes, 200 dpi B&W").

Every chart of the OGL open table (`Bathymetric Maps - Open`, `wsa_bathymetry_maps.csv` `MAP_IMAGE_FILENAME_PDF`) is
rendered page by page with pdfium at 200 dpi in grey, cut at THRESHOLD (darker = ink) and stored as ONE 1-bit CCITT G4
image per page, drawn over a page of exactly the original's shown size (crop box, rotation applied). Bilevel, grey and
colour scans and vector-only charts all go the same way; the typed disclaimer is kept as image (no longer selectable).

    scripts/heavy.sh pdf200 50 -- python -m pipeline.bathymetry.compress            # the whole open set, 4 workers
    python -m pipeline.bathymetry.compress --only 00005801.PDF --out /tmp/x            # one chart

Rules:
  * the source PDFs (`SOURCE/bathymetry_pdfs`) are only read, never written;
  * output goes OUTSIDE git (default `~/data-offgit/bc-bathymetry-pdf-200dpi/`), one `<name>.pdf` per chart plus
    `manifest.json` (per chart: bytes in/out, pages, page sizes, the original's image kind, status);
  * deterministic: same input, same bytes (no clocks, pikepdf `deterministic_id`);
  * every chart is checked after writing: page count and every page's size (to 0.01 pt) equal the original's;
  * a chart that is missing, broken or fails the check is a row with its reason AND a non-zero exit — never skipped
    quietly.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

DPI = 200
THRESHOLD = 160          # grey level 0..255: below = black. 150 dpi closed the smallest soundings (first pass)
SIZE_TOL_PT = 0.01
MAX_WORKERS = 4
# Pillow refuses images over ~179 M pixels as a "decompression bomb". Every image here is our OWN render of a page
# (00170001 at 200 dpi is 268 M pixels), so the bound is raised to a stated size, never switched off.
MAX_PIXELS = 600_000_000


def default_out() -> Path:
    return Path.home() / "data-offgit" / "bc-bathymetry-pdf-200dpi"


# ---- one chart ------------------------------------------------------------------------------------------------------

def image_kind(src: Path) -> str:
    """What the original holds: bilevel | gray | colour | vector (no image) | mixed. Evidence for sampling only."""
    import pikepdf
    kinds = set()
    with pikepdf.open(src) as pdf:
        for page in pdf.pages:
            for _, raw in page.get_images().items():
                if raw.get("/ImageMask", False) or int(raw.get("/BitsPerComponent", 8)) == 1:
                    kinds.add("bilevel")
                    continue
                cs = raw.get("/ColorSpace")
                n = _components(cs)
                kinds.add("gray" if n == 1 else "colour")
    if not kinds:
        return "vector"
    return kinds.pop() if len(kinds) == 1 else "mixed"


def _components(cs) -> int:
    import pikepdf
    if cs is None:
        return 1
    if isinstance(cs, pikepdf.Name):
        return {"/DeviceGray": 1, "/CalGray": 1, "/DeviceCMYK": 4}.get(str(cs), 3)
    if isinstance(cs, pikepdf.Array) and len(cs):
        head = str(cs[0])
        if head == "/ICCBased":
            return int(cs[1].get("/N", 3))
        if head == "/Indexed":
            return _components(cs[1])
        if head in ("/CalGray",):
            return 1
        if head == "/Separation":
            return 1
    return 3


def to_bilevel(gray, threshold: int = THRESHOLD):
    """An 8-bit grey PIL image -> mode "1": below `threshold` black, the rest white, no dithering."""
    from PIL import Image
    lut = [0 if v < threshold else 255 for v in range(256)]
    return gray.point(lut).convert("1", dither=Image.Dither.NONE)


def g4(bw) -> bytes:
    """A mode "1" image as one CCITT G4 strip (Pillow's libtiff writer; PIL "1" stores 0 = black)."""
    from PIL import Image
    buf = io.BytesIO()
    bw.save(buf, "TIFF", compression="group4", strip_size=1 << 31)
    t = Image.open(io.BytesIO(buf.getvalue()))
    offs, counts = t.tag_v2[273], t.tag_v2[279]
    if len(offs) != 1:
        raise RuntimeError(f"G4 writer gave {len(offs)} strips, want 1")
    photometric = t.tag_v2.get(262)
    if photometric != 1:
        raise RuntimeError(f"G4 writer photometric {photometric}, want 1 (min-is-black)")
    data = buf.getvalue()
    return data[offs[0]:offs[0] + counts[0]]


def page_sizes(path: Path) -> List[Tuple[float, float]]:
    import pypdfium2 as pdfium
    doc = pdfium.PdfDocument(str(path))
    try:
        return [tuple(round(v, 3) for v in doc[i].get_size()) for i in range(len(doc))]
    finally:
        doc.close()


def compress_one(src: Path, dst: Path, dpi: int = DPI, threshold: int = THRESHOLD) -> dict:
    """Write `dst` from `src`; returns the manifest row (raises on any failure)."""
    import pikepdf
    import pypdfium2 as pdfium
    from PIL import Image

    Image.MAX_IMAGE_PIXELS = MAX_PIXELS
    with open(src, "rb") as fh:
        if not fh.read(1024).lstrip().startswith(b"%PDF"):
            raise RuntimeError(f"broken source: not a PDF ({src.stat().st_size} bytes)")
    kind = image_kind(src)
    out = pikepdf.new()
    sizes = []
    doc = pdfium.PdfDocument(str(src))
    try:
        for i in range(len(doc)):
            page = doc[i]
            w, h = page.get_size()
            sizes.append((round(w, 3), round(h, 3)))
            gray = page.render(scale=dpi / 72.0, grayscale=True).to_pil().convert("L")
            bw = to_bilevel(gray, threshold)
            pw, ph = bw.size
            img = pikepdf.Stream(out, g4(bw))
            img["/Type"] = pikepdf.Name.XObject
            img["/Subtype"] = pikepdf.Name.Image
            img["/Width"], img["/Height"] = pw, ph
            img["/ColorSpace"] = pikepdf.Name.DeviceGray
            img["/BitsPerComponent"] = 1
            img["/Filter"] = pikepdf.Name.CCITTFaxDecode
            img["/DecodeParms"] = pikepdf.Dictionary(K=-1, Columns=pw, Rows=ph, BlackIs1=True)
            content = pikepdf.Stream(out, f"q {w:.4f} 0 0 {h:.4f} 0 0 cm /Im0 Do Q".encode())
            out.pages.append(pikepdf.Page(pikepdf.Dictionary(
                Type=pikepdf.Name.Page, MediaBox=[0, 0, round(w, 4), round(h, 4)],
                Resources=pikepdf.Dictionary(XObject=pikepdf.Dictionary(Im0=img)), Contents=content)))
    finally:
        doc.close()
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    out.save(tmp, deterministic_id=True, compress_streams=True,
             object_stream_mode=pikepdf.ObjectStreamMode.generate)
    out.close()
    got = page_sizes(tmp)
    if len(got) != len(sizes):
        tmp.unlink()
        raise RuntimeError(f"page count {len(got)} != {len(sizes)}")
    for k, (a, b) in enumerate(zip(sizes, got)):
        if abs(a[0] - b[0]) > SIZE_TOL_PT or abs(a[1] - b[1]) > SIZE_TOL_PT:
            tmp.unlink()
            raise RuntimeError(f"page {k + 1} size {b} != {a}")
    os.replace(tmp, dst)
    return {"kind": kind, "pages": len(sizes), "sizes_pt": [list(s) for s in sizes],
            "in_bytes": src.stat().st_size, "out_bytes": dst.stat().st_size, "status": "ok"}


# ---- the set --------------------------------------------------------------------------------------------------------

def open_set(open_csv: Path) -> List[str]:
    """The OGL open table's PDF names, as listed (sorted, unique)."""
    with open(open_csv, encoding="utf-8-sig") as fh:
        return sorted({(r["MAP_IMAGE_FILENAME_PDF"] or "").strip() for r in csv.DictReader(fh)} - {""})


def _job(args) -> dict:
    name, src, dst, dpi, threshold = args
    row = {"name": name}
    if src is None:
        row.update(status="missing", error="not in the local source folder")
        return row
    row["source"] = src.name
    try:
        row.update(compress_one(src, dst, dpi, threshold))
    except Exception as e:  # reported as a row AND by the exit code, never dropped
        row.update(status="failed", error=f"{type(e).__name__}: {e}", in_bytes=src.stat().st_size)
    return row


def run(names: Sequence[str], src_dir: Path, out_dir: Path, workers: int, dpi: int = DPI,
        threshold: int = THRESHOLD) -> dict:
    from multiprocessing import Pool
    local: Dict[str, Path] = {p.name.lower(): p for p in src_dir.iterdir() if p.is_file()}
    jobs = [(n, local.get(n.lower()), out_dir / (Path(n).stem + ".pdf"), dpi, threshold) for n in names]
    if workers <= 1:
        rows = [_job(j) for j in jobs]
    else:
        with Pool(min(workers, MAX_WORKERS)) as pool:
            rows = list(pool.imap(_job, jobs, chunksize=4))
    rows.sort(key=lambda r: r["name"])
    ok = [r for r in rows if r["status"] == "ok"]
    summary = {
        "charts_in": len(rows), "charts_out": len(ok),
        "status": dict(sorted(Counter(r["status"] for r in rows).items())),
        "mb_in_ok": round(sum(r["in_bytes"] for r in ok) / 1e6, 1),
        "mb_out": round(sum(r["out_bytes"] for r in ok) / 1e6, 1),
        "mb_in_all": round(sum(r.get("in_bytes", 0) for r in rows) / 1e6, 1),
        "by_kind": {k: {"charts": sum(1 for r in ok if r["kind"] == k),
                        "mb_in": round(sum(r["in_bytes"] for r in ok if r["kind"] == k) / 1e6, 1),
                        "mb_out": round(sum(r["out_bytes"] for r in ok if r["kind"] == k) / 1e6, 1)}
                    for k in sorted({r["kind"] for r in ok})},
        "grew": sorted(r["name"] for r in ok if r["out_bytes"] > r["in_bytes"]),
        "not_ok": [{"name": r["name"], "status": r["status"], "error": r.get("error")} for r in rows
                   if r["status"] != "ok"],
        "pages_and_sizes_checked": len(ok),
    }
    return {"_about": "T634 chart PDFs at 200 dpi 1-bit (pipeline.bathymetry.compress). Source PDFs untouched.",
            "settings": {"dpi": dpi, "threshold": threshold, "encoding": "CCITT G4, one image per page",
                         "page_size_tolerance_pt": SIZE_TOL_PT},
            "summary": summary, "charts": rows}


def main(argv=None) -> int:
    from pipeline.common.curated import SOURCE

    ap = argparse.ArgumentParser(description="chart PDFs -> 200 dpi 1-bit (T634)")
    ap.add_argument("--out", type=Path, default=default_out(), help="output folder, outside git")
    ap.add_argument("--workers", type=int, default=MAX_WORKERS)
    ap.add_argument("--only", nargs="*", help="these open-table names only (a manifest is still written)")
    a = ap.parse_args(argv)
    if a.workers > MAX_WORKERS:
        raise SystemExit(f"--workers {a.workers}: at most {MAX_WORKERS}")
    src_dir = Path(SOURCE / "bathymetry_pdfs")
    out_dir = a.out.expanduser().resolve()
    repo = Path(__file__).resolve().parents[2]
    if out_dir == repo or repo in out_dir.parents:
        raise SystemExit(f"{out_dir}: output must be outside the repository")
    names = open_set(Path(SOURCE / "wsa_bathymetry_maps.csv"))
    if a.only:
        want = {n.lower() for n in a.only}
        names = [n for n in names if n.lower() in want]
        if len(names) != len(want):
            raise SystemExit(f"--only: {sorted(want - {n.lower() for n in names})} not in the open table")
    res = run(names, src_dir, out_dir, a.workers)
    out_dir.mkdir(parents=True, exist_ok=True)
    man = out_dir / ("manifest.json" if not a.only else "manifest-only.json")
    tmp = man.with_name(man.name + ".tmp")
    tmp.write_text(json.dumps(res, indent=1, sort_keys=True) + "\n")
    os.replace(tmp, man)
    print(json.dumps(res["summary"], indent=1))
    print(f"wrote {out_dir} ({res['summary']['charts_out']} charts) and {man.name}")
    return 0 if not res["summary"]["not_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
