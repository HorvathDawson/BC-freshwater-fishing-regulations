"""WHERE EACH SYNOPSIS ROW IS ON ITS PAGE — the `row_boxes.json` sidecar the review tool reads (T134).

A one-off helper beside the extractor. No LLM, no credits, no network: it re-runs the extractor's
own `FishingSynopsisParser.extract_rows` over the pages that hold table rows and, where the
extractor would SAVE A ROW CROP, records the crop's box instead (`save_row_crop` is swapped for a
recorder — the row numbering, the table bounds and the column divider are the extractor's, never
re-derived). Nothing else is written: not `synopsis_raw_data.json`, not `row_images/`.

    python -m pipeline.regs.extraction.row_boxes            # -> <generated>/regs/extraction/row_boxes.json
    python -m pipeline.regs.extraction.row_boxes --out X     # anywhere else

Why: the row crop the extractor saved is the whole table width (~530 pt) at 400 dpi, 2,855 px
wide. Shown in the review pane it is scaled to ~24 % and cannot be read (PLAN §1). With the box in
PDF points the tool renders the row at a legible scale, the name column above the regulation
column, and marks the row on its whole printed page.

The sidecar is keyed by the row's image key (`page_016_row_000.png`, the `image` every
`synopsis_raw_data.json` row carries) and states which extraction it belongs to
(`raw_data_sha256`): the tool refuses a sidecar cut from another extraction. A page that no longer
yields the rows the extraction recorded (same key, same water) is REFUSED here, naming the row —
a box for another row is worse than no box.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

#: the sidecar's shape version — the review tool refuses another
VERSION = 1
#: the extractor's own crop margin around a row, in points (`save_row_crop`)
MARGIN_PT = 5.0


def default_out() -> Path:
    from pipeline.common.curated import GENERATED
    return GENERATED.regs.extraction / "row_boxes.json"


def raw_data_path() -> Path:
    from pipeline.common.curated import GENERATED
    return GENERATED.regs.extraction / "synopsis_raw_data.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _page_boxes(args) -> list[dict]:
    """Run the extractor on one PDF page; return one box per row it would crop, in its order."""
    pdf_path, pdf_page = args
    import pdfplumber
    from pipeline.regs.extraction.extract_synopsis import FishingSynopsisParser
    got: list[dict] = []
    with tempfile.TemporaryDirectory() as tmp:
        parser = FishingSynopsisParser(output_dir=tmp)

        def record(page, page_num, row_index, section, x0, x1, div_x):
            key = f"page_{page_num:03d}_row_{row_index:03d}.png"
            got.append({"image": key, "pdf_page": page_num,
                        "box": [round(float(x0), 2), round(float(section["y0"]), 2),
                                round(float(x1), 2), round(float(section["y1"]), 2)],
                        "div_x": round(float(div_x), 2),
                        "page_size": [round(float(page.width), 2), round(float(page.height), 2)]})
            return key

        parser.save_row_crop = record                       # the one thing changed: record, not save
        with pdfplumber.open(pdf_path) as pdf:
            result = parser.extract_rows(pdf.pages[pdf_page - 1], page_num=pdf_page)
    by_key = {b["image"]: b for b in got}
    out = []
    for row in result.rows:
        b = dict(by_key[row.image])
        b.update(printed=row.page, water=row.water, raw_regs=row.raw_regs)
        out.append(b)
    return out


def check_against(rows: list[dict], boxes: dict[str, dict]) -> tuple[list[str], list[str]]:
    """Every row of the extraction must have its box, from the same row: same key, same water.
    Returns (refusals, text drift). The regulation TEXT is compared too but only reported: the
    extractor's character order is not stable run to run (page 18's Heber River reads "Folpye
    fnishing" on one run and "Fly fishing" on another) — a reordered glyph does not move the row."""
    bad, drift = [], []
    for r in rows:
        b = boxes.get(r.get("image"))
        if b is None:
            bad.append(f"{r.get('image')}: no box (the extractor no longer yields this row)")
        elif b["water"] != r.get("water"):
            bad.append(f"{r.get('image')}: the extractor now reads {b['water']!r} here, the "
                       f"extraction recorded {r.get('water')!r}")
        elif b["raw_regs"] != r.get("raw_regs"):
            drift.append(r["image"])
    return bad, drift


def build(pdf_path: Path, raw_path: Path, workers: int = 4) -> dict:
    pages = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    rows = [r for p in pages for r in p.get("rows") or [] if r.get("image")]
    pdf_pages = sorted({int(r["pdf_page"]) for r in rows})
    with ProcessPoolExecutor(max_workers=max(1, min(workers, 4))) as ex:
        found = [b for page in ex.map(_page_boxes, [(str(pdf_path), n) for n in pdf_pages])
                 for b in page]
    boxes = {b["image"]: {k: v for k, v in b.items() if k != "image"} for b in found}
    bad, drift = check_against(rows, boxes)
    if bad:
        raise SystemExit("row_boxes: the extractor does not reproduce the extraction on disk — "
                         "re-run the extraction first:\n  " + "\n  ".join(bad[:40]))
    keep = {r["image"] for r in rows}
    return {"version": VERSION, "margin_pt": MARGIN_PT,
            "pdf_sha256": sha256(pdf_path), "raw_data_sha256": sha256(raw_path),
            "text_drift": drift,
            "rows": {k: {x: v for x, v in boxes[k].items() if x != "raw_regs"}
                     for k in sorted(keep)}}


def main(argv=None) -> int:
    from pipeline.common.synopsis_pages import default_pdf
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args(argv)
    out = args.out or default_out()
    doc = build(default_pdf(), raw_data_path(), args.workers)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, out)
    drift = doc["text_drift"]
    print(f"row_boxes: {len(doc['rows'])} rows -> {out}"
          + (f" ({len(drift)} read in another glyph order this run: {', '.join(drift)})"
             if drift else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
