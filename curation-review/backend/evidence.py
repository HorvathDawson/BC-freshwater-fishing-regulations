"""THE BOOK, LEGIBLE (T134, PLAN §4 phase 3 "Evidence"): where an entry is printed, and a crop of it
a reviewer can read.

  a water row   its box from the `row_boxes.json` sidecar (`pipeline.regs.extraction.row_boxes`,
                the extractor's own row geometry, no credits): the row rendered at a legible scale
                — the WATER column above the REGULATION column, so the crop is ~370 pt wide, not
                the 530 pt table row the old crop scaled to 24 % — and its box marked on the page
  a zone entry  its passage FOUND BY TEXT on its printed chapter page(s): the page's words in the
                PDF's reading order, aligned with the entry's own `regs_verbatim`
                (`difflib.SequenceMatcher`, runs of 3+ words); the words found are marked on the
                page and cropped, and the share of the passage found is stated, never rounded up
                — a paraphrase finds less, and the reviewer is told how much

Read only: renders the repo copy of the PDF (`synopsis_pages.PDF_PATH`) and reads the sidecar.

NO SILENT FALLBACK (AGENTS 62). A missing sidecar, one of another shape, or one cut from another
extraction (`raw_data_sha256`) is a `Missing` with the command that makes it — the evidence pane
shows that message, not a picture of something else.
"""
from __future__ import annotations

import difflib
import hashlib
import io
import json
import os
import re
from functools import lru_cache
from pathlib import Path

from pipeline.common.curated import GENERATED
from pipeline.regs.parsing.catalogue import clean_verbatim

import synopsis_pages

#: the sidecar (`CURATION_ROW_BOXES` points it at a copy)
ROW_BOXES_PATH = (Path(os.environ["CURATION_ROW_BOXES"]) if os.environ.get("CURATION_ROW_BOXES")
                  else GENERATED.regs.extraction / "row_boxes.json")
RAW_DATA_PATH = GENERATED.regs.extraction / "synopsis_raw_data.json"
ROW_BOXES_COMMAND = "python -m pipeline.regs.extraction.row_boxes"
ROW_BOXES_VERSION = 1

#: px per PDF point. 2.6 px/pt is 187 dpi: the book's 6.5 pt row text renders ~17 px tall.
CROP_SCALE = 2.6
#: the whole page, as `synopsis_pages.page_png` renders it
PAGE_SCALE = 1.6
#: points kept around a crop (a table row: `ROW_PAD_Y_PT` above and below)
PAD_PT = 4.0
ROW_PAD_Y_PT = 1.0
#: a passage alignment keeps runs of at least this many words (shorter runs are chance)
MIN_RUN = 3

_TOK = re.compile(r"[a-z0-9]+")


class Missing(Exception):
    """The evidence cannot be shown, and why — with the command that fixes it when there is one."""


# --------------------------------------------------------------------------- the sidecar

def _stamp(p: Path) -> tuple:
    try:
        st = p.stat()
        return (str(p), st.st_mtime_ns, st.st_size)
    except FileNotFoundError:
        return (str(p), None, None)


def row_boxes() -> dict:
    """The sidecar's rows, checked to be this extraction's. Raises `Missing`."""
    return _row_boxes(_stamp(ROW_BOXES_PATH), _stamp(RAW_DATA_PATH),
                      _stamp(synopsis_pages.PDF_PATH))["rows"]


@lru_cache(maxsize=1)
def _row_boxes(key: tuple, raw_key: tuple, pdf_key: tuple = (None, None, None)) -> dict:
    if key[1] is None:
        raise Missing(f"{ROW_BOXES_PATH} does not exist — make it with `{ROW_BOXES_COMMAND}` "
                      f"(no credits; a few minutes)")
    doc = json.loads(ROW_BOXES_PATH.read_text(encoding="utf-8"))
    if doc.get("version") != ROW_BOXES_VERSION:
        raise Missing(f"{ROW_BOXES_PATH} is version {doc.get('version')!r}, the tool reads "
                      f"{ROW_BOXES_VERSION} — remake it with `{ROW_BOXES_COMMAND}`")
    if raw_key[1] is not None:
        now = hashlib.sha256(RAW_DATA_PATH.read_bytes()).hexdigest()
        if doc.get("raw_data_sha256") != now:
            raise Missing(f"{ROW_BOXES_PATH} was cut from another extraction than "
                          f"{RAW_DATA_PATH.name} — remake it with `{ROW_BOXES_COMMAND}`")
    # THE BOXES ARE POINTS ON ONE PDF (T134 adversarial review): a new book dropped in
    # data/source before the extraction is re-run keeps the raw data's sha, so that check passes —
    # and every crop would be cut from the new book at the old book's places. The crop is rendered
    # from `synopsis_pages.PDF_PATH`, so the sidecar must be of that file's bytes.
    if pdf_key[1] is not None:
        pdf = hashlib.sha256(Path(pdf_key[0]).read_bytes()).hexdigest()
        if doc.get("pdf_sha256") != pdf:
            raise Missing(f"{ROW_BOXES_PATH} was cut from another PDF than {pdf_key[0]} — "
                          f"re-run the extraction on it, then `{ROW_BOXES_COMMAND}`")
    return doc


# --------------------------------------------------------------------------- the PDF

@lru_cache(maxsize=12)
def _render(pdf_page: int, scale: float):
    """The PDF page rendered (PIL, RGB) — the repo copy the entries were read from."""
    import pypdfium2 as pdfium
    with synopsis_pages.PDFIUM_LOCK:                 # pdfium is not thread-safe (synopsis_pages)
        doc = pdfium.PdfDocument(str(synopsis_pages.PDF_PATH))
        try:
            return doc[pdf_page - 1].render(scale=scale).to_pil().convert("RGB")
        finally:
            doc.close()


@lru_cache(maxsize=24)
def _page_words(pdf_page: int) -> tuple:
    """(token, (x0, top, x1, bottom)) for every word on the page, in the PDF's reading order."""
    import pdfplumber
    with pdfplumber.open(str(synopsis_pages.PDF_PATH)) as pdf:
        words = pdf.pages[pdf_page - 1].extract_words(use_text_flow=True)
    return tuple((t, (w["x0"], w["top"], w["x1"], w["bottom"]))
                 for w in words for t in _TOK.findall(w["text"].lower()))


def tokens(text: str) -> list[str]:
    return _TOK.findall(clean_verbatim(text or "").lower())


def find_passage(pdf_page: int, text: str) -> dict:
    """Where `text` is printed on the page: `{boxes, found, total}` — `boxes` one per run of words
    found in order (a run broken where the page's reading order jumps up, i.e. a new column),
    `found` / `total` the words of `text` found / asked."""
    want = tokens(text)
    words = _page_words(pdf_page)
    sm = difflib.SequenceMatcher(None, [t for t, _ in words], want, autojunk=False)
    boxes: list[list[float]] = []
    found = 0
    for blk in sm.get_matching_blocks():
        if blk.size < MIN_RUN:
            continue
        found += blk.size
        cur = None
        for _, (x0, top, x1, bottom) in words[blk.a:blk.a + blk.size]:
            if cur is not None and (top < cur[1] - 2 or x0 > cur[2] + 40 and top < cur[3]):
                boxes.append(cur)               # a column break: start a new box
                cur = None
            cur = [x0, top, x1, bottom] if cur is None else \
                [min(cur[0], x0), min(cur[1], top), max(cur[2], x1), max(cur[3], bottom)]
        if cur is not None:
            boxes.append(cur)
    return {"boxes": _merge(boxes), "found": found, "total": len(want)}


def _merge(boxes: list[list[float]]) -> list[list[float]]:
    """Boxes that overlap or touch vertically within one column become one."""
    out: list[list[float]] = []
    for b in sorted(boxes, key=lambda b: (round(b[0] / 60), b[1])):
        if out and b[1] <= out[-1][3] + 3 and b[0] < out[-1][2] and b[2] > out[-1][0]:
            o = out[-1]
            out[-1] = [min(o[0], b[0]), min(o[1], b[1]), max(o[2], b[2]), max(o[3], b[3])]
        else:
            out.append(b)
    return out


# --------------------------------------------------------------------------- one entry

def locate(entry: dict, image_key: str | None) -> dict:
    """Where the entry is printed: `{kind, printed, pdf_page, boxes, strips, found, total}`.

    kind `row` (a water row with its box: `strips` are the name column and the regulation column),
    `passage` (a zone entry found on its chapter page by text) or `none` (`why` says why).
    Raises `Missing` when the sidecar is not there for a water row."""
    pages = [p for p in entry.get("source_pages") or [] if isinstance(p, int)]
    if not str(entry.get("entry_id", "")).startswith("z"):
        if not image_key:
            return {"kind": "none", "why": "no source row matched this entry's text (several rows "
                    "print it, none under this name)", "printed": pages[0] if pages else None}
        b = row_boxes().get(image_key)
        if b is None:
            raise Missing(f"{image_key} is not in {ROW_BOXES_PATH.name} — remake it with "
                          f"`{ROW_BOXES_COMMAND}`")
        x0, y0, x1, y1 = b["box"]
        div = b["div_x"]
        return {"kind": "row", "printed": b["printed"], "pdf_page": b["pdf_page"],
                "boxes": [[x0, y0, x1, y1]],
                "strips": [[x0, y0, div, y1], [div, y0, x1, y1]], "found": None, "total": None}
    best = None
    for printed in pages:
        n = synopsis_pages.pdf_page(printed)
        if n is None:
            continue
        got = find_passage(n, entry.get("regs_verbatim") or "")
        if best is None or got["found"] > best["found"]:
            best = {"kind": "passage", "printed": printed, "pdf_page": n, **got,
                    "strips": got["boxes"]}
    if best is None or not best["found"]:
        return {"kind": "none", "printed": pages[0] if pages else None,
                "why": "the passage's words were not found on its printed page(s)"}
    return best


def _png(img) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def crop_png(where: dict, scale: float = CROP_SCALE) -> bytes:
    """The located text, legible: each strip rendered at `scale` and stacked top to bottom."""
    from PIL import Image
    page = _render(where["pdf_page"], scale)
    # a table row sits between its neighbours with no white between them: 1 pt above and below,
    # or the next row's text shows cut in half
    pad_y = ROW_PAD_Y_PT if where["kind"] == "row" else PAD_PT
    parts = []
    for x0, y0, x1, y1 in where["strips"]:
        box = (max(0, int((x0 - PAD_PT) * scale)), max(0, int((y0 - pad_y) * scale)),
               min(page.width, int((x1 + PAD_PT) * scale)), min(page.height, int((y1 + pad_y) * scale)))
        if box[2] > box[0] and box[3] > box[1]:
            parts.append(page.crop(box))
    if not parts:
        raise Missing("nothing to crop")
    gap = int(3 * scale)
    out = Image.new("RGB", (max(p.width for p in parts),
                            sum(p.height for p in parts) + gap * (len(parts) - 1)), "white")
    y = 0
    for p in parts:
        out.paste(p, (0, y))
        y += p.height + gap
    return _png(out)


def page_png(where: dict, scale: float = PAGE_SCALE) -> bytes:
    """The whole printed page with the located text marked (a translucent band and an outline)."""
    from PIL import Image, ImageDraw
    page = _render(where["pdf_page"], scale).copy()
    over = Image.new("RGBA", page.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(over)
    for x0, y0, x1, y1 in where["boxes"]:
        r = [(x0 - 2) * scale, (y0 - 2) * scale, (x1 + 2) * scale, (y1 + 2) * scale]
        d.rectangle(r, fill=(255, 214, 0, 70), outline=(220, 30, 30, 255), width=3)
    return _png(Image.alpha_composite(page.convert("RGBA"), over).convert("RGB"))
