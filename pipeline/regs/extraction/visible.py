"""VISIBLE TEXT ONLY (T1190): which characters of a synopsis page a viewer actually sees, decided by the PDF's own
painting rules, never by sampling pixels.

The extractor reads characters with pdfplumber; this module says which of them are on the paper. It replaced the
400 dpi render + background palette + ink count + DBSCAN clustering + "first line overlap" heuristics
(`get_cleaned_page` before T1190), which every number of was tuned by hand to one book's colours.

THE ONLY MODULE THAT IMPORTS PyMuPDF (MuPDF's display list; AGPL-3.0 or Artifex commercial, see requirements.txt).
It is the one established library that hands every painted object of a page in paint order with ONE shared sequence
number: text spans (`get_texttrace`: render mode, opacity, colour, optional-content layer, per-glyph boxes) and paths
(`get_drawings(extended=True)`: fill colour and opacity, the transparency groups with their blend modes, and the clip
tree). Swapping the library means re-implementing `open_document` / `_trace` / `_paths` / `_images` /
`_clipped_origins` on another one; the rules and their tests (`pipeline/tests/test_visible_text.py`, PDFs written by
hand) stay.

A glyph is VISIBLE unless one of these rules hides it (the reason is recorded on the glyph):
  mode          text render mode 3 or 7: the glyph paints nothing
  alpha         fill opacity 0
  layer         in an optional-content layer that is OFF in the default view (the page is traced with every layer on)
  crop          its box's centre lies outside the page's crop box
  clip          outside the clip path in force when it was painted (MuPDF's own TEXT_CLIP)
  covered       every sample point of its box lies under some LATER opaque fill (Normal blend, opacity 1 all the way up
                its groups, inside that fill's own clip): the painter's model. Two later fills meeting under one glyph
                cover it together.
  image         a LATER opaque image (no mask) covers its box
  overdrawn     the same glyph (text, font, size) is painted again LATER at the same origin, in another span: the earlier
                copy is wholly under the later one (faux bold, a header drawn twice)
  same-colour   its fill colour EQUALS the colour of the topmost fill under it (white on white); no verdict when
                anything else was painted under it in between
  span-hidden   a space whose span's every other glyph is hidden
A space is judged like any glyph: covered or overdrawn, it is not on the page.

STRIKE-THROUGH is data, not visibility (owner 2026-11-26: keep the struck text, marked): a thin horizontal line painted
AFTER a visible glyph, through the middle band of its box, lying mostly on text, marks that glyph `struck`.

PAIRING. pdfplumber's characters and MuPDF's glyphs are the same content stream read by two interpreters: they are
paired in content order, by text (a ligature is one pdfplumber char and several MuPDF glyphs at one origin). A text
disagreement means the method cannot say which character it judged: `PageNotJudged`, naming the page. An ORIGIN
disagreement (the same glyph placed elsewhere by pdfminer: PDF p.61's scale-bar "8", 47 pt off where both renderers
draw it) is kept on the verdict; the extractor refuses such a page only if it reads rows from it.
"""
from __future__ import annotations

import unicodedata
import weakref
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import pymupdf

Rect = Tuple[float, float, float, float]

#: pdfplumber and MuPDF must put one glyph's origin within this many points of each other
ORIGIN_AGREEMENT_PT = 0.6
#: a glyph box's sample points are pulled in by this share of its size (a glyph's ink lies inside its box)
SAMPLE_INSET = 0.15
#: a strike-through line: at most this tall (pt), at least this long (pt), shorter than a rule across a column (pt)
STRIKE_MAX_HEIGHT, STRIKE_MIN_LENGTH, STRIKE_MAX_LENGTH = 1.2, 2.0, 200.0
#: ... crossing the band between these shares of the glyph box's height from its top, over at least half its width,
#: with at least this share of the line's length lying on glyphs of the line (a strike runs over text, not past it)
STRIKE_BAND, STRIKE_MIN_OVERLAP, STRIKE_ON_TEXT = (0.35, 0.75), 0.5, 0.6


class PageNotJudged(ValueError):
    """The method cannot judge this page: it names the page and why. Never caught into an empty answer."""

    def __init__(self, page_no: int, why: str) -> None:
        super().__init__(f"PDF page {page_no}: visible-text method cannot judge the page: {why}")
        self.page_no, self.why = page_no, why


@dataclass
class Glyph:
    i: int                     # index in content (paint) order on the page
    text: str
    seqno: int                 # MuPDF display-list sequence number of its span
    origin: Tuple[float, float]
    bbox: Rect
    size: float
    font: str
    color: Tuple[float, ...]
    mode: int                  # text render mode: 0 fill, 1 stroke, 2 fill+stroke, 3 invisible, 4-7 the same + clip
    opacity: float
    layer: str
    hidden: List[str] = field(default_factory=list)
    struck: bool = False

    @property
    def visible(self) -> bool:
        return not self.hidden


@dataclass
class PageVerdict:
    """For one pdfplumber page, by index into `page.chars`: the visible ones, the struck ones, and every char whose
    origin the two interpreters disagree on (`(index, text, pdfplumber origin, MuPDF origin)`)."""
    page_no: int
    visible: Set[int]
    struck: Set[int]
    disagreements: List[Tuple[int, str, Tuple[float, float], Tuple[float, float]]]
    hidden_reasons: Dict[int, List[str]]


# ---------------------------------------------------------------- geometry

def _winding(x: float, y: float, poly: List[Tuple[float, float]]) -> int:
    w = 0
    n = len(poly)
    for k in range(n):
        x1, y1 = poly[k]
        x2, y2 = poly[(k + 1) % n]
        if y1 <= y < y2 and (x2 - x1) * (y - y1) - (x - x1) * (y2 - y1) > 0:
            w += 1
        elif y2 <= y < y1 and (x2 - x1) * (y - y1) - (x - x1) * (y2 - y1) < 0:
            w -= 1
    return w


def _crossings(x: float, y: float, poly: List[Tuple[float, float]]) -> int:
    c = 0
    n = len(poly)
    for k in range(n):
        x1, y1 = poly[k]
        x2, y2 = poly[(k + 1) % n]
        if (y1 > y) != (y2 > y) and x1 + (y - y1) * (x2 - x1) / (y2 - y1) > x:
            c += 1
    return c


def _subpaths(items) -> List[List[Tuple[float, float]]]:
    """A path's items as closed polygons (a curve by its control polygon: enough for 'does this fill cover that glyph
    box' on the rectangles and rounded boxes a book is made of)."""
    polys: List[List[Tuple[float, float]]] = []
    cur: List[Tuple[float, float]] = []
    last = None
    for it in items:
        kind = it[0]
        if kind == "re":
            r = it[1]
            polys.append([(r.x0, r.y0), (r.x1, r.y0), (r.x1, r.y1), (r.x0, r.y1)])
            last = None
            continue
        if kind == "qu":
            q = it[1]
            polys.append([(q.ul.x, q.ul.y), (q.ur.x, q.ur.y), (q.lr.x, q.lr.y), (q.ll.x, q.ll.y)])
            last = None
            continue
        pts = [(p.x, p.y) for p in it[1:]]
        start = pts[0]
        if last is None or abs(start[0] - last[0]) > 1e-3 or abs(start[1] - last[1]) > 1e-3:
            if len(cur) > 2:
                polys.append(cur)
            cur = [start]
        cur.extend(pts[1:])
        last = pts[-1]
    if len(cur) > 2:
        polys.append(cur)
    return polys


def _inside(x: float, y: float, r: Rect) -> bool:
    return r[0] <= x <= r[2] and r[1] <= y <= r[3]


def _overlap(a: Rect, b: Rect) -> bool:
    return a[0] < b[2] and b[0] < a[2] and a[1] < b[3] and b[1] < a[3]


def _isect(a: Rect, b: Rect) -> Rect:
    return (max(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), min(a[3], b[3]))


def _samples(b: Rect) -> List[Tuple[float, float]]:
    """The box's centre and its four corners pulled in by SAMPLE_INSET."""
    w, h = b[2] - b[0], b[3] - b[1]
    x0, y0, x1, y1 = b[0] + w * SAMPLE_INSET, b[1] + h * SAMPLE_INSET, b[2] - w * SAMPLE_INSET, b[3] - h * SAMPLE_INSET
    return [((x0 + x1) / 2, (y0 + y1) / 2), (x0, y0), (x1, y0), (x0, y1), (x1, y1)]


@dataclass
class Fill:
    seqno: int
    rect: Rect
    polys: List[List[Tuple[float, float]]]
    even_odd: bool
    color: Optional[Tuple[float, ...]]
    opaque: bool
    clip: Rect

    def covers_point(self, x: float, y: float) -> bool:
        if not (_inside(x, y, self.clip) and _inside(x, y, self.rect)):
            return False
        if self.even_odd:
            return sum(_crossings(x, y, p) for p in self.polys) % 2 == 1
        return sum(_winding(x, y, p) for p in self.polys) != 0

    def covers_box(self, b: Rect) -> bool:
        return all(self.covers_point(x, y) for x, y in _samples(b))


# ---------------------------------------------------------------- what MuPDF paints

def _trace(page: "pymupdf.Page") -> List[Glyph]:
    glyphs: List[Glyph] = []
    for s in page.get_texttrace():
        for c in s["chars"]:
            ucs, _gid, org, bb = c[0], c[1], c[2], c[3]
            glyphs.append(Glyph(len(glyphs), chr(ucs) if ucs >= 0 else "�", s["seqno"], (org[0], org[1]),
                                tuple(bb), s["size"], s["font"], tuple(s["color"] or ()), s["type"],
                                s["opacity"], s.get("layer") or ""))
    return glyphs


def _paths(page: "pymupdf.Page", off_layers: frozenset) -> Tuple[List[Fill], List[dict]]:
    """Every filled path with whether it is OPAQUE (Normal blend, opacity 1 all the way up its group stack) and the
    clip rectangle in force (the clip path's box); plus every painted path, for strike detection. A path in a layer
    that is off paints nothing and is left out."""
    fills: List[Fill] = []
    lines: List[dict] = []
    page_r = tuple(page.rect)
    stack: List[Tuple[int, str, object]] = []          # (level, kind, value) for the open groups and clips
    for x in page.get_drawings(extended=True):
        lvl = x.get("level", 0)
        while stack and stack[-1][0] >= lvl:
            stack.pop()
        t = x["type"]
        if t == "group":
            stack.append((lvl, "group", (x.get("blendmode", "Normal"), x.get("opacity", 1.0))))
            continue
        if t == "clip":
            stack.append((lvl, "clip", tuple(x["scissor"])))
            continue
        if x.get("layer") and x["layer"] in off_layers:
            continue
        clip = page_r
        normal, alpha = True, 1.0
        for _, kind, v in stack:
            if kind == "clip":
                clip = _isect(clip, v)
            else:
                bm, op = v
                normal = normal and bm in ("Normal", "Compatible")
                alpha *= op
        if "f" in t:
            fill = tuple(x["fill"]) if x.get("fill") is not None else None
            opaque = normal and alpha >= 1.0 and (x.get("fill_opacity") or 0) >= 1.0 and fill is not None
            fills.append(Fill(x["seqno"], tuple(x["rect"]), _subpaths(x["items"]), bool(x.get("even_odd")),
                              fill, opaque, clip))
        lines.append({"seqno": x["seqno"], "rect": tuple(x["rect"])})
    return fills, lines


def _images(page: "pymupdf.Page", log, off_layers: frozenset) -> List[Tuple[int, Rect, bool]]:
    """Painted images in paint order (the bbox log's index is MuPDF's seqno) with opaque = a plain image, no mask, in
    a layer that is on."""
    info = page.get_image_info()
    painted = [(seq, tuple(r), kind, layer) for seq, (kind, r, layer) in enumerate(log)
               if kind in ("fill-image", "fill-imgmask")]
    plain = [p for p in painted if p[2] == "fill-image"]
    if len(plain) != len(info):
        raise PageNotJudged(page.number + 1, f"{len(plain)} images painted but {len(info)} described: cannot tell "
                                             f"which image has a mask")
    out, k = [], 0
    for seq, r, kind, layer in painted:
        opaque = False
        if kind == "fill-image":
            opaque = not info[k].get("has-mask") and not (layer and layer in off_layers)
            k += 1
        out.append((seq, r, opaque))
    return out


def _clipped_origins(page: "pymupdf.Page") -> Set[Tuple[float, float]]:
    """Origins of glyphs MuPDF's text device drops when it honours the clip (TEXT_CLIP) and keeps without it."""
    base = pymupdf.TEXTFLAGS_RAWDICT & ~pymupdf.TEXT_CLIP

    def origins(flags) -> Set[Tuple[float, float]]:
        out = set()
        for b in page.get_text("rawdict", flags=flags)["blocks"]:
            for ln in b.get("lines", []):
                for s in ln["spans"]:
                    for c in s["chars"]:
                        out.add((round(c["origin"][0], 1), round(c["origin"][1], 1)))
        return out
    return origins(base) - origins(base | pymupdf.TEXT_CLIP)


# ---------------------------------------------------------------- the rules

def judge(page: "pymupdf.Page", off_layers: frozenset = frozenset()) -> List[Glyph]:
    """Every glyph MuPDF paints on `page`, in content order, each with the reasons (if any) it is not seen.

    `page` must come from a document opened with every layer ON (`open_document`), so the trace holds the content
    of layers that are off too (as pdfplumber's chars do); `off_layers` names the layers off in the default view."""
    crop = tuple(page.cropbox)
    glyphs = _trace(page)
    fills, lines = _paths(page, off_layers)
    log = page.get_bboxlog(layers=True)
    images = _images(page, log, off_layers)
    clipped = _clipped_origins(page)
    painted_other = [(seq, tuple(r)) for seq, (kind, r, layer) in enumerate(log)
                     if kind in ("fill-image", "fill-imgmask", "fill-shade", "stroke-path", "fill-path")
                     and not (layer and layer in off_layers)]
    opaque_fills = [f for f in fills if f.opaque]

    for g in glyphs:
        b = g.bbox
        if g.mode in (3, 7):
            g.hidden.append("mode")
        if g.opacity <= 0.0:
            g.hidden.append("alpha")
        if g.layer and g.layer in off_layers:
            g.hidden.append("layer")
        if not _inside((b[0] + b[2]) / 2, (b[1] + b[3]) / 2, crop):
            g.hidden.append("crop")
        if (round(g.origin[0], 1), round(g.origin[1], 1)) in clipped:
            g.hidden.append("clip")
        if (b[2] - b[0]) <= 0 or (b[3] - b[1]) <= 0:
            # a zero-size box (some spaces): judge the point just above the origin
            b = (g.origin[0] - 0.01, g.origin[1] - 0.5, g.origin[0] + 0.01, g.origin[1] - 0.49)
        later = [f for f in opaque_fills if f.seqno > g.seqno and _overlap(f.rect, b)]
        if later and all(any(f.covers_point(x, y) for f in later) for x, y in _samples(b)):
            g.hidden.append("covered")
        for seq, r, opaque in images:
            if opaque and seq > g.seqno and r[0] <= b[0] and r[1] <= b[1] and r[2] >= b[2] and r[3] >= b[3]:
                g.hidden.append("image")
                break

    # overdrawn: a later copy of the same glyph at the same origin, in ANOTHER span (a ligature's parts share an
    # origin inside one span), wholly covers the earlier one
    seen: Dict[Tuple, int] = {}
    for g in reversed(glyphs):
        if g.mode not in (0, 2):
            continue
        key = (g.text, round(g.origin[0], 1), round(g.origin[1], 1), round(g.size, 1), g.font)
        later = seen.get(key)
        if later is not None and glyphs[later].seqno != g.seqno and glyphs[later].visible:
            g.hidden.append("overdrawn")
        else:
            seen.setdefault(key, g.i)

    # same colour as the fill under it
    for g in glyphs:
        if g.hidden or g.text.strip() == "" or g.mode != 0 or not g.color:
            continue
        b = g.bbox
        under = None
        for f in fills:                                     # any fill: a Multiply fill over white shows its colour
            if f.seqno < g.seqno and f.color is not None and f.covers_box(b):
                under = f                                   # the topmost (last) one
        top = under.seqno if under is not None else -1
        # anything else painted under the glyph after that fill (an image, a shading, a stroke, a fill that does not
        # cover the whole box) makes the background unknown: no verdict
        if any(top < seq < g.seqno and _overlap(r, b) for seq, r in painted_other):
            continue
        bg = under.color if under is not None else (1.0,) * len(g.color)     # the page itself is white
        if len(bg) == len(g.color) and all(round(a, 4) == round(c, 4) for a, c in zip(bg, g.color)):
            g.hidden.append("same-colour")

    # a space whose span shows nothing else
    by_seq: Dict[int, List[Glyph]] = {}
    for g in glyphs:
        by_seq.setdefault(g.seqno, []).append(g)
    for g in glyphs:
        if g.text.strip() == "" and not g.hidden:
            sib = [o for o in by_seq[g.seqno] if o.text.strip()]
            if sib and not any(o.visible for o in sib):
                g.hidden.append("span-hidden")

    _mark_strikes(glyphs, lines, opaque_fills)
    return glyphs


def _mark_strikes(glyphs: List[Glyph], lines: List[dict], fills: List[Fill]) -> None:
    """A strike-through: a thin horizontal path (stroke or fill) painted after a visible glyph, itself visible, crossing
    the glyph's middle band over at least half its width and lying mostly on glyphs of that line."""
    def shown(s) -> bool:
        r = s["rect"]
        xm, ym = (r[0] + r[2]) / 2, (r[1] + r[3]) / 2
        return not any(f.seqno > s["seqno"] and f.covers_point(xm, ym) for f in fills)

    thin = [s for s in lines if (s["rect"][3] - s["rect"][1]) <= STRIKE_MAX_HEIGHT
            and STRIKE_MIN_LENGTH <= (s["rect"][2] - s["rect"][0]) < STRIKE_MAX_LENGTH and shown(s)]
    if not thin:
        return
    vis = [g for g in glyphs if g.visible and g.text.strip()]
    for g in vis:
        x0, y0, x1, y1 = g.bbox
        h, w = y1 - y0, x1 - x0
        lo, hi = y0 + STRIKE_BAND[0] * h, y0 + STRIKE_BAND[1] * h
        for s in thin:
            r = s["rect"]
            ym = (r[1] + r[3]) / 2
            if s["seqno"] < g.seqno or not (lo <= ym <= hi):
                continue                                    # painted before the glyph: under it, not through it
            if min(x1, r[2]) - max(x0, r[0]) < STRIKE_MIN_OVERLAP * w:
                continue
            on = sum(max(0.0, min(o.bbox[2], r[2]) - max(o.bbox[0], r[0])) for o in vis
                     if o.bbox[1] <= ym <= o.bbox[3])
            if on >= STRIKE_ON_TEXT * (r[2] - r[0]):
                g.struck = True
                break


# ---------------------------------------------------------------- pairing with pdfplumber's chars

def _nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def pair(glyphs: List[Glyph], pchars: List[dict], page_height: float, page_no: int) -> List[Glyph]:
    """For each pdfplumber char (content order), the MuPDF glyph it is. A ligature ('fi') is one pdfplumber char and
    several MuPDF glyphs at one origin: it takes the first. Raises `PageNotJudged` when the two text streams differ."""
    out: List[Glyph] = []
    k = 0
    for pc in pchars:
        t = pc["text"]
        if k >= len(glyphs):
            raise PageNotJudged(page_no, f"MuPDF's glyphs ran out at pdfplumber's {t!r} (char {len(out)})")
        g = glyphs[k]
        if g.text == t or _nfkc(g.text) == _nfkc(t):
            out.append(g)
            k += 1
            continue
        cat, j = "", k
        while j < len(glyphs) and len(cat) < len(_nfkc(t)):
            cat += _nfkc(glyphs[j].text)
            j += 1
        if cat != _nfkc(t):
            raise PageNotJudged(page_no, f"char {len(out)}: MuPDF reads {g.text!r}, pdfplumber {t!r}")
        out.append(glyphs[k])
        k = j
    if k != len(glyphs):
        raise PageNotJudged(page_no, f"{len(glyphs) - k} MuPDF glyph(s) after pdfplumber's last char")
    return out


# ---------------------------------------------------------------- the extractor's entry point

def open_document(data: bytes) -> Tuple["pymupdf.Document", frozenset]:
    """The PDF opened for judging, and the names of its layers that are OFF in the default view.

    MuPDF leaves a hidden layer's content out of its display list altogether, while pdfplumber reads its characters:
    the two streams would not pair. So the document is re-opened with EVERY layer on, and the layer rule hides what
    the default view hides. A layer name that is both on and off cannot be told apart by name: refused."""
    doc = pymupdf.open(stream=data, filetype="pdf")
    cfg = doc.get_layer(-1) if doc.get_ocgs() else {}
    off = cfg.get("off") or []
    if not off:
        return doc, frozenset()
    ocgs = doc.get_ocgs()
    off_names = {ocgs[x]["name"] for x in off}
    on_names = {o["name"] for x, o in ocgs.items() if x not in off}
    if off_names & on_names:
        raise ValueError(f"layer name(s) {sorted(off_names & on_names)} are both on and off: the visible-text "
                         f"method judges layers by name")
    doc.set_layer(-1, on=sorted(ocgs), off=[])
    return pymupdf.open(stream=doc.tobytes(), filetype="pdf"), frozenset(off_names)


_DOCS: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()


def _mupdf_doc(pdf) -> Tuple["pymupdf.Document", frozenset]:
    """The judging document for a pdfplumber PDF, opened once per PDF object (from its path, else its stream)."""
    got = _DOCS.get(pdf)
    if got is None:
        if getattr(pdf, "path", None) is not None:
            data = open(pdf.path, "rb").read()
        else:
            pos = pdf.stream.tell()
            pdf.stream.seek(0)
            data = pdf.stream.read()
            pdf.stream.seek(pos)
        got = open_document(data)
        if got[0].page_count != len(pdf.pages):
            raise ValueError(f"MuPDF reads {got[0].page_count} pages, pdfplumber {len(pdf.pages)}: not one file")
        _DOCS[pdf] = got
    return got


def page_verdict(plumber_page) -> PageVerdict:
    """Which of `plumber_page.chars` are visible and which struck, by index into that list."""
    n = plumber_page.page_number
    doc, off_layers = _mupdf_doc(plumber_page.pdf)
    glyphs = judge(doc[n - 1], off_layers)
    chars = plumber_page.chars
    paired = pair(glyphs, chars, plumber_page.height, n)
    visible, struck, reasons, disagree = set(), set(), {}, []
    for i, (c, g) in enumerate(zip(chars, paired)):
        if g.visible:
            visible.add(i)
        else:
            reasons[i] = list(g.hidden)
        if g.struck:
            struck.add(i)
        ox, oy = c["matrix"][4], plumber_page.height - c["matrix"][5]
        if abs(ox - g.origin[0]) > ORIGIN_AGREEMENT_PT or abs(oy - g.origin[1]) > ORIGIN_AGREEMENT_PT:
            disagree.append((i, c["text"], (round(ox, 2), round(oy, 2)),
                             (round(g.origin[0], 2), round(g.origin[1], 2))))
    return PageVerdict(n, visible, struck, disagree, reasons)
