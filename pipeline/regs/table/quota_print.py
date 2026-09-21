"""THE PRINT, READ BY MACHINE: the standing quota tables against the synopsis PDFs.

WHY
  Once a region's base is validated against the printed book and frozen, every section's
  table is provably base + named overrides, and a defect report stops being "why is this
  number wrong?" and becomes "which delta did this?". That only holds if the base is checked
  against the BOOK — not against the catalogue's `verbatim`, which is itself the thing under
  test. So every expectation here is a line found in text extracted from an official PDF,
  parsed into a claim, and matched against the base ledger by CONTENT (fish, number, size,
  kind of water, origin, dates), and every line reports the file, the edition, the page and
  the words it came from, so an auditor can put a finger on it.

  Both directions are checked: a printed line the table does not carry, and a table line the
  print does not carry. A line the parser cannot read is a failure too — an unread line is a
  line nobody checked.

SOURCES  data/source/fishing_synopsis.pdf — the full synopsis in the repository, edition
         2025-2027, 88 pages, md5 6eb14ec7. Its bytes differ from the file gov.bc.ca serves
         today, so every box read from it is proved identical, line for line, to the same box in
         the regional chapter that IS byte-identical to gov.bc.ca on 2026-09-17 (`cross_check`).
         Regional boxes: one page each — 15, 23, 30, 36, 48, 55, 64, 70, 74. The province:
         pages 8–12.
"""
from __future__ import annotations
import hashlib, os, re
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Tuple

from pipeline.regs.table.authority import Authority, source_of
from pipeline.regs.table.build import allowances
from pipeline.regs.table.ledger import Allowance, Ledger
from pipeline.regs.table.subject import expand, Origin

GOV = ("https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/"
       "fishing-and-hunting/freshwater-fishing/")
EDITION = "2025-2027"
#: THE SOURCE: the full synopsis in the repository. Edition 2025-2027, 88 pages. Its bytes are
#: not those gov.bc.ca serves today (a different save of the same edition), so every box read
#: from it is proved identical, line for line, to the same box in the regional chapter that IS
#: byte-identical to gov.bc.ca — see `cross_check`.
PDF = os.path.join("data", "source", "fishing_synopsis.pdf")
PDF_MD5 = "6eb14ec7"
FULL_URL = GOV + "fishing_synopsis.pdf"

#: region -> (page index in the full synopsis, [ (x0, x1, y0, y1, mode) windows ]). Measured on
#: the PDF; every page is 576 pt wide, three columns. "box" reads every line; "general" reads
#: only the closure sentences of the General Regulations paragraph (the gear rules there are
#: the gear table's own check).
CHAPTERS: Dict[str, Tuple[int, list]] = {
    "1":  (14, [(0, 205, 85, 130, "general"), (0, 205, 240, 505, "box"), (205, 380, 212, 295, "box")]),
    "1hg": (14, [(205, 380, 70, 205, "box"), (205, 380, 212, 295, "box")]),
    "2":  (22, [(205, 380, 70, 232, "box"), (205, 380, 260, 368, "box"), (205, 380, 438, 515, "box")]),
    "3":  (29, [(0, 205, 88, 130, "general"), (0, 205, 258, 525, "box"), (205, 380, 70, 100, "box")]),
    "4":  (35, [(0, 205, 84, 152, "general"), (0, 205, 283, 590, "box"), (205, 380, 68, 100, "box")]),
    "5":  (47, [(0, 205, 88, 160, "general"), (0, 205, 224, 600, "box")]),
    "6":  (54, [(0, 205, 85, 330, "general"), (205, 380, 70, 445, "box"), (205, 380, 496, 540, "box")]),
    "7a": (63, [(0, 205, 190, 222, "general"), (0, 205, 300, 440, "box"), (205, 380, 70, 240, "box"), (205, 380, 295, 378, "box")]),
    "7b": (69, [(205, 380, 208, 525, "box"), (380, 576, 70, 320, "box")]),
    "8":  (73, [(0, 205, 84, 125, "general"), (0, 205, 160, 440, "box"), (205, 380, 68, 100, "box")]),
}
#: The same chapters as gov.bc.ca serves them one region at a time — byte-identical to the
#: site on 2026-09-17 (md5 as recorded) — each holding the box on its page 2.
GOV_CHAPTERS = {"1": ("region_1_vancouver_island.pdf", "9b8c35eb"), "1hg": ("region_1_vancouver_island.pdf", "9b8c35eb"),
                "2": ("region_2_lower_mainland.pdf", "b5828afe"), "3": ("region_3_thompson.pdf", "c63be622"),
                "4": ("region_4_kootenay.pdf", "01dac4c5"), "5": ("region_5_cariboo.pdf", "8a771eb5"),
                "6": ("region_6_skeena.pdf", "aa18c81c"), "7a": ("region_7a_omineca.pdf", "47763e79"),
                "7b": ("region_7b_peace.pdf", "c62ffadb"), "8": ("region_8_okanagan.pdf", "a92a4156")}
#: Where gov.bc.ca's own copies live when they have been fetched: `SYNOPSIS_DIR`, else
#: data/source/official/ (untracked). Nothing the suite GATES on may live anywhere ephemeral;
#: what needs these files skips, naming the file, when they are absent.
OFFICIAL_DIR = os.environ.get("SYNOPSIS_DIR") or os.path.join("data", "source", "official")
REGION_OF = {"1hg": "1"}
#: The province's own lines: pages 8–12 of the full synopsis, found by pattern per column.
PROVINCE_PAGES = [7, 8, 9, 10, 11]

# ----------------------------------------------------------------------------------------
# reading the PDF
# ----------------------------------------------------------------------------------------
def _md5(folder: str, file: str) -> str:
    return hashlib.md5(open(os.path.join(folder, file), "rb").read()).hexdigest()[:8]


def _raw_lines(folder: str, file: str, page: int, x0, x1, y0, y1) -> List[Tuple[float, float, str]]:
    """Upright words only (the page carries rotated map art), grouped into lines by their
    top, in reading order — the reviewer's extractor, bounded to one column and one box."""
    import pdfplumber
    with pdfplumber.open(os.path.join(folder, file)) as pdf:
        pg = pdf.pages[page]
        ws = [w for w in pg.extract_words(extra_attrs=["upright"])
              if w.get("upright") and x0 <= w["x0"] and w["x1"] <= x1 and y0 <= w["top"] <= y1]
    ws.sort(key=lambda w: (w["top"], w["x0"]))
    out, line, top = [], [], None
    for w in ws:
        if top is None or abs(w["top"] - top) > 3:
            if line:
                line.sort(key=lambda t: t["x0"])
                out.append((top, line[0]["x0"], " ".join(t["text"] for t in line)))
            line, top = [], w["top"]
        line.append(w)
    if line:
        line.sort(key=lambda t: t["x0"])
        out.append((top, line[0]["x0"], " ".join(t["text"] for t in line)))
    return out


_CONT_END = re.compile(r"(over|from|no more|,|and|or|TO|the|in|of|for|than|to|except|\(|-|including|"
                       r"Skeena|Fraser|Liard|Williams|Williams Lake|Peace|Fraser River|possession quota|Side|"
                       r"Jan|Feb|Mar|Apr|May|June?|July?|Aug|Sept?|Oct|Nov|Dec)$")


def _logical_lines(raw: List[Tuple[float, float, str]], col_x0: float) -> List[str]:
    """Join a wrapped line onto the line it continues: indented, or lowercase, or following
    a line that plainly did not end."""
    out: List[str] = []
    for _, x, t in raw:
        t = t.strip()
        if not t:
            continue
        unclosed = out and out[-1].count("(") > out[-1].count(")")
        cont = (out and not t.startswith(("•", "―"))
                and (x > col_x0 + 6 or t[0].islower() or t[0] in "=" or re.match(r"^\d+\.?$", t)
                     or _CONT_END.search(out[-1]) or unclosed))
        if cont and not re.match(r"^(And you|Possession|Annual|NOTE|Region (Zone )?\w+ Daily|Haida|Daily|Exception|\(See tables|Trout/char:|Bass:|Burbot:)", t):
            out[-1] = out[-1] + " " + t
        else:
            out.append(t)
    return out


def box_lines(reg: str, pdf: str = PDF, page: Optional[int] = None) -> List[Tuple[str, str]]:
    """(mode, line) for every logical line in the region's windows — from the repo synopsis
    by default, or from any file holding the same page at `page`."""
    pidx, windows = CHAPTERS[reg]
    if page is not None:
        pidx = page
    out: List[Tuple[str, str]] = []
    for x0, x1, y0, y1, mode in windows:
        out += [(mode, t) for t in _logical_lines(_raw_lines(os.path.dirname(pdf), os.path.basename(pdf), pidx, x0, x1, y0, y1), x0 + 36)]
    return out


def cross_check(folder: str = OFFICIAL_DIR) -> Dict[str, str]:
    """Is each box in the repo synopsis identical, line for line, to the same box in the
    chapter gov.bc.ca serves (byte-identical to the site)? This is what makes the repo copy
    a source: not its bytes, but its every line agreeing with the official file's.
    Per region: "same", "differs", "md5 changed: <file>" (the local copy is not the file
    recorded from the site), or "missing: <path>" — a missing file is not a disagreement."""
    out = {}
    for reg, (file, md5) in GOV_CHAPTERS.items():
        path = os.path.join(folder, file)
        if not os.path.exists(path):
            out[reg] = f"missing: {path}"; continue
        if _md5(folder, file) != md5:
            out[reg] = f"md5 changed: {file} is {_md5(folder, file)}, the site served {md5}"; continue
        out[reg] = "same" if box_lines(reg) == box_lines(reg, path, 1) else "differs"
    return out


_CLOSURE = re.compile(r"^(No [Ff]ishing|Spring closure|Summer closure|Trout/char release)")


def parse_general(line: str) -> List[Claim]:
    """A closure sentence from the General Regulations paragraph: "No Fishing: in any stream
    in Region 4 from Apr 1-June 14", "Trout/char release: in streams from Nov 1-Mar 31",
    "No fishing: in all rivers and streams for steelhead, May 15-June 15"."""
    t = re.sub(r"\s+", " ", line)
    if not _CLOSURE.match(t):
        return []
    low = t.lower()
    note = ("not-base: area (management units)" if "management unit" in low else
            "not-base: watershed-scoped" if "watershed" in low else "")
    water = "stream" if "stream" in low else None
    win = _window(t)
    if low.startswith("trout/char release"):
        return [Claim(line, frozenset({"TROUT_CHAR"}), "release", 0, water=water, window=win, note=note)]
    sp = frozenset({"ST"}) if "for steelhead" in low else frozenset({"ALL_GAME_FISH"})
    return [Claim(line, sp, "closed", 0, water=water, window=win, note=note)]


# ----------------------------------------------------------------------------------------
# what a printed line claims
# ----------------------------------------------------------------------------------------
SPECIES = [  # longest first
    ("trout/char", "TROUT_CHAR"), ("trout and char", "TROUT_CHAR"), ("white sturgeon", "WSG"),
    ("yellow perch", "YP"), ("northern pike", "NP"), ("arctic grayling", "GR"),
    ("rainbow trout", "RB"), ("cutthroat trout", "CT"), ("brook trout", "EB"),
    ("lake trout", "LT"), ("bull trout", "BT"), ("dolly varden", "DV"), ("steelhead", "ST"),
    ("whitefish", "WHITEFISH"), ("kokanee", "KO"), ("crayfish", "CRA"), ("crappie", "BCB"),
    ("burbot", "BB"), ("walleye", "WP"), ("goldeye", "GE"), ("inconnu", "IN"), ("bass", "BASS"),
    ("char", "CHAR"), ("trout", "TROUT"),
]
_MON = {m: i + 1 for i, m in enumerate(("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"))}
_DATES = re.compile(r"\b(Jan|Feb|Mar|Apr|May|June?|July?|Aug|Sept?|Oct|Nov|Dec)\.? ?(\d{1,2})\s*[-–]\s*(Jan|Feb|Mar|Apr|May|June?|July?|Aug|Sept?|Oct|Nov|Dec)\.? ?(\d{1,2})", re.I)


def _window(text: str):
    m = _DATES.search(text)
    if not m:
        return None
    return ((_MON[m.group(1)[:3].lower()], int(m.group(2))), (_MON[m.group(3)[:3].lower()], int(m.group(4))))


@dataclass
class Claim:
    text: str                       # the printed line, verbatim
    species: FrozenSet[str]         # the fish, as codes (groups kept as groups)
    kind: str                       # quota | closed | release | unlimited | gate | cap | multiple | annual
    n: Optional[int] = None
    size: Optional[Tuple[str, int]] = None      # ("over"|"under", cm) — a cap counts, a gate forbids
    slot: Optional[Tuple[int, int]] = None      # (lo, hi) — must be between
    water: Optional[str] = None                 # stream | lake | None
    origin: Optional[str] = None                # wild | hatchery | None
    window: Optional[tuple] = None
    pooled: bool = False
    note: str = ""                              # "not-base: watershed-scoped" etc.
    within: Optional[FrozenSet[str]] = None     # a cap inside this group's number


def _species(words: str) -> FrozenSet[str]:
    w = words.lower().replace("(dolly varden)", "/dolly varden").replace(" and/or ", "/").replace(" or ", "/")
    m = re.search(r"\(([^)]*)\)", w)
    if m and any(name in m.group(1) for name, _ in SPECIES):
        w = m.group(1)                     # the list in brackets says which fish the word means
    out = set()
    for part in re.split(r"/|,", w):
        part = part.strip()
        for name, code in SPECIES:
            if name in part:
                out.add(code); part = part.replace(name, "")
    return frozenset(out)


IGNORE = re.compile(r"^(\(?See tables|NOTE: There is no general minimum|Daily (&|and) Annual|Please refer|Salmon Regulations|"
                    r"Region (Zone )?\w+ Daily Quotas|Haida Gwaii Daily Quotas|Possession Quotas$|Annual Quotas$|"
                    r"NOTE: Bull trout and Dolly Varden are two|Steelhead fishing:|Annual quota for all B\.C\.:$|"
                    r"\(excluding Haida Gwaii\)|Size limit:|• Fraser River: CLOSED TO ALL FISHING in the Fraser areas)", re.I)


def parse(lines) -> List[Claim]:
    """Every line of a Daily Quotas box, as claims. A line nothing here can read becomes a
    claim of kind `unread`, which no table can satisfy."""
    claims: List[Claim] = []
    ctx: Optional[FrozenSet[str]] = None       # the species a bullet belongs to
    sub: Optional[FrozenSet[str]] = None       # the species a "―" sub-bullet belongs to
    mode = ""                                   # "release" after "And you must release:"
    for item in lines:
        wmode, raw = item if isinstance(item, tuple) else ("box", item)
        if wmode == "general":
            claims += parse_general(raw); continue
        t = re.sub(r"\s+", " ", raw).strip()
        if not t or IGNORE.match(t):
            continue
        if t.startswith("―") and mode == "release":
            claims += _release(raw, t.lstrip("― ").strip(), sub or ctx); continue
        m = re.match(r"^• ([A-Za-z/ ()]+):$", t)       # "• Bull trout (Dolly Varden):" — a heading
        if m and _species(m.group(1)):
            sub = _species(m.group(1)); continue
        # the Region 5 sturgeon lines are scoped to a watershed
        if re.match(r"^(CATCH AND RELEASE|CLOSED TO ALL FISHING) in the Fraser River", t):
            claims.append(Claim(raw, frozenset({"WSG"}), "release" if t.startswith("CATCH") else "closed", 0, note="not-base: watershed-scoped")); continue
        low = t.lower()
        m = re.match(r"^(And you must release ?:|And you may retain:)\s*(.*)$", t)
        if m:
            mode = "release" if "release" in m.group(1) else "retain"
            t = m.group(2).strip()
            if not t:
                continue
        # -- possession & annual ---------------------------------------------------------
        m = re.match(r"^Possession quotas? = (\d+) daily quotas?", t)
        if m:
            claims.append(Claim(raw, frozenset({"ALL_GAME_FISH"}), "multiple", int(m.group(1)))); continue
        m = re.match(r"^Exception: possession quota = (\d+) daily quota for (.+)$", t)
        if m:
            for part in re.split(r", and |, | and ", m.group(2)):
                mm = re.match(r"(\d+) daily quota for (.+)", part) or re.match(r"(.+)", part)
                n = int(mm.group(1)) if mm.lastindex == 2 else int(m.group(1))
                sp = _species(mm.group(2) if mm.lastindex == 2 else mm.group(1))
                if sp: claims.append(Claim(raw, sp, "multiple", n))
            continue
        m = re.match(r"^(.+?): possession quota = (\d+) daily quota", t)
        if m:
            claims.append(Claim(raw, _species(m.group(1)), "multiple", int(m.group(2)))); continue
        m = re.match(r"^Annual (?:catch )?quota for all B\.C\.:\s*(\d+) (?:hatchery )?steelhead", t)
        if m:
            claims.append(Claim(raw, frozenset({"ST"}), "annual", int(m.group(1)), origin="hatchery")); continue
        m = re.match(r"^(\d+) hatchery steelhead per licence year", t)
        if m:
            claims.append(Claim(raw, frozenset({"ST"}), "annual", int(m.group(1)), origin="hatchery")); continue
        # -- the NOTE on bull trout retention windows ---------------------------------------
        m = re.match(r"^NOTE: (Bull trout(?: \(Dolly Varden\))?) may only be retained (?:from )?(.+?)\. These fish may only be (?:taken )?from (lakes|the Liard River watershed[^.]*?) and only (\d+)-(\d+) cm", t)
        if m:
            sp = _species(m.group(1)); win = _window(m.group(2))
            note = "" if m.group(3) == "lakes" else "not-base: watershed-scoped"
            for c in claims:                   # the bullet above ("• 1 bull trout") is what this qualifies
                if c.kind == "cap" and c.species == sp and c.water is None and not c.note:
                    c.water, c.note = "lake", note
            claims.append(Claim(raw, sp, "cap", 1, water="lake", window=win, note=note))
            claims.append(Claim(raw, sp, "gate", 0, slot=(int(m.group(4)), int(m.group(5))), water="lake", note=note))
            continue
        # -- bullets ------------------------------------------------------------------------
        if t.startswith(("•", "―")):
            b = t.lstrip("•― ").strip()
            if mode == "release" or (mode == "retain" and False):
                claims += _release(raw, b, ctx); continue
            m = re.match(r"^(\d+) over (\d+) cm(?: \((\d+) hatchery steelhead over (\d+) cm allowed[^)]*\))?(?: \(quota includes hatchery steelhead\))?$", b)
            if m and ctx:
                claims.append(Claim(raw, ctx, "cap", int(m.group(1)), size=("over", int(m.group(2))), pooled=len(expand(ctx)) > 1, within=ctx))
                if m.group(3):
                    claims.append(Claim(raw, frozenset({"ST"}), "cap", int(m.group(3)), size=("over", int(m.group(4))), origin="hatchery", within=ctx))
                elif "includes hatchery steelhead" in b:
                    claims.append(Claim(raw, frozenset({"ST"}), "cap", int(m.group(1)), size=("over", int(m.group(2))), origin="hatchery", within=ctx))
                continue
            m = re.match(r"^(\d+) from streams(?: \(must be hatchery\))?(?: \(only (\d+) over (\d+) cm\))?$", b)
            if m and ctx:
                origin = "hatchery" if "must be hatchery" in b else None
                claims.append(Claim(raw, ctx, "quota", int(m.group(1)), water="stream", origin=origin, pooled=len(expand(ctx)) > 1))
                if m.group(2):
                    claims.append(Claim(raw, ctx, "cap", int(m.group(2)), size=("over", int(m.group(3))), water="stream", origin=origin, within=ctx))
                continue
            m = re.match(r"^(\d+) (rainbow trout or cutthroat trout) over (\d+) cm$", b)
            if m and ctx:
                claims.append(Claim(raw, _species(m.group(2)), "cap", int(m.group(1)), size=("over", int(m.group(3))), pooled=True, within=ctx)); continue
            m = re.match(r"^(\d+) trout from streams (.+)$", b)
            if m and ctx:
                claims.append(Claim(raw, frozenset({"TROUT"}), "cap", int(m.group(1)), water="stream", window=_window(m.group(2)), within=ctx)); continue
            m = re.match(r"^(\d+) \(none under (\d+) cm and only (\d+) over (\d+) cm\)$", b)
            if m and ctx:
                claims.append(Claim(raw, ctx, "quota", int(m.group(1))))
                claims.append(Claim(raw, ctx, "gate", 0, size=("under", int(m.group(2)))))
                claims.append(Claim(raw, ctx, "cap", int(m.group(3)), size=("over", int(m.group(4))), within=ctx)); continue
            m = re.match(r"^(\d+) (.+?)(?: of any size)?(?: combined)?(?:, none under (\d+) cm)?$", b)
            if m and ctx:
                sp = _species(m.group(2))
                if sp:
                    claims.append(Claim(raw, sp, "cap", int(m.group(1)), pooled=len(sp) > 1, within=ctx))
                    if m.group(3):
                        claims.append(Claim(raw, sp, "gate", 0, size=("under", int(m.group(3)))))
                    continue
            claims.append(Claim(raw, frozenset(), "unread")); continue
        if mode == "retain":
            m = re.match(r"^(\d+) (.+?) from streams$", t)
            if m:
                claims.append(Claim(raw, _species(m.group(2)), "quota", int(m.group(1)), water="stream")); mode = ""; continue
        # -- a species line -------------------------------------------------------------------
        m = re.match(r"^([A-Za-z/ ]+?):\s*(.*)$", t)
        if m and _species(m.group(1)):
            mode = ""
            sp = _species(m.group(1)); rest = m.group(2).strip()
            ctx = sp
            if not rest:
                continue
            if re.match(r"^0 quota, CLOSED TO( ALL)? FISHING", rest) or rest.startswith("CLOSED TO ALL FISHING") and "Watershed" not in rest:
                claims.append(Claim(raw, sp, "closed", 0)); continue
            if re.match(r"^(CATCH AND RELEASE ONLY|catch and release only)$", rest):
                claims.append(Claim(raw, sp, "release", 0)); continue
            if rest.startswith("CLOSED TO ALL FISHING in the Fraser River Watershed") or rest.startswith("CATCH AND RELEASE in the Fraser"):
                claims.append(Claim(raw, sp, "closed", 0, note="not-base: watershed-scoped")); continue
            if rest.startswith("unlimited"):
                claims.append(Claim(raw, sp, "unlimited")); continue
            mm = re.match(r"^(\d+)(.*)$", rest)
            if mm:
                n, tail = int(mm.group(1)), mm.group(2)
                pooled = "all species combined" in tail or len(expand(sp)) > 1
                if not re.search(r"but not more than", tail):
                    claims.append(Claim(raw, sp, "quota", n, pooled=pooled))
                else:
                    claims.append(Claim(raw, sp, "quota", n, pooled=pooled))
                ms = re.search(r"\(none from streams(?:, except (.+?))?\)", tail)
                if ms:
                    claims.append(Claim(raw, sp, "release", 0, water="stream",
                                        note=("except " + ms.group(1)) if ms.group(1) else ""))
                ms = re.search(r"no more than (\d+) over (\d+) cm|\(only (\d+) over (\d+) cm\)", tail)
                if ms:
                    c, cm = (ms.group(1) or ms.group(3)), (ms.group(2) or ms.group(4))
                    claims.append(Claim(raw, sp, "cap", int(c), size=("over", int(cm)), within=sp))
                ms = re.search(r"\(excluding (.+?)(?: - see page \d+ for quota)?\)", tail)
                if ms:
                    claims[-1].note = "except " + ms.group(1)
                continue
        # a release line with no bullet (Region 8 prints them bare)
        if mode == "release":
            claims += _release(raw, t, ctx); continue
        claims.append(Claim(raw, frozenset(), "unread"))
    return claims


def _release(raw: str, b: str, ctx) -> List[Claim]:
    """"Bull trout (Dolly Varden) from streams, Aug 1-Oct 31", "Hatchery trout/char under
    30 cm from streams", "all from streams, Apr 1-May 15" (the species is the line above)."""
    text = b.rstrip(":")
    low = text.lower()
    if "watershed" in low or "williston lake" in low:
        return [Claim(raw, _species(text) or ctx or frozenset(), "release", 0, note="not-base: watershed-scoped")]
    origin = "wild" if re.search(r"\bwild\b", low) else "hatchery" if re.search(r"\bhatchery\b", low) else None
    water = "stream" if re.search(r"from (any )?streams?", low) else "lake" if "from lakes" in low else None
    win = _window(text)
    sp = _species(re.sub(r"\(includes dolly varden\)", "/dolly varden", low)) or ctx or frozenset()
    if not sp:
        return [Claim(raw, frozenset(), "unread")]
    m = re.search(r"under (\d+) cm", low)
    if m:
        return [Claim(raw, sp, "gate", 0, size=("under", int(m.group(1))), water=water, origin=origin, window=win)]
    return [Claim(raw, sp, "release", 0, water=water, origin=origin, window=win)]


# ----------------------------------------------------------------------------------------
# the base, by content
# ----------------------------------------------------------------------------------------
def base_ledger(reg: str, kind: str) -> Ledger:
    """The standing table for a region from the corpus — the same rules `build.base` uses,
    without going through a section, so a region with no shipped water is still checked."""
    from pipeline.regs.table.corpus import rules
    if reg == "1hg":
        rs = [x for x in rules() if x["entry"].startswith("z1:hg_") or
              (x["entry"].startswith("zp:") and source_of(x).is_base)]
    else:
        rs = [x for x in rules() if (x["entry"].startswith(f"z{reg}:") or x["entry"].startswith("zp:"))
              and source_of(x).is_base]
    alw, lifted, fam, mults, duties, unresolved = allowances(rs, kind)
    return Ledger(alw, lifted=lifted, family=fam, multiples=mults, duties=duties, water_kind=kind)


def _size_of(a: Allowance):
    s = a.scope.size
    if s.kind == "counts_over": return ("over", s.lo)
    if s.kind == "counts_under": return ("over", s.hi)
    if s.kind == "none_under": return ("under", s.hi)
    if s.kind == "none_over": return ("over", s.lo)
    return None


def _fish_eq(a: Allowance, sp: FrozenSet[str]) -> bool:
    return expand(a.scope.fish) == expand(sp) or a.scope.fish == sp


def _windows_eq(a: Allowance, win) -> bool:
    if win is None:
        return not a.applies.windows or a.applies.unless
    return tuple(a.applies.windows[:1]) == (win,) if a.applies.windows else False


def satisfies(L: Ledger, c: Claim, kind: str) -> Optional[Allowance]:
    """The base allowance that says what the printed line says — or None."""
    if c.kind == "multiple":
        for subj, n, src in L.multiples:
            if n == c.n and (expand(subj.fish) == expand(c.species) or (c.species == {"ALL_GAME_FISH"} and subj.is_everything)):
                return Allowance(subj, __import__("pipeline.regs.table.outcome", fromlist=["Outcome"]).Outcome("unlimited"), src)
        return None
    for a in L.allowances:
        if a.derived_from is not None or not _fish_eq(a, c.species):
            continue
        if c.origin and a.scope.origin.value != c.origin:
            continue
        if not c.origin and a.scope.origin is not Origin.both and c.kind not in ("annual",):
            continue
        if c.kind == "annual":
            if a.period == "annual" and a.n == c.n: return a
            continue
        if a.period != "daily":
            continue
        if c.kind == "release" and a.kind == "release" and _windows_eq(a, c.window) and a.scope.size.is_any: return a
        if c.kind == "unlimited" and a.kind == "unlimited": return a
        if c.kind == "closed" and a.kind == "closed" and _windows_eq(a, c.window): return a
        if c.kind == "gate" and a.kind == "gate":
            if c.slot and a.scope.size.kind in ("slot", "band") and {a.scope.size.lo, a.scope.size.hi} == set(c.slot): return a
            if c.size and _size_of(a) == c.size and (c.window is None or _windows_eq(a, c.window)): return a
        if c.kind == "quota" and a.kind == "quota" and a.n == c.n and a.scope.size.is_any and not a.within \
                and _windows_eq(a, c.window):
            return a
        # A CAP: the same number on the same size class. The print sometimes states the
        # window in a NOTE two lines away, so a cap with no printed window matches a windowed one.
        if c.kind == "cap" and a.kind == "quota" and a.n == c.n and (_size_of(a) == c.size) \
                and (c.window is None or _windows_eq(a, c.window)):
            return a
    return None


# ----------------------------------------------------------------------------------------
# the panel: print beside table, line by line
# ----------------------------------------------------------------------------------------
@dataclass
class Check:
    line: str
    ok: bool
    why: str = ""
    rule: str = ""
    label: str = ""          # "" | "curation" | "bug" | "not-base" | "unread"
    found: str = ""          # on a ✓: the table's words and the sentence they were read from


def _proof(a: Allowance, name) -> str:
    """What a ✓ rests on: the table's line, and the catalogue sentence behind it."""
    return _words(a, name) + (f" — “{a.source.verbatim}”" if a.source.verbatim else "")


@dataclass
class Panel:
    region: str
    kind: str
    title: str
    source: str
    url: str
    md5: str
    lines: List[str]
    checks: List[Check] = field(default_factory=list)
    gov: str = ""            # the gov.bc.ca chapter this box was proved identical to


def _words(a: Allowance, name) -> str:
    who, q = a.scope.words(name)
    return f"{a.word()} {who}" + (f" · {q}" if q else "") + (f" · {a.applies.detail}" if a.applies.detail else "")


def check_region(reg: str, kind: str, name=lambda c: c, pdf: str = PDF) -> Panel:
    page, _ = CHAPTERS[reg]
    lines = box_lines(reg, pdf)
    gov_file, gov_md5 = GOV_CHAPTERS[reg]
    src = f"{os.path.basename(pdf)} · {EDITION} · page {page + 1}"
    P = Panel(reg, kind, f"Region {REGION_OF.get(reg, reg).upper()}{' — Haida Gwaii' if reg == '1hg' else ''} · {kind}s",
              src, GOV + gov_file, _md5(os.path.dirname(pdf), os.path.basename(pdf)),
              [t for m, t in lines if m == "box" or _CLOSURE.match(t)])
    P.gov = f"{gov_file} · md5 {gov_md5}"
    L = base_ledger(reg, kind)
    claimed = set()
    for c in parse(lines):
        if c.kind == "unread":
            P.checks.append(Check(c.text, False, "this line was not understood", label="unread")); continue
        if c.note.startswith("not-base"):
            P.checks.append(Check(c.text, True, "scoped to a watershed — an override, not a line of the standing table", label="not-base")); continue
        if c.water and c.water != kind:
            # a stream line on the lake table (or the reverse): the table must NOT carry it
            a = satisfies(L, c, kind)
            P.checks.append(Check(c.text, a is None,
                                  f"the {kind} table carries a {c.water} rule" if a else f"a {c.water} line — correctly absent from the {kind} table",
                                  a.rule_id if a else "", "other-kind" if a is None else "bug"))
            continue
        a = satisfies(L, c, kind)
        if a is not None:
            claimed.add(a.rule_id)
            for b in L.allowances:             # a second rule saying the same thing is claimed too
                if b is not a and b.derived_from is None and satisfies(Ledger([b]), c, kind) is not None:
                    claimed.add(b.rule_id)
            P.checks.append(Check(c.text, True, "", a.rule_id, found=_proof(a, name)))
        else:
            have = [_words(x, name) for x in L.allowances if x.derived_from is None and _fish_eq(x, c.species)]
            P.checks.append(Check(c.text, False, "the table has: " + ("; ".join(have) if have else "nothing for this fish"), "", "bug"))
    # the other direction: a region line the print does not carry
    for a in L.allowances:
        if a.derived_from is not None or a.source.authority is not Authority.region or a.rule_id in claimed:
            continue
        if not L.in_force(a) and L.status[a] != "replaced here by its own clause for this kind of water":
            continue
        if a.applies.kind == "somewhere":
            continue
        P.checks.append(Check(f"[table] {_words(a, name)}", False, "in the table, not in the printed box", a.rule_id, "bug"))
    return P


def check_province(name=lambda c: c, pdf: str = PDF) -> Panel:
    """The province's lines, found by pattern on the pages that carry them — read column by
    column, since the page's three columns interleave when read whole."""
    from pipeline.regs.table.corpus import rules
    folder, file = os.path.dirname(pdf), os.path.basename(pdf)
    flat = " ".join(t for p in PROVINCE_PAGES for col in ((0, 205), (205, 380), (380, 576))
                    for _, _, t in _raw_lines(folder, file, p, col[0], col[1], 55, 725))
    flat = re.sub(r"\s+", " ", flat)
    P = Panel("p", "any", "Provincial", f"{file} · {EDITION} · pages {PROVINCE_PAGES[0] + 1}–{PROVINCE_PAGES[-1] + 1}",
              FULL_URL, _md5(folder, file), [])
    rs = [x for x in rules() if x["entry"].startswith("zp:") and source_of(x).is_base]
    alw, lifted, fam, mults, duties, unresolved = allowances(rs, "lake")
    L = Ledger(alw, lifted=lifted, family=fam, multiples=mults, duties=duties, water_kind="lake")
    def find(pattern):
        m = re.search(pattern, flat, re.I)
        return m.group(0) if m else None
    s = find(r"The annual province-wide quota for hatchery steelhead is (\d+)\.")
    P.lines.append(s or "(not found: annual hatchery steelhead)")
    a = satisfies(L, Claim(s or "", frozenset({"ST"}), "annual", 10, origin="hatchery"), "lake")
    P.checks.append(Check(s or "annual hatchery steelhead: 10", a is not None, "" if a else "no province-wide annual steelhead counter", a.rule_id if a else "", "" if a else "bug", found=_proof(a, name) if a else ""))
    s = find(r"All wild steelhead must be released\.")
    P.lines.append(s or "(not found: wild steelhead)")
    a = satisfies(L, Claim(s or "", frozenset({"ST"}), "release", 0, origin="wild"), "lake")
    P.checks.append(Check(s or "All wild steelhead must be released", a is not None, "" if a else "no wild steelhead release", a.rule_id if a else "", "" if a else "bug", found=_proof(a, name) if a else ""))
    s = find(r"Possession quota[^.]*?twice the daily quota[^.]*?\.")
    P.lines.append(s or "(not found: possession quota)")
    a = satisfies(L, Claim(s or "", frozenset({"ALL_GAME_FISH"}), "multiple", 2), "lake")
    P.checks.append(Check(s or "possession = twice the daily quota", a is not None, "" if a else "no province-wide possession multiple", a.rule_id if a else "", "" if a else "bug", found=_proof(a, name) if a else ""))
    s = find(r"It is illegal to fish for, or catch and retain[^.]*\.")
    P.lines.append(s or "(not found: protected species)")
    a = next((x for x in L.allowances if x.scope.fish == {"PROTECTED_SPECIES"} and x.kind == "closed"), None)
    P.checks.append(Check(s or "protected species: closed", a is not None, "" if a else "no protected-species closure", a.rule_id if a else "", "" if a else "bug", found=_proof(a, name) if a else ""))
    return P


def all_panels(name=lambda c: c, pdf: str = PDF) -> List[Panel]:
    out = [check_province(name, pdf)]
    for reg in CHAPTERS:
        for kind in ("lake", "stream"):
            out.append(check_region(reg, kind, name, pdf))
    return out


def edition(pdf: str = PDF) -> str:
    import pdfplumber
    with pdfplumber.open(pdf) as doc:
        m = re.search(r"20\d\d\s*[-–]\s*20\d\d", doc.pages[0].extract_text() or "")
        return re.sub(r"\s+", "", m.group(0)) if m else ""


def panel_json(P: Panel) -> dict:
    return {"region": P.region, "kind": P.kind, "title": P.title, "source": P.source, "url": P.url,
            "md5": P.md5, "gov": P.gov, "lines": P.lines,
            "checks": [{"line": c.line, "ok": c.ok, "why": c.why, "rule": c.rule, "label": c.label, "found": c.found} for c in P.checks]}
