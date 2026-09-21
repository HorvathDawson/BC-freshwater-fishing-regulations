"""THE PRINT, READ BY MACHINE: the standing gear tables against the synopsis PDFs.

Every expectation here is a sentence found by a pattern in text extracted from an official
PDF — never a curated label, never a `verbatim` field (a curated verbatim is the thing under
test). Every line reports the file, the edition, the page and the sentence it matched, so an
auditor can put a finger on it. The table side is matched on CONTENT — the content a term sets,
its water kind, who authored it, a verdict — never on the words of a label.

SOURCES  data/source/fishing_synopsis.pdf — the full synopsis in the repository, edition
         2025-2027, git-tracked (see quota_print for its md5 and why it is the source).
         Regional chapters: the "General Regulations" panel on each region's second page —
         15, 23, 30, 36, 48, 55, 64, 70, 74. The province: page 10, "Provincial Regulations".
         gov.bc.ca's own chapter files, when fetched into data/source/official/, are the
         cross-check: the panel text read from the repo copy must equal the text read from
         the byte-identical chapter (`cross_check`).
"""
from __future__ import annotations
import hashlib, os, re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pipeline.regs.table.authority import Authority
from pipeline.regs.table.method import MethodTable, MethodRow, Term, METHODS, NAMES
from pipeline.regs.table.method_build import region_base, provincial_base

from pipeline.regs.table.quota_print import (PDF, PDF_MD5, EDITION, GOV, GOV_CHAPTERS, OFFICIAL_DIR,
                                             FULL_URL)
#: region -> (page index in the full synopsis, the column of the regional-regulations panel as
#: a width fraction). The regional chapter files carry the same page as their page 2.
CHAPTERS = {"1": (14, (0, .36)), "2": (22, (0, .36)), "3": (29, (0, .36)), "4": (35, (0, .36)),
            "5": (47, (0, .36)), "6": (54, (0, .36)), "7a": (63, (0, .36)), "7b": (69, (.34, .655)),
            "8": (73, (0, .36))}
PROVINCE_PAGE = 9
PROVINCE_URL = GOV + "2025-2027_freshwater_fishing_regulations_synopsis.pdf"
#: the R5 chapter prints its ice-hut warning in the middle column
EXTRA_COLUMNS = {"5": (.34, .68)}


@dataclass
class Check:
    line: str                 # the expectation, in the print's own words
    ok: bool
    sentence: str             # the sentence the pattern matched, verbatim from the PDF
    source: str               # "region_6_skeena.pdf · 2025-2027 · page 2"
    url: str
    why: str = ""             # on ✗: what the table says instead
    label: str = ""           # "" | "curation" | "bug" | "not-print-verified"


@dataclass
class Panel:
    title: str
    source: str
    url: str
    md5: str
    text: str
    checks: List[Check] = field(default_factory=list)


def _pdf_text(pdf: str, page: int, col: Tuple[float, float]) -> str:
    import pdfplumber
    p = pdfplumber.open(pdf).pages[page]
    w, h = p.width, p.height
    t = p.crop((w * col[0], 0, w * col[1], h)).extract_text() or ""
    # A column crop catches the first letters of the next column ("Ru", "“B", "us"); a line of
    # three characters or fewer is that debris, never a sentence.
    return "\n".join(l for l in t.splitlines() if len(l.strip()) > 3)


def _md5(pdf: str) -> str:
    return hashlib.md5(open(pdf, "rb").read()).hexdigest()[:8]


def _sentences(text: str) -> List[str]:
    """Lines joined, then split at sentence ends and at the bold lead-ins the panel uses."""
    t = re.sub(r"\s+", " ", text)
    return [s.strip() for s in re.split(r"(?<=[.!])\s+(?=[A-Z“\"(])", t) if s.strip()]


def _find(sentences: List[str], pattern: str) -> Optional[str]:
    rx = re.compile(pattern, re.I)
    for s in sentences:
        if rx.search(s):
            return s
    return None


# -- what the table says, by content ------------------------------------------------------
def _region_rig(T: MethodTable, key: str, says_has: Dict[str, str] = None, water: str = None) -> Optional[Term]:
    for ts in T.rig("angling").values():
        for t in ts:
            if t.source.authority is not Authority.region or t.key != key:
                continue
            d = dict(t.says)
            if says_has and any(d.get(k) != v for k, v in says_has.items()):
                continue
            if water and d.get("water") != water:
                continue
            return t
    for t, st in T.folded("angling"):
        if t.source.authority is Authority.region and t.key == key and (not water or dict(t.says).get("water") == water):
            return t
    return None


def _condition_with(T: MethodTable, method: str, word: str) -> Optional[Term]:
    for t in MethodRow(T, method).conditions():
        if word.lower() in t.text.lower():
            return t
    return None


# -- the regional chapters ---------------------------------------------------------------
def region_panel(reg: str, pdf: str = PDF, page: Optional[int] = None) -> Panel:
    """The General Regulations panel of a region — from the repo synopsis by default, or
    from any file that holds the same page at `page` (a gov.bc.ca chapter's page 2)."""
    pidx, col = CHAPTERS[reg]
    if page is not None:
        pidx = page
    text = _pdf_text(pdf, pidx, col)
    if reg in EXTRA_COLUMNS:
        text += "\n" + _pdf_text(pdf, pidx, EXTRA_COLUMNS[reg])
    src = f"{os.path.basename(pdf)} · {EDITION} · page {pidx + 1}"
    return Panel(f"Region {reg.upper()} — General Regulations", src, GOV + GOV_CHAPTERS[reg][0], _md5(pdf), text)


def cross_check(folder: str = OFFICIAL_DIR) -> Dict[str, str]:
    """Does each region's panel text read from the repo synopsis equal the text read from
    the chapter gov.bc.ca serves? Per region: "same", "differs", "md5 changed: …" or
    "missing: <path>" — a missing file is not a disagreement."""
    out = {}
    for reg, (pidx, _) in CHAPTERS.items():
        file, md5 = GOV_CHAPTERS[reg]
        path = os.path.join(folder, file)
        if not os.path.exists(path):
            out[reg] = f"missing: {path}"; continue
        if _md5(path) != md5:
            out[reg] = f"md5 changed: {file} is {_md5(path)}, the site served {md5}"; continue
        out[reg] = "same" if region_panel(reg).text == region_panel(reg, path, 1).text else "differs"
    return out


def check_region(reg: str, kind: str, pdf: str = PDF) -> Panel:
    P = region_panel(reg, pdf)
    S = _sentences(P.text)
    T = region_base(reg, kind)
    zone = {"7a": "Zone A", "7b": "Zone B"}.get(reg, f"Region {reg}")
    def add(line, ok, sentence, why="", label="", needs_sentence=True):
        if needs_sentence and sentence is None:
            ok, why, label = False, "the pattern found no such sentence in the PDF text", "checker"
        P.checks.append(Check(line, ok, sentence or "", P.source, P.url, why, "" if ok else (label or "bug")))

    # Single barbless hook: must be used in all streams of Region N, all year.
    s = _find(S, rf"single barbless hook:? must be used in all streams of {re.escape(zone)}")
    t = _region_rig(T, "barbless", {"barbless": "True", "hook_count": "1"}, "stream")
    if kind == "stream":
        add("Single barbless hook in all streams, all year", s is not None and t is not None and t.applies.always, s,
            "" if t else "no region-authored barbless+single-hook stream rule in the table")
    else:
        add("Single barbless hook is a STREAM rule — a lake must not carry it", s is not None and t is None, s,
            "the lake table carries a stream barbless rule" if t else "")
    # Bait ban: applies to all streams of Region N, all year.
    s = _find(S, rf"bait ban:? applies to all streams of {re.escape(zone)}")
    t = _region_rig(T, "bait:any", None, "stream")
    if s is not None:
        if kind == "stream":
            add("Bait ban in all streams, all year", t is not None and t.applies.always, s,
                "" if t else "no region-authored stream bait ban in the table")
        else:
            add("Bait ban is a STREAM rule — a lake must not carry it", t is None, s)
    elif t is not None:
        add("table prints a region-wide stream bait ban the panel does not state", False, "", f"{t.text}", "bug", needs_sentence=False)
    # Fin fish may not be used as bait in any waters of Zone B.
    s = _find(S, r"fin fish.*may not be used as bait in any waters")
    if s is not None:
        t = _region_rig(T, "bait:fin_fish")
        add("Fin fish may not be used as bait in any waters (both kinds)", t is not None, s,
            "" if t else "no region-authored fin-fish bait rule")
    # Set lining
    s = _find(S, r"set lining:? is (only permitted in the lakes|not permitted)")
    row = MethodRow(T, "set_lining")
    if s is not None:
        want = "not allowed" if "not permitted" in s.lower() or kind == "stream" else "allowed"
        add(f"Set lining {want} on {kind}s", row.verdict_word() == want, s,
            "" if row.verdict_word() == want else f"the table says “{row.verdict_word()}”")
        if want == "allowed":
            for word, line in (("single hook", "one line with a single hook"), ("3 cm", "gap of not less than 3 cm"),
                               ("marked", "marked with the angler's name, address and telephone number")):
                c = _condition_with(T, "set_lining", word)
                add(f"Set line condition: {line}", c is not None, s, "" if c else f"no set-line condition mentions “{word}”")
            L = T.keep("set_lining")
            rel = L is not None and any(a.outcome.kind == "release" and "BB" in a.scope.excepts for a in L.allowances)
            add("Any game fish other than burbot taken on a set line must be released", rel, s)
    else:
        add("Set lining: not stated in this chapter — the province's rule decides (page 10)", row.verdict_word() == "not allowed",
            "", "" if row.verdict_word() == "not allowed" else f"the table says “{row.verdict_word()}”", needs_sentence=False)
    # Ice fishing huts
    s = _find(S, r"ice fishing huts")
    if s is not None:
        c = _condition_with(T, "ice_fishing", "hut")
        if kind == "lake":
            add("Ice fishing huts warning is a condition of ice fishing", c is not None, s,
                "" if c else "no ice-fishing condition mentions huts")
        else:
            add("Ice fishing huts is a LAKE rule (feature_types: lake) — a stream must not carry it", c is None, s)
    # Crayfish trapping advisory (Region 8)
    s = _find(S, r"crayfish trapping\b.*(turtle|trap)")
    if s is not None:
        c = _condition_with(T, "crayfish_trapping", "trap")
        add("Crayfish trapping advisory (turtles) is a condition of crayfish trapping", c is not None, s,
            "" if c else "no crayfish condition mentions traps", "curation" if c is None else "")
    # Named-water bait bans the panel states are WATER rules, not the standing table (Skeena/Nass)
    s = _find(S, r"bait ban:? in the .* river, including tributaries")
    if s is not None:
        t = _region_rig(T, "bait:any", None, None)
        stream_ban = t is not None and dict(t.says).get("water") == "stream"
        add("A named-water bait ban (Skeena, Nass) is an override on those waters, not a line of the standing table",
            t is None or stream_ban, s)
    # Spear fishing: page 10 decides the region list; the chapter says nothing. Marked as such.
    spear = MethodRow(T, "spear_fishing").verdict_word()
    want = "not allowed" if reg in ("1", "2", "4") else "allowed"
    P.checks.append(Check(f"Spear fishing {want} (page 10: “No spear fishing of any kind is permitted in Region 1, 2, and 4”)",
                          spear == want, "", f"{os.path.basename(pdf)} · {EDITION} · page {PROVINCE_PAGE + 1}", PROVINCE_URL,
                          "" if spear == want else f"the table says “{spear}”", "" if spear == want else "bug"))
    freed = {f for fish, _ in T.lifted_fish.get("spear_fishing", []) for f in fish}
    want_free = {"BB"} if reg in ("3", "5", "6", "7a", "7b", "8") else set()
    P.checks.append(Check(("Burbot may be speared here" if want_free else "Burbot may not be speared here")
                          + " (page 10: “except burbot, which may also be speared in Regions 3, 5, 6, 7 and 8”)",
                          freed == want_free, "", f"{os.path.basename(pdf)} · {EDITION} · page {PROVINCE_PAGE + 1}", PROVINCE_URL,
                          "" if freed == want_free else f"the table frees {sorted(freed)}", "" if freed == want_free else "bug"))
    return P


# -- the province, page 10 ------------------------------------------------------------------
def province_panel(pdf: str = PDF) -> Panel:
    page = PROVINCE_PAGE
    text = "\n\n".join(_pdf_text(pdf, page, c) for c in ((0, .335), (.335, .655), (.655, 1.0)))
    return Panel("Provincial Regulations", f"{os.path.basename(pdf)} · {EDITION} · page {page + 1}", PROVINCE_URL, _md5(pdf), text)


def check_province(kind: str, pdf: str = PDF) -> Panel:
    P = province_panel(pdf)
    S = _sentences(P.text)
    T = provincial_base(kind)
    printed = {t.key: t for ts in T.rig("angling").values() for t in ts}
    printed_all = [t for ts in T.rig("angling").values() for t in ts] + [t for t, _ in T.folded("angling")]
    def has(key, **says):
        for t in printed_all:
            if t.key == key and all(dict(t.says).get(k) == v for k, v in says.items()):
                return t
        return None
    def add(line, ok, sentence, why=""):
        if sentence is None:
            ok, why = False, "the pattern found no such sentence in the PDF text"
        P.checks.append(Check(line, ok, sentence or "", P.source, P.url, why, "" if ok else ("checker" if sentence is None else "bug")))
    # what a licence entitles you to
    s = _find(S, r"angle with one fishing line to which only one hook, one artificial lure or one artificial fly")
    add("One fishing line, one hook or lure or fly", has("max_lines", max_lines="1") is not None and has("hook_count", hook_count="1") is not None, s)
    s = _find(S, r"angle with a downrigger")
    add("A downrigger, with a quick-release", any("downrigger" in t.text for t in printed_all), s)
    s = _find(S, r"ice fish with one line and one lure")
    ice = [t for t in MethodRow(T, "ice_fishing").conditions()]
    add("Ice fishing permitted: one line, one lure; warn others of your ice hole",
        MethodRow(T, "ice_fishing").verdict_word() == "allowed" and any(dict(t.says).get("max_lines") == "1" for t in ice)
        and any("lure" in t.text.lower() for t in ice) and _condition_with(T, "ice_fishing", "ice hole") is not None, s)
    s = _find(S, r"you may only fish with a set line .* in lakes of region 6 and region 7a")
    gov = MethodRow(T, "set_lining").standing()
    add("Set line only in lakes of Region 6 and Region 7A — so not allowed in the province at large",
        MethodRow(T, "set_lining").verdict_word() == "not allowed" and "Regions 6 and 7A" in gov.source.verbatim, s)
    s = _find(S, r"fish with a spear or an arrow")
    add("Spear or bow permitted", MethodRow(T, "spear_fishing").verdict_word() == "allowed", s)
    s = _find(S, r"only non-game fish .* may be speared")
    L = T.keep("spear_fishing")
    add("Only non-game fish may be speared — game fish closed to the spear",
        L is not None and any(a.outcome.kind == "closed" and "ALL_GAME_FISH" in a.scope.fish for a in L.allowances), s)
    s = _find(S, r"no spear fishing of any other game fish.*pacific salmon")
    add("Pacific salmon closed to the spear", L is not None and any(a.outcome.kind == "closed" and "SA" in a.scope.fish for a in L.allowances), s)
    s = _find(S, r"trap crayfish with any number or size of traps")
    add("Crayfish trapping permitted; release all fin fish caught in the trap",
        MethodRow(T, "crayfish_trapping").verdict_word() == "allowed" and T.keep("crayfish_trapping") is not None
        and any(a.outcome.kind == "release" and "CRA" in a.scope.excepts for a in T.keep("crayfish_trapping").allowances), s)
    s = _find(S, r"all other methods of taking fin fish and crayfish are illegal")
    add("All other methods are illegal — the default for every method the book does not permit here",
        "All other methods" in gov.source.verbatim, s)
    # unlawful
    s = _find(S, r"use barbed hooks or a hook with more than one point in any river, stream, creek or slough")
    if kind == "stream":
        add("Barbless, single-point hooks in streams", has("barbless", barbless="True", water="stream") is not None
            and has("hook_count", hook_count="1", water="stream") is not None, s)
    else:
        add("Barbed hooks are permitted in lakes — no barbless rule on a lake", has("barbless") is None, s)
    s = _find(S, r"more than one artificial fly")
    add("No more than one artificial fly on the line", has("max_flies") is not None, s)
    s = _find(S, r"use a light in any manner to attract fish")
    add("No light to attract fish unless submerged within 1 m of the hook", any("light" in t.text.lower() for t in printed_all), s)
    s = _find(S, r"fish with nets")
    add("Nets not allowed", MethodRow(T, "netting").verdict_word() == "not allowed", s)
    s = _find(S, r"snag \(foul hook\) fish")
    add("Snagging not allowed; a snagged fish must be released", MethodRow(T, "snagging").verdict_word() == "not allowed"
        and T.keep("snagging") is not None, s)
    s = _find(S, r"angle with more than one line, except a person who is alone in a boat on a lake")
    if kind == "lake":
        add("Two lines when alone in a boat on a lake", has("max_lines", max_lines="2") is not None, s)
    else:
        add("Two-lines-alone-in-a-boat is a LAKE exception — not on a stream", has("max_lines", max_lines="2") is None, s)
    s = _find(S, r"place any fishing gear in any water during a no fishing period")
    add("No gear in the water during a No Fishing period", any(t.kind == "while_closed" for t in T.terms), s)
    s = _find(S, r"more than 1 kg of weight")
    add("No more than 1 kg of weight on the line", has("max_weight_kg") is not None, s)
    # bait
    s = _find(S, r"roe.*you must not have more than 1 kg of roe")
    add("Roe may be used (up to 1 kg in possession)", has("bait:roe") is not None and has("bait:roe").allows, s)
    s = _find(S, r"you may use freshwater invertebrates .* in streams as bait unless a bait ban applies")
    if kind == "stream":
        add("Freshwater invertebrates may be used in streams", has("bait:invertebrate") is not None and has("bait:invertebrate").allows, s)
    else:
        s2 = _find(S, r"no person shall use as bait .* freshwater invertebrate .* at a lake")
        add("No freshwater invertebrates as bait at a lake", has("bait:invertebrate") is not None and not has("bait:invertebrate").allows, s2)
    s = _find(S, r"chumming.*is prohibited")
    add("Chumming not allowed", MethodRow(T, "chumming").verdict_word() == "not allowed", s)
    s = _find(S, r"the use of fin fish \(dead or alive\) or parts of fin fish other than roe is prohibited")
    add("Fin fish may not be used as bait, province-wide", has("bait:fin_fish") is not None and not has("bait:fin_fish").allows, s)
    return P


def all_panels(pdf: str = PDF) -> List[Tuple[str, str, Panel]]:
    out = []
    for kind in ("stream", "lake"):
        out.append(("province", kind, check_province(kind, pdf)))
    for reg in CHAPTERS:
        for kind in ("stream", "lake"):
            out.append((reg, kind, check_region(reg, kind, pdf)))
    return out


def failures(pdf: str = PDF) -> List[Tuple[str, str, Check]]:
    return [(reg, kind, c) for reg, kind, P in all_panels(pdf) for c in P.checks if not c.ok]


if __name__ == "__main__":
    for reg, kind, P in all_panels():
        print(f"\n== {reg} · {kind}s   [{P.source}  md5 {P.md5}]")
        for c in P.checks:
            print(f"   {'✓' if c.ok else '✗'} {c.line}" + (f"   — {c.why}" if c.why else ""))
            if c.sentence: print(f"       “{c.sentence[:110]}”")
