"""THE REGION HEADER (owner 2026-10-28): the rules DFO prints above each region's table — daylight only, the 100 m
facility closure, the aggregate daily / possession / annual limits, minimum sizes, how a fish is measured, what an
"adult chinook" is — read as TYPED NOTES, each carrying DFO's own sentence.

    parse_header(html, slug) -> [note]          every list item / paragraph between the page title and "Related links"
    resolve(notes, entry_file, upstream=None)   the waters a scoped note names -> registry items (never guessed)

A note is `{id, k, t, ...}`: `k` its kind, `t` DFO's words verbatim (a nested item carries its lead-in: "An 'adult
chinook' in Region 2 is ... over 62 cm: the Fraser River from Sep 1 to Dec 31"). Values are DERIVED from those words
only (`n` a count, `cm` a length, `sp` species codes, `on` day-index windows); a sentence nothing here recognises is
kept as kind `other`, quoted, never dropped. NEVER INVENTED: a note exists only where DFO printed the sentence.

REGION-WIDE vs SCOPED. A note with `w` (the waters it names: `[{n, ws, key}]`, `ws` = the whole watershed) holds only on
those waters; `x` (waters it excepts) / `xp` (excepted PARTS of waters, which no item can stand for, so the note still
shows there, verbatim) narrow a region-wide note. Names become items only through the curated DFO water bindings
(`entries.WaterBinding`, the one place a name becomes geometry) — exact name, or the name with its "(…)" qualifier
dropped when that is one water; a watershed adds every item upstream (the registry's `item_down`, read by the caller).
A name that binds nothing is UNRESOLVED and listed for the owner, never matched by nearness.

KINDS: daylight · facility · closed (no salmon fishing until further notice) · daily (all salmon combined) · possession
· annual / monthly (a species over a length) · min_size · measure · adult / jack (a species' length class) · aggregate
(a scoped combined limit) · bait_ban · other.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pipeline.common import calendar_spec

FORMAT = "dfo-header/1"
_SP = {"chinook": "CH", "coho": "CO", "sockeye": "SK", "pink": "PK", "chum": "CM"}
_MONTHS = {m: i for i, m in enumerate("jan feb mar apr may jun jul aug sep oct nov dec".split(), 1)}
_RE_STOP = re.compile(r"<h[1-6][^>]*>\s*(?:Related links|Salmon limits, openings and closures)|<(?:p|li)[^>]*>\s*(?:This table refers to"
                      r"|Unless otherwise stated in the table|There is no fishing for salmon in [^<]* except for the opportunities)", re.I)
_DATE = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})"
_RE_LEADIN = re.compile(r"\s*You must be aware of these [^:]*:\s*$", re.I)
_RE_MUS = re.compile(r"Management Units \d", re.I)
_RE_WINDOW = re.compile(rf"\bfrom\s+{_DATE}\s+(?:to|-|–)\s+{_DATE}", re.I)


def _norm(t: str) -> str:
    t = t.replace(" ", " ").replace("“", '"').replace("”", '"').replace("’", "'")
    return re.sub(r"\s+", " ", t).strip()


def _items(html: str) -> List[Tuple[str, Optional[str], bool]]:
    """(text, lead-in of the item it is nested under, has children) for every header list item and paragraph."""
    from bs4 import BeautifulSoup
    from pipeline.regs.dfo_salmon.parse import _RE_H1, _RE_TABLE, _content_slice
    if not _RE_H1.search(html):                                 # an older layout: the first <h1> is the title
        h1 = re.search(r"<h1\b[^>]*>", html, re.I)
        html = html[h1.start():].replace(h1.group(0), '<h1 id="wb-cont">', 1) if h1 else html
    tm = _RE_TABLE.search(html)
    frag = _content_slice(html, tm.start() if tm else None)
    stop = _RE_STOP.search(frag)
    if stop:
        frag = frag[:stop.start()]
    soup = BeautifulSoup(frag, "html.parser")
    out: List[Tuple[str, Optional[str], bool]] = []

    def own_text(node) -> str:
        bits = [c.get_text(" ", strip=True) if getattr(c, "name", None) else str(c)
                for c in node.children if getattr(c, "name", None) not in ("ul", "ol", "li")]
        return _norm(" ".join(bits))

    for node in soup.find_all(["li", "p"]):
        if node.name == "p" and node.find_parent("li"):
            continue
        text = own_text(node)
        if not text or len(text) < 4 or _RE_MUS.search(text):
            continue                                             # the part's own title ("... Management Units 5-6 to 5-11")
        a = node.find("a")
        if a is not None and _norm(a.get_text(" ", strip=True)) == text:
            continue                                             # a bare link ("Learn more about fishing for salmon")
        parent = node.find_parent("li")
        lead = own_text(parent) if parent is not None else None
        out.append((text, lead, bool(node.find(["ul", "ol"]))))
    return out


def _species(text: str) -> List[str]:
    low = text.lower()
    return [code for name, code in _SP.items() if re.search(rf"\b{name}\b", low)]


def _day(mon: str, d: str) -> int:
    """The release's day index (Jan 1 = 0, Feb 29 = 59: `deliver.salmon.DAYS`)."""
    return calendar_spec.day_index(_MONTHS[mon[:3].lower()], int(d)) - 1


def _windows(text: str) -> List[List[int]]:
    return [[_day(a, b), _day(c, d)] for a, b, c, d in _RE_WINDOW.findall(text)]


def _names(phrase: str) -> List[str]:
    """"the Chehalis and Harrison Rivers" -> [Chehalis River, Harrison River]; "the Chilliwack/Vedder River (including
    Sumas River) and the Capilano River" -> both, verbatim."""
    p = _RE_WINDOW.sub("", phrase).strip().rstrip(".,;: ")
    p = re.sub(r"^the\s+", "", p, flags=re.I)
    m = re.fullmatch(r"(.+?) and (.+?) (Rivers|Creeks|Lakes)", p)
    if m:
        kind = m.group(3)[:-1]
        return [f"{m.group(1)} {kind}", f"{m.group(2)} {kind}"]
    return [re.sub(r"^the\s+", "", x.strip(), flags=re.I) for x in re.split(r",?\s+and\s+the\s+|;\s*", p) if x.strip()]


def _water(name: str, ws: bool = False) -> dict:
    from pipeline.common.names import key
    return {"n": name, "ws": ws, "key": key(name) + ("+ws" if ws else "")}


def classify(text: str, lead: Optional[str] = None) -> Optional[dict]:
    """One header sentence -> a typed note (None for a bare lead-in like "You must be aware of these limits:")."""
    full = f"{lead} {text}" if lead else text
    low = text.lower()
    if lead and re.search(r"adult (chinook|coho)", lead, re.I):
        # a nested exception of an "adult" definition: the waters (and windows) where the other length holds
        m = re.search(r"(?:over|greater than)\s+(\d+)\s*cm:?\s*$", lead, re.I)
        sp = _species(lead.split("defined")[0])
        return {"k": "adult", "t": full, "sp": sp, **({"cm": int(m.group(1))} if m else {}),
                "w": [_water(n) for n in _names(text)], **({"on": _windows(text)} if _windows(text) else {})}
    if lead and re.search(r"bait ban", lead, re.I):
        m = re.search(r"except:?\s*(.+?)(?:,?\s*unless otherwise described below)?[.:]?$", text, re.I)
        out = {"k": "bait_ban", "t": full}
        if m:
            parts = [x.strip() for x in m.group(1).split(";") if x.strip()]
            whole = [x for x in parts if not re.search(r"\b(downstream|upstream|above|below|between)\b", x, re.I)]
            out["x"] = [_water(x) for x in whole]
            out["xp"] = [x for x in parts if x not in whole]
        return out
    if text.endswith(":") and not re.search(r"\d", text) and "measured" not in low and "taken from" not in low:
        return None                                              # a lead-in only ("You must be aware of ...:")
    if re.search(r"no fishing for salmon in region .* until further notice", low):
        return {"k": "closed", "t": text}
    if "daylight hours" in low:
        return {"k": "daylight", "t": text}
    m = re.search(r"within (\d+)\s*met(?:er|re)s of any government facility", low)
    if m:
        return {"k": "facility", "t": text, "m": int(m.group(1))}
    m = re.match(r"(.+?) watershed:\s*(.+)", text, re.I)
    if m and re.search(r"limit", low):
        n = re.search(r"\bis\s+(?:one|two|three|four|\w+)?\s*\(?(\d+)\)?|\bis\s+(\d+)\s+fish", m.group(2), re.I)
        num = next((int(g) for g in (n.groups() if n else ()) if g), None)
        river = m.group(1).strip()
        return {"k": "aggregate", "t": text, "sp": _species(m.group(2)), **({"n": num} if num is not None else {}),
                **({"hatchery": True} if "hatchery" in low else {}),
                "w": [_water(river if re.search(r"(River|Creek|Lake)$", river) else f"{river} River", ws=True)]}
    m = re.search(r"daily limit for all species of pacific salmon .*? is (\d+)", low)
    if m:
        return {"k": "daily", "t": text, "n": int(m.group(1))}
    if "possession limit" in low:
        m = re.search(r"more than (\d+) salmon", low)
        return {"k": "possession", "t": text, **({"n": int(m.group(1))} if m else {})}
    m = re.search(r"(annual|monthly) limit (?:for |of )?(?:(\d+) )?(\w+)(?: salmon)? \(?over (\d+) ?cm\.?\)?(?: is (\d+))?", low)
    if m:
        n = m.group(2) or m.group(5)
        return {"k": m.group(1), "t": text, "sp": _species(m.group(3)), "cm": int(m.group(4)),
                **({"n": int(n)} if n else {})}
    m = re.search(r"all retained (.+?) must measure (\d+)\s*cm or more", low)
    if m:
        return {"k": "min_size", "t": text, "sp": _species(m.group(1)), "cm": int(m.group(2))}
    if re.search(r"measurements are taken from", low):
        return {"k": "measure", "t": text.split(" You must")[0]}
    m = re.search(r'"?(adult|jack)"? (chinook|coho)', low)
    if m:
        cls, sp = m.group(1), _SP[m.group(2)]
        if cls == "jack":
            r = re.search(r"between (\d+)\s*-\s*(\d+)\s*cm", low)
            return {"k": "jack", "t": text, "sp": [sp], **({"cm": [int(r.group(1)), int(r.group(2))]} if r else {})}
        r = re.search(r"(?:over|greater than) (\d+)\s*cm", low)
        out = {"k": "adult", "t": text, "sp": [sp], **({"cm": int(r.group(1))} if r else {})}
        place = re.search(r"cm in the (.+?)\.?$", text)
        if place and not re.search(r"\bin region\b", low):
            out["w"] = [_water(n) for n in _names(place.group(1))]
        return out
    if "bait ban" in low:
        return {"k": "bait_ban", "t": text}
    return {"k": "other", "t": text}


def parse_header(html: str, slug: str) -> List[dict]:
    """The page's header notes, in page order, each with a stable id (`<slug>:<kind>:<hash of its words>`)."""
    notes: List[dict] = []
    seen = set()
    for text, lead, has_kids in _items(html):
        # DFO's lead-in ("You must be aware of these measurements:") is no rule; a sentence before it is
        text = _RE_LEADIN.sub("", text).strip()
        if not text:
            continue
        if has_kids and not re.search(r"adult|bait ban", text, re.I):
            # a lead-in with children: kept only when it says something itself (not "You must be aware of ...:")
            n = classify(text)
        elif has_kids:
            # an "adult chinook ... except in the following areas ... over 62 cm:" — the region-wide part is its own note
            head = re.split(r"\s+except\b", text, maxsplit=1, flags=re.I)[0]
            n = classify(head) if "adult" in text.lower() else None
            if n:
                n["t"] = text.rstrip(":")
        else:
            n = classify(text, lead if lead and re.search(r"adult (chinook|coho)|bait ban", lead, re.I) else None)
        if not n:
            continue
        h = hashlib.sha1(n["t"].encode("utf-8")).hexdigest()[:8]
        n = {"id": f"{slug}:{n['k']}:{h}", **n}
        if n["id"] not in seen:
            seen.add(n["id"])
            notes.append(n)
    return notes


def digest(notes: Sequence[dict]) -> str:
    """What a header change is detected by: its notes' words and values (the ids change with the words)."""
    return hashlib.sha256(json.dumps(list(notes), sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


# ── names -> items ───────────────────────────────────────────────────────────────────────────────────────────────────

def _bindings(ef) -> Dict[str, List[List[str]]]:
    """name key -> the item lists of the curated waters it names (exact names and aliases; then qualifier-less)."""
    from pipeline.common.names import drop_notes, key
    exact: Dict[str, List[List[str]]] = {}
    loose: Dict[str, List[List[str]]] = {}
    for w in getattr(ef, "waters", ()):
        if not w.item_ids:
            continue
        for n in (w.name, *w.aliases):
            exact.setdefault(key(n), []).append(sorted(w.item_ids))
            loose.setdefault(key(drop_notes(n)), []).append(sorted(w.item_ids))
    return {"exact": exact, "loose": loose}                     # type: ignore[return-value]


def _items_of(name: str, b) -> Optional[List[str]]:
    from pipeline.common.names import key
    k = key(name)
    for table in (b["exact"], b["loose"]):
        hits = {tuple(x) for x in table.get(k, [])}
        if len(hits) == 1:
            return list(next(iter(hits)))
        if len(hits) > 1:
            return None                                          # two different waters by that name: the owner's call
    return None


def resolve(notes: Iterable[dict], ef, upstream: Optional[Callable[[Sequence[str]], Iterable[str]]] = None
            ) -> Tuple[Dict[str, List[str]], List[dict]]:
    """({scope key: items}, unresolved [{note, name}]). A "/"-joined name ("Bella Coola/Atnarko River") is read as its
    rivers only when the whole name binds nothing and every part binds exactly one curated water."""
    b = _bindings(ef)
    scopes: Dict[str, List[str]] = {}
    unresolved: List[dict] = []
    for n in notes:
        for w in [*(n.get("w") or []), *(n.get("x") or [])]:
            if w["key"] in scopes:
                continue
            items = _items_of(w["n"], b)
            if items is None and "/" in w["n"]:
                m = re.fullmatch(r"(.+?)/(.+?) (River|Creek)", w["n"])
                if m:
                    parts = [_items_of(f"{m.group(1)} {m.group(3)}", b), _items_of(f"{m.group(2)} {m.group(3)}", b)]
                    if all(parts):
                        items = sorted({i for p in parts for i in p})
            if items is None:
                unresolved.append({"note": n["id"], "name": w["n"]})
                continue
            if w.get("ws") and upstream is not None:
                items = sorted(set(items) | set(upstream(items)))
            scopes[w["key"]] = items
    return scopes, unresolved
