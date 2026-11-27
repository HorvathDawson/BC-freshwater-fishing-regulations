"""T807: A DFO PLACE'S SIDE STREAMS ARE DERIVED FROM DFO'S OWN WORDS — the place's line, else the EXACT heading its row
sits under — and a saved value is only a stated exception.

Owner 2026-11-16 (`docs/OWNER-TODO.md`, "Answers by batch"): "this depends if the name has tributraries included
in it. if it doesnt then no tribs otrherwise yes tribs"; "The heading counts"; "the locations should be matched only to
their exact naming then"; the ten disagreeing places "river only for all ten"; (11-16 "A named creek keeps its side streams", replaced:)
the Kitimat outfall strip "Heading counts here too"; 2026-11-17 17:01, on the Tatshenshini's named creeks: "only
locating that specific tributary … that was a mistake on my mark that i wanted you to fix" (a place naming its creeks
is those creeks only); on the three dormant saves: "dfo words but make sure it is compatible with the name tributaruies
or not" (no exception kept). The audit (2026-11-16) was cut in T1081; git history holds it.

THE RULE, in order (`derive`):
  1. the place's OWN words, when they speak (the part before a "Note:" / "except" / "unless" clause, which is about
     other water):
       "mainstem and all (other) tributaries"                 side streams in
       "all tributaries …" / "tributaries of / to the …"       side streams ONLY (not the river itself)
       "excluding / not including tributaries"                 river only
       "including / and (its / all) tributaries"               side streams in
       "mainstem" (no tributary words)                         river only
       a place that only NAMES waters ("Kwatini Creek, Stanley Creek and Goat Creek", "Blanchard River")
                                                               those waters only, no side streams (owner
                                                               2026-11-17 17:01: "only locating that specific
                                                               tributary")
  2. else the EXACT heading the row sits under on DFO's page ("Morice River (including tributaries)" — in; "Morice
     River" — out), DFO's "tpinributaries" typo read as the word;
  3. else river only.
A stored `binding.tributaries` on a named-water place is an EXCEPTION to that, and must say why
(`EntryLocation.tributaries_exception`: reason + the save it came from); the water-level flag never decides
(`WaterBinding.tributaries` is the merged name's, which is how nine places went wrong).

The heading is stamped on every place (`EntryLocation.heading`) from the cached pages — the live page first, then the
history newest first for a place DFO no longer lists — by `python -m pipeline.regs.dfo_salmon.side_streams stamp`;
`check` (read-only) fails when a live place's heading on the page differs from the stamped one, when one place's
wordings sit under two different headings, or when a named-water place's stored setting differs from the derived one
without a stated reason.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

#: a "Note:" / "except" / "unless" clause, or a second sentence after a semicolon ("…; and you shall not fish for
#: Salmon in that portion … known as the Northrop Spawning Channel … including its tributary", Stave River), describes
#: something other than this place (`untangle._RE_TRAILING_CLAUSE`)
_CLAUSE = re.compile(r"\b(?:note:|except\b|unless\b)|;", re.I)
#: DFO misspells the word on one heading ("Kitsumkalum River (including tpinributaries)")
_TRIB = r"t\w*ributar(?:y|ies)"
_MAINSTEM_AND_ALL = re.compile(r"\bmain\s?stem\s+and\s+(?:all\s+)?(?:other\s+)?" + _TRIB, re.I)
#: side streams ONLY: the words START with them ("All tributaries to the Bulkley …", "tributaries of the X") or say "all
#: tributaries of / to" — never "including tributaries to that part" (Chehalis, Little Campbell: in)
_ONLY = re.compile(r"^\s*(?:all\s+(?:other\s+)?)?" + _TRIB + r"\b|\ball\s+(?:other\s+)?" + _TRIB + r"\s+(?:of|to)\b",
                   re.I)
_OFF = re.compile(r"\b(?:excluding|not\s+including)\s+(?:all\s+)?(?:its\s+)?(?:the\s+)?" + _TRIB, re.I)
_ON = re.compile(r"\b(?:including|in)\s+(?:all\s+)?(?:its\s+)?(?:the\s+)?" + _TRIB
                 + r"|\band\s+(?:its\s+|all\s+)?" + _TRIB, re.I)
_MAINSTEM = re.compile(r"\bmain\s?stem\b", re.I)
#: words that make a place a STRETCH or a spot, not a list of named waters
_PLACE_WORDS = re.compile(r"\b(?:from|to|upstream|downstream|above|below|between|within|signs?|bridge|mouth|"
                          r"confluence|outlet|inlet|portion|km|metres?|m\b|of)\b", re.I)
#: a place that only names waters: "Blanchard River", "Kwatini Creek, Stanley Creek and Goat Creek"
_NAMED = re.compile(r"^(?:[A-Z][\w'’.-]*\s+)+(?:Creek|River|Lake|Slough|Channel)"
                    r"(?:\s*(?:,|\band\b)\s*(?:[A-Z][\w'’.-]*\s+)+(?:Creek|River|Lake|Slough|Channel))*\.?$")


@dataclass(frozen=True)
class SideStreams:
    """What DFO's words say about one place's side streams."""

    tributaries: bool          # side streams in
    only: bool                 # side streams ONLY (the place's own water is not in)
    source: str                # "own words" | "named water" | "heading" | "nothing said" | "exception"
    words: str                 # the words that decided it (empty when nothing was said)

    def as_dict(self) -> dict:
        return {"tributaries": self.tributaries, "only": self.only, "source": self.source, "words": self.words}


def own_words(text: str) -> Optional[SideStreams]:
    """What the place's OWN line says, or None when it is silent about side streams."""
    t = re.sub(r"\s+", " ", text or "").strip()
    m = _CLAUSE.search(t)
    head = t[:m.start()].strip() if m else t
    if not head:
        return None
    if re.search(r"t\w*ributaries", head, re.I) or _ON.search(head) or _OFF.search(head):
        if (x := _MAINSTEM_AND_ALL.search(head)):
            return SideStreams(True, False, "own words", x.group(0))
        if (x := _ONLY.search(head)):
            return SideStreams(True, True, "own words", x.group(0).strip())
        off, on = _OFF.search(head), _ON.search(head)
        if off and not on:
            return SideStreams(False, False, "own words", off.group(0))
        if on and not off:
            return SideStreams(True, False, "own words", on.group(0))
        return None                                   # both, or neither form: unclear — `derive` says so
    if (x := _MAINSTEM.search(head)):
        return SideStreams(False, False, "own words", x.group(0))
    if _NAMED.match(head) and not _PLACE_WORDS.search(head):
        # owner 2026-11-17 17:01 (OWNER-TODO), on the Tatshenshini's Blanchard River and "Kwatini, Stanley and Goat
        # creeks": "only locating that specific tributary" — the named waters themselves, not their side streams
        return SideStreams(False, False, "named water", head.rstrip("."))
    return None


def unclear(text: str) -> bool:
    """The place's own line names side streams in a way `own_words` cannot read (both in and out, or another form):
    such a place needs a stated exception, never a guess."""
    t = re.sub(r"\s+", " ", text or "").strip()
    m = _CLAUSE.search(t)
    head = t[:m.start()].strip() if m else t
    # "tributary" alone names one water ("that portion of the Fairy Lake tributary"), it says nothing of side streams
    return bool(head and re.search(r"t\w*ributaries", head, re.I)) and own_words(t) is None


def heading_says(heading: str) -> Optional[SideStreams]:
    """What the EXACT heading says, or None when it is silent."""
    h = re.sub(r"\s+", " ", heading or "").strip()
    if (x := re.search(r"\bexcluding\s+" + _TRIB, h, re.I)):
        return SideStreams(False, False, "heading", h)
    if re.search(r"\b(?:including|and)\s+(?:all\s+)?(?:its\s+)?" + _TRIB, h, re.I):
        return SideStreams(True, False, "heading", h)
    return None


def own_part(specific_area: str, heading: Optional[str]) -> str:
    """The row's OWN words: the place's specific area less the scope its heading carries ("Tatshenshini River
    (upstream of the BC/ Yukon border – along the Haines Hwy.)" puts "upstream of …; " in front of every row under it,
    `untangle`) — that part is the heading's, read with it."""
    from pipeline.regs.dfo_salmon.untangle import split_name
    text = re.sub(r"\s+", " ", specific_area or "").strip()
    if heading:
        emb = split_name(heading)[4]
        if emb:
            if text == emb:
                return ""
            if text.startswith(emb + "; "):
                return text[len(emb) + 2:]
    return text


def derive(specific_area: str, heading: Optional[str]) -> SideStreams:
    """THE RULE (module doc): own words, else the exact heading, else river only."""
    own = own_words(own_part(specific_area, heading))
    if own is not None:
        return own
    got = heading_says(heading or "")
    if got is not None:
        return got
    return SideStreams(False, False, "nothing said", "")


def applies(loc) -> bool:
    """The rule is for a NAMED WATER's place (a region / section / area default or a closure scope is a basin, and
    keeps its own setting)."""
    kind = loc.get("kind") if isinstance(loc, Mapping) else getattr(loc, "kind", None)
    return kind == "water"


def _loc_get(loc, *keys):
    if isinstance(loc, Mapping):
        cur = loc
        for k in keys:
            cur = (cur or {}).get(k) if isinstance(cur, Mapping) else getattr(cur, k, None)
        return cur
    cur = loc
    for k in keys:
        cur = cur.get(k) if isinstance(cur, Mapping) else getattr(cur, k, None)
    return cur


def derived_of(loc) -> SideStreams:
    """`derive` for a place record (an `EntryLocation` or its dict)."""
    return derive(str(_loc_get(loc, "source_text", "specific_area") or ""), _loc_get(loc, "heading"))


def effective(loc, water=None) -> SideStreams:
    """THE SIDE-STREAM SETTING A PLACE BINDS WITH — what `entries.to_reach_input` hands the builder and the review app
    shows. A named-water place: its stated exception (`binding.tributaries` + `tributaries_exception`), else the derived
    one. Anything else: its own stored setting, else its water's (the old reading; a default is not a named water)."""
    stored = _loc_get(loc, "binding", "tributaries")
    only = bool(_loc_get(loc, "binding", "tributaries_only"))
    if applies(loc):
        if stored is not None:
            why = _loc_get(loc, "tributaries_exception") or {}
            return SideStreams(bool(stored), only, "exception", str(why.get("reason") or ""))
        d = derived_of(loc)
        # "tributaries only" is a piece's call too (`binding.tributaries_only`): kept when the words say only
        return SideStreams(d.tributaries, d.only or (only and d.tributaries), d.source, d.words)
    wflag = _loc_get(water, "tributaries") if water is not None else None
    val = stored if stored is not None else wflag
    return SideStreams(bool(val), only, "stored", "")


# --------------------------------------------------------------------------------------------- the page's headings

def _fp_headings(parsed) -> Dict[str, List[str]]:
    """fingerprint -> the exact headings (whitespace-normalised) its rows sit under, on one parsed page."""
    from pipeline.regs.dfo_salmon.locations import fingerprint
    from pipeline.regs.dfo_salmon.untangle import _norm, split_name
    out: Dict[str, List[str]] = {}
    for r in parsed.rows:
        if r.precedence != 3:
            continue
        name, _a, _n, _t, emb = split_name(r.waters)
        scope = _norm(r.specific_area)
        full = _norm(f"{emb}; {scope}") if emb and scope else (scope or emb or "")
        fp = fingerprint(r.section_key, name, full)
        h = _norm(r.waters)
        if h not in out.setdefault(fp, []):
            out[fp].append(h)
    return out


def page_headings(slug: str, cache_dir: Optional[Path] = None) -> Dict[str, List[str]]:
    """fingerprint -> headings on the LIVE cached page of region `slug`."""
    from pipeline.regs.dfo_salmon.fetch import DEFAULT_CACHE
    from pipeline.regs.dfo_salmon.parse import parse_cached
    return _fp_headings(parse_cached(slug, Path(cache_dir or DEFAULT_CACHE)))


def history_headings(slug: str, cache_dir: Optional[Path] = None) -> List[Tuple[str, Dict[str, List[str]]]]:
    """[(snapshot name, fingerprint -> headings)] for the region's archived pages, NEWEST first."""
    from pipeline.regs.dfo_salmon.fetch import DEFAULT_CACHE
    from pipeline.regs.dfo_salmon.parse import parse_region
    hist = Path(cache_dir or DEFAULT_CACHE) / "history"
    out = []
    for f in sorted(hist.glob(f"region{slug}_*.html"), reverse=True):
        try:
            out.append((f.name, _fp_headings(parse_region(f.read_text(encoding="utf-8", errors="replace"), slug))))
        except Exception as exc:                     # an archived page the parser cannot read is named, never hidden
            print(f"  r{slug}: {f.name} unreadable ({exc}); skipped", file=sys.stderr)
    return out


def headings_for(loc: Mapping, live: Mapping[str, List[str]],
                 history: Iterable[Tuple[str, Mapping[str, List[str]]]]) -> Tuple[List[str], str]:
    """The headings a place's wordings sit under, and where they were read: the live page, else the NEWEST archived
    page holding any of its wordings. ([], "") when no page holds it."""
    fps = list(loc.get("fingerprints") or [])
    got = sorted({h for fp in fps for h in live.get(fp, ())})
    if got:
        return got, "live page"
    for name, page in history:
        got = sorted({h for fp in fps for h in page.get(fp, ())})
        if got:
            return got, name
    return [], ""


# --------------------------------------------------------------------------------------------- stamp / check

def survey(entries_dir: Optional[Path] = None, cache_dir: Optional[Path] = None) -> List[dict]:
    """One row per named-water place: its stamped heading, the heading(s) the pages show, the derived setting, the
    stored one and what binds now. Read-only."""
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    from pipeline.regs.dfo_salmon import entries as E
    root = Path(entries_dir or E.ENTRIES_DIR)
    rows = []
    for slug in [s for s in ALL_SLUGS if not PAGES[s].is_stub]:
        p = root / f"region-{slug}.json"
        if not p.exists():
            continue
        doc = json.loads(p.read_text(encoding="utf-8"))
        live = page_headings(slug, cache_dir)
        hist = None
        for loc in doc.get("locations") or []:
            if not applies(loc):
                continue
            heads, where = headings_for(loc, live, ())
            if not heads:
                hist = hist if hist is not None else history_headings(slug, cache_dir)
                heads, where = headings_for(loc, {}, hist)
            stamped = loc.get("heading")
            page = heads[0] if len(heads) == 1 else None
            d = derive(str((loc.get("source_text") or {}).get("specific_area") or ""), stamped if stamped else page)
            rows.append({"region": slug, "location_id": loc["location_id"], "status": loc.get("status") or "active",
                         "stamped": stamped, "page": heads, "where": where,
                         "derived": d.as_dict(), "unclear": unclear(own_part(
                             str((loc.get("source_text") or {}).get("specific_area") or ""), stamped or page)),
                         "stored": (loc.get("binding") or {}).get("tributaries"),
                         "stored_only": bool((loc.get("binding") or {}).get("tributaries_only")),
                         "exception": loc.get("tributaries_exception")})
    return rows


def problems(rows: Iterable[dict]) -> List[str]:
    """What `check` fails on (module doc)."""
    out = []
    for r in rows:
        lid = f"r{r['region']} {r['location_id']}"
        if len(r["page"]) > 1:
            out.append(f"{lid}: its wordings sit under {len(r['page'])} different headings on {r['where']}: "
                       + " | ".join(r["page"]) + " — the place cannot say which heading it is under")
        elif r["page"] and r["stamped"] != r["page"][0] and r["where"] == "live page":
            out.append(f"{lid}: DFO's heading changed: stamped {r['stamped']!r}, the page now {r['page'][0]!r} "
                       "— re-stamp (`side_streams stamp`) and read the side-stream answer again")
        elif not r["page"] and not r["stamped"]:
            out.append(f"{lid}: no page holds this place, and it has no stamped heading")
        if r["stored"] is not None and not (r["exception"] or {}).get("reason"):
            same = r["stored"] == r["derived"]["tributaries"]
            out.append(f"{lid}: a stored side-stream setting ({'in' if r['stored'] else 'out'}) with no stated reason"
                       + (" — it is what the words say: store nothing" if same else
                          f" — the words say {'in' if r['derived']['tributaries'] else 'out'} "
                          f"({r['derived']['source']}): an exception needs its reason"))
        if r["unclear"] and r["stored"] is None:
            out.append(f"{lid}: its own words speak of side streams in a way the rule cannot read — "
                       "state the answer as an exception")
    return out


def stamp(entries_dir: Optional[Path] = None, cache_dir: Optional[Path] = None, *, write: bool = True) -> List[str]:
    """Write each named-water place's exact heading (`heading`) from the pages (live, else newest archive), and drop a
    stored side-stream setting that only repeats what the words say. Places whose wordings sit under two headings are
    left alone (`check` names them). Returns the change lines."""
    from pipeline.common import curated_write
    from pipeline.regs.dfo_salmon import entries as E
    root = Path(entries_dir or E.ENTRIES_DIR)
    rows = {(r["region"], r["location_id"]): r for r in survey(root, cache_dir)}
    changes = []
    for p in sorted(root.glob("region-*.json")):
        slug = p.stem.split("-", 1)[1]
        doc = json.loads(p.read_text(encoding="utf-8"))
        moved = False
        for loc in doc.get("locations") or []:
            r = rows.get((slug, loc["location_id"]))
            if r is None or len(r["page"]) != 1:
                continue
            if loc.get("heading") != r["page"][0]:
                changes.append(f"r{slug} {loc['location_id']}: heading {loc.get('heading')!r} -> {r['page'][0]!r} "
                               f"({r['where']})")
                loc["heading"] = r["page"][0]
                moved = True
            # A SAVE THAT MATCHES THE WORDS STORES NOTHING (owner 2026-11-16): a stored setting equal to what DFO's words
            # say is dropped; one that differs stays, and `check` asks for its reason
            b = loc.get("binding") or {}
            d = derive(str((loc.get("source_text") or {}).get("specific_area") or ""), loc["heading"])
            if b.get("tributaries") is not None and b["tributaries"] == d.tributaries \
                    and not loc.get("tributaries_exception"):
                changes.append(f"r{slug} {loc['location_id']}: stored side streams "
                               f"{'in' if d.tributaries else 'out'} = the words ({d.source}): stored nothing")
                b["tributaries"] = None
                moved = True
        if moved and write:
            curated_write.write(p, json.dumps(doc, indent=2, ensure_ascii=False) + "\n",
                                tool="dfo_salmon.side_streams stamp", source="cached DFO pages")
    return changes


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["stamp", "check", "survey"])
    ap.add_argument("--dry-run", action="store_true", help="stamp: print what would change, write nothing")
    args = ap.parse_args(argv)
    if args.command == "stamp":
        for line in stamp(write=not args.dry_run):
            print(line)
        return 0
    rows = survey()
    if args.command == "survey":
        print(json.dumps(rows, indent=1, ensure_ascii=False))
        return 0
    bad = problems(rows)
    for b in bad:
        print(b)
    print(f"side streams: {len(rows)} named-water places, {len(bad)} problem(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
