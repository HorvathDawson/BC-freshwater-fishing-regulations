"""T230 owner add-on (2026-10-28): fill `matches.json` AUTOMATICALLY where a row is unambiguous.

    python -m pipeline.regs.inseason.auto [--feed PATH] [--matches PATH] [--dry-run] [--no-verify]

For each live feed row:

1. THE WATER, as printed, through the provincial matcher (`pipeline.regs.matching.matcher.match_row`: registry names,
   aliases, name variants and the curated overrides, region-scoped). Two same-named waters in the region are told apart
   only by a relationship the row itself prints: "<X> River classified licence" names the river whose watershed the
   water is in (FWA code prefix) — exactly one candidate inside it, else ambiguous.
2. THE PLACE, from the water cell's "(…)" and the change text: "between A and B", "from A to B", "upstream/downstream
   of A[, to B]", "above/below A"; none → the whole water. Every locator found is one extent (their UNION); an
   "upstream of S" beside a "downstream of S" is the whole water.
3. EACH BOUNDARY → an EXISTING named split on that water (`splits_on`, its splits.json label), by the label's words:
   the same words, else one label whose words are all in the phrase (or the phrase's in it). Nothing is invented.

AUTO only when the water is exactly ONE item and every boundary is one split (or the whole water), and — unless
`--no-verify` — the extents bind at least one section by the builder's own resolver (`extent.resolve_extent`, as the
review backend previews). The match carries `source: "auto"`, its `evidence` and a `confidence`. Anything else is a
GUESS (`guesses[id]`: best-guess extents + why) that pre-fills the review tab; the row stays unmatched.

AN OWNER MATCH ALWAYS WINS: auto never writes over `source: "owner"`. An auto match whose row now reads differently is
replaced (or cleared to a guess); one that still reads the same is left as it is (no new history). Deterministic: the
same feed + files give the same text (`at` is the only clock, and it is written only on a change).
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from pipeline.common.utils import wsc as _wsc
from pipeline.regs.inseason import matches as M

MONTHS = ("january|february|march|april|may|june|july|august|september|october|november|december|"
          "jan|feb|mar|apr|jun|jul|aug|sept|sep|oct|nov|dec")
#: where a place phrase ends: punctuation, a date, a clause word
_STOP = re.compile(
    r"^(?:\s*[.;:,()“”\"]|\s+(?:to|and|when|including|excluding|from|located|exempt|for|until|"
    rf"(?:downstream|upstream)\s+to|{MONTHS})\b|\s*$)", re.I)
_TRIG = re.compile(r"\b(between|from|upstream of|downstream of|above|below)\s+", re.I)
_TO = re.compile(r"^,?\s+(?:(?:downstream|upstream)\s+)?to\s+", re.I)
_AND = re.compile(r"^\s+and\s+", re.I)
_DATEISH = re.compile(rf"^(?:{MONTHS})\b|^\d|^page\b|^noon\b|^midnight\b", re.I)
_LOCATED = re.compile(rf",\s*located\b.*?(?=\s+(?:{MONTHS})\b|[.;](?:\s|$)|$)", re.I)
_OTHER_WATER = re.compile(r"\bin the ((?:[A-Z][\w’']*\s+)+(?:Spawning\s+|Side\s+)?(?:Channel|Slough))\b")

#: words that say nothing about WHICH place
_NOISE = {"the", "of", "at", "a", "an", "on", "near", "located", "approximately", "about", "fishing", "boundary",
          "sign", "signs", "confluence", "with", "into", "mouth", "main", "mainstem", "its", "from", "to", "m", "km",
          "in", "and"}
#: words a place shares with many others: a match on these alone is only medium
_GENERIC = {"bridge", "dam", "falls", "creek", "river", "hwy", "canyon", "lake", "outlet", "trestle", "top", "lower",
            "upper", "road", "channel", "downstream", "upstream", "above", "below", "post"}
_RANK = {"high": 2, "medium": 1, "low": 0}


@dataclass
class Locator:
    op: str                      # upstream_of | downstream_of | between
    places: list[str]
    quote: str


def _wsc_codes(item) -> list[str]:
    """An item's watershed codes, from its registry ref ids (`wsc:100-1`)."""
    return [ref.split(":", 1)[1] for ref in item.ref_ids if ref.startswith("wsc:")]


def _clean(text: str) -> str:
    t = text.replace("’", "'").replace("‘", "'")
    return _LOCATED.sub("", t)


def _take(text: str, i: int) -> tuple[str, int]:
    """The place phrase starting at `i`, and where it ends."""
    j = i
    while j < len(text) and not _STOP.match(text[j:]):
        j += 1
    return text[i:j].strip(), j


def locators(water_cell: str, change: str) -> list[Locator]:
    """Every place the row prints, in order (the water cell's "(…)" first)."""
    paren = " ".join(re.findall(r"\(([^)]*)\)", water_cell))
    out: list[Locator] = []
    for text in (paren, change):
        text = _clean(text)
        pos = 0
        while True:
            m = _TRIG.search(text, pos)
            if not m:
                break
            kind = m.group(1).lower()
            start = m.end()
            if kind == "from":
                sub = re.match(r"(?:(?:downstream|upstream)\s+of\s+)?(?:the\s+confluence\s+with\s+)?", text[start:], re.I)
                start += sub.end() if sub else 0
            a, end = _take(text, start)
            if not a or _DATEISH.match(a):
                pos = m.end()
                continue
            if kind == "between":
                am = _AND.match(text[end:])
                if not am:
                    pos = m.end()
                    continue
                b, end2 = _take(text, end + am.end())
                if b:
                    out.append(Locator("between", [a, b], text[m.start():end2].strip()))
                    pos = end2
                    continue
                pos = m.end()
                continue
            tm = _TO.match(text[end:])
            if tm:
                b, end2 = _take(text, end + tm.end())
                if b and not _DATEISH.match(b):
                    out.append(Locator("between", [a, b], text[m.start():end2].strip()))
                    pos = end2
                    continue
            if kind == "from":                 # "from A" with no "to": not a place
                pos = end
                continue
            op = "upstream_of" if kind in ("upstream of", "above") else "downstream_of"
            out.append(Locator(op, [a], text[m.start():end].strip()))
            pos = end
    return out


def tokens(s: str, host: set[str] = frozenset()) -> set[str]:
    s = s.lower().replace("’", "'").replace("'s", "s").replace("'", "")
    s = re.sub(r"\([^)]*\)", " ", s).replace("→", " ")
    s = re.sub(r"\bhighway\b|\bhwy\b\.?", "hwy", s)
    return {t for t in re.findall(r"[a-z0-9]+", s) if t not in _NOISE and t not in host}


def match_split(place: str, splits: list[dict], host_name: str) -> tuple[Optional[str], str, list[str]]:
    """(split id | None, confidence | why, candidate ids)."""
    host = tokens(host_name)
    want = tokens(place, host)
    if not want:
        return None, "the place has no words to match", []
    labelled = [(sp["id"], tokens(str(sp.get("label") or ""), host)) for sp in splits if sp.get("id")]
    same = [sid for sid, t in labelled if t and t == want]
    if len(same) == 1:
        return same[0], "high", same
    if len(same) > 1:
        return None, f"{len(same)} splits read the same", same
    sub = [(sid, t) for sid, t in labelled if t and (t <= want or want <= t)]
    if len(sub) == 1:
        sid, t = sub[0]
        return sid, ("high" if (t & want) - _GENERIC else "medium"), [sid]
    if len(sub) > 1:
        inside = [(sid, t) for sid, t in sub if t <= want]
        if len(inside) == 1:
            sid, t = inside[0]
            return sid, ("high" if (t & want) - _GENERIC else "medium"), [s for s, _ in sub]
        return None, f"{len(sub)} splits could be '{place}'", [s for s, _ in sub]
    return None, f"no split on this water reads '{place}'", []


@dataclass
class Context:
    registry: dict
    name_index: dict
    id_index: dict
    ov_index: dict
    splits_doc: dict
    #: extents -> (binds?, why); None = not verified
    verify: Optional[Callable[[list[M.InSeasonExtent]], tuple[bool, str]]] = None


@dataclass
class Proposal:
    rid: str
    auto: bool
    extents: list[M.InSeasonExtent] = field(default_factory=list)
    confidence: str = "high"
    evidence: dict = field(default_factory=dict)
    why: str = ""


def _region_arg(region: str) -> str:
    m = re.match(r"(\d)", region or "")
    return f"REGION {m.group(1)}" if m else ""


def _licence_parent(change: str) -> Optional[str]:
    m = re.search(r"\b((?:[A-Z][\w.']*\s+)+River)\s+classified\s+licen[cs]e", change)
    return m.group(1).strip() if m else None


def resolve_water(row: dict, ctx: Context) -> tuple[list[str], dict, str]:
    """(item ids, evidence, why-not). One id = resolved."""
    from pipeline.regs.matching.matcher import _norm, match_row
    r = match_row(0, {"water": row["water"], "region": _region_arg(row.get("region", ""))},
                  ctx.registry, ctx.name_index, ctx.id_index, ctx.ov_index)
    ev = {"water": row["water"], "via": r.via or r.status, "reason": r.reason}
    if r.status in ("matched", "override") and r.item_id:
        ids = [r.item_id, *r.also]
        ev["items"] = {i: ctx.registry[i].name for i in ids if i in ctx.registry}
        return ids, ev, ("" if len(ids) == 1 else f"the name resolves to {len(ids)} waters")
    if r.status == "ambiguous" and r.candidates:
        ev["candidates"] = list(r.candidates)
        parent = _licence_parent(row.get("change", ""))
        if parent:
            hits = ctx.name_index.get(_norm(parent)) or []
            reg = re.match(r"(\d)", row.get("region", "") or "")
            hits = [h for h in hits if not reg or any(m.startswith(reg.group(1) + "-") for m in ctx.registry[h].mus)]
            wscs = [w for h in hits for w in _wsc_codes(ctx.registry[h])]
            inside = sorted({c for c in r.candidates for w in _wsc_codes(ctx.registry[c])
                             if any(_wsc.is_strictly_below(w, p) for p in wscs)})
            if len(inside) == 1:
                ev["disambiguated_by"] = f"in the {parent} watershed (the row's classified licence)"
                ev["items"] = {inside[0]: ctx.registry[inside[0]].name}
                return inside, ev, ""
        return [], ev, f"{len(r.candidates)} waters named '{row['water']}' in the region"
    return [], ev, r.reason or "no water by that name"


def propose(row: dict, ctx: Context) -> Proposal:
    from pipeline.atlas.splits.splits import splits_on
    from pipeline.regs.matching.matcher import _item_region_nums
    rid, change = row["id"], row.get("change", "")
    ids, ev, why = resolve_water(row, ctx)
    p = Proposal(rid, False, evidence=ev)
    conf = "medium" if ev.get("disambiguated_by") else "high"
    notes: list[str] = []
    blockers: list[str] = []
    if why:
        blockers.append(why)
    if re.search(r"tributar", row["water"], re.I):
        blockers.append("the row is the water's TRIBUTARIES: an extent cannot say tributaries only")
    other = _OTHER_WATER.search(change)
    if other and tokens(other.group(1)) - tokens(row["water"]):
        blockers.append(f"the change is in another water: '{other.group(1)}'")
        ev["other_water"] = other.group(1)
    if re.search(r"including (?:its )?tributaries", change, re.I):
        notes.append("the row includes tributaries: the banner is on the water only")
    elif re.search(r"tributar", change, re.I):
        notes.append("the text speaks of tributaries")
        conf = "medium"
    kind = re.match(r"\s*(clarification|rescind)", change, re.I)
    if kind:
        notes.append(f"a {kind.group(1).lower()} row: informational, bound to its water")
    locs = locators(row["water"], change)
    ev["locators"] = [loc.quote for loc in locs]
    primary = ids[0] if ids else None
    exts: list[M.InSeasonExtent] = []
    if primary:
        it = ctx.registry[primary]
        splits = splits_on(ctx.splits_doc, set(it.ref_ids))
        ev["splits"] = {}
        for loc in locs:
            got = []
            for place in loc.places:
                sid, how, cands = match_split(place, splits, it.name)
                if sid:
                    got.append(sid)
                    ev["splits"][place] = sid
                    if how == "medium":
                        conf = "medium"
                else:
                    blockers.append(f"'{place}': {how}" + (f" ({', '.join(cands)})" if cands else ""))
            if len(got) == len(loc.places):
                exts.append(M.InSeasonExtent(op=loc.op, item_id=primary, splits=got))
        if not locs:
            exts = [M.InSeasonExtent(op="whole", item_id=c) for c in ids]
        else:
            ups = {e.splits[0] for e in exts if e.op == "upstream_of"}
            downs = {e.splits[0] for e in exts if e.op == "downstream_of"}
            if ups & downs:
                notes.append(f"upstream and downstream of {sorted(ups & downs)[0]}: the whole water")
                exts = [M.InSeasonExtent(op="whole", item_id=primary)]
            # one extent per distinct place
            exts = list({json.dumps(e.model_dump(), sort_keys=True): e for e in exts}.values())
        if exts and not blockers and any(e.op != "between" for e in exts):
            regions = _item_region_nums(it)
            rn = re.match(r"(\d)", row.get("region", "") or "")
            if rn and regions - {rn.group(1)}:
                notes.append(f"the water runs into region(s) {', '.join(sorted(regions - {rn.group(1)}))}: "
                             f"the extent is not clipped to region {row.get('region')}")
                conf = "medium"
    if notes:
        ev["notes"] = notes
    if not blockers and exts and ctx.verify is not None:
        ok, vwhy = ctx.verify(exts)
        if not ok:
            blockers.append(f"the extent binds no section: {vwhy}")
    p.extents, p.confidence = exts, conf
    if blockers or not exts:
        p.why = "; ".join(blockers) or "nothing to bind"
        if not exts and ids:
            p.extents = [M.InSeasonExtent(op="whole", item_id=c) for c in ids[:6]]
        return p
    p.auto = True
    return p


def _same(m: M.Match, p: Proposal) -> bool:
    return ([e.model_dump() for e in m.extents] == [e.model_dump() for e in p.extents]
            and m.evidence == p.evidence and m.confidence == p.confidence)


def apply(doc: M.Matches, feed: dict, ctx: Context, at: str) -> tuple[M.Matches, list[Proposal]]:
    """`doc` with every live row's auto match / guess brought up to date. Owner matches are never touched."""
    matches, history, guesses = dict(doc.matches), list(doc.history), {}
    props: list[Proposal] = []
    live = {r["id"] for r in feed.get("rows") or []}
    for row in sorted(feed.get("rows") or [], key=lambda r: r["id"]):
        rid = row["id"]
        cur = matches.get(rid)
        if cur is not None and cur.source == "owner":
            continue
        p = propose(row, ctx)
        props.append(p)
        if p.auto:
            if cur is not None and _same(cur, p):
                continue
            matches[rid] = M.Match(region=str(row.get("region") or ""), water=str(row.get("water") or ""),
                                   extents=p.extents, note="auto", by="auto", at=at, source="auto",
                                   evidence=p.evidence, confidence=p.confidence)
            history.append(M.HistoryRow(at=at, id=rid, action="auto", by="auto",
                                        note=f"auto ({p.confidence}): " + "; ".join(
                                            f"{e.op} {e.item_id}{' ' + ','.join(e.splits) if e.splits else ''}"
                                            for e in p.extents), extents=p.extents))
        else:
            if cur is not None:          # an auto match that no longer holds
                matches.pop(rid)
                history.append(M.HistoryRow(at=at, id=rid, action="clear", by="auto",
                                            note=f"auto withdrawn: {p.why}"))
            guesses[rid] = {"why": p.why, "evidence": p.evidence,
                            **({"extents": [e.model_dump() for e in p.extents]} if p.extents else {})}
    # guesses only for live, unmatched rows
    guesses = {k: v for k, v in guesses.items() if k in live and k not in matches}
    return doc.model_copy(update={"matches": matches, "history": history, "guesses": guesses}), props


def context(verify: bool = True) -> Context:
    from pipeline.atlas.reach.covered import DEFAULT_OVERRIDES
    from pipeline.atlas.registry.io import load_registry
    from pipeline.common.curated import CURATED, GENERATED
    from pipeline.regs.matching.matcher import (build_id_index, build_name_index, build_override_index,
                                                load_overrides)
    reg = load_registry(GENERATED.registry())
    ctx = Context(reg, build_name_index(reg), build_id_index(reg), build_override_index(load_overrides(DEFAULT_OVERRIDES)),
                  json.loads(Path(CURATED.waters.splits).read_text(encoding="utf-8")))
    if verify:
        ctx.verify = _graph_verifier(reg)
    return ctx


def _graph_verifier(reg):
    """The review backend's preview, without the backend: `extent.resolve_extent` on the default build's graph."""
    from pipeline.atlas.reach import extent as X
    from pipeline.atlas.reach import position
    from pipeline.atlas.registry import regions
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    build = GENERATED.require_build()
    g = read_artifact(str(build / "graph.pkl"))
    regions.attach_from(build, g, reg)
    position.attach(g, build)

    def verify(exts: list[M.InSeasonExtent]) -> tuple[bool, str]:
        for e in exts:
            reasons: list = []
            got = X.resolve_extent(reg, g, [e.item_id], e.model_dump(), reasons=reasons)
            if not (got or {}).get("sections"):
                return False, "; ".join(f"{c}{': ' + d if d else ''}" for c, d in reasons) or f"{e.op} binds nothing"
        return True, ""
    return verify


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--feed", type=Path, default=None)
    ap.add_argument("--matches", type=Path, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-verify", action="store_true", help="skip the section check (no graph load)")
    ap.add_argument("--backup-dir", type=Path, default=None)
    a = ap.parse_args(argv)
    feed_p = a.feed or (GENERATED.inseason / "feed" / "index.json")
    path = a.matches or M.default_path()
    feed = json.loads(Path(feed_p).read_text(encoding="utf-8"))
    doc = M.parse(Path(path).read_text(encoding="utf-8"))
    at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new, props = apply(doc, feed, context(verify=not a.no_verify), at)
    errs = M.problems(new, split_ids=M.all_split_ids())
    if errs:
        raise SystemExit("refused: " + "; ".join(errs))
    for p in props:
        if p.auto:
            print(f"AUTO  {p.rid} {p.confidence:6} " + "; ".join(
                f"{e.op} {e.item_id} {','.join(e.splits)}".strip() for e in p.extents))
        else:
            print(f"GUESS {p.rid} {p.why}")
    n_auto = sum(p.auto for p in props)
    print(f"{n_auto} auto, {len(props) - n_auto} left for the owner, "
          f"{sum(m.source == 'owner' for m in new.matches.values())} owner matches untouched")
    text = M.dumps(new)
    if a.dry_run or text == Path(path).read_text(encoding="utf-8"):
        print("dry run: nothing written" if a.dry_run else "unchanged")
        return 0
    if a.backup_dir:
        a.backup_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, a.backup_dir / f"matches.{at.replace(':', '')}.json")
    Path(path).write_text(text, encoding="utf-8")
    print(f"-> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
