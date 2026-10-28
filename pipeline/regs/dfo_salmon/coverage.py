"""DFO EXTENT COVERAGE (owner 2026-10-27: "every one of the historic DFO salmon locations gets an extent
proposal"): the pass `suggest.run` makes over its rows once the matcher and the split hunt are done.

    cluster(rows, locs)   REWORDINGS: the locations on one water that describe the same place (DFO rewords
                          a reach year to year; the page keeps the geography). A cluster member that has no
                          curated extent INHERITS a curated member's extent; an unplaced member whose own
                          suggestion is weaker ADOPTS the cluster's best one. Each row says which.
    classify(row)         WHY a location is unplaced, one word (`WHY`).
    outcome(row)          what it gets: existing / whole / new_<conf> / unresolvable.
    coverage_md(...)      the summary (`handoff/PLAN/curation-review/dfo-coverage.md`).

Read only, like the rest of suggest: the rows are suggestions the review app's DFO mode shows; the owner's
mark is the only write. Memory `dfo-salmon-churn-asymmetry`: waters never move, scope text drifts — so a
rewording is matched on its place words, never on the rules it carries.
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Iterable, Mapping, Optional

from pipeline.regs.dfo_salmon.suggest import CONF_RANK, GENERIC, TYPE_WORDS, _clean, direction, tokens

#: why a location had no extent (the breakdown, one per unplaced location)
WHY = {
    "variant_of_placed": "a rewording of a location that already has a curated extent",
    "variant": "a rewording of another unplaced location (one decision covers the cluster)",
    "review_pending": "existing cuts (or the whole water) say it — never curated, owner review pending",
    "needs_split": "the water lacks a cut it needs — a NEW split is proposed",
    "lake_area": "a part of a lake (a line across it / a radius) — not a cut on a line; lake splits are deferred",
    "side_channel": "a side channel FWA does not draw — needs an added stream or an area",
    "tributary_set": "tributaries only — a mark cannot say tributaries-only",
    "unknown_landmark": "the notice's landmark is named nowhere public",
    "no_water": "the location's water is not matched to the registry",
    "other": "other",
}

#: words that never tell two places apart
_NOISE = GENERIC | {"downstream", "upstream", "above", "below", "up", "down", "located", "approximately",
                    "approx", "from", "to", "known", "following", "areas", "listed", "described", "except",
                    "excluding", "tributaries", "tributary", "including", "salmon", "fish", "you", "shall", "not",
                    "no", "please", "note", "year", "round", "allowed", "closure", "closed", "mainstem"}


def place_words(text: str) -> set[str]:
    """The words that say WHERE: the cleaned place text's tokens, less boilerplate."""
    return {t for t in tokens(_clean(text or "")) if t not in _NOISE and len(t) > 1}


def similarity(a: str, b: str) -> float:
    """Jaccard of the place words (type words count half: "bridge" alone says little)."""
    A, B = place_words(a), place_words(b)
    if not A or not B:
        return 0.0
    w = lambda t: 0.5 if t in TYPE_WORDS else 1.0
    inter = sum(w(t) for t in A & B)
    union = sum(w(t) for t in A | B)
    return inter / union if union else 0.0


def _point_key(p: Mapping, new_splits: list[Mapping]) -> str:
    if p.get("kind") == "new":
        n = next((x for x in new_splits if x.get("ref") == p.get("split_id")), {})
        c = n.get("coord")
        return "new:" + (n.get("split_id") or "") + ":" + (n.get("name") or "") + (
            f"@{c[0]:.3f},{c[1]:.3f}" if c else "") + f"{n.get('offset_m') or ''}"
    return f"{p.get('kind')}:{p.get('split_id') or p.get('name') or p.get('item_id') or p.get('coord')}"


def signature(sugg: Optional[Mapping]) -> Optional[tuple]:
    """What a suggestion binds: op, its points (order-free), its items, the tributary call."""
    if not sugg:
        return None
    ns = list(sugg.get("new_splits") or [])
    return (sugg["op"], tuple(sorted(_point_key(p, ns) for p in sugg.get("points") or [])),
            tuple(sugg.get("items") or []), sugg.get("tributaries"), sugg.get("area"))


def placed_suggestion(loc: Mapping, labels: Optional[Mapping[str, str]] = None) -> Optional[dict]:
    """A curated location's extent said as a suggestion (one extent, a cut / whole / area op), or None."""
    b = loc.get("binding") or {}
    ex = b.get("extents") or []
    if len(ex) != 1:
        return None
    e = ex[0]
    op = e.get("op")
    pts = [{"kind": "split", "split_id": s} for s in e.get("splits") or []]
    area = e.get("within_area") or e.get("area_id")
    if op == "whole" and e.get("within_area"):
        op = "within"
    if op not in ("whole", "upstream_of", "downstream_of", "between", "within"):
        return None
    want = {"whole": 0, "within": 0, "upstream_of": 1, "downstream_of": 1, "between": 2}[op]
    if len(pts) != want or (op == "within") != bool(area):
        return None
    words = ("the whole water" if op == "whole" else f"within {area}" if op == "within"
             else f"{op.replace('_', ' ')} " + " and ".join((labels or {}).get(p["split_id"]) or p["split_id"] for p in pts))
    s = {"op": op, "points": pts, "items": [e["item_id"]] if e.get("item_id") else [],
         "tributaries": b.get("tributaries"), "new_splits": [], "words": words}
    if area and op == "within":
        s["area"] = area
    return s


def _rank(r: Mapping) -> tuple:
    return (CONF_RANK.get(r.get("confidence") or "none", 0), bool(r.get("hunt")), r.get("status") == "active")


def cluster(rows: list[dict], locs: Mapping[str, tuple[str, dict, Optional[dict]]],
            labels: Optional[Mapping[str, str]] = None) -> list[list[str]]:
    """Group the locations on each water that describe the same place, and let the unplaced members
    inherit / adopt (module doc). Mutates `rows` (adds `cluster`; may replace the suggestion, keeping
    the matcher's as `before`). Returns the clusters (location ids), placed members included."""
    by_row = {r["location_id"]: r for r in rows}
    by_water: dict[str, list[str]] = defaultdict(list)
    for lid, (_s, loc, _w) in locs.items():
        text = (loc.get("source_text") or {}).get("specific_area") or ""
        if place_words(text):                      # a water's own (empty) row is not a place
            by_water[loc.get("water_id") or lid].append(lid)
    out: list[list[str]] = []
    for wid, lids in sorted(by_water.items()):
        text = {l: (locs[l][1].get("source_text") or {}).get("specific_area") or "" for l in lids}
        placed = {l for l in lids if (locs[l][1].get("binding") or {}).get("extents")}
        sig = {l: signature(placed_suggestion(locs[l][1]) if l in placed else (by_row.get(l) or {}).get("suggestion"))
               for l in lids}
        parent = {l: l for l in lids}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        edge: dict[tuple[str, str], str] = {}
        for i, a in enumerate(lids):
            for b in lids[i + 1:]:
                da, db = locs[a][1].get("duplicate_of"), locs[b][1].get("duplicate_of")
                sim = similarity(text[a], text[b])
                why = None
                ops = {(sig[a] or ("",))[0], (sig[b] or ("",))[0]}
                if ops == {"upstream_of", "downstream_of"} or {direction(text[a]), direction(text[b])} == {
                        "upstream_of", "downstream_of"}:
                    continue                    # the two sides of one cut are two places
                if da == b or db == a:
                    why = "dup"
                elif sim >= 0.8:
                    why = "text"
                elif sig[a] and sig[a] == sig[b] and sim >= 0.45:
                    why = "same extent"
                if why:
                    edge[(a, b)] = edge[(b, a)] = f"{why} {sim:.2f}"
                    parent[find(a)] = find(b)
        groups: dict[str, list[str]] = defaultdict(list)
        for l in lids:
            groups[find(l)].append(l)
        for n, members in enumerate(sorted(groups.values(), key=lambda m: sorted(m)[0])):
            if len(members) < 2:
                continue
            members = sorted(members)
            out.append(members)
            cid = f"{wid}#{n + 1}"
            pl = [m for m in members if m in placed]
            open_ = [m for m in members if m in by_row]
            canon = (sorted(pl, key=lambda m: (locs[m][1].get("status") != "active", m))[0] if pl
                     else max(open_, key=lambda m: (_rank(by_row[m]), m)) if open_ else members[0])
            for m in open_:
                r = by_row[m]
                info = {"id": cid, "members": members, "placed": pl, "canonical": canon, "role": "member",
                        "links": {o: edge[(m, o)] for o in members if (m, o) in edge}}
                # the placed member this one rewords: DFO's own duplicate link first, else the most alike
                cm = canon
                if pl:
                    dup = locs[m][1].get("duplicate_of")
                    cm = dup if dup in pl else max(pl, key=lambda o: (similarity(text[m], text[o]), o))
                direct = (m, cm) in edge
                sim = similarity(text[m], text[cm])
                if cm in placed and (direct or sim >= 0.6):
                    s = placed_suggestion(locs[cm][1], labels)
                    if s is not None:
                        conf = "high" if (edge.get((m, cm), "").startswith(("dup", "text")) or sim >= 0.8) else "medium"
                        if signature(r.get("suggestion")) != signature(s):
                            r["before"] = {"suggestion": r.get("suggestion"), "confidence": r.get("confidence"),
                                           "matched": r.get("matched"), "reason": r.get("reason")}
                        r["suggestion"] = s
                        r["confidence"] = conf
                        r["matched"] = "existing" if s["points"] or s["op"] == "within" else "whole"
                        r["reason"] = (f"{conf}: the same place as `{cm}` — its CURATED extent "
                                       f"(\"{text[cm][:120]}\"; place words {sim:.0%} alike"
                                       + (", DFO's own duplicate link" if edge.get((m, cm), '').startswith('dup') else "")
                                       + ")")
                        r.pop("hunt", None) if r.get("hunt", {}).get("status") == "not_found" else None
                        info["role"], info["canonical"] = "inherits", cm
                elif m == canon:
                    info["role"] = "canonical"
                elif canon in by_row:
                    c = by_row[canon]
                    same = (signature(c.get("suggestion")) == signature(r.get("suggestion"))
                            and c.get("caveat") == r.get("caveat"))
                    if same:
                        # the same binding, read from two wordings: the surer reading's confidence holds
                        info["role"] = "agrees"
                        if c.get("suggestion") and _rank(c)[0] > _rank(r)[0]:
                            r["before"] = {"suggestion": r.get("suggestion"), "confidence": r.get("confidence"),
                                           "matched": r.get("matched"), "reason": r.get("reason")}
                            r["confidence"] = c["confidence"]
                            r["reason"] = (f"{c['confidence']} (raised from {r['before']['confidence']}): the same "
                                           f"binding as `{canon}`, a clearer wording of this place — "
                                           + (r.get("reason") or ""))
                    elif ((sim >= 0.8 or edge.get((m, canon), "").startswith("dup"))
                          and (r.get("hunt") or {}).get("status") != "found"
                          and c.get("suggestion") and _rank(c)[0] > _rank(r)[0]):
                        # a hand-hunted reading is never overwritten by a rewording's
                        r["before"] = {"suggestion": r.get("suggestion"), "confidence": r.get("confidence"),
                                       "matched": r.get("matched"), "reason": r.get("reason")}
                        for k in ("suggestion", "confidence", "matched", "points", "caveat"):
                            if k in c:
                                r[k] = c[k]
                        if c.get("hunt"):
                            r["hunt"] = c["hunt"]
                        r["reason"] = f"a rewording of `{canon}` ({sim:.0%} alike) — its suggestion: " + (c.get("reason") or "")
                        info["role"] = "adopts"
                r["cluster"] = info
    return out


def classify(r: Mapping) -> str:
    """WHY this location had no extent (`WHY`)."""
    h = r.get("hunt") or {}
    role = (r.get("cluster") or {}).get("role")
    if not r.get("items"):
        return "no_water"
    if role == "inherits":
        return "variant_of_placed"
    if h.get("category") in WHY:
        return h["category"]
    if not r.get("suggestion"):
        return "other"
    if role in ("adopts", "agrees"):
        return "variant"
    if r.get("matched") == "new":
        return "needs_split"
    return "review_pending"


def outcome(r: Mapping) -> str:
    if not r.get("suggestion"):
        return "unresolvable"
    if r.get("matched") == "new":
        return f"new_{r['confidence']}"
    return "whole" if r.get("matched") == "whole" else f"existing_{r['confidence']}"


def annotate(rows: list[dict], locs: Mapping, labels: Optional[Mapping[str, str]] = None) -> list[list[str]]:
    cl = cluster(rows, locs, labels)
    for r in rows:
        r["why"] = classify(r)
        r["outcome"] = outcome(r)
    return cl


def coverage_md(rows: list[dict], clusters: list[list[str]], n_total: int, n_placed: int) -> str:
    open_ = {r["location_id"] for r in rows}
    clusters = [c for c in clusters if any(m in open_ for m in c)]
    oc = Counter(r["outcome"] for r in rows)
    why = Counter(r["why"] for r in rows)
    roles = Counter((r.get("cluster") or {}).get("role") for r in rows if r.get("cluster"))
    new = {k: oc.get(f"new_{k}", 0) for k in ("high", "medium", "low")}
    ex = {k: oc.get(f"existing_{k}", 0) for k in ("high", "medium", "low")}
    L = ["# DFO extent coverage — every historic location (owner 2026-10-27)", "",
         f"{n_total} historic DFO salmon locations (all scrapes, active + dormant); {n_placed} have a curated "
         f"extent; the other **{len(rows)}** are below — each with a proposal + confidence + evidence in the "
         "review app's DFO mode (`accept suggestion` pre-fills the editor; **Save extent** — your mark — is "
         "the only write). Nothing touched splits.json, the catalogue or marks.jsonl.", "",
         "Regenerate: `python -m pipeline.regs.dfo_salmon.suggest` (matcher → split hunt "
         "`dfo-split-hunt.json` → this pass, `pipeline/regs/dfo_salmon/coverage.py`).", "",
         "## Outcome", "",
         "| outcome | high | medium | low | total |", "|---|---|---|---|---|",
         f"| matched to EXISTING cuts | {ex['high']} | {ex['medium']} | {ex['low']} | {sum(ex.values())} |",
         f"| the whole water (no cut) | | | | {oc.get('whole', 0)} |",
         f"| NEW split proposed | {new['high']} | {new['medium']} | {new['low']} | {sum(new.values())} |",
         f"| unresolvable (why below) | | | | {oc.get('unresolvable', 0)} |", "",
         f"Clusters of rewordings with an unplaced member: **{len(clusters)}** (covering {sum(len(c) for c in clusters)} locations, "
         f"placed members included). Unplaced members: inherit a curated extent {roles.get('inherits', 0)}, "
         f"adopt the cluster's better suggestion {roles.get('adopts', 0)}, already agree {roles.get('agrees', 0)}, "
         f"canonical {roles.get('canonical', 0)}, member (own reading kept — differs in meaning) {roles.get('member', 0)}.", "",
         "## Why each was unplaced", "", "| why | n | meaning |", "|---|---|---|"]
    for k, v in why.most_common():
        L.append(f"| {k} | {v} | {WHY.get(k, k)} |")
    L += ["", "## Unresolvable", "", "| location | why | evidence / route |", "|---|---|---|"]
    for r in sorted(rows, key=lambda r: (r["why"], r["location_id"])):
        if r["outcome"] == "unresolvable":
            w = ((r.get("hunt") or {}).get("why") or r.get("reason") or "").replace("|", "/")
            L.append(f"| `{r['location_id']}` | {r['why']} | {w} |")
    L += ["", "## Caveats on a proposal (read before accepting)", ""]
    for r in sorted(rows, key=lambda r: r["location_id"]):
        if r.get("caveat"):
            L.append(f"- `{r['location_id']}` — {r['caveat']}")
    L += ["", "## Clusters", "", "Canonical first; **placed** = has a curated extent; role of each unplaced member.", ""]
    by = {r["location_id"]: r for r in rows}
    for c in clusters:
        rr = [by[m] for m in c if m in by]
        if not rr:
            continue                    # every member already placed
        info = rr[0].get("cluster") or {}
        parts = []
        for m in sorted(c, key=lambda m: m != info.get("canonical")):
            role = "placed" if m not in by else (by[m].get("cluster") or {}).get("role", "?")
            parts.append(f"`{m}` ({role})")
        L.append("- " + " · ".join(parts))
    L += ["", "## Every location", "", "| location | status | why | proposal | conf |", "|---|---|---|---|---|"]
    for r in sorted(rows, key=lambda r: r["location_id"]):
        s = r.get("suggestion") or {}
        L.append(f"| `{r['location_id']}` | {r.get('status', '')} | {r['why']} | "
                 f"{(s.get('words') or '—').replace('|', '/')} | {r['confidence']} |")
    return "\n".join(L) + "\n"


def summary(rows: Iterable[Mapping]) -> dict:
    return dict(sorted(Counter(r["outcome"] for r in rows).items()))
