"""Waterbody-grouped split curation — SOURCE-FIRST.

Build the split-curation structure FROM THE ORIGINAL REGS SOURCE (not from the curated file), so
nothing is missed: enumerate every regulation entry that contains a split, expand it into all its
locators (with the info block an author needs), then BACKFILL the curation we've already done, and
WARN about any curated row that no longer maps to a source locator (drift).

Sources
  - SPINE   output/pipeline/extraction/synopsis_raw_data.json  (the raw synopsis rows: water, mu,
            region, raw_regs, symbols, page, image — the authoritative entry list, 1395 entries)
  - PARSE   output/pipeline/parsing/synopsis_parsed.json       (per-reg `rules`; a rule with a
            non-empty `location_text` is a boundary = a split)   join: normalize(raw_regs)==regs_verbatim
  - CURATE  stream_sections/docs/14-locators-to-curate.json     (our curation; backfilled in, never mutated)

An entry is SPLIT-BEARING if it has ≥1 boundary rule. Each such entry becomes a card with all its
locators (src = rule|except|entry|name) and, per locator, the curated row(s) that resolve it.

Flags per locator: `MISSING` (no curated row) · `todo`/resolved status from the backfilled row(s).
Per entry: NO_CURATION (split-bearing but zero curated rows — a whole reg never started) ·
MISSING_SPLITS (≥1 boundary with no row) · INCOMPLETE (todo remains) · COMPLETE.
Plus a global DRIFT list: curated rows that matched no source locator.

The matcher is normalized substring — treat MISSING/DRIFT as review candidates, not gospel.

Run from repo root:
    .venv/bin/python -m stream_sections.oneoff.waterbody_splits              # write JSON+MD, summary
    .venv/bin/python -m stream_sections.oneoff.waterbody_splits show "DEAN RIVER"
    .venv/bin/python -m stream_sections.oneoff.waterbody_splits missing      # boundaries with no row
    .venv/bin/python -m stream_sections.oneoff.waterbody_splits incomplete   # entries not fully resolved
    .venv/bin/python -m stream_sections.oneoff.waterbody_splits drift        # curated rows not in source
See docs/18-waterbody-split-curation.md.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "output/pipeline/extraction/synopsis_raw_data.json"
PARSE = ROOT / "output/pipeline/parsing/synopsis_parsed.json"
CURATED = ROOT / "stream_sections/docs/14-locators-to-curate.json"
OUT_JSON = ROOT / "stream_sections/docs/waterbody-splits.json"
OUT_MD = ROOT / "stream_sections/docs/waterbody-splits.md"

RESOLVED = {"curated", "manual", "not_applicable", "deferred"}


def norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("*", "")).strip().lower()


def entry_id(water: str, mu, reg: str) -> str:
    return hashlib.sha1(f"{water}|{','.join(mu or [])}|{norm(reg)}".encode()).hexdigest()[:8]


def _match(a: str, b: str) -> bool:
    a, b = norm(a), norm(b)
    return bool(a) and bool(b) and (a == b or a in b or b in a)


# a water-name parenthetical is a BOUNDARY (not just a note) when it carries reach language
_BOUNDARY_RE = re.compile(
    r"\b(downstream of|upstream of|from |between |below |above |within |to the |downstream to|"
    r"downstream|upstream|except )", re.I)


def water_parts(water: str):
    """(base_name, parenthetical, paren_is_boundary). 'ALEXANDER CREEK (downstream of X)' ->
    ('ALEXANDER CREEK', 'downstream of X', True); 'NAHATLATCH LAKE (east and west)' -> (..., False)."""
    m = re.search(r"\(([^)]*)\)", water)
    paren = m.group(1).strip() if m else ""
    base = re.sub(r"\s*\([^)]*\)", "", water).strip()
    return base, paren, bool(paren and _BOUNDARY_RE.search(paren))


def name_key(water: str) -> str:
    """Normalized name for the curation<->source join: drop ALIAS parentheticals (e.g. (McNaughton),
    ("Blackwater")) but KEEP boundary ones (e.g. (downstream of falls)) so up/down entries stay
    distinct. Tolerant of the truncated curated name via prefix match at the call site."""
    def repl(m):
        return m.group(0) if _BOUNDARY_RE.search(m.group(1)) else ""
    return norm(re.sub(r"\s*\(([^)]*)\)", repl, water))


_KM_RE = re.compile(r"(\d+(?:\.\d+)?)\s*km\b")
_CANON_STOP = {"the", "of", "a", "fishing", "approximately", "to", "from", "and", "between",
               "near", "at", "on", "signs", "sign"}


def canon(t: str | None):
    """Normalized tokens for endpoint↔reach matching: km→m, above→upstream, below→downstream,
    drop filler. 'signs ~0.5 km above the canyon' -> ['500','m','upstream','canyon']."""
    t = (t or "").lower().replace("~", " ")
    t = _KM_RE.sub(lambda m: f"{int(float(m.group(1)) * 1000)} m", t)
    t = re.sub(r"\babove\b", "upstream", t)
    t = re.sub(r"\bbelow\b", "downstream", t)
    return [w for w in re.findall(r"[a-z0-9.]+", t) if w not in _CANON_STOP]


def label_matches(r: dict, boundary_text: str) -> bool:
    """Link an *endpoint* row (empty locator_text, identity in `label`) to a reach boundary: the
    row's label tokens (e.g. 'Crag Creek' or 'signs 500 m upstream of canyon'), normalized for
    km/m + above/upstream, must ALL appear in the boundary text. Scoped to empty-lt rows only."""
    lab = re.sub(r"\s*\([^)]*\)", "", r.get("label") or "")
    lab = re.sub(r"\b(confluence|boundary)\b", "", lab, flags=re.I)
    at = canon(lab)
    return bool(at) and set(at) <= set(canon(boundary_text))


def trim_row(r: dict, water: str, mus) -> dict:
    """Curation-only view of a row for embedding under its locator: entry-level fields (water, mu,
    region, reg_text, entry_id) live on the CARD, so keep only per-row curation and any curated
    name/mu that actually diverges from the card (so the grouped file is a lossless, de-duped
    successor to 14-locators-to-curate.json)."""
    out = {"id": r["id"], "status": r["status"], "anchor_kind": r["anchor_kind"]}
    for k in ("resolver_hint", "label", "notes"):
        if (r.get(k) or "").strip():
            out[k] = r[k]
    if r.get("coord"):
        out["coord"] = r["coord"]
    if r.get("target"):
        out["target"] = r["target"]
    if name_key(r.get("name_verbatim")) != name_key(water):
        out["name_verbatim"] = r.get("name_verbatim")
    if set(r.get("mus") or []) != set(mus or []):
        out["mus"] = r.get("mus")
    return out


def tidy_locator(l: dict) -> dict:
    """Readable locator: source boundary text + optional parse info block + the trimmed rows.
    Drops empty/derivable fields (row_ids/statuses/anchor_kinds, OK flag, empty dates/type)."""
    out = {"src": l["src"], "text": l["text"]}
    info = {k: l[k] for k in ("restriction_type", "dates", "includes_tributaries")
            if l.get(k) not in (None, [], "")}
    if info:
        out["info"] = info
    if l.get("flag") == "MISSING":
        out["flag"] = "MISSING"
    out["resolved"] = l["resolved"]
    out["rows"] = l["rows"]
    return out


def build():
    raw = json.loads(RAW.read_text())
    rows = [r for pg in raw for r in pg["rows"]]
    parsed = json.loads(PARSE.read_text())
    curated = json.loads(CURATED.read_text())["locators"]

    parse_by_reg = defaultdict(list)
    for e in parsed:
        parse_by_reg[norm(e.get("regs_verbatim"))].append(e)

    # Join curation<->source by WATER NAME + MU (reg text is unreliable: near-identical entries
    # differ by punctuation, and curated name_verbatim is truncated). cur_norm caches (row, norm
    # name, mus) once; candidates() finds the curated rows belonging to a source entry.
    cur_norm = [(r, name_key(r.get("name_verbatim")), set(r.get("mus") or [])) for r in curated]
    consumed: set[str] = set()
    BOUNDARY_SRC = ("water", "rule", "except", "entry")

    def candidates(water: str, mu):
        nw, mus = name_key(water), set(mu or [])
        out = []
        for r, nn, rmus in cur_norm:
            if nn and (mus & rmus or not mus) and (nw.startswith(nn) or nn in nw or nw in nn):
                out.append(r)
        return out

    def backfill(cands: list, loc: dict, water: str):
        """Match curated row(s) (already scoped to this source entry by name+MU) to a source locator:
        name/water locators match `src=name` rows against the water name/parenthetical; boundary
        locators match by locator_text."""
        hits = []
        for r in cands:
            lt = r.get("locator_text", "")
            if loc["src"] in ("name", "water"):
                if r.get("src") == "name" and (
                        _match(lt, loc["text"]) or _match(lt, water)
                        or (loc["src"] == "name" and name_key(lt) == name_key(water))):
                    hits.append(r)
            elif norm(lt):
                if _match(lt, loc["text"]):
                    hits.append(r)
            elif label_matches(r, loc["text"]):  # endpoint row (empty locator_text) ↔ reach
                hits.append(r)
        for r in hits:
            consumed.add(r["id"])
        return hits

    cards = {}
    for row in rows:
        reg = row["raw_regs"]
        reg_key = norm(reg)
        water = row["water"]
        pe = parse_by_reg.get(reg_key, [{}])[0]
        rules = pe.get("rules", []) if pe else []
        _, paren, paren_is_boundary = water_parts(water)

        locators = []
        if paren_is_boundary:  # boundary encoded in the water name, e.g. "X (downstream of falls)"
            locators.append({"src": "water", "text": paren})
        for rule in rules:
            lt = (rule.get("location_text") or "").strip()
            if lt:
                locators.append({"src": "rule", "text": lt,
                                 "restriction_type": rule.get("restriction_type"),
                                 "dates": rule.get("dates") or [],
                                 "includes_tributaries": rule.get("includes_tributaries")})
            ex = (rule.get("exception") or "").strip()
            if ex:
                locators.append({"src": "except", "text": ex,
                                 "restriction_type": rule.get("restriction_type")})
        elt = (pe.get("entry_location_text") or "").strip() if pe else ""
        if elt:
            locators.append({"src": "entry", "text": elt})
        # whole-water / tributary-set membership (matches full-name `src=name` rows; consumes them)
        locators.append({"src": "name", "text": water})

        # backfill EVERY entry (so name/whole-water rows are consumed -> real drift only)
        mu = row.get("mu")
        cands = candidates(water, mu)
        for l in locators:
            hits = backfill(cands, l, water)
            l["rows"] = [trim_row(r, water, mu) for r in hits]  # curation-only (entry fields on card)
            l["resolved"] = bool(hits) and all(r["status"] in RESOLVED for r in hits)
            l["flag"] = "MISSING" if (l["src"] in BOUNDARY_SRC and not hits) else "OK"

        boundary_locs = [l for l in locators if l["src"] in BOUNDARY_SRC]
        row_ids = {r["id"] for l in locators for r in l["rows"]}
        if not boundary_locs and not row_ids:
            continue  # neither a split nor any curation on this entry -> nothing to track

        eid = entry_id(water, mu, reg)
        n_missing = sum(1 for l in boundary_locs if l["flag"] == "MISSING")
        n_unresolved = sum(1 for l in boundary_locs if l["flag"] != "MISSING" and not l["resolved"])
        if not boundary_locs:
            completeness = "NO_SPLIT"  # entry carries only whole-water / tributary-set rows (n/a)
        elif not row_ids:
            completeness = "NO_CURATION"
        elif n_missing:
            completeness = "MISSING_SPLITS"
        elif n_unresolved:
            completeness = "INCOMPLETE"
        else:
            completeness = "COMPLETE"
        cards[eid] = {
            "entry_id": eid, "water": water, "mu": mu,
            "region": row.get("region") or (pe.get("region") if pe else None),
            "page": row.get("page"), "image": row.get("image"), "symbols": row.get("symbols"),
            "reg_text": reg, "completeness": completeness,
            "n_boundaries": len(boundary_locs),
            "locators": [tidy_locator(l) for l in locators],
        }

    # drift: curated rows never matched to any source locator (real anomalies / manual additions)
    drift = [{"id": r["id"], "water": r["name_verbatim"], "mu": r.get("mus"),
              "src": r.get("src"), "locator_text": r.get("locator_text", ""), "status": r["status"]}
             for r in curated if r["id"] not in consumed and norm(r.get("locator_text"))]
    return cards, drift


def _rank(c):
    order = {"NO_CURATION": 0, "MISSING_SPLITS": 1, "INCOMPLETE": 2, "COMPLETE": 3, "NO_SPLIT": 4}
    return (order[c["completeness"]], -c["n_boundaries"])


def write_outputs(cards, drift):
    OUT_JSON.write_text(json.dumps({"cards": cards, "drift": drift}, indent=2, ensure_ascii=False) + "\n")
    tally = Counter(c["completeness"] for c in cards.values())
    L = ["# Waterbody split cards — SOURCE-FIRST (generated by oneoff/waterbody_splits.py)\n",
         "One card per split-bearing reg entry from `synopsis_raw_data.json`; each reg boundary is",
         "linked to the curated row(s) that resolve it. Regenerate; do not hand-edit.\n",
         "| completeness | entries |", "|---|--:|"]
    for k in ["NO_CURATION", "MISSING_SPLITS", "INCOMPLETE", "COMPLETE", "NO_SPLIT"]:
        L.append(f"| {k} | {tally.get(k, 0)} |")
    L.append(f"\n**Drift** (curated rows matching no source locator): {len(drift)}\n")
    for c in sorted(cards.values(), key=_rank):
        if c["completeness"] in ("COMPLETE", "NO_SPLIT"):
            continue
        L.append(f"## {c['water']} · MU {c['mu']} · p{c['page']} · [{c['completeness']}] ({c['entry_id']})")
        for l in c["locators"]:
            if l["src"] == "name":
                continue
            mark = "❌ MISSING" if l.get("flag") == "MISSING" else "•"
            rows = ", ".join(f'{r["id"]}[{r["status"]}]' for r in l["rows"]) or "—"
            L.append(f"- {mark} [{l['src']}] “{l['text'][:66]}” → {rows}")
        L.append("")
    if drift:
        L.append("## ⚠️ DRIFT — curated rows not found in the source (review)\n")
        for d in drift:
            L.append(f"- `{d['id']}` [{d['status']}] {d['water']} — src={d['src']} “{d['locator_text'][:50]}”")
    OUT_MD.write_text("\n".join(L) + "\n")


def main():
    args = sys.argv[1:]
    cards, drift = build()
    if args and args[0] == "show":
        q = " ".join(args[1:]).lower()
        for c in sorted(cards.values(), key=_rank):
            if q in c["water"].lower():
                print(json.dumps(c, indent=2, ensure_ascii=False))
        return
    if args and args[0] == "missing":
        for c in sorted(cards.values(), key=_rank):
            for l in c["locators"]:
                if l.get("flag") == "MISSING":
                    print(f"{c['water']:34.34s} MU{c['mu']} p{c['page']} | [{l['src']}] {l['text']}")
        return
    if args and args[0] == "incomplete":
        for c in sorted(cards.values(), key=_rank):
            if c["completeness"] != "COMPLETE":
                miss = sum(1 for l in c["locators"] if l.get("flag") == "MISSING")
                print(f"[{c['completeness']:14s}] {c['water']:32.32s} MU{c['mu']} "
                      f"bounds={c['n_boundaries']} missing={miss}")
        return
    if args and args[0] == "drift":
        for d in drift:
            print(f"{d['id']:46s} [{d['status']:14s}] src={d['src']:6s} “{d['locator_text'][:45]}”")
        return
    if args and args[0] == "stamp":
        # write each curated row's entry_id = the card it belongs to (reliable name+MU join),
        # so row <-> card is a durable bidirectional link. Rows in no card get a self-id.
        r2e = {r["id"]: c["entry_id"] for c in cards.values() for l in c["locators"] for r in l["rows"]}
        doc = json.loads(CURATED.read_text())
        linked = 0
        for r in doc["locators"]:
            eid = r2e.get(r["id"])
            if eid:
                linked += 1
            r["entry_id"] = eid or entry_id(r["name_verbatim"], r.get("mus"), r.get("full_regulation"))
        CURATED.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
        print(f"stamped entry_id on {len(doc['locators'])} rows ({linked} linked to a card)")
        return
    write_outputs(cards, drift)
    tally = Counter(c["completeness"] for c in cards.values())
    n_missing = sum(1 for c in cards.values() for l in c["locators"] if l.get("flag") == "MISSING")
    print(f"split-bearing entries: {len(cards)} | {dict(tally)}")
    print(f"MISSING boundaries: {n_missing} | DRIFT rows: {len(drift)}")
    print(f"wrote {OUT_JSON.relative_to(ROOT)} and {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
