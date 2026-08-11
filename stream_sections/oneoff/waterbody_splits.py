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
See docs/14-waterbody-split-curation.md.
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
OUT_REGS_MD = ROOT / "stream_sections/docs/waterbody-splits-regs.md"

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


def trim_row(r: dict, water: str, mus, loc_text: str) -> dict:
    """Curation-only view of a row for embedding under its locator: entry-level fields (water, mu,
    region, reg_text, entry_id) live on the CARD, so keep only per-row curation and any field that
    actually diverges from the card/locator — so the grouped file is a **lossless, de-duped**
    successor to 14-locators-to-curate.json (see flatten_curation / `verify-flatten`)."""
    out = {"id": r["id"], "status": r["status"], "anchor_kind": r["anchor_kind"], "src": r.get("src")}
    for k in ("resolver_hint", "label", "notes"):
        if (r.get(k) or "").strip():
            out[k] = r[k]
    if r.get("coord"):
        out["coord"] = r["coord"]
    if r.get("offset"):  # {anchor:[lon,lat], anchor_label, m, dir} — preserves distance context for splits.json
        out["offset"] = r["offset"]
    if r.get("polygon"):  # ordered [[lon,lat],...] closure-area corners (e.g. sign-bounded No-Fishing area)
        out["polygon"] = r["polygon"]
    if r.get("target"):
        out["target"] = r["target"]
    if name_key(r.get("name_verbatim")) != name_key(water):
        out["name_verbatim"] = r.get("name_verbatim")
    if set(r.get("mus") or []) != set(mus or []):
        out["mus"] = r.get("mus")
    # locator_text only when it diverges (exact) from the locator's boundary text — covers truncated
    # rows, the empty-lt endpoint rows, and case/whitespace variants; needed to reconstruct on flatten.
    if (r.get("locator_text") or "") != (loc_text or ""):
        out["locator_text"] = r.get("locator_text", "")
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


def build(curated=None):
    raw = json.loads(RAW.read_text())
    rows = [r for pg in raw for r in pg["rows"]]
    parsed = json.loads(PARSE.read_text())
    if curated is None:
        curated = load_curation()
    curated = sorted(curated, key=lambda r: r["id"])  # stable order -> deterministic backfill (idempotent rebuild)
    # whole-waterbody human review flag: an entry reviewed in totality carries `reviewed` on its rows
    reviewed_by_entry = {r["entry_id"]: r["reviewed"] for r in curated if r.get("reviewed") and r.get("entry_id")}

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
            l["rows"] = [trim_row(r, water, mu, l["text"]) for r in hits]  # curation-only (entry fields on card)
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
        if reviewed_by_entry.get(eid):
            cards[eid]["reviewed"] = reviewed_by_entry[eid]  # whole-entry human review (see docs)

    # drift: curated rows never matched to any source locator (real anomalies / manual additions)
    drift = [{"id": r["id"], "water": r["name_verbatim"], "mu": r.get("mus"),
              "src": r.get("src"), "locator_text": r.get("locator_text", ""), "status": r["status"],
              "row": r}  # full row kept so drift is recoverable on flatten
             for r in curated if r["id"] not in consumed and norm(r.get("locator_text"))]
    return cards, drift


# fields that MUST round-trip grouped->flat (curation + identity); region/full_regulation are
# metadata regenerated from source, so excluded from the verify.
CUR_FIELDS = ("status", "anchor_kind", "resolver_hint", "coord", "offset", "polygon", "target", "label",
              "notes", "name_verbatim", "mus", "src", "locator_text", "reviewed")


def flatten_curation(cards, drift):
    """Reconstruct the flat 14-locators list from the grouped file — the inverse of the grouping.
    entry fields come from the card, per-row from the trimmed row (with divergent name/mu/lt kept),
    deduped by id. Drift rows are recovered from their stored full row. Lossless on CUR_FIELDS."""
    seen = {}
    for c in cards.values():
        rev = c.get("reviewed")  # whole-entry review flag lives on the card; push back onto its rows
        for l in c["locators"]:
            for r in l["rows"]:
                if r["id"] in seen:
                    continue
                src = r.get("src") or ("name" if l["src"] == "water" else l["src"])
                seen[r["id"]] = {
                    "id": r["id"], "name_verbatim": r.get("name_verbatim", c["water"]),
                    "region": c.get("region") or "", "mus": r.get("mus", c["mu"]), "src": src,
                    "locator_text": r.get("locator_text", l["text"]), "full_regulation": c["reg_text"],
                    "anchor_kind": r["anchor_kind"], "resolver_hint": r.get("resolver_hint", ""),
                    "status": r["status"], "target": r.get("target", ""), "coord": r.get("coord"),
                    "label": r.get("label", ""), "notes": r.get("notes", ""), "entry_id": c["entry_id"],
                }
                if r.get("offset"):
                    seen[r["id"]]["offset"] = r["offset"]
                if r.get("polygon"):
                    seen[r["id"]]["polygon"] = r["polygon"]
                if rev:
                    seen[r["id"]]["reviewed"] = rev
    for d in drift:
        if d["id"] not in seen and d.get("row"):
            seen[d["id"]] = d["row"]
    return list(seen.values())


def _eq(a, b):
    empty = (None, "", [], {})
    if a in empty and b in empty:
        return True
    return a == b


def load_curation():
    """Curation rows: from the flat file if it still exists, else reconstructed (losslessly) from the
    grouped file — so the grouped file is self-sufficient once 14-locators-to-curate.json is archived."""
    if CURATED.exists():
        return json.loads(CURATED.read_text())["locators"]
    grouped = json.loads(OUT_JSON.read_text())
    return flatten_curation(grouped["cards"], grouped.get("drift", []))


def save_curation(rows):
    """Persist edited curation rows. While the flat file exists, write it (legacy behaviour). After
    it's archived, rebuild the grouped file from the rows — the grouped file is the source of truth."""
    if CURATED.exists():
        CURATED.write_text(json.dumps({"locators": rows}, indent=2, ensure_ascii=False) + "\n")
    else:
        cards, drift = build(rows)
        write_outputs(cards, drift)
        write_regs_md(cards)


def _rank(c):
    order = {"NO_CURATION": 0, "MISSING_SPLITS": 1, "INCOMPLETE": 2, "COMPLETE": 3, "NO_SPLIT": 4}
    return (order[c["completeness"]], -c["n_boundaries"])


def _esc_cell(s: str) -> str:
    """Make text safe inside a GFM table cell: collapse newlines, escape pipes."""
    return re.sub(r"\s+", " ", (s or "")).strip().replace("|", "\\|")


def _is_live(l: dict) -> bool:
    """A locator is a LIVE split (→ highlight) unless it is explicitly n/a. A boundary with no row
    yet (MISSING) is still a live phrase; a whole-water/tributary-set `name` locator never is."""
    if l["src"] not in ("rule", "except", "entry"):
        return False
    rows = l["rows"]
    return not rows or not all(r["status"] == "not_applicable" for r in rows)


def render_reg(reg_text: str, live_phrases: list[str]) -> tuple[str, int]:
    """Reg text for a table cell with each live boundary phrase bolded inline. Whitespace-flexible,
    case-insensitive match; overlapping hits merged. Returns (markdown, n_phrases_highlighted)."""
    disp = re.sub(r"\s+", " ", (reg_text or "").replace("*", "")).strip()  # drop source ** markup
    low = disp.lower()
    spans: list[tuple[int, int]] = []
    for p in live_phrases:
        pd = re.sub(r"\s+", " ", p or "").strip().lower()
        if len(pd) < 4:
            continue
        i = low.find(pd)
        if i >= 0:
            spans.append((i, i + len(pd)))
            continue
        m = re.search(re.escape(pd).replace(r"\ ", r"\s+"), low)  # tolerate internal whitespace diffs
        if m:
            spans.append((m.start(), m.end()))
    if not spans:
        return _esc_cell(disp), 0
    spans.sort()
    merged = [spans[0]]
    for s, e in spans[1:]:
        if s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    esc = lambda t: t.replace("|", "\\|")  # disp is already whitespace-collapsed; don't strip spans
    out, prev = [], 0
    for n, (s, e) in enumerate(merged, 1):  # number each boundary [n] within the entry
        out.append(esc(disp[prev:s]))
        out.append(f"**[{n}] " + esc(disp[s:e]).strip() + "**")
        prev = e
    out.append(esc(disp[prev:]))
    return "".join(out), len(merged)


def write_regs_md(cards):
    """Original-synopsis-style table of split-bearing reg entries, with live split locators highlighted
    inline in the reg text. Entries whose locators are ALL `not_applicable` (whole-water / tributary-set /
    lake-outlet / exemption clauses — no live split) are hidden; see `waterbody-splits.json` for those.
    A read-only review aid; prototype of the future curation_status `regs-md` view. See docs/14."""
    active, deferred, n_hi, n_hidden = [], [], 0, 0
    for c in sorted(cards.values(), key=lambda c: (c.get("region") or "", c.get("page") or 0,
                                                    c["water"])):
        live = [l for l in c["locators"] if _is_live(l)]
        if not live:  # all-n/a / whole-water entry — hide it, keep this doc to split-bearing regs
            n_hidden += 1
            continue
        # number per BOUNDARY row (a "from X to Y" reach with -a/-b rows gets a number for each
        # endpoint via the row's own locator_text); fall back to the locator phrase when a row
        # has no sub-text or the boundary isn't curated yet (MISSING).
        phrases = []
        for l in live:
            live_rows = [r for r in l["rows"] if r["status"] != "not_applicable"]
            if not live_rows:
                phrases.append(l["text"])
            else:
                phrases.extend(r.get("locator_text") or l["text"] for r in live_rows)
        cell, k = render_reg(c["reg_text"], phrases)
        n_hi += k
        mu = ", ".join(c["mu"] or [])
        row = f"| {_esc_cell(c['water'])} | {mu} | {c.get('page') or ''} | {cell} |"
        # a card is DEFERRED if every live locator resolves only to deferred rows (lake-splits etc.);
        # rowless (MISSING/todo) or curated/manual locators keep it in the active table.
        is_def = all(l["rows"] and all(r["status"] == "deferred" for r in l["rows"]) for l in live)
        (deferred if is_def else active).append(row)
    L = ["# Regulations with split locators highlighted (generated by oneoff/waterbody_splits.py)\n",
         "Split-bearing reg entries only (source-first, booklet order). **[n] bold** = a live split boundary,",
         "numbered per entry.",
         "Entries whose locators are ALL `not_applicable` (whole-water / tributary-set / lake-outlet /",
         "exemption clauses) are omitted — see `waterbody-splits.json` for the full set. Regenerate; do not hand-edit.\n",
         f"{len(active)} active split-bearing entries · {n_hi} highlighted boundary phrases "
         f"({n_hidden} all-n/a hidden, {len(deferred)} deferred — see table at end).\n",
         "| Water | MU | p | Regulation |", "|---|---|--:|---|"]
    L.extend(active)
    if deferred:
        L += ["", f"## Deferred entries ({len(deferred)}) — splits agreed-deferred (lake-splits etc.), not yet resolved\n",
              "| Water | MU | p | Regulation |", "|---|---|--:|---|"]
        L.extend(deferred)
    OUT_REGS_MD.write_text("\n".join(L) + "\n")
    return n_hi


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
    if args and args[0] == "verify-flatten":
        if not CURATED.exists():
            print("14-locators-to-curate.json is archived; grouped file is now the source (nothing to compare).")
            return
        flat = flatten_curation(cards, drift)
        orig = json.loads(CURATED.read_text())["locators"]
        fi = {r["id"]: r for r in flat}
        oi = {r["id"]: r for r in orig}
        miss, extra = set(oi) - set(fi), set(fi) - set(oi)
        diffs = [(i, k, oi[i].get(k), fi[i].get(k)) for i in set(oi) & set(fi)
                 for k in CUR_FIELDS if not _eq(oi[i].get(k), fi[i].get(k))]
        print(f"flat {len(flat)} | orig {len(orig)} | missing {len(miss)} | extra {len(extra)} "
              f"| field diffs {len(diffs)}  => {'LOSSLESS ✅' if not (miss or extra or diffs) else 'MISMATCH ❌'}")
        for i in list(miss)[:8]:
            print("  MISSING", i)
        for i in list(extra)[:8]:
            print("  EXTRA  ", i)
        for i, k, a, b in diffs[:20]:
            print(f"  DIFF {i} .{k}: {a!r:.34} != {b!r:.34}")
        return
    if args and args[0] == "regs-md":
        n_hi = write_regs_md(cards)
        print(f"wrote {OUT_REGS_MD.relative_to(ROOT)} ({n_hi} highlighted; all-n/a entries hidden)")
        return
    if args and args[0] == "stamp":
        # write each curated row's entry_id = the card it belongs to (reliable name+MU join),
        # so row <-> card is a durable bidirectional link. Rows in no card get a self-id.
        if not CURATED.exists():
            print("14-locators-to-curate.json is archived; entry_id is derived in the grouped file (no-op).")
            return
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
    write_regs_md(cards)
    tally = Counter(c["completeness"] for c in cards.values())
    n_missing = sum(1 for c in cards.values() for l in c["locators"] if l.get("flag") == "MISSING")
    print(f"split-bearing entries: {len(cards)} | {dict(tally)}")
    print(f"MISSING boundaries: {n_missing} | DRIFT rows: {len(drift)}")
    print(f"wrote {OUT_JSON.relative_to(ROOT)} and {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
