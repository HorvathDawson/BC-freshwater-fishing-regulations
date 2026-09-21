"""Per-water split dossiers: every DFO locator that names a place, beside the cut-points that exist.

`dossier.py` answers "which registry item is this water"; this answers "which cut-points does its
regulation need, and which are already curated". Matching is done — 160 of 161 waters are bound — so
what is left is 147 locators that name a bridge, a sign, a dam or a road and have no coordinate.

Grouped BY WATER on purpose. A river's locators reference each other ("from the signs 200 m above the
bridge down to the cable car 200 m below it"), its existing cuts are the vocabulary the next one
should reuse, and a curator opening a map is opening it once per river, not once per rule.

An anchor is classified by what it would take to resolve it:

    satisfied   an existing cut-point's label already appears in the locator text
    confluence  the locator names a tributary whose watershed code is a CHILD of this water's —
                derivable from FWA topology, no coordinate needed
    lake        the locator names a lake and an auto lake boundary exists
    coordinate  a bridge / sign / road / dam — a human has to place it on a map

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.splitwork            # the worklist
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.splitwork "Morice"   # one water
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from pipeline.regs.dfo_salmon.dossier import ENTRIES, item_pin, osm_link
from pipeline.common.curated import GENERATED

HISTORY = Path("cache/dfo_salmon/history")
RAW = Path("cache/dfo_salmon/raw")
#: Locators already carrying an extent are done and drop off the queue. NOTHING ELSE IS FILTERED.
#:
#: This used to be `SKIP_OPS` — a set of parsed `op` values whose locators were dropped before the
#: worklist was built, on the theory that they named no place. It hid 123 unbound locators carrying
#: 253 rules, and the classifier was plainly wrong about them:
#:
#:     [described]         Cayeghle River  "including Colonial River."
#:     [named_tributaries] Qualicum River  "All open portions of the Qualicum River"
#:     [described]         Babine Lake     "within a 400 m radius of the mouth of Pinkut Creek."
#:
#: `described` is the classifier's own "could not decompose" bucket, which is exactly the set a
#: human most needs to see. The op is kept as a triage HINT on each row and never as a filter: a
#: complete locator is its own row, and whether it needs a cut is a curator's call, not a regex's.
COORD_ANCHORS = {"bridge", "boundary_sign", "road", "dam_or_hatchery", "point", "place_name"}
TRIB_RE = re.compile(r"\b([A-Z][A-Za-z'\-]*(?:\s+[A-Z][A-Za-z'\-]*)*\s+(?:River|Creek))\b")

#: Words that appear in almost every locator and every cut label, so sharing one means nothing.
STOP = {"river", "creek", "lake", "lakes", "boundary", "boundaries", "sign", "signs", "fishing",
        "waters", "water", "mainstem", "upstream", "downstream", "from", "the", "and", "of", "at",
        "to", "near", "only", "approximately", "located", "between", "above", "below", "white",
        "triangular", "square", "may", "present", "delineate", "these", "this", "that", "with",
        "portion", "confluence", "area", "areas", "including", "except", "excluding", "point"}


def _tokens(text: str) -> set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", (text or "").lower())
            if len(t) > 3 and t not in STOP}


def cover_candidates(text: str, cuts, water: str = "") -> list[tuple[str, str, int]]:
    """Existing cut-points that MIGHT already cover this locator, by distinctive-word overlap.

    Deliberately a suggestion, not a verdict. The binary `satisfied` test — an existing label
    appearing verbatim inside the locator — misses any rewording, and DFO rewords constantly: the
    Tatshenshini's 'BC boundary' cuts against 'downstream of the BC/Yukon border', or the Somass's
    'boundary signs ~1km u/s' against 'the northeast corner of Collins Farm'. Reporting those as
    NEEDING A CUT invents work; silently treating them as covered would hide a real gap. So: show
    the overlap and let a human decide.
    """
    # The water's OWN name is in every one of its cut ids, so it is not evidence of anything —
    # 'tatshenshini' matched five unrelated lake cuts before this was stripped.
    own = _tokens(water)
    want = _tokens(text) - own
    out = []
    for cid, label, kind in cuts:
        n = len(want & ((_tokens(label) | _tokens(cid)) - own))
        if n:
            out.append((cid, label, n))
    return sorted(out, key=lambda x: -x[2])[:3]


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


_PAGES: dict[str, list[tuple[str, str]]] = {}


def _pages(slug: str) -> list[tuple[str, str]]:
    """[(when, flattened page text)] for one region — the LIVE page plus every archived snapshot.

    Read so a dormant locator can say WHICH DFO pages carried it. Without a date a curator cannot
    tell an obsolete reach from one the scrape simply missed, and the honest answer to "where does
    this come from" is a date, not an assurance.
    """
    if slug in _PAGES:
        return _PAGES[slug]
    sources = [("LIVE", RAW / f"region{slug}-eng.html")]
    sources += [(f.stem.split("_")[-1], f) for f in sorted(HISTORY.glob(f"region{slug}_*.html"))]
    out: list[tuple[str, str]] = []
    for label, path in sources:
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        out.append((label, re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", raw))))
    _PAGES[slug] = out
    return out


def seen_on(slug: str, text: str) -> list[str]:
    """Which cached pages carry this locator's wording. Matched on a distinctive leading slice: a
    page's entities and whitespace differ from the extracted text, so a full-string compare fails."""
    probe = re.sub(r"\s+", " ", (text or "")).strip()[:55]
    if len(probe) < 12:
        return []
    return [when for when, body in _pages(slug) if probe in body]


def _registry(path=None):
    from pipeline.atlas.registry import default_registry_path, load_registry
    p = Path(path) if path else GENERATED.build() / "registry.json"
    return load_registry(p if p.exists() else default_registry_path())


def rule_weight() -> dict[str, int]:
    """`location_id` -> how many typed rules ride on it, joined from the feed.

    **This is what makes the queue orderable by consequence.** An unbound extent is not just a
    gap in the geometry: every rule on that locator resolves to nothing until it is cut, so a
    locator carrying nine rules costs nine regulations and one carrying one costs one. Before the
    rules were typed there was no way to tell those apart, and the worklist could only be ranked
    by how many anchors a water happened to mention.

    Returns an empty map when the feed has not been built — the column then reads 0 and the
    worklist behaves exactly as it did before, rather than failing.
    """
    feed_dir = GENERATED.regs.dfo_salmon / "typed"
    if not feed_dir.exists():
        return {}
    per_fp: dict[str, int] = {}
    for f in sorted(feed_dir.glob("region-*.json")):
        for loc in json.loads(f.read_text(encoding="utf-8")).get("locators", []):
            per_fp[loc["fingerprint"]] = per_fp.get(loc["fingerprint"], 0) + len(loc["rules"])

    out: dict[str, int] = {}
    for f in sorted(ENTRIES.glob("region-*.json")):
        for loc in json.loads(f.read_text(encoding="utf-8")).get("locations", []):
            n = sum(per_fp.get(fp, 0) for fp in loc.get("fingerprints") or [])
            if n:
                out[loc["location_id"]] = n
    return out


def waters(reg) -> list[dict]:
    """One row per DFO water that has at least one place-naming locator."""
    by_stream_name: dict[str, list] = {}
    for it in reg.values():
        if it.kind == "stream":
            by_stream_name.setdefault(it.name.lower(), []).append(it)

    def wscs(it):
        return [x.split(":", 1)[1] for x in it.ref_ids if x.startswith("wsc:")]

    weights = rule_weight()
    out: list[dict] = []
    for p in sorted(ENTRIES.glob("region-*.json")):
        doc = json.loads(p.read_text(encoding="utf-8"))
        by_id = {w["water_id"]: w for w in doc["waters"]}
        locs: dict[str, list] = {}
        for loc in doc["locations"]:
            st = loc.get("source_text") or {}
            if loc.get("binding", {}).get("extents"):
                continue                      # already bound — the only reason to drop a locator
            if loc.get("water_id") in by_id:
                locs.setdefault(loc["water_id"], []).append(loc)
        for wid, ls in locs.items():
            w = by_id[wid]
            items = [reg[i] for i in (w.get("item_ids") or []) if i in reg]
            cuts = [(b.id, b.label or "", b.kind) for it in items for b in it.boundaries]
            mine = {c for it in items for c in wscs(it)}
            rows = []
            for loc in ls:
                st = loc["source_text"]
                text = st.get("specific_area") or ""
                hit = next((c for c in cuts if len(_norm(c[1])) > 4 and _norm(c[1]) in _norm(text)), None)
                kind, detail = "coordinate", ""
                if hit:
                    kind, detail = "satisfied", hit[0]
                else:
                    a = set(st.get("anchor_types") or [])
                    tribs = []
                    for mm in TRIB_RE.finditer(text):
                        for cand in by_stream_name.get(mm.group(1).lower(), []):
                            if any(t.startswith(m + "-") for t in wscs(cand) for m in mine):
                                tribs.append((mm.group(1), cand.id, wscs(cand)[0]))
                    if tribs and not (a & COORD_ANCHORS):
                        kind = "confluence"
                        detail = "; ".join("%s %s (%s)" % t for t in tribs)
                    elif a & {"lake_end"} and not (a & COORD_ANCHORS):
                        kind, detail = "lake", "auto lake boundary"
                rows.append({"op": st.get("op"), "anchors": st.get("anchor_types") or [],
                             "text": text, "kind": kind, "detail": detail,
                             "status": loc.get("status") or "active",
                             "section": loc.get("section") or "",
                             "loc": loc["location_id"],
                             "rules": weights.get(loc["location_id"], 0)})
            out.append({"slug": p.stem.split("region-")[1], "water": w["name"],
                        "region": w.get("region_number"), "sections": w.get("sections") or [],
                        "items": items, "cuts": cuts, "locators": rows})
    return out


def render(w: dict) -> str:
    L = ["=" * 96,
         "%s    [region %s%s]" % (w["water"], w["region"],
                                  (" · DFO scope " + ", ".join(w["sections"])) if w["sections"] else ""),
         "=" * 96]
    L.append("  BOUND TO")
    for it in w["items"] or []:
        pin = item_pin(it.id)
        L.append("    %-18s %-30s kind=%-6s mus=%s sections=%d"
                 % (it.id, repr(it.name), it.kind, list(it.mus), len(it.section_ids)))
        if pin:
            L.append("        %s" % osm_link(*pin))
    if not w["items"]:
        L.append("    (nothing — this water is still unmatched)")

    # DORMANT locators came from an EARLIER DFO page and are kept by the superset seeding so a
    # seasonal reappearance is an exact hit rather than a review item. They are not current work:
    # counting them made the Somass look like 7 outstanding cuts when the live page has 2.
    need = [r for r in w["locators"] if r["kind"] != "satisfied" and r["status"] == "active"]
    need_dorm = [r for r in w["locators"] if r["kind"] != "satisfied" and r["status"] != "active"]
    L.append("\n  EXISTING CUT-POINTS (%d)" % len(w["cuts"]))
    for cid, label, kind in w["cuts"]:
        L.append("    · %-52s %-42s [%s]" % (cid, repr(label), kind))
    if not w["cuts"]:
        L.append("    (none)")

    act = [r for r in w["locators"] if r["status"] == "active"]
    dor = [r for r in w["locators"] if r["status"] != "active"]
    L.append("\n  DFO LOCATORS — %d active (%d need a cut), %d dormant (%d need a cut)"
             % (len(act), len(need), len(dor), len(need_dorm)))
    for group, label in ((act, "ACTIVE — on the DFO page today"),
                         (dor, "DORMANT — on an archived page, not the live one. A regulation "
                               "update can bring any of these back, so they are worth pinning too; "
                               "they are just lower priority than the live page.")):
        if not group:
            continue
        L.append("\n    %s" % label)
        for r in group:
            tag = {"satisfied": "OK ", "confluence": "TRIB",
                   "lake": "LAKE", "coordinate": "MAP "}[r["kind"]]
            L.append("      [%s] %-16s anchors=%s" % (tag, r["op"], r["anchors"]))
            L.append("           %s" % r["text"])
            if r["detail"]:
                L.append("           -> %s" % r["detail"])
            if r["kind"] != "satisfied":
                for cid, label, n in cover_candidates(r["text"], w["cuts"], w["water"]):
                    L.append("           ? maybe already covered by %s  %r  (%d word%s in common)"
                             % (cid, label, n, "" if n == 1 else "s"))
            when = seen_on(w["slug"], r["text"])
            if when:
                L.append("           seen on: %s" % ", ".join(when))
            elif r["status"] != "active":
                L.append("           seen on: (no cached page carries this wording verbatim)")
    return "\n".join(L)


#: How a locator's `op` becomes an extent, and how many cut-points that op needs.
#: `whole_water_excluding` is NOT mapped to `whole`. Its text carries two ends — "upstream of the
#: CNR bridge to a point above the Babine confluence, excluding the areas below" — and the
#: exclusion is the sibling locators carving out, which precedence resolves. Calling it `whole`
#: would silently bind the entire river.
_OP_ARITY = {"between": ("between", 2), "upstream_of": ("upstream_of", 1),
             "downstream_of": ("downstream_of", 1), "whole_water": ("whole", 0),
             "tributaries_only": ("whole", 0), "whole_water_excluding": ("between", 2)}

_RULES_CACHE: dict = {}
_COORD_CACHE: dict = {}


def _rules_for(location_id: str) -> list:
    """The typed rules riding on one locator. A locator carrying nine rules costs nine
    regulations if its cut is wrong; one carrying none costs nothing."""
    if not _RULES_CACHE:
        from pipeline.regs.parsing.catalogue import CatalogueRule, label
        feed = GENERATED.regs.dfo_salmon / "typed"
        by_fp: dict = {}
        if feed.exists():
            for fd in sorted(feed.glob("region-*.json")):
                for l in json.loads(fd.read_text(encoding="utf-8"))["locators"]:
                    by_fp[l["fingerprint"]] = [label(CatalogueRule(**r)) for r in l["rules"]]
        for f in sorted(ENTRIES.glob("region-*.json")):
            for loc in json.loads(f.read_text(encoding="utf-8")).get("locations", []):
                _RULES_CACHE[loc["location_id"]] = [
                    x for fp in (loc.get("fingerprints") or []) for x in by_fp.get(fp, [])]
        _RULES_CACHE.setdefault("", [])
    return _RULES_CACHE.get(location_id, [])


def split_coords(item_ids) -> dict:
    """`{split_id -> (lat, lon)}` for the cuts on these items.

    The build resolves a split to `(blk, route_measure)` and stops — no coordinate, because
    nothing downstream needed one. A curator does: a cut you cannot open on a map is a cut you
    cannot check. Interpolated from the FWA line, once per water.
    """
    key = tuple(sorted(item_ids))
    if key in _COORD_CACHE:
        return _COORD_CACHE[key]
    out: dict = {}
    try:
        import geopandas as gpd
        from project_config import get_config

        resolved = json.loads((GENERATED.build() / "splits.resolved.json").read_text())
        blks = {str(r["blk"]) for r in resolved if r.get("blk")}
        want = [r for r in resolved if str(r.get("blk")) in blks]
        mine = {b for b in blks}
        # only the lines these items actually sit on
        from pipeline.atlas.registry import load_registry
        reg = load_registry(GENERATED.build() / "registry.json")
        item_blks = {str(getattr(n, "blk", "")) for i in item_ids if i in reg
                     for n in [reg[i]] for _ in [0]}
        lines = {str(r["blk"]) for r in want if r["split_id"] in
                 {b.id for i in item_ids if i in reg for b in reg[i].boundaries}}
        if not lines:
            return out
        st = gpd.read_file(str(get_config().fwa_data_gpkg), layer="streams",
                           where="BLUE_LINE_KEY IN (%s)" % ",".join(sorted(lines)),
                           engine="pyogrio").to_crs(4326).sort_values("DOWNSTREAM_ROUTE_MEASURE")
        by_line = {b: g for b, g in st.groupby(st["BLUE_LINE_KEY"].astype(str))}
        for r in want:
            b = str(r["blk"])
            if b not in by_line:
                continue
            g = by_line[b]
            seg = g[g["DOWNSTREAM_ROUTE_MEASURE"] <= r["route_measure"]].tail(1)
            if seg.empty:
                seg = g.head(1)
            row = seg.iloc[0]
            geom = row.geometry.geoms[0] if hasattr(row.geometry, "geoms") else row.geometry
            frac = (r["route_measure"] - row["DOWNSTREAM_ROUTE_MEASURE"]) / max(row["LENGTH_METRE"], 1)
            p = geom.interpolate(max(0.0, min(1.0, frac)), normalized=True)
            out[r["split_id"]] = (round(p.y, 5), round(p.x, 5))
    except Exception:
        return {}
    _COORD_CACHE[key] = out
    return out


_PAGE_ORDER: dict = {}


def page_order() -> dict:
    """`{fingerprint -> position}` in the order the DFO page prints them.

    A curator reads the card beside the webpage, so the card must be in the PAGE's order — its
    sections in order, and the rows inside each section in the order they are printed. Sorting by
    anything else (rules at stake, say) scrambles that and makes the two impossible to follow
    together. `locations/region-N.json` already holds the current scrape in page order; the entry
    file does not, because superset seeding appends every archived version as it finds them.
    """
    if not _PAGE_ORDER:
        src = GENERATED.regs.dfo_salmon / "locations"
        if src.exists():
            for f in sorted(src.glob("region-*.json")):
                for i, l in enumerate(json.loads(f.read_text(encoding="utf-8")).get("locations", [])):
                    _PAGE_ORDER.setdefault(l["fingerprint"], i)
        _PAGE_ORDER.setdefault("", 10 ** 6)
    return _PAGE_ORDER


def _page_pos(loc_id: str) -> int:
    """Where this locator sits on the live page, or a large number if it is not on it."""
    order = page_order()
    for f in sorted(ENTRIES.glob("region-*.json")):
        for loc in json.loads(f.read_text(encoding="utf-8")).get("locations", []):
            if loc["location_id"] == loc_id:
                hits = [order[fp] for fp in (loc.get("fingerprints") or []) if fp in order]
                return min(hits) if hits else 10 ** 6
    return 10 ** 6


def _short(split_id: str, water: str) -> str:
    """`skeena_river__cedarvale` -> `cedarvale`. The water is already the heading."""
    pre = re.sub(r"[^a-z0-9]+", "_", water.lower()).strip("_") + "__"
    s = split_id[len(pre):] if split_id.lower().startswith(pre) else split_id
    return s.replace("_into_" + pre.rstrip("_"), "")


def _wrap(text: str, width: int, indent: str) -> list:
    import textwrap
    return textwrap.wrap(text, width=width, initial_indent=indent, subsequent_indent=indent) or [indent]


def render_solve(w: dict) -> str:
    """One water, one screen. Every locator verbatim, its guessed extent, and what is missing.

    Deliberately terse. The long card is for reading a river you do not know; this is for WORKING
    one, and the thing a curator needs in front of them is the source text, the guess, and the
    gap — not a re-explanation of the method each time.

    Near-identical wordings are grouped, because the superset seeding keeps every version DFO ever
    published and 9% of them are drift variants of another. Chilliwack/Vedder has four. Grouping
    them is what stops the same reach being pinned twice.
    """
    items = [it.id for it in (w["items"] or [])]
    coords = split_coords(items)
    cuts = {cid: (label, kind) for cid, label, kind in w["cuts"]}

    head = "%s   %s · %d cuts" % (
        w["water"], ", ".join(items) or "UNMATCHED", len(w["cuts"]))
    out = ["", "=" * 92, head, "=" * 92]

    # ONE ROW PER LOCATOR, IN THE PAGE'S OWN ORDER. Near-identical wordings are NOT folded
    # together: each is its own locator with its own id, its own extent and its own rules, and only
    # a curator can say whether two of them are the same reach reworded.
    #
    # The order is the DFO page's, because the card is read beside it. Live rows first, in the
    # order printed; then the dormant ones, which are on no page today.
    ordered = sorted(w["locators"],
                     key=lambda r: (r["status"] != "active", _page_pos(r["loc"]), r["text"]))
    n = 0
    waiting = 0
    for r in ordered:
        n += 1
        op, arity = _OP_ARITY.get(r["op"] or "", (None, None))
        # A `cover_candidate` is a FUZZY match — "a rewording the strict label test cannot see" —
        # so it is a GUESS TO CONFIRM, never an answer. Only `detail` on a satisfied locator is a
        # strict hit: the cut's own label appears in the locator text.
        fuzzy = [c[0] for c in cover_candidates(r["text"], w["cuts"], w["water"])]
        strict = [r["detail"]] if (r["kind"] == "satisfied" and r["detail"]) else []
        have = (strict + [h for h in fuzzy if h not in strict])[:arity or 0]
        n_guessed = len([h for h in have if h not in strict])
        missing = (arity or 0) - len(have)
        rules = _rules_for(r["loc"])
        if missing:
            waiting += len(rules)

        if op is None:
            guess, mark = "op=%s — needs a curator's call" % (r["op"] or "none"), "--"
        elif arity == 0:
            guess, mark = "whole", "OK"
        else:
            slots = [_short(h, w["water"]) for h in have] + ["????"] * missing
            guess = "%s(%s)" % (op, ", ".join(slots))
            mark = "??" if missing else ("?" if n_guessed else "OK")

        tail = "%d rule%s" % (len(rules), "" if len(rules) == 1 else "s")
        if r.get("section"):
            tail = "%-7s %s" % (r["section"], tail)
        if r["status"] != "active":
            tail += " · DORMANT"
        out.append("")
        out.append(" %2d %-2s %-58s %s" % (n, mark, guess, tail))
        for line in _wrap(r["text"], 84, "      "):
            out.append(line)
        if missing:
            out.append("      NEED %d more cut-point%s." % (missing, "" if missing == 1 else "s"))
        for h in have:
            if h in coords:
                lat, lon = coords[h]
                flag = "guess" if h not in strict else "exact"
                out.append("      %-5s %-34s %s" % (flag, _short(h, w["water"]), osm_link(*coords[h])))
        for lab in rules[:3]:
            out.append("        · %s" % lab)

    out.append("")
    out.append(" %d locator%s · %d rules waiting on a missing cut-point" %
               (n, "" if n == 1 else "s", waiting))
    out.append(" OK = every end is an exact hit · ? = a GUESS to confirm · ?? = a cut is missing"
               " · -- = no extent shape, a curator decides")
    return "\n".join(out)


def render_extents(water_name: str, slug: str, reg, graph) -> str:
    """What a water's regulations actually SELECT, once its locators are bound.

    The companion to `render`: that one asks "which cut-points are missing", this one asks "what did
    binding them produce". Locators are grouped by RESOLVED SECTIONS rather than by text, which is
    the only grouping that works — DFO reworded one Somass reach three times and the wordings score
    0.126 on string similarity while resolving to identical sections.
    """
    from pipeline.regs.dfo_salmon.entries import load, to_reach_input
    from pipeline.atlas.reach.build import build_reach

    ef = load(slug)
    water = next((w for w in ef.waters if w.name == water_name), None)
    if water is None:
        return "no water %r in region %s" % (water_name, slug)
    L = ["=" * 100, "%s   ->   %s" % (water.name, water.item_ids or "(unbound)"), "=" * 100]

    if water.item_ids:
        item = reg.get(water.item_ids[0])
        measures = {s["split_id"]: s["route_measure"]
                    for s in _resolved() if s["split_id"] in {b.id for b in (item.boundaries if item else ())}}
        L.append("\n  CUT-POINTS (%d)" % len(item.boundaries if item else ()))
        for b in sorted(item.boundaries if item else (), key=lambda x: measures.get(x.id, 1e12)):
            m = measures.get(b.id)
            L.append("    %-10s %-52s %r" % (("%9.1f" % m) if m else "", b.id, b.label))

    groups: dict = {}
    unbound: list = []
    for loc in ef.locations:
        if loc.water != water_name:
            continue
        entry, rules = to_reach_input(loc, [{"species": None}], water)
        if not rules or not rules[0]["extents"]:
            unbound.append(loc)
            continue
        b, _ = build_reach(entry, rules[0], reg, graph)
        groups.setdefault(tuple(sorted(b.sections)), []).append((loc, b, rules[0]["extents"]))

    total = sum(len(v) for v in groups.values()) + len(unbound)
    L.append("\n  %d LOCATORS  ->  %d DISTINCT EXTENTS%s"
             % (total, len(groups), (", %d UNBOUND" % len(unbound)) if unbound else ""))
    for n, (secs, items) in enumerate(sorted(groups.items(), key=lambda kv: -len(kv[1])), 1):
        _loc, b0, ex = items[0]
        L.append("\n" + "-" * 100)
        ops = "; ".join("%s(%s)" % (e["op"], ", ".join(e.get("splits") or []) or "-") for e in ex)
        L.append("  EXTENT %d  %s" % (n, ops))
        L.append("     -> %s, %d section(s): %s"
                 % (b0.outcome.value, len(secs), ", ".join(secs)))
        L.append("     %d locator(s) resolve here:" % len(items))
        for loc, _b, _e in items:
            L.append("")
            L.append("       [%s] op=%s anchors=%s"
                     % (loc.status, loc.source_text.get("op"), loc.source_text.get("anchor_types") or []))
            L.append("       %s" % (loc.source_text.get("specific_area") or "(whole water — no specific area)"))
            if loc.duplicate_of:
                L.append("       duplicate_of: %s  (confirmed=%s)" % (loc.duplicate_of, loc.duplicate_confirmed))
            for nt in loc.binding.notes:
                L.append("       note: %s" % nt)
    for loc in unbound:
        L.append("\n" + "-" * 100)
        L.append("  UNBOUND")
        L.append("       [%s] op=%s anchors=%s"
                 % (loc.status, loc.source_text.get("op"), loc.source_text.get("anchor_types") or []))
        L.append("       %s" % loc.source_text.get("specific_area"))
        for nt in loc.binding.notes:
            L.append("       note: %s" % nt)
    return "\n".join(L)


_RESOLVED: list = []


def _resolved() -> list:
    import json
    if not _RESOLVED:
        p = GENERATED.build() / "splits.resolved.json"
        _RESOLVED.extend(json.loads(p.read_text(encoding="utf-8")) if p.exists() else [])
    return _RESOLVED


def main() -> None:
    ap = argparse.ArgumentParser(description="Per-water DFO split dossiers.")
    ap.add_argument("name", nargs="?", help="water name (substring); omit for the worklist")
    ap.add_argument("--registry")
    ap.add_argument("--all", action="store_true", help="render every water needing a cut")
    ap.add_argument("--by-impact", action="store_true",
                    help="rank the worklist by how many RULES are waiting on each water's cuts, "
                         "rather than by how many anchors it mentions")
    ap.add_argument("--solve", action="store_true",
                    help="one water, everything needed to finish it in one sitting: every locator "
                         "verbatim, the rules riding on it, which cuts are derivable (with the "
                         "evidence) and which need a pin, and the extent each would get. It "
                         "PROPOSES and never writes.")
    ap.add_argument("--extents", action="store_true",
                    help="show what the water's locators RESOLVE TO once bound, grouped by section "
                         "rather than by wording. Needs a built graph.")
    args = ap.parse_args()

    reg = _registry(args.registry)
    ws = waters(reg)
    if args.extents:
        if not args.name:
            raise SystemExit("--extents needs a water name")
        from pipeline.common.io.serialize import read_artifact
        graph = read_artifact(GENERATED.build() / "graph.pkl")
        for w in [x for x in ws if args.name.casefold() in x["water"].casefold()]:
            print(render_extents(w["water"], w["slug"], reg, graph))
        return
    if args.name and not args.all:
        hits = [w for w in ws if args.name.casefold() in w["water"].casefold()]
        if not hits:
            print("no water matching %r with place-naming locators" % args.name)
            return
        for w in hits:
            print(render_solve(w) if args.solve else render(w))
        return

    def _need(w, status=None):
        return sum(1 for r in w["locators"] if r["kind"] != "satisfied"
                   and (status is None or (r["status"] == "active") == (status == "active")))

    def _stalled(w, status="active"):
        """Rules that resolve to nothing until this water's cuts are placed."""
        return sum(r.get("rules", 0) for r in w["locators"]
                   if r["kind"] != "satisfied"
                   and (r["status"] == "active") == (status == "active"))
    # ranked by ACTIVE need first — the live page is the priority — then dormant, so a water with
    # only dormant gaps still appears rather than dropping off the list entirely.
    if args.by_impact:
        ranked = sorted(ws, key=lambda w: (-_stalled(w), -_need(w, "active"), w["water"]))
    else:
        ranked = sorted(ws, key=lambda w: (-_need(w, "active"), -_need(w, "dormant"), w["water"]))
    todo = [w for w in ranked if _need(w)]
    if args.all:
        for w in todo:
            print(render(w) + "\n")
        return
    from collections import Counter
    tot = Counter(r["kind"] for w in ws for r in w["locators"] if r["status"] == "active")
    dorm = sum(1 for w in ws for r in w["locators"]
               if r["status"] != "active" and r["kind"] != "satisfied")
    print("%d waters carry place-naming locators; %d still need at least one cut" % (len(ws), len(todo)))
    print("ACTIVE locators: " + " · ".join("%s %d" % (k, v) for k, v in tot.most_common()))
    print("plus %d DORMANT locators needing a cut — archived wordings a regulation update can "
          "bring back,\n     so worth pinning, just after the live page\n" % dorm)
    stalled = sum(_stalled(w) for w in todo)
    print("%d rules resolve to nothing until these cuts are placed "
          "(%d more behind dormant locators)\n"
          % (stalled, sum(_stalled(w, "dormant") for w in todo)))
    print("%-30s %-6s %5s %5s %5s %5s %6s %6s  %s"
          % ("WATER", "REGION", "rules", "need", "MAP", "TRIB", "maybe?", "dormnt", "cuts"))
    print("   'maybe?' = of the ACTIVE ones, how many already have a plausible existing cut —\n"
          "   a rewording the strict label test cannot see. Check those before pinning anything.")
    for w in todo:
        c = Counter(r["kind"] for r in w["locators"] if r["status"] == "active")
        maybe = sum(1 for r in w["locators"]
                    if r["kind"] != "satisfied" and r["status"] == "active"
                    and cover_candidates(r["text"], w["cuts"], w["water"]))
        print("%-30s r%-5s %5d %5d %5d %5d %6d %6d  %d"
              % (w["water"][:30], w["region"], _stalled(w),
                 sum(v for k, v in c.items() if k != "satisfied"),
                 c["coordinate"], c["confluence"], maybe, _need(w, "dormant"), len(w["cuts"])))


if __name__ == "__main__":
    main()
