"""match — bind DFO water names to registry items. Exact hits only.

**Nothing here writes a binding except an exact registry name/variant hit.** Everything
else is a *suggestion* for a curator, recorded but never applied. That rule is not
caution for its own sake: the heuristics that used to run here bound Yakoun River to
Yakoun Lake, Lakelse River to Lakelse Lake, and Long Lake to Long Creek — all
confidently, all wrong, none detectable downstream.

What a suggestion looks like, and why it is only ever that:

    retry      "Somass River tributaries" -> "Somass River"      a guess at what DFO meant
    spelling   "Kwinimass River" ~ "Kwinamass River"             0.93, and "Cayeghle
                                                                 River" ~ "Eagle River"
                                                                 scores well too

A name that does not hit exactly is **unbound**, full stop, and lands in the worklist.
The curator answers it in the entry file — including the many-to-one cases a heuristic
could never get right: "Chilliwack/Vedder River (including Sumas River)" is three
registry items, "Adam and Eve Rivers" is two. `WaterBinding.item_ids` is a list for
exactly that, so an override is expressed in the entry file rather than a side file.

The binding lives on the WATER, not the reach: Skeena River has 15 reaches and one
answer to "which blue line is this".

CLI
---
    .venv/bin/python -m pipeline.dfo_salmon.match report
    .venv/bin/python -m pipeline.dfo_salmon.match apply       # bind exact hits only
    .venv/bin/python -m pipeline.dfo_salmon.match worklist --regions 6
"""

from __future__ import annotations

import argparse
import collections
import difflib
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.dfo_salmon.entries import ENTRIES_DIR, EntryLocation, load, save
from pipeline.dfo_salmon.fetch import ALL_SLUGS, PAGES, normalize_slug
from pipeline.dfo_salmon.locations import normalize
from pipeline.dfo_salmon.untangle import named_streams

logger = logging.getLogger(__name__)

REGISTRY = Path("output/v2/full/registry.json")
SPLITS = Path("pipeline/splits.json")

_FEATURES = ("River", "Creek", "Lake", "Slough", "Channel")

#: Feature words that may be swapped for one another. River<->Creek only: DFO calls
#: small streams "River" where the FWA gazetteer says "Creek" (Washlawlis, Braverman).
#: Lake is NEVER swapped — a lake and the river draining it are different waters and
#: usually BOTH exist, so the swap silently binds the wrong one (measured: it turned
#: Yakoun River into Yakoun Lake, Lakelse River into Lakelse Lake, Long Lake into
#: Long Creek).
_SWAPPABLE = {"river", "creek"}
#: Below this ratio a near-spelling is not even worth showing.
_FUZZY_FLOOR = 0.86


def name_candidates(name: str) -> List[Tuple[str, str]]:
    """`(lookup, why)` retries for a name that did NOT match exactly.

    **Suggestions only.** These encode how DFO writes names — scope appended to the
    name column, two waters compounded, a feature word the FWA gazetteer disagrees
    with — and any one of them can land on a different river. They are shown to a
    curator and never applied.
    """
    out: List[Tuple[str, str]] = []
    seen = set()

    def add(n: str, why: str) -> None:
        n = (n or "").strip(" .,")
        k = normalize(n)
        if k and k not in seen:
            seen.add(k)
            out.append((n, why))

    add(name, "verbatim")
    base = re.sub(r"\s*[\(\[][^)\]]*[\)\]]", " ", name).strip()
    add(base, "drop parenthetical")

    trimmed = re.sub(r"\s+(?:and\s+|incl(?:uding)?\.?\s+)?trib\w*\b.*$", "", base, flags=re.I)
    add(trimmed, "drop 'tributaries'")
    trimmed = re.sub(r"\s+Hatchery\b.*$", "", trimmed, flags=re.I).strip()
    add(trimmed, "drop 'Hatchery'")

    # "Chilliwack/Vedder River" — two names sharing one feature word.
    if "/" in trimmed:
        left, right = trimmed.split("/", 1)
        m = re.search(r"\b(%s)\b" % "|".join(_FEATURES), right, re.I)
        sfx = m.group(1) if m else ""
        add(f"{left.strip()} {sfx}".strip(), "left of '/'")
        add(right.strip(), "right of '/'")

    # "Adam and Eve Rivers" — two waters, plural feature word.
    m = re.match(r"^(.*?)\s+and\s+(.*?)\s+((?:%s)s)$" % "|".join(_FEATURES), trimmed, re.I)
    if m:
        sfx = m.group(3).rstrip("sS")
        add(f"{m.group(1)} {sfx}", "compound, first")
        add(f"{m.group(2)} {sfx}", "compound, second")

    # DFO and the FWA gazetteer disagree on the feature word more often than on the
    # name: "Washlawlis River" is FWA's Washlawlis Creek.
    m = re.match(r"^(.*?)\s+(%s)$" % "|".join(_FEATURES), trimmed, re.I)
    if m and m.group(2).lower() in _SWAPPABLE:
        for alt in sorted(_SWAPPABLE - {m.group(2).lower()}):
            add(f"{m.group(1)} {alt.capitalize()}", f"'{m.group(2)}' -> '{alt.capitalize()}'")
    elif not m and trimmed:
        # A bare name with no feature word at all ("Khutze").
        for alt in ("River", "Creek", "Lake"):
            add(f"{trimmed} {alt}", f"add '{alt}'")
    return out


def drop_skips(overrides: List[dict]) -> List[dict]:
    """Ignore `skip` overrides. They are a SYNOPSIS-ROW fact, not a name fact.

    In the synopsis a `skip` means "this row is a pointer to another row that carries
    the real regulations" — e.g. "VEDDER RIVER: See Chilliwack River". That is about how
    the *synopsis is laid out*, and says nothing about which blue line the name means.
    DFO's table is laid out differently, so honouring it only suppresses good matches.

    Measured: all three skips DFO hit are names the registry already resolves on its own.

        ISHKHEENICKH RIVER  → gnis:4069  (carried as a variant of Ksi Hlginx)
        TSEAX RIVER         → gnis:3828  (carried as a variant of Ksi Sii Aks)
        LITTLE CAMPBELL RIVER → gnis:7250 (a variant of the MU 2-4 Campbell River —
                                the region gate keeps it off the Island one)

    "There is no registry item for this name" is a real outcome, but it belongs in the
    entry file as `not_found` with a reason, not in a file whose job is name → geometry.
    """
    return [e for e in overrides if not e.get("skip")]


@dataclass
class Proposal:
    water_id: str
    region: str
    water: str
    #: matched (exact) | ambiguous (exact, several items) | unmatched
    status: str
    item_ids: List[str] = field(default_factory=list)
    via: str = ""
    reason: str = ""
    #: Registry items the exact name resolved to, when there was more than one.
    candidates: List[Dict[str, str]] = field(default_factory=list)
    #: Never applied. `{kind: retry|spelling, name, item_ids, why}` for the worklist.
    suggestions: List[Dict[str, str]] = field(default_factory=list)

    @property
    def bindable(self) -> bool:
        """An exact, unambiguous hit — one item OR an override that names several.

        NOT `len(item_ids) == 1`: an override naming several waters is the most
        deliberate binding in the system (Fraser River in Region 2 names 13 items,
        Nicomen Slough names 7), and requiring a single id silently refused every one
        of them. "Ambiguous" means the matcher could not choose; a curated list is the
        opposite of that.
        """
        return self.status == "matched" and bool(self.item_ids)


def _spelling_suggestions(name: str, name_index: Dict[str, List[str]]) -> List[Dict[str, str]]:
    key = normalize(name)
    near = difflib.get_close_matches(key, list(name_index), n=3, cutoff=_FUZZY_FLOOR)
    out = []
    for n in near:
        ratio = difflib.SequenceMatcher(None, key, n).ratio()
        # Only offer a near-spelling that keeps the feature word — "Cayeghle River" vs
        # "Eagle River" scores well on letters and is a different river.
        if key.split()[-1:] != n.split()[-1:]:
            continue
        out.append({"name": n, "item_ids": name_index[n][:3], "ratio": round(ratio, 3)})
    return out


def propose(ef, registry, name_index, id_index, overrides) -> List[Proposal]:
    """One proposal per water. Exact hits bind; everything else is a suggestion."""
    from pipeline.matching.matcher import build_override_index, match_row

    ov_index = build_override_index(overrides)
    out: List[Proposal] = []
    for w in ef.waters:
        r = match_row(0, {"water": w.name, "region": f"REGION {w.region_number}"},
                      registry, name_index, id_index, ov_index)

        if r.status in ("matched", "override"):
            out.append(Proposal(w.water_id, w.region, w.name, "matched",
                                item_ids=[r.item_id] + list(r.also),
                                via=("override" if r.status == "override" else "exact name"),
                                reason=r.reason))
            continue

        if r.status == "ambiguous":
            out.append(Proposal(
                w.water_id, w.region, w.name, "ambiguous", via="exact name",
                reason=r.reason,
                candidates=[{"item_id": c, "name": registry[c].name} for c in r.candidates]))
            continue

        # No exact hit. Collect suggestions for the curator; bind nothing.
        sug: List[Dict[str, str]] = []
        for lookup, why in name_candidates(w.name)[1:]:
            hits = name_index.get(normalize(lookup)) or []
            if hits:
                sug.append({"kind": "retry", "name": lookup, "why": why,
                            "item_ids": hits[:3],
                            "resolves_to": registry[hits[0]].name})
        for sp in _spelling_suggestions(w.name, name_index):
            sug.append({"kind": "spelling", "name": sp["name"], "why": f"ratio {sp['ratio']}",
                        "item_ids": sp["item_ids"],
                        "resolves_to": registry[sp["item_ids"][0]].name})
        out.append(Proposal(w.water_id, w.region, w.name, "unmatched",
                            reason=r.reason or "no exact name or variant match",
                            suggestions=sug[:5]))
    return out


# ---------------------------------------------------------------------------
# Worklist
# ---------------------------------------------------------------------------


def _splits_by_water() -> Dict[str, List[dict]]:
    if not SPLITS.exists():
        return {}
    out: Dict[str, List[dict]] = collections.defaultdict(list)
    for wb in json.loads(SPLITS.read_text(encoding="utf-8")).get("waterbodies", []):
        key = normalize(re.sub(r"\s*\([^)]*\)", "", wb.get("name", "")))
        for s in wb.get("splits") or []:
            out[key].append(s)
    return out


def worklist(slug: str, entries_dir: Path = ENTRIES_DIR, unbound_only: bool = False) -> str:
    """The curation card: one block per water, everything needed to author its splits."""
    ef = load(slug, entries_dir)
    splits = _splits_by_water()
    by_water: Dict[str, List[EntryLocation]] = {}
    for loc in ef.locations:
        if loc.kind == "water" and loc.water_id:
            by_water.setdefault(loc.water_id, []).append(loc)

    waters = sorted(ef.waters, key=lambda w: (w.bound, w.name))   # unbound first
    if unbound_only:
        waters = [w for w in waters if not w.bound]

    n_reach = sum(1 for ls in by_water.values() for l in ls if not l.is_variant)
    n_var = sum(1 for ls in by_water.values() for l in ls if l.is_variant)
    L: List[str] = [
        f"REGION {slug} — {ef.region_name}",
        f"{len(ef.waters)} waters ({sum(1 for w in ef.waters if not w.bound)} UNBOUND), "
        f"{n_reach} reaches to bind"
        + (f" ({n_var} more are the same reach reworded — shown nested)" if n_var else ""),
        "",
    ]
    for w in waters:
        locs = by_water.get(w.water_id) or []
        m = w.match or {}
        L.append("=" * 78)
        head = w.name
        if w.sections:
            head += f"   [section {', '.join(w.sections)}]"
        L.append(head)

        if w.bound:
            L.append(f"  registry : BOUND -> {', '.join(w.item_ids)}"
                     + (f"   (via {m.get('via')})" if m.get("via") else ""))
        else:
            L.append(f"  registry : **UNBOUND** — {m.get('reason', 'not matched yet')}")
            for c in (m.get("candidates") or [])[:5]:
                L.append(f"             candidate : {c.get('name')}   {c.get('item_id')}")
            for sg in (m.get("suggestions") or []):
                L.append(f"             suggestion: {sg.get('name')!r} -> "
                         f"{sg.get('resolves_to')}   {(sg.get('item_ids') or [''])[0]}"
                         f"   [{sg.get('kind')}: {sg.get('why')}]")
            L.append(f"             SET item_ids IN entries/region-{slug}.json "
                     f"(water_id {w.water_id}); a list — one DFO name can be several items")

        curated = splits.get(normalize(w.name)) or []
        if curated:
            L.append(f"  splits already curated ({len(curated)}):")
            for sp in curated:
                L.append(f"     - {sp.get('label','')}   [{sp.get('id','')}]")
        else:
            L.append("  splits already curated: none")

        variants_of: Dict[str, List[EntryLocation]] = {}
        for loc in locs:
            if loc.is_variant:
                variants_of.setdefault(loc.duplicate_of, []).append(loc)

        for loc in sorted((l for l in locs if not l.is_variant), key=lambda x: x.location_id):
            scope = loc.source_text.get("specific_area") or "(whole water)"
            types = ", ".join(loc.source_text.get("anchor_types") or []) or "-"
            need = {"upstream_of": 1, "downstream_of": 1, "between": 2}.get(
                loc.source_text.get("op") or "", 0)
            L.append("")
            L.append(f"  · {loc.location_id}"
                     + (f"   [{loc.status}]" if loc.status != "active" else ""))
            L.append(f"    op={loc.source_text.get('op')}  needs {need} split(s)  anchors: {types}")
            L.append(f"    SCOPE: {scope}")
            for n in loc.binding.notes:
                L.append(f"    note : {n}")
            for v in sorted(variants_of.get(loc.location_id, []), key=lambda x: x.location_id):
                L.append(f"    same reach, reworded ({v.duplicate_score}): {v.location_id}")
                L.append(f"       {v.source_text.get('specific_area') or '(whole water)'}")
                L.append(f"       not the same? python -m pipeline.dfo_salmon.entries "
                         f"contest --location-id {v.location_id}")
            L.append("    SPLITS TO ADD: ______________________________________________")
        L.append("")
    return "\n".join(L)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _load_all(entries_dir: Path, slugs: List[str]):
    return {s: load(s, entries_dir) for s in slugs}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["report", "apply", "worklist"])
    ap.add_argument("--regions", nargs="+", choices=ALL_SLUGS)
    ap.add_argument("--entries-dir", type=Path, default=ENTRIES_DIR)
    ap.add_argument("--registry", type=Path, default=REGISTRY)
    ap.add_argument("--out", type=Path, help="worklist: write here instead of stdout")
    ap.add_argument("--unbound-only", action="store_true",
                    help="worklist: only waters with no registry item yet")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    slugs = [normalize_slug(s) for s in (args.regions or
             [s for s in ALL_SLUGS if not PAGES[s].is_stub])]

    if args.command == "worklist":
        text = "\n\n".join(worklist(s, args.entries_dir, args.unbound_only) for s in slugs)
        if args.out:
            args.out.write_text(text + "\n", encoding="utf-8")
            print(f"-> {args.out}")
        else:
            print(text)
        return 0

    from pipeline.matching.matcher import build_id_index, build_name_index, load_overrides
    from pipeline.reach.covered import DEFAULT_OVERRIDES
    from pipeline.registry.io import load_registry

    registry = load_registry(str(args.registry))
    name_index = build_name_index(registry)
    id_index = build_id_index(registry)
    # `"__default__"` is a sentinel resolved by `reach.covered.make_matcher`, NOT a
    # path — passing it to load_overrides() silently loads zero overrides, which is
    # what this module did until the shared file was found to already answer six of
    # its seven ambiguous waters.
    # source="dfo" also pulls in overrides tagged as DFO-only — answers to a question only this
    # matcher asks, which the provincial matcher must never see.
    overrides = load_overrides(DEFAULT_OVERRIDES if DEFAULT_OVERRIDES.exists() else None, source="dfo")
    overrides = drop_skips(overrides)
    logger.info("loaded %d usable overrides from %s", len(overrides), DEFAULT_OVERRIDES)

    totals = collections.Counter()
    for slug in slugs:
        ef = load(slug, args.entries_dir)
        props = propose(ef, registry, name_index, id_index, overrides)
        counts = collections.Counter(p.status for p in props)
        totals.update(counts)

        if args.command == "apply":
            by_id = {p.water_id: p for p in props}
            for w in ef.waters:
                p = by_id.get(w.water_id)
                if p is None or w.locked:
                    continue
                # A CURATOR ANSWER OUTRANKS THE MATCHER. `item_ids` was already protected, but the
                # match block was not — so a re-run silently reverted a decided water to "ambiguous"
                # and threw away the reasoning, leaving item_ids bound with nothing saying why.
                # Yakoun River (curator: the river, not Yakoun Lake) was reverted exactly this way.
                if (w.match or {}).get("via") == "curator":
                    continue
                w.match = {"status": p.status, "via": p.via, "reason": p.reason,
                           "candidates": p.candidates, "suggestions": p.suggestions}
                # ONLY an exact, unambiguous hit is ever written.
                if p.bindable and not w.item_ids:
                    w.item_ids = list(p.item_ids)
            save(ef, args.entries_dir)

        bound = sum(1 for w in ef.waters if w.bound)
        print(f"region {slug:<3} {ef.region_name:<38} waters={len(ef.waters):<4} "
              f"bound={bound:<4} " +
              "  ".join(f"{k}={v}" for k, v in sorted(counts.items()) if k != "matched"))
        if args.command == "report":
            for p in props:
                if p.status == "matched":
                    continue
                print(f"    [{p.status}] {p.water}: {p.reason}")
                for c in p.candidates[:4]:
                    print(f"        candidate: {c['name']}  {c['item_id']}")
                for sg in p.suggestions:
                    print(f"        suggestion ({sg['kind']}): {sg['name']!r} "
                          f"-> {sg['resolves_to']}  {sg['item_ids'][0]}   [{sg['why']}]")

    n = sum(totals.values())
    print(f"\n{n} waters")
    print(f"  BOUND — exact registry name/variant or override : {totals['matched']:>4}"
          f"  ({totals['matched']/n:.0%})")
    print(f"  ambiguous — exact name, several items           : {totals['ambiguous']:>4}"
          f"  ({totals['ambiguous']/n:.0%})")
    print(f"  unmatched — needs a curator                     : {totals['unmatched']:>4}"
          f"  ({totals['unmatched']/n:.0%})")
    print("\nNothing but an exact hit is ever written. Suggestions are shown, never applied.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
