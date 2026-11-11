"""ONE WATER ACROSS CODES (T664, owner 2026-11-11: "the oxbows should be combined as one item").

The registry groups a stream by its gnis, else its watershed code (`build.item_id`). A water the curator NAMED across
several unnamed lines — the Okanagan River Oxbows: 26 channels and child codes + 5 lines named up to their hillside
cut (T637), beside 14 oxbow polygons FWA names and the slough fold made streams — leaves the build as one item per
code: 45 search hits, 45 cards, 45 labels for one water under one rule.

A `name_variants.json` name record may say `"one_water": true`. Then every STREAM item whose name or a variant is that
name (by `names.key`) becomes ONE item. The survivor is the first line item (gnis, then wsc, then blk; a polygon's
`wbk:` only when no line holds the name), lowest id; it takes every member's sections, ref ids (so the owner's
watershed codes and waterbody keys still resolve through the matcher's `id_index`), variants, name sources, MUs and
boundaries, and each absorbed id goes into its `aliases` (rule 55's three read points map it; everything else refuses
it; curated files name the survivor only — restamp `matched`).

What it never does:
- change a KIND. A same-named LAKE or WETLAND item is left as it is and listed (`kept_apart`): folding it into a stream
  would move zone rules (lake rules off, stream rules on) — the owner's call (`ONE_WATER_TAKES_STILL_WATER`).
- touch a section: membership only, so the graph, the handles and every cut are as before.
- drop a boundary that carries a cut alias. A lake boundary onto a polygon the item now holds is not "another water"
  (F6, as `flowing` does) and goes, unless a cut is aliased onto it: then the pass REFUSES.
- merge two waters that merely share a name somewhere else in BC: the flag is per name and set by the curator for one
  water; a name held by items in two regions is refused (`ValueError`), never merged.
"""
from __future__ import annotations

from dataclasses import replace

from pipeline.atlas.registry.flowing import _own_polygon_wbks, _union_by_id
from pipeline.common.models import RegistryItem
from pipeline.common.names import key

#: Owner default (T664, not yet ruled): a same-named lake / wetland item stays its own item, so no rule moves.
ONE_WATER_TAKES_STILL_WATER = False

_RANK = {"gnis": 0, "wsc": 1, "blk": 2}


def one_water_names(name_variants: list[dict]) -> list[str]:
    """Every curated name flagged `one_water` (in file order, once each)."""
    out: list[str] = []
    for e in name_variants or ():
        for n in (e or {}).get("names") or ():
            if isinstance(n, dict) and n.get("one_water") and str(n.get("name") or "").strip():
                nm = str(n["name"]).strip()
                if key(nm) not in {key(x) for x in out}:
                    out.append(nm)
    return out


def _survivor(ids: list[str]) -> str:
    return min(ids, key=lambda i: (_RANK.get(i.split(":", 1)[0], 9), i))


def _region(it: RegistryItem) -> set[str]:
    return {m.split("-", 1)[0] for m in it.mus or () if "-" in m}


def merge_one_waters(registry: dict[str, RegistryItem], names: list[str], report: dict | None = None
                     ) -> dict[str, RegistryItem]:
    """`registry` with every stream item answering to each flagged name made one item. Returns a NEW dict
    (`registry` itself when no name is flagged). `report` gets {name: {survivor, absorbed, kept_apart, sections}}."""
    rep = report if report is not None else {}
    if not names:
        return registry
    out = dict(registry)
    still = ("stream", "lake", "wetland") if ONE_WATER_TAKES_STILL_WATER else ("stream",)
    for nm in names:
        k = key(nm)
        holders = sorted(i for i, it in out.items()
                         if it.kind in ("stream", "lake", "wetland")
                         and k in {key(x) for x in (it.name, *it.variants) if x})
        members = [i for i in holders if out[i].kind in still]
        apart = [i for i in holders if i not in members]
        regions = set().union(*(_region(out[i]) for i in members)) if members else set()
        if len(regions) > 1:
            raise ValueError(f"one_water {nm!r}: its items lie in regions {sorted(regions)} — two waters share the "
                             f"name; name the one meant by a target, never merge them")
        if len(members) < 2:
            rep[nm] = {"survivor": members[0] if members else "", "absorbed": [], "kept_apart": apart,
                       "sections": len(out[members[0]].section_ids) if members else 0}
            continue
        sid = _survivor(members)
        parts = [out[sid]] + [out[i] for i in members if i != sid]
        absorbed = [p.id for p in parts[1:]]
        secs = tuple(dict.fromkeys(s for p in parts for s in p.section_ids))
        own = _own_polygon_wbks(secs)
        bounds = []
        for b in _union_by_id(b for p in parts for b in p.boundaries):
            if b.kind == "lake" and b.wbk in own:
                if b.aliases:
                    raise ValueError(f"one_water {nm!r}: boundary {b.id} onto its own polygon wbk:{b.wbk} carries "
                                     f"cut alias(es) {list(b.aliases)} — give the cut an anchor first")
                continue                                           # F6: its own polygon is not another water
            bounds.append(b)
        head = parts[0]
        out[sid] = replace(
            head, section_ids=secs, boundaries=tuple(bounds),
            variants=tuple(sorted({v for p in parts for v in p.variants} | {nm})),
            name_sources=tuple({r.name: r for p in reversed(parts) for r in p.name_sources}[n]
                               for n in dict.fromkeys(r.name for p in parts for r in p.name_sources)),
            mus=tuple(sorted({m for p in parts for m in p.mus})),
            ref_ids=tuple(sorted({r for p in parts for r in p.ref_ids} | set(absorbed))),
            aliases=tuple(sorted({a for p in parts for a in p.aliases} | set(absorbed))),
        )
        for i in absorbed:
            out.pop(i)
        rep[nm] = {"survivor": sid, "absorbed": absorbed, "kept_apart": apart, "sections": len(secs)}
    return out
