"""A NAMED WATER'S OWN CHANNELS (owner 2026-11-15, mark 2578: "make sure that when i checked fraser as water for the
mainstem items it includes all channels too. like sea bird island etc.").

A named side channel is its own registry item on purpose (folded in, its name became the river's), so a DFO place's
water lists them beside the river (`WaterBinding.item_ids`: the Fraser and twelve channels and sloughs). An extent that
names the river alone (`item_id: gnis:39325`, what the review editor sends when the owner ticks "Fraser River") held
on the river's own sections only: Mission -> Spuzzum bound 41 sections, not the 50 the same stretch binds on the
water, and Maria Slough (Seabird Island) and Jeperson Side Channel — 19.5 km — fell out.

THIS WATER IS THE WATER WITH ITS OWN CHANNELS: an extent of a DFO place that names items holds on those items AND
every item of the place's water that is a SIDE CHANNEL of one of them (a channel off a channel too). A side channel is
told from a tributary by direction, never by name: it both TAKES water from the item and RETURNS it (a tributary only
flows in), it is a stream (a lake on the line takes and returns too: Cowichan Lake is no channel of the Cowichan),
and it is the SHORTER of the two (leave-and-rejoin is symmetric). Only the place's own water is read (`among`): a
channel the curated water leaves out (Nicomen Slough, with a page of its own) stays out.

Resolution time, in the builder, so every path agrees — the review app's preview, the reach run, a mark made by any
editor, a suggestion, a hand edit, and every mark already saved (no mark or place file is rewritten).
`DFO_NAMED_WATER_TAKES_ITS_CHANNELS` False is the old reading (the mutation pin)."""
from __future__ import annotations

from typing import Iterable, Mapping, Optional

#: POLICY (owner 2026-11-15): an extent of a DFO place naming a water holds on that water's own side channels too
DFO_NAMED_WATER_TAKES_ITS_CHANNELS = True
#: the ENTRY key a DFO place's reach input carries (`dfo_salmon.entries.to_reach_input`, never the corpus)
CHANNELS_KEY = "_named_water_takes_its_channels"

_OWNERS: dict = {}           # (id(registry),) -> {section: [items]}
_MEMO: dict = {}             # (id(registry), id(graph)) -> {item: (side channels,)}


def _owners(registry) -> Mapping[str, list]:
    key = id(registry)
    got = _OWNERS.get(key)
    if got is None:
        got = {}
        for iid, it in registry.items():
            if str(iid).startswith("area:"):
                continue
            for s in it.section_ids:
                got.setdefault(s, []).append(iid)
        _OWNERS.clear()
        _OWNERS[key] = got
    return got


def _length(registry, graph, iid: str) -> float:
    it = registry.get(iid)
    if it is None:
        return 0.0
    return sum((n.length_m or 0.0) for s in it.section_ids if (n := graph.nodes.get(s)) is not None)


def side_channels(registry, graph, item_id: str) -> tuple[str, ...]:
    """Registry STREAM items that are anabranches of `item_id`: each takes water from it and returns water to it, and
    is shorter (sorted). The review app's map reads the same (`reuse.item_side_channels`)."""
    key = (id(registry), id(graph))
    memo = _MEMO.get(key)
    if memo is None:
        _MEMO.clear()
        memo = _MEMO[key] = {}
    if item_id in memo:
        return memo[item_id]
    it = registry.get(item_id)
    if it is None:
        memo[item_id] = ()
        return ()
    own = set(it.section_ids)
    takes_from, gives_to = set(), set()
    for nid in own:
        for i in graph.down_adj.get(nid, []):
            if (t := graph.edges[i].to_node) not in own:
                takes_from.add(t)                  # water leaves us and enters them
        for i in graph.up_adj.get(nid, []):
            if (f := graph.edges[i].from_node) not in own:
                gives_to.add(f)                    # water comes back from them
    owners = _owners(registry)
    a = {o for n in takes_from for o in owners.get(n, ()) if o != item_id}
    b = {o for n in gives_to for o in owners.get(n, ()) if o != item_id}
    mine = _length(registry, graph, item_id)
    got = tuple(sorted(i for i in (a & b) if getattr(registry.get(i), "kind", None) == "stream"
                       and _length(registry, graph, i) < mine))
    memo[item_id] = got
    return got


def own_channels(registry, graph, items: Iterable[str], among: Iterable[str]) -> list[str]:
    """The side channels of `items` (and of those channels, repeatedly) that are among the place's own water
    (`among`), in `among`'s order; none of `items` itself."""
    pool = [i for i in dict.fromkeys(among or ()) if i in registry]
    named = list(dict.fromkeys(items or ()))
    have, todo = set(named), list(named)
    while todo:
        x = todo.pop()
        for c in side_channels(registry, graph, x):
            if c in pool and c not in have:
                have.add(c)
                todo.append(c)
    return [i for i in pool if i in have and i not in named]


def with_own_channels(ex: Mapping, registry, graph, among: Iterable[str]) -> dict:
    """One extent with its named water's own channels added to the items it holds on (`item_ids`); an extent naming
    no item (it holds on the whole place's water already) is returned as it is."""
    named = ([ex["item_id"]] if ex.get("item_id") else []) + list(ex.get("item_ids") or [])
    if not named:
        return dict(ex)
    more = own_channels(registry, graph, named, among)
    if not more:
        return dict(ex)
    out = {k: v for k, v in ex.items() if k not in ("item_id", "item_ids")}
    out["item_ids"] = named + more
    return out


def expand_rule(entry: Mapping, rule: Mapping, registry, graph) -> Optional[dict]:
    """The rule with every extent's named water widened to its own channels — only for an entry carrying
    `CHANNELS_KEY` (a DFO place) under the policy; None when nothing changes."""
    if not (DFO_NAMED_WATER_TAKES_ITS_CHANNELS and entry.get(CHANNELS_KEY)):
        return None
    among = list(entry.get("matched") or [])
    if len(among) < 2:
        return None
    exts = [with_own_channels(x, registry, graph, among) if isinstance(x, Mapping) else x
            for x in rule.get("extents") or []]
    if exts == list(rule.get("extents") or []):
        return None
    return {**rule, "extents": exts}
