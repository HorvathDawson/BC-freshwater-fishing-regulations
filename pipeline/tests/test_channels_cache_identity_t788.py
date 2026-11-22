"""(T788 item 6) `atlas.reach.channels` caches by the registry and graph THEMSELVES, never by `id(...)`.

The caches were keyed by `id(registry)` without holding the object: after a promote the old registry could be freed and
the new one land at the same address, and the review app served the old build's side-channel table with no restart.
The test makes every id collide (`channels.id` -> 1): the old cache answered the second build from the first one's
table (mutation: key by `id(...)` again -> fails); keyed by identity, the second build is read afresh.
"""
from __future__ import annotations

from types import SimpleNamespace


def _world(channel_kind: str):
    """a river (f1 -> f2 -> f3) and a channel s1 that leaves at f1 and rejoins at f3; `channel_kind` stream or lake"""
    edges, down, up = [], {}, {}
    for a, b in (("f1", "f2"), ("f2", "f3"), ("f1", "s1"), ("s1", "f3")):
        edges.append(SimpleNamespace(from_node=a, to_node=b))
        down.setdefault(a, []).append(len(edges) - 1)
        up.setdefault(b, []).append(len(edges) - 1)
    g = SimpleNamespace(nodes={n: SimpleNamespace(length_m=1000.0) for n in ("f1", "f2", "f3", "s1")},
                        edges=edges, down_adj=down, up_adj=up)
    reg = {"river": SimpleNamespace(section_ids=("f1", "f2", "f3"), kind="stream"),
           "chan": SimpleNamespace(section_ids=("s1",), kind=channel_kind)}
    return reg, g


def test_a_new_build_at_a_reused_address_is_read_afresh(monkeypatch):
    from pipeline.atlas.reach import channels as CH
    monkeypatch.setattr(CH, "id", lambda _o: 1, raising=False)     # every object at "the same address"
    old_reg, old_g = _world("stream")
    assert CH.side_channels(old_reg, old_g, "river") == ("chan",)
    new_reg, new_g = _world("lake")                                 # the new build: s1 is a lake now, no channel
    assert CH.side_channels(new_reg, new_g, "river") == ()
    assert CH.own_channels(new_reg, new_g, ["river"], ["river", "chan"]) == []


def test_the_cache_holds_what_it_was_built_from():
    from pipeline.atlas.reach import channels as CH
    reg, g = _world("stream")
    CH.side_channels(reg, g, "river")
    assert CH._MEMO[0] is reg and CH._MEMO[1] is g and CH._OWNERS[0] is reg
