"""(T369) `section_measure`: where every STREAM section lies on its own blue line — FWA stream key, start, end (metres).

A link carries a PLACE (the stream key + a distance along it, AGENTS rule 5), never a section id; the app resolves the
place to the section of the release it holds (`@app/data` regs/line.ts, the `lines` file). The span is read off
the graph, never guessed: the start is the section id's own measure (`{blk}:{int(down_m)}`), the end the node's
`down_m + length_m` (its `up_m`) cut to a whole metre the same way — so a piece ends exactly where the next piece on the
line starts (`int(up_m)` = the next id's `int(down_m)`), and only a real gap (a lake between two pieces) leaves one.

Every stream handle has a row; a lake / wetland handle (`lake:<wbk>`, after every stream handle in the handle order)
has none. A stream handle the graph lacks, or one that is not a stream node, stops the build: a missing row would be a
place that silently resolves nowhere.
"""
from __future__ import annotations

import math
from typing import Iterable, List, Sequence, Tuple

from pipeline.common.section_handles import parse_section_id

Row = Tuple[int, int, int, int]     # (sid, blk, from_m, to_m)


def stream_head(node_id: str):
    """`(blk, measure)` of a stream section id `{blk}:{measure}`; None for a namespaced id (`lake:<wbk>`)."""
    return parse_section_id(node_id)


def rows(handle_ids: Sequence[str], nodes) -> List[Row]:
    """One row per stream handle (`handle_ids[h - 1]` is handle h), from the graph's `nodes` {node_id: StreamNode}."""
    out: List[Row] = []
    bad: List[str] = []
    for h, nid in enumerate(handle_ids, start=1):
        at = stream_head(nid)
        if at is None:
            continue
        node = nodes.get(nid)
        if node is None or node.kind != "stream":         # NodeKind is a str enum
            bad.append(nid)
            continue
        blk, start = at
        length = float(node.length_m or 0.0)
        if not math.isfinite(length) or length < 0:
            bad.append(nid)
            continue
        # 1e-6: `down_m + (up_m - down_m)` can come back a hair under a whole `up_m`
        out.append((h, blk, start, max(start, math.floor(float(node.down_m) + length + 1e-6))))
    if bad:
        raise SystemExit(f"section_measure: {len(bad)} stream handle(s) with no stream node of a usable length in the "
                         f"graph (e.g. {bad[:3]}) — the handle table and graph.pkl are from different atlas runs")
    return out


def write(db, handle_ids: Sequence[str], graph, cov) -> None:
    rs = rows(handle_ids, graph.nodes)
    db.executemany("INSERT INTO section_measure (sid, blk, from_m, to_m) VALUES (?, ?, ?, ?)", rs)
    cov.filled("section_measure", len(rs))


def read(db) -> Iterable[Row]:
    return db.execute("SELECT sid, blk, from_m, to_m FROM section_measure ORDER BY sid")
