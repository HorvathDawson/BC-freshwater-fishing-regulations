"""A GAUGE ON A DIVERSION, AND THE RIVER ABOVE ITS INTAKE (T696).

OWNER 2026-11-12 (RULINGS P54, two dialog answers, verbatim): Q103 "Canal gauge also serves river", then, for the
Theodosia, "Show 08GC005 above the canal". 08GC005 THEODOSIA RIVER DIVERSION ABOVE OLSEN LAKE stands on the canal that
takes the Theodosia's water at an intake dam and carries it to Olsen Lake (Powell River's power system). Everything the
river carries above the intake reaches the intake; the canal gauge measures the share the canal takes (HYDAT's drainage
for it, 73.41 km², is the catchment above the intake). The river's own gauges there, 08GC006 "Diversion Bypass" and
08GC007 "below Olsen Lake Diversion", stand BELOW the intake and measure what the dam lets past.

WHAT A ROW SAYS. `data/curated/gauges/diversion_stations.json` (AUTHORED) names the station, the FWA blue line of the
river its canal leaves (`river_blk`, a durable key), the gauges it speaks INSTEAD OF above the intake (`in_place_of`,
the owner's answer), the owner's words and why. The graph supplies the rest, so nothing here goes stale with a cut:

    intake    the section of `river_blk` that flows INTO the station's canal (the canal climbed from the station's own
              section along its own blue line) — exactly one, or the build stops naming what it found
    above     that section and every section whose water reaches it (the upstream closure): "above the canal"

WHAT THE BUNDLE DOES WITH IT (`bundle_tables.write`): the station is PLACED AT THE INTAKE for the donor panels, the
route the app draws and `gauge.sid` (where it reads the river); its walks only CLIMB from there (it never speaks for the
river below the intake, which its water never reaches); `in_place_of` stations are taken off every section `above`;
`gauge.diversion` = 1 so the app labels it ("measures a diversion"). The canal keeps its own match (`section_gauge`).

CHECKED AGAINST THE RECORD: HYDAT's own station name must say DIVERSION, and every `in_place_of` station must be in
HYDAT — a row cannot label a river gauge a diversion.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from pipeline.common.curated import CURATED

#: The authored list, owned here like `tidal.TIDAL_FILE`: the bundle asks `diversion_stations` and never names a
#: curated file itself (`test_one_data_flow`).
DIVERSION_FILE = CURATED.gauges.diversion_stations

_STATION = re.compile(r"^\d{2}[A-Z]{2}\d{3}$")
_BLK = re.compile(r"^\d{9}$")
_KEYS = {"river_blk", "in_place_of", "owner", "why"}


@dataclass(frozen=True)
class Diversion:
    station: str
    river_blk: str
    in_place_of: tuple[str, ...]
    why: str


def load_diversions(path: Path | None = None) -> dict[str, Diversion]:
    """The authored rows, `station -> Diversion`. RAISES on a missing file (AGENTS 37) and on a row that is not exactly
    river_blk / in_place_of / owner / why, with a station id, a 9-digit blue line key, at least one station it speaks
    instead of (never itself), and the owner's words and a reason."""
    path = Path(path or DIVERSION_FILE)
    blob = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, Diversion] = {}
    for st, row in sorted(blob["stations"].items()):
        if not _STATION.match(st):
            raise ValueError(f"diversion: {st!r} is not a station id ({path})")
        if not isinstance(row, dict) or set(row) != _KEYS:
            raise ValueError(f"diversion: {st} must carry exactly {', '.join(sorted(_KEYS))} ({path})")
        if not isinstance(row["river_blk"], str) or not _BLK.match(row["river_blk"]):
            raise ValueError(f"diversion: {st}: river_blk {row['river_blk']!r} is not an FWA blue line key ({path})")
        others = row["in_place_of"]
        if (not isinstance(others, list) or not others or st in others
                or not all(isinstance(o, str) and _STATION.match(o) for o in others)):
            raise ValueError(f"diversion: {st}: in_place_of must list the other stations it speaks instead of ({path})")
        for k in ("owner", "why"):
            if not isinstance(row[k], str) or not row[k].strip():
                raise ValueError(f"diversion: {st} carries no {k} ({path})")
        out[st] = Diversion(st, row["river_blk"], tuple(sorted(others)), row["why"])
    return out


def diversion_stations(hydat: Path, curated: Path | None = None) -> dict[str, Diversion]:
    """The authored rows, EACH CHECKED AGAINST HYDAT: the station's own name says DIVERSION, and every station it
    speaks instead of exists. A missing HYDAT raises (P2)."""
    if not Path(hydat).exists():
        raise FileNotFoundError(f"diversion: no {hydat} (python data/fetch_data.py --layers hydat)")
    rows = load_diversions(curated)
    wanted = sorted({s for d in rows.values() for s in (d.station, *d.in_place_of)})
    names: dict[str, str] = {}
    with sqlite3.connect(f"file:{hydat}?mode=ro", uri=True) as con:
        for st in wanted:
            names.update(con.execute("SELECT STATION_NUMBER, STATION_NAME FROM STATIONS WHERE STATION_NUMBER = ?",
                                     (st,)).fetchall())
    for st, d in rows.items():
        if "DIVERSION" not in str(names.get(st, "")).upper():
            raise ValueError(f"diversion: {st}: HYDAT names it {names.get(st)!r} — the record does not say it "
                             f"measures a diversion")
        missing = [o for o in d.in_place_of if o not in names]
        if missing:
            raise ValueError(f"diversion: {st}: in_place_of {missing} not in HYDAT")
    return rows


def intake(graph, station_node: str, river_blk: str) -> tuple[str, float]:
    """`(the river section that flows into the station's canal, metres from the intake down the canal to the
    station)`. The canal is climbed from the station's own section along its own blue line; the intake is the one
    section of `river_blk` with an edge into it. None found, or more than one, RAISES naming them — a diversion the
    graph cannot place speaks for nothing rather than for a guess."""
    node = graph.nodes.get(station_node)
    if node is None:
        raise ValueError(f"diversion: station section {station_node} not in this graph")
    canal_blk = str(node.blk)
    if canal_blk == str(river_blk):
        raise ValueError(f"diversion: {station_node} is on the river's own line {river_blk}, not on a canal off it")
    chain, metres, seen, cur = [], 0.0, {station_node}, station_node
    while True:
        chain.append(cur)
        metres += float(getattr(graph.nodes[cur], "length_m", 0.0) or 0.0)
        nxt = sorted({graph.edges[ix].from_node for ix in graph.up_adj.get(cur, ())
                      if str(getattr(graph.nodes.get(graph.edges[ix].from_node), "blk", "")) == canal_blk} - seen)
        if not nxt:
            break
        if len(nxt) > 1:
            raise ValueError(f"diversion: the canal above {station_node} forks ({nxt})")
        cur = nxt[0]
        seen.add(cur)
    found = sorted({graph.edges[ix].from_node for c in chain for ix in graph.up_adj.get(c, ())
                    if str(getattr(graph.nodes.get(graph.edges[ix].from_node), "blk", "")) == str(river_blk)})
    if len(found) != 1:
        raise ValueError(f"diversion: the canal of {station_node} ({canal_blk}) is fed by {found or 'no section'} "
                         f"of {river_blk} — expected exactly one intake")
    return found[0], metres


def above(graph, intake_node: str) -> frozenset[str]:
    """The intake's section and every section whose water reaches it — "above the canal" (the upstream closure)."""
    out, todo = {intake_node}, [intake_node]
    while todo:
        cur = todo.pop()
        for ix in graph.up_adj.get(cur, ()):
            nxt = graph.edges[ix].from_node
            if nxt not in out and nxt in graph.nodes:
                out.add(nxt)
                todo.append(nxt)
    return frozenset(out)


@dataclass(frozen=True)
class Intake:
    """A diversion placed on this graph: the river section at its `node`, the canal's metres from the intake down to
    the station, every section `above` the intake, and the stations it speaks `in_place_of` there."""
    node: str
    canal_m: float
    above: frozenset[str]
    in_place_of: tuple[str, ...]


def intakes(graph, rows: dict[str, Diversion], placed: dict[str, str]) -> dict[str, Intake]:
    """`station -> Intake` for every authored row whose station is placed on this graph (`placed`: station -> its own
    section). A row whose station is not placed RAISES: the owner named it, so it must land."""
    out: dict[str, Intake] = {}
    for st, d in sorted(rows.items()):
        if st not in placed:
            raise ValueError(f"diversion: {st} is not placed on this graph — its row cannot be honoured")
        node, canal_m = intake(graph, placed[st], d.river_blk)
        out[st] = Intake(node, canal_m, above(graph, node), d.in_place_of)
    return out
