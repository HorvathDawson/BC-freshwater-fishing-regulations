"""WHAT THE MAP DOES NOT DRAW MUST NOT NEED DRAWING (T64).

The tile export leaves some sections out on purpose and says which (`layers/undrawn.json`,
`tiles.export.UNDRAWN_FILE`): unnamed wetlands in no item, which only zone and area-only rules bind
(owner 2026-10-23: "removing all unnamed wetlands without direct regulation ... including dropping
ones only regulated by admin area regs like liard"). A section nobody can see is a section nobody
can tap, so the cut is safe only while each of them is answered by its land or area alone.

`check` proves it on the bundle being built, from the rows just written: no undrawn section is in a
registry item, carries a gauge, or is bound by a rule of a WATER entry (a row whose water is an item
other than an area: by its own extents or by the tributary walk). Zone tables (`z…`) and area-only rows
(an entry whose water is an `area:` item or none — "Liard River watershed") are what the cut is
allowed to leave unseen. A failure names the sections; the fix is a tile rebuild (the export keeps
what an item owns) or a reading of why a water rule reached an unnamed polygon.

TILES THAT LEFT NOTHING OUT state no `undrawn` in `atlas.meta.json` (they predate the cut and drew
every wetland); a tile set whose sidecar names undrawn sections must carry the file.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from pipeline.common import entry_id as EID


def load(tiles: Path) -> dict[str, list[int]]:
    """{layer: [section handle]} the tiles left out on purpose; {} for tiles that left nothing out."""
    from pipeline.deliver.tiles.export import UNDRAWN_FILE

    meta = json.loads((tiles / "atlas.meta.json").read_text(encoding="utf-8"))
    p = tiles / "layers" / UNDRAWN_FILE
    if "undrawn" not in meta:
        return {}
    if not p.exists():
        raise SystemExit(f"undrawn: {tiles}/atlas.meta.json says {meta['undrawn']} sections were left "
                         f"out, and {p} is missing — copy the tile set's layers/{UNDRAWN_FILE} with it")
    got = json.loads(p.read_text(encoding="utf-8"))
    out = {k: sorted(int(x) for x in v) for k, v in got.items() if not k.startswith("$")}
    if {k: len(v) for k, v in out.items()} != meta["undrawn"]:
        raise SystemExit(f"undrawn: {p} does not match atlas.meta.json {meta['undrawn']} — "
                         "the two come from different tile builds")
    return out


def tidal_problems(db: sqlite3.Connection, undrawn: dict[str, list[int]]) -> list[str]:
    """T166: THE TIDAL PIECES THE TILES LEFT OUT are exactly the bundle's tidal water below a tidal
    boundary (`tidal` rows owned `split:<id>` — the reach run's set; both come from
    `reach.outside.tidal_cut_owner` on the same atlas). They are in items (the river's lower end) and
    carry no rule by construction (`rules.tables` proves it), so the T64 water-row test does not apply."""
    drawn_off = set(undrawn.get("tidal") or ())
    cut = {s for s, e in db.execute("SELECT sid, entry_id FROM tidal") if str(e).startswith("split:")}
    out = []
    if drawn_off - cut:
        out.append(f"{len(drawn_off - cut)} section(s) the tiles left off as tidal are no tidal piece in "
                   f"the bundle, e.g. {sorted(drawn_off - cut)[:5]} — tiles and reach run from different atlases")
    if cut - drawn_off:
        out.append(f"{len(cut - drawn_off)} tidal piece(s) in the bundle are still drawn by the tiles, "
                   f"e.g. {sorted(cut - drawn_off)[:5]} — rebuild the tiles on this atlas")
    return out


def tidal_gauges(db: sqlite3.Connection, undrawn: dict[str, list[int]]) -> list[tuple[int, str]]:
    """T166: the gauges on a tidal piece the map no longer draws — (section, station). Reported: the
    station is not reachable by tapping its water any more (the tidal area's tap says it is tidal)."""
    off = set(undrawn.get("tidal") or ())
    if not off:
        return []
    cols = [r[1] for r in db.execute("PRAGMA table_info(section_gauge)")]
    st = "station_id" if "station_id" in cols else (cols[1] if len(cols) > 1 else cols[0])
    return sorted((s, str(g)) for s, g in db.execute(f"SELECT sid, {st} FROM section_gauge") if s in off)


def problems(db: sqlite3.Connection, undrawn: dict[str, list[int]]) -> list[str]:
    """Why an undrawn section would need the map, one line each (empty: the cut is safe)."""
    out_tidal = tidal_problems(db, undrawn)
    undrawn = {k: v for k, v in undrawn.items() if k != "tidal"}
    sids = {s for v in undrawn.values() for s in v}
    if not sids:
        return out_tidal
    kind_of = dict(db.execute("SELECT item_id, kind FROM item"))
    water_entry = {e for e, iid in db.execute("SELECT entry_id, item_id FROM entry")
                   if not EID.is_table(e) and iid and kind_of.get(iid, "area") != "area"}
    water_sets = {s for s, e in db.execute("SELECT set_id, entry_id FROM ruleset") if e in water_entry}
    out: list[str] = []
    owned = {s for (s,) in db.execute("SELECT sid FROM item_section")} & sids
    out += [f"section {s} is in a registry item" for s in sorted(owned)]
    gauged = {s for (s,) in db.execute("SELECT sid FROM section_gauge")} & sids
    out += [f"section {s} carries a gauge" for s in sorted(gauged)]
    for s, set_id in db.execute("SELECT sid, set_id FROM section_ruleset"):
        if s in sids and set_id in water_sets:
            names = sorted(e for (e,) in db.execute(
                "SELECT DISTINCT entry_id FROM ruleset WHERE set_id = ?", (set_id,)) if e in water_entry)
            out.append(f"section {s} is bound by a water row: {', '.join(names[:3])}")
    return out_tidal + out


def check(db: sqlite3.Connection, tiles: Path, cov=None) -> int:
    """Refuse a bundle that needs a section the tiles did not draw; returns how many were undrawn."""
    undrawn = load(Path(tiles))
    bad = problems(db, undrawn)
    if bad:
        raise SystemExit(f"undrawn: {len(bad)} section(s) the tiles left out are needed on the map "
                         f"(T64):\n  " + "\n  ".join(bad[:20]))
    n = sum(len(v) for v in undrawn.values())
    if cov is not None and n:
        print(f"     undrawn: {n:,} section(s) left out of the tiles, none needed on the map")
    lost = tidal_gauges(db, undrawn)
    if lost:
        print(f"     undrawn: {len(lost)} gauge(s) on a tidal piece, not tappable on its water now: "
              + ", ".join(f"{g}@{s}" for s, g in lost[:12]))
    return n
