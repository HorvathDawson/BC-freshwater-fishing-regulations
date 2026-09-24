"""Build `data/generated/regs/sections.json` — which rules fall on which stretch of which water.

THIS IS AN INPUT TO THE TABLE LAYER, not a design document. It used to be written into a
`<script id="d">` block inside a 7.7 MB prototype web page, and `corpus.py` and `build.py` each
opened that page and pulled the block back out with a regular expression, at import. The
pipeline's input was therefore a design file that could not be deleted, regenerated or reviewed
without risking the pipeline. The page is gone; the data is a file.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.build_section_data          # in place
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.build_section_data --print  # to stdout
    ... --bundle <side>.sqlite                        # from a side bundle, before it is promoted

The waters are named below rather than discovered — chosen to exercise the cases the table
layer has to get right. The bundle holds every water; `pipeline/tools/export_ui_rules.py` exports
all of them, and this file stays a worked sample with geometry.

WHAT IT READS, and why each one:

    bundle.sqlite      EVERY regulation fact: the rules, the interned rulesets, the entries and
                       the waters each entry matched (`entry.matched`), the places. The shipped
                       artifact, so the page cannot show a rule the app would not. A bundle
                       without `entry.matched`, or one shipping a retired rule field, is refused.
    section_handles    the integer the bundle calls a section -> the node id everything else
                       calls it. Never derived; the file is the owner (see that module).
    graph.pkl          chainage. `down_m`/`up_m` are metres along the blue line, which is what
                       turns a set of sections into a STRETCH with a start and an end.
    geometries.pkl     the line to draw, in BC Albers, reprojected here and nowhere else.
    lake geometry      a lake's shoreline from the FWA geopackage; a lake PART's polygon from the
                       build's `waterbody_polys.pkl` (the polygon the atlas cut with). Which
                       items are parts of which lake is `item.part_of`, in the bundle. Nothing
                       under data/curated is read.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

from pipeline.common.curated import GENERATED, REPO_ROOT

#: (display name, why it is here). The `why` is not decoration — it is the test for whether a
#: water still belongs, and the reason the list is not just "big rivers".
WATERS: list[tuple[str, str, str]] = [
    ("Fraser River",     "the long one: many stretches, several entries, and the reach that "
                         "no cut-point can express because Region 6 holds no Fraser mainstem"),
    ("Chilliwack River", "one row covering several registry items, and a name change mid-river"),
    ("Harrison River",   "short, heavily regulated, and a lake at one end"),
    ("Fording River",    "two entries split at a falls — the two-stage question in miniature"),
    ("Atnarko River",    "tributary binding: most of its water is reached by the walk, not named"),
    ("Bella Coola River", "the Atnarko's receiving water; the pair shows a reach crossing items"),
    ("Skeena River",     "the watershed case — a rule bound by tributary walk over 84,000 sections"),
    ("Elk River",        "two waters share the name in different regions; the id must disambiguate"),
    ("Coquihalla River", "a short river with a dense stack of gear and vessel rules"),
    ("Kootenay River",   "an alias-bound cut-point: the split the page names lost to a gauge"),
    ("Okanagan River",   "McIntyre Dam — the alias case again, and a chain of dams and lakes"),
    ("Babine River",     "counting-fence boundaries, and a lake run in the middle of the river"),
    ("Yakoun River",     "Haida Gwaii: MUs 6-12/6-13 but administratively Region 1, so it "
                         "takes the Region 1 stream rules AND its own quotas on top"),

    # A PAIR, AND THE PAIR IS THE POINT. Region 1's summer closure is written for a GROUP of
    # management units — "No Fishing in any stream in Management Units 1-1 to 1-6, July 15-Aug
    # 31" — not for the region. Nothing else separates these two rivers: both are Region 1,
    # both on Vancouver Island, both take the region's bait ban, its barbless-hook rule and its
    # quotas. The Cowichan is in MU 1-4 and carries the closure; the Stamp is in MU 1-7 and does
    # not. Side by side they are the test for whether a reader can see WHICH geography a rule
    # came from, when the two waters are otherwise alike.
    ("Cowichan River",   "MU 1-4, inside the 1-1 to 1-6 summer closure group; read beside the "
                         "Stamp, which is Region 1 but outside it"),
    ("Stamp River",      "MU 1-7, Region 1 but OUTSIDE the 1-1 to 1-6 group — everything the "
                         "Cowichan has except the summer closure"),
    # AND THE BOUNDARY RUNS THROUGH ONE RIVER. The Campbell's synopsis row is filed under MU
    # 1-10, but 15 of its 20 sections lie inside the 1-1 to 1-6 polygon and 5 do not, so the
    # summer closure covers part of it and stops. Filed by row it would be in or out; bound by
    # geography it is both, which is what the book means and what the ladder can show.
    ("Campbell River",   "the MU 1-1 to 1-6 boundary crosses the river itself: 15 of its 20 "
                         "sections are inside the closure group and 5 are not"),

    # ---- lakes -------------------------------------------------------------------
    # Every screen above is a river, and a lake exercises branches none of them reach: set
    # lining, which is permitted ONLY on lakes and so appears on no river in the document;
    # barbed hooks, which lakes allow and streams do not; and the whole stream-only half of
    # the regional quota tables, which has to be filtered OUT rather than shown.
    ("Shuswap Lake",     "two stamps that exist for this lake alone, plus closures cut by "
                         "boundary signs rather than by chainage", "lake"),
    ("Atlin Lake",       "a pure quota lake — four northern species, each with its own size "
                         "sub-limit, and an aggregation note across the whole lake", "lake"),
    ("Okanagan Lake",    "the river's own lake: bass and perch quotas, and vessel rules given "
                         "as buoyed and signed rather than as a reach", "lake"),
    # THE FIRST LAKE THE BOOK SPLITS THAT THE ATLAS CAN NOW DRAW. Shannon Lake has one row for
    # the lake and another for "the netted off portion on the south end"; the corner was cut in
    # the lake splitter and is `wbk:-15`, 0.19 ha, leaving 14.56 ha as Shannon Lake proper. It is
    # here to exercise the case a lake with parts renders as a LADDER, the way a river does —
    # one screen, a rung per water — rather than as two unrelated screens.
    # No item id here even though BC has three Shannon Lakes: a lake that has been CUT is the
    # one the bundle's `item.part_of` names, and `_parent_of` finds it. Naming it twice is how
    # the page and the curation come to disagree about which lake this is.
    ("Shannon Lake",     "a lake with curated parts: one row for the lake, one for the netted-"
                         "off corner, drawn as two rungs of one ladder", "lake"),
    # THE HARD CASE, included BECAUSE it does not work yet. The book divides Kootenay Lake into
    # Main Body, Upper West Arm and Lower West Arm — in a zone entry that belongs to no registry
    # item, because a definition is not a rule — and two real rules then name one of those areas.
    # Until the three polygons exist, both are carried on the whole lake, unplaced and marked.
    # `data/curated/waters/sub_lake_areas.json` is the worklist.
    ("Kootenay Lake",    "a lake the book cuts into three named arms that the atlas cannot draw "
                         "— the rules that need them are shown unplaced, not widened",
                         "lake", "wbk:328974235"),
    # Named by item id: three lakes are called Elk Lake and two of them have their own row.
    ("Elk Lake",         "a small Region 1 lake whose rules are almost all about boats, and a "
                         "`facility` — the one rule type no river in this document carries",
                         "lake", "wbk:329676313"),
]


def log(*a):
    """Progress goes to stderr; --print puts JSON on stdout and nothing else."""
    print(*a, file=sys.stderr)


def _albers_to_lonlat():
    from pyproj import Transformer
    return Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True).transform


def _pts(geom, to_lonlat, ndigits: int = 4) -> list[list[float]]:
    """One section's line as [[lon, lat], ...], rounded.

    Four decimals is ~11 m, which is finer than the line is drawn at any zoom this document
    uses, and it is the difference between a 368 KB file and a 3 MB one.
    """
    # A LAKE'S GEOMETRY IS MULTI-PART. A river section is one LineString; a lake is a
    # MultiLineString — its shoreline, or several through-lines — and `coords` raises on those
    # rather than flattening them. The longest part is the one worth drawing: a lake's outline,
    # not the stub where a creek meets it.
    if geom.geom_type.startswith("Multi") or geom.geom_type == "GeometryCollection":
        parts = [g for g in geom.geoms if not g.is_empty and hasattr(g, "coords")]
        if not parts:
            return []
        geom = max(parts, key=lambda g: g.length)
    xs, ys = zip(*list(geom.coords))
    lon, lat = to_lonlat(list(xs), list(ys))
    return [[round(a, ndigits), round(b, ndigits)] for a, b in zip(lon, lat)]



def _lake_outline(item_id: str, to_lonlat, ndigits: int = 4):
    """A lake's own shoreline, from FWA, as [[lon, lat], ...] — or None.

    THE LINE UNDER A LAKE IS NOT THE LAKE. The stream graph carries a lake as a node on the
    blue line that runs THROUGH it, so its geometry is that through-line: a stroke across the
    middle of the water, which drawn on a map looks like a river with a name a reader does not
    recognise. Shuswap Lake is not a 60 km line.

    The polygons are in the fisheries geopackage, one row per waterbody, keyed on the same
    WATERBODY_KEY the registry uses for `wbk:` ids — so this is a lookup, not a guess. Returns
    None when the lake has no polygon, and the caller falls back to the through-line rather than
    drawing nothing.
    """
    import sqlite3
    if not str(item_id).startswith("wbk:"):
        return None
    src = REPO_ROOT / "data" / "source" / "bc_fisheries_data.gpkg"
    if not src.exists():
        return None
    try:
        from shapely import wkb as _wkb
        db = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
        row = db.execute("SELECT geom FROM lakes WHERE WATERBODY_KEY = ?"
                         " ORDER BY AREA_HA DESC LIMIT 1", (int(item_id[4:]),)).fetchone()
        db.close()
        if not row or not row[0]:
            return None
        blob = row[0]
        # GeoPackage binary: "GP", version, flags, then srs_id and an envelope whose size the
        # flags encode, then the WKB proper.
        flags = blob[3]
        env = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get((flags >> 1) & 0x07, 0)
        geom = _wkb.loads(bytes(blob[8 + env:]))
        if geom.is_empty:
            return None
        if geom.geom_type.startswith("Multi"):
            geom = max(geom.geoms, key=lambda g: g.area)
        ring = geom.exterior
        # A shoreline can carry tens of thousands of vertices; the ladder draws it a few hundred
        # pixels wide. Simplifying to ~50 m keeps every bay a reader could name.
        ring = ring.simplify(50.0, preserve_topology=False)
        xs, ys = zip(*list(ring.coords))
        lon, lat = to_lonlat(list(xs), list(ys))
        pts = [[round(a, ndigits), round(b, ndigits)] for a, b in zip(lon, lat)]
        # km², from the projected polygon. SIX DECIMALS, not one: a tenth of a km² is 10 ha,
        # and rounding there reported Shannon Lake — 14.7 ha — as 0.1, which the page then
        # printed as "0 KM²". The page decides whether to say km² or hectares; it can only do
        # that if the number reaching it still knows the difference. Same precision the lake
        # PARTS are already stored at, so a parent and its parts cannot disagree.
        return pts, round(geom.area / 1e6, 6)
    except Exception:
        return None



def _widened(db) -> set:
    """Rules the book restricts to PART of a water and the atlas could only give the whole of.

    The signal is structural and in the bundle: the rule is BOUND (`uncertain` = 0), its extents
    are only `op: "whole"` — the entire water — and it still carries an `extent_text`, the
    parser's record of a place it was told about and could not express. "Rainbow trout — 20 per
    licence year over 50 cm" is written for the MAIN BODY of Kootenay Lake, and binds to all
    423 km² of it including both West Arms.

    It is the over-application direction — the app claims a rule covers more water than it does
    — and unlike the unplaced rules, which bind nothing and are obvious, these bind everything
    and look completely ordinary. So they are marked, and the row says the book names a part.
    """
    out = set()
    for eid, rid, text, cond, uncertain in db.execute(
            "SELECT entry_id, rule_id, extent_text, conditions, uncertain FROM rule"):
        if uncertain or not (text or "").strip():
            continue
        ops = {x.get("op") for x in json.loads(cond or "{}").get("extents") or []}
        if ops and ops <= {"whole"}:
            out.add((eid, rid))
    return out


def name_of_item(db, item_id: str):
    row = db.execute("SELECT name FROM item WHERE item_id = ?", (item_id,)).fetchone()
    return row[0] if row else None


def _parent_of(db, name: str):
    """The item id of the lake of this name that has PARTS, or None.

    A CUT LAKE IS NAMED BY THE LAKE ITS PARTS ARE PART OF. Three lakes in BC are called Shannon
    Lake, and resolving the page's name against `item` alone picked wbk:329244252 — which has no
    parts, so the page drew one rung and looked exactly as though the split had failed. The
    bundle's `item.part_of` says which one the book's two rows are about, because it is copied
    from the polygons that were cut; asking it is how the page and the curation stay the same
    answer instead of two.
    """
    row = db.execute(
        "SELECT p.item_id FROM item p WHERE p.name = ? COLLATE NOCASE AND p.kind = 'lake'"
        "   AND EXISTS (SELECT 1 FROM item c WHERE c.part_of = p.item_id)"
        " ORDER BY p.ord LIMIT 1", (name or "",)).fetchone()
    return row[0] if row else None


def _lake_parts(db, item_id: str):
    """`[(child_item_id, name)]` — the pieces curated OUT of this lake, from `item.part_of`.

    A LAKE THE BOOK WRITES AS SEVERAL WATERS IS STILL ONE LAKE ON THE MAP. Shannon Lake has a
    netted-off corner with its own row; Kootenay Lake is a Main Body and two West Arms. Each
    piece becomes its own registry item — `added_lakes` ingest re-stamps the fids inside the
    polygon, so the corner is `wbk:-15` — and as separate items they would open as separate,
    unrelated screens, with no way to see that one is part of the other or to compare their rules.

    A river with different rules along it is not shown that way: it is one screen with a ladder
    of stretches. A lake with different rules across it is the same question, and gets the same
    answer. The bundle carries the relation (`item.part_of`, written at bundle build from the
    polygons' own `part_of`), so this reads the shipped artifact and nothing curated.
    """
    return [(c, n or "Part") for c, n in db.execute(
        "SELECT item_id, name FROM item WHERE part_of = ? ORDER BY ord", (item_id,))]


_POLYS = None


def _part_ring(build: Path, child_item_id: str, to_lonlat, ndigits: int = 5):
    """`(ring [[lon, lat], ...], km²)` for a lake part, from the atlas's own polygon — or None.

    FWA has no polygon for a part — that is the whole reason it was drawn — so the shoreline
    lookup used for a real lake finds nothing and the piece would draw as a blank stretch. The
    atlas keeps the polygon it cut with (`waterbody_polys.pkl`, keyed `lake:{wbk}`, in BC
    Albers), which is the same build as the graph and geometries this page already reads.

    The area is measured on that projected polygon. km², kept to 6 dp: a netted-off corner is
    0.0019 km² and rounding it to 2 gives 0.0, which the page then cannot tell from 'no area at
    all'.
    """
    global _POLYS
    if not str(child_item_id).startswith("wbk:-"):
        return None
    if _POLYS is None:
        from pipeline.common.io.serialize import read_artifact
        _POLYS = read_artifact(str(build / "waterbody_polys.pkl"))
    geom = _POLYS.get(f"lake:{child_item_id[4:]}")
    if geom is None or geom.is_empty:
        return None
    area = round(geom.area / 1e6, 6)
    if geom.geom_type.startswith("Multi"):
        geom = max(geom.geoms, key=lambda g: g.area)
    xs, ys = zip(*list(geom.exterior.coords))
    lon, lat = to_lonlat(list(xs), list(ys))
    return [[round(a, ndigits), round(b, ndigits)] for a, b in zip(lon, lat)] or None, area


def _co_items(db, item_id: str) -> set[str]:
    """The OTHER registry items a synopsis row covers alongside this one — `entry.matched`, every
    water the row matched (`entry.item_id` is only the first)."""
    out: set[str] = set()
    for (raw,) in db.execute("SELECT matched FROM entry WHERE matched LIKE ?",
                             (f'%"{item_id}"%',)):
        m = [x for x in json.loads(raw or "[]") if x]
        if item_id in m:
            out.update(x for x in m if x != item_id)
    return out


def _bound_label(end) -> str:
    """What a cut-point is CALLED. A stretch named "km 704" is a stretch nobody can find."""
    if end is None:
        return ""
    lab = (getattr(end, "label", "") or "").strip()
    return lab


def _km_of(runs, lon, lat):
    """The km along the drawn river nearest this point, or None if it is nowhere near it.

    Coarse on purpose — the vertices are already rounded to ~11 m and the ladder places a tick,
    not a survey mark. Anything more than ~0.25 deg away is not on this river and is dropped
    rather than pinned to whichever end happened to be closest.
    """
    best = None
    for r in runs:
        span = (r["to"] - r["from"]) or 0.0
        pts = [p for seg in r["pts"] for p in seg]
        for i, p in enumerate(pts):
            d = (p[0] - lon) ** 2 + (p[1] - lat) ** 2
            if best is None or d < best[0]:
                frac = i / max(len(pts) - 1, 1)
                best = (d, round(r["from"] + span * frac, 1))
    # ~0.05 deg is about 5 km. Anything further is not on this river; pinning it to whichever
    # end happened to be closest is how a town two valleys over became a landmark at km 0.
    if best is None or best[0] > 0.0025:
        return None
    return best[1]


def _at_km(runs, km):
    """The [lon, lat] at this chainage along the drawn river, or None if it is off the ends.

    Walks to the run containing the km and takes the vertex at the matching fraction of it —
    the same approximation `_km_of` uses in reverse, and on the same vertices the map draws, so
    a tick can never land off the line."""
    for r in runs:
        if not (r["from"] - 0.2 <= km <= r["to"] + 0.2):
            continue
        pts = [p for seg in r["pts"] for p in seg]
        if not pts:
            continue
        span = (r["to"] - r["from"]) or 1.0
        frac = min(max((km - r["from"]) / span, 0.0), 1.0)
        return pts[min(int(round(frac * (len(pts) - 1))), len(pts) - 1)]
    return None


def _species_names() -> dict[str, str]:
    """Code -> the words a reader sees.

    THE CATALOGUE'S OWN WORDS WIN. The official table has no row for TROUT_CHAR, WHITEFISH or
    ALL_GAME_FISH — they are the synopsis's groups, not taxa — so a map built from the table
    alone printed the raw code in the quota table: "TROUT_CHAR | 4 | all species combined".
    """
    from pipeline.regs.parsing.species import SPECIES
    from pipeline.regs.parsing.catalogue import _SPECIES_WORDS
    out = {c: r.common_name for c, r in SPECIES.items()}
    out.update(_SPECIES_WORDS)
    return out


def _species_groups() -> list[dict]:
    """The groups the SYNOPSIS prints, from the catalogue — not a hand list.

    The old file carried `TRT` and a hand-made trout set. The catalogue's groups are the
    words the page actually uses, and `ALL_GAME_FISH` is the closed list from definitions.md.
    """
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS, _SPECIES_WORDS

    # A GROUP MUST CLAIM ITS SUB-GROUPS, not only its members. `TROUT` and `CHAR` are groups in
    # their own right, and ten rules name one of them — "No fishing for trout, Sept 1-Nov 15" on
    # the Fraser. Listing only TROUT_CHAR and the individual codes left those ten matching no
    # chip on the page, so they rendered under nothing.
    # `SA` is the CSV's generic "Salmon" row — an alias for the group, not a member of it. The
    # spear rule names it on all twelve waters, so leaving it out left that rule ungrouped.
    kin = {"TROUT_CHAR": ("TROUT", "CHAR"),
           "SALMON": ("SA",),
           "ALL_GAME_FISH": ("TROUT", "CHAR", "TROUT_CHAR", "WHITEFISH", "BASS")}
    # `ALL_FIN_FISH` IS EVERY GROUP'S BUSINESS, NOT A GROUP OF ITS OWN. Three rules name it —
    # "any fish snagged must be released", "release all fin fish caught in your trap", the Pine
    # River's catch-and-release — and each is true of trout, of salmon, of bass alike. As its own
    # chip it would sit beside "All game fish" saying almost the same words; listed in every
    # group's codes it does what it says, and appears wherever the reader is looking.
    return [{"id": code.lower(), "name": _SPECIES_WORDS[code],
             "codes": [code, *kin.get(code, ()), *SPECIES_GROUPS[code], "ALL_FIN_FISH"]}
            for code in ("TROUT_CHAR", "SALMON", "WHITEFISH", "BASS", "ALL_GAME_FISH")]


def _group_members() -> dict[str, list[str]]:
    """The members of the groups a rule may name but the page cannot expand on its own.

    `_species_groups` builds the five the reader FILTERS by. `PROTECTED_SPECIES` is not one of
    those — nobody filters for "the fish it is illegal to fish for" — but a row that says
    "No fishing for protected species" and names none of them tells a reader to look up twelve
    taxa somewhere else. The closed list is in the catalogue; this carries it across so the page
    can print the names instead of asserting them.
    """
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS
    return {"PROTECTED_SPECIES": list(SPECIES_GROUPS["PROTECTED_SPECIES"])}


def _rules_by_id(db: sqlite3.Connection) -> dict[tuple[str, str], dict]:
    cols = [r[1] for r in db.execute("PRAGMA table_info(rule)")]
    out = {}
    for row in db.execute(f"SELECT {', '.join(cols)} FROM rule"):
        r = dict(zip(cols, row))
        out[(r["entry_id"], r["rule_id"])] = r
    return out



def _entry_areas(db) -> dict[str, list[str]]:
    """entry_id -> the AREAS its extents declare, from the bundle's `entry.extents`.

    A zone entry says where it applies in its own extents — `area:region:1`,
    `area:mu_group:management_units_6_12_and_6_13` — and the page needs that because the id
    prefix does not carry it. Both of those entries are filed under `z1:`, and one of them is
    the Region 1 quota table that the book prints "(excluding Haida Gwaii)" while the other is
    Haida Gwaii's own. Told apart by prefix they are indistinguishable, and the Yakoun River
    showed Trout 4 and Trout/char 5 one above the other, both labelled "Region 1".

    The ENTRY's own extents are a column of their own, because they are not recoverable from its
    rules: a rule with narrower extents does not say what the entry's were, and taking the union
    across rules widened `zp:bait` from the region to three named regions and lost
    `z5:spring_stream_closure` entirely.
    """
    out: dict[str, list[str]] = {}
    for eid, raw in db.execute("SELECT entry_id, extents FROM entry"):
        got: list[str] = []
        for ex in json.loads(raw or "[]"):
            a = ex.get("area_id") or (("kind:" + ex["area_kind"]) if ex.get("area_kind") else "")
            if a and a not in got:
                got.append(a)
        if got:
            out[eid] = got
    return out



#: How fine a zone entry's geography is. The synopsis prints one quota table for Region 1
#: "(excluding Haida Gwaii)" and a second for Haida Gwaii itself, rather than writing an
#: exception list — so the narrower statement is the one that governs where it reaches, and
#: the data needs to say which is narrower. There is no `excludes` operator and none is needed.
_TIER = (("area:mu_group:", 2), ("area:region:", 1))


def _area_tier(areas: list[str]) -> int:
    for prefix, tier in _TIER:
        if any(str(a).startswith(prefix) for a in areas or ()):
            return tier
    return 0


def _set_by(eid: str, db, eareas: dict[str, list[str]] | None,
            via: str = "", this_water: str = "") -> tuple[str, int]:
    """Who set this rule, in the curator's words, and how fine their geography is.

    The page derived both from the entry id — `z1:` gave "Region 1" for every entry in the
    Region 1 chapter. `z1:trout_quota` and `z1:hg_quota` are both filed there and they are
    different jurisdictions: one is the region excluding Haida Gwaii, the other is Haida Gwaii.
    On the Yakoun both are in force, and the screen showed Trout 4 above Trout/char 5 and
    Kokanee 5 above Kokanee 10, every row labelled "Region 1".

    Each zone entry already carries a display name that says exactly this, so it is read rather
    than rebuilt. Returns ("", 0) for a water's own entry — it is not a zone at all.
    """
    if not str(eid).startswith("z"):
        # A RULE THAT ARRIVED BY THE TRIBUTARY WALK SHOULD NAME THE WATER IT WAS WRITTEN FOR.
        #
        # It read "Set by: this water (as a tributary)", which is both true and useless: the
        # reader is looking at the Elk River and is told the rule is the Elk's, in a parenthesis
        # that quietly means it is not. The rule is "KOOTENAY LAKE'S TRIBUTARIES — bull trout
        # catch and release", and the entry has always carried that name.
        if via == "trib":
            row = db.execute("SELECT name FROM entry WHERE entry_id = ?", (eid,)).fetchone()
            name = row[0] if row and row[0] else ""
            if name and name.strip().lower() != (this_water or "").strip().lower():
                return name, 0
        return "", 0
    # NOT for the provincial chapter. `zp:` IS the province, and an entry there can be NAMED
    # for a part of it: `zp:white_sturgeon_licence` is "Fraser watershed, Mission to Williams
    # Lake River", which describes its licence and quota rules — but its third rule is an
    # advisory scoped to every region ("the only white sturgeon fishery in the province's
    # non-tidal waters"), and that one reaches the Yakoun River on Haida Gwaii. Labelled from
    # the entry it read "Set by: Fraser watershed" on a river 800 km away.
    #
    # A region chapter has the opposite problem — "Region 1" is false for the Haida Gwaii
    # table inside it — which is what the name is read for. The province is not ambiguous
    # about which province it is.
    if str(eid).startswith("zp:"):
        return "", 0
    row = db.execute("SELECT name FROM entry WHERE entry_id = ?", (eid,)).fetchone()
    return (row[0] if row and row[0] else ""), _area_tier((eareas or {}).get(eid, []))


def _one_water(db, graph, geoms, handles, to_lonlat, name: str, kind: str = "stream",
               eareas: dict[str, list[str]] | None = None,
               want_item: str = "", wide: set | None = None, build: Path | None = None):
    """One water's stretches, rules, cut-points and landmarks.

    THE FIRST STAGE OF THE DOCUMENT, computed the way the app computes it: collapse adjacent
    sections that share a RULESET into one stretch. The set id is the compression the bundle
    already did — `section_ruleset` interns 1.9M (section, rule) pairs into ~5,200 sets — so
    "these sections have the same rules" is an integer comparison here, not a rule diff.
    """
    # HANDLES ARE ONE-BASED and 0 means "no section" — see pipeline/common/section_handles.
    # Indexing `ids` directly returns the NEXT section, which is the exact failure that module
    # exists to prevent: a valid handle pointing at a different piece of river. Inverting the
    # node->handle map cannot be off by one.
    ids_by_handle, by_node = handles
    ids = {h: n for n, h in by_node.items()}
    # A LAKE IS A WATER TOO, and every screen in this document was a river.
    #
    # The two differ in exactly one structural way: a river is a chain of pieces along a blue
    # line with chainage, and a lake is ONE piece with none — `down_m` and `up_m` are both 0, and
    # `blk` is empty. Everything downstream of that (one run, no ladder, no ruler) is the shape
    # the app already uses for the 93.59% of waters that have a single stretch, so the lake case
    # is the simple case rather than a new one.
    #
    # A name may belong to both kinds (Elk River and Elk Lake), so the caller says which.
    # A NAME IS NOT AN IDENTIFIER. Three lakes in the province are called Elk Lake, two of them
    # with synopsis rows of their own — one in Region 1 and one in Region 5 — and taking the
    # first by `ord` drew a Victoria lake with the Skeena's tagging notices on it.
    #
    # So the caller may name the registry item outright, and where it does not, an item the
    # synopsis actually writes about beats one it does not.
    # A CUT LAKE IS NAMED BY THE LAKE ITS PARTS ARE PART OF, before any name lookup. Three
    # lakes are called Shannon Lake and the plain lookup picked wbk:329244252 — which has no
    # parts, so the page drew one rung and looked exactly as though the split had failed.
    want_item = want_item or (_parent_of(db, name) if kind == "lake" else "") or ""
    if want_item:
        row = db.execute("SELECT ord, item_id, kind FROM item WHERE item_id = ?",
                         (want_item,)).fetchone()
    else:
        row = db.execute(
            "SELECT i.ord, i.item_id, i.kind FROM item i"
            "  LEFT JOIN entry e ON e.item_id = i.item_id"
            " WHERE i.name = ? AND i.kind = ?"
            " ORDER BY (e.entry_id IS NULL), i.ord LIMIT 1", (name, kind)).fetchone()
    if row is None:
        return None
    ord_, item_id, water_kind = row

    # EVERY ITEM THE SAME SYNOPSIS ROW COVERS, and no more. "CHILLIWACK / VEDDER RIVERS" is
    # ONE row over two registry items, and taking only the item that carries the name dropped
    # every rule bound on the Vedder.
    #
    # THE TEST IS `matched`, NOT "an entry that binds here". Following any binding entry pulled
    # the Elk River wholesale into the Fording (they share an inherited rule) and the Skeena
    # into the Babine — a 77 km river reported as 214 km, drawn as its receiving water.
    ords = {ord_}
    co = _co_items(db, item_id)
    kin = {item_id} | co
    for co in sorted(co):
        for (o,) in db.execute("SELECT ord FROM item WHERE item_id = ? AND kind = ?",
                               (co, kind)):
            ords.add(o)
    qs = ",".join("?" * len(ords))
    sids = [r[0] for r in db.execute(
        f"SELECT sid FROM item_section WHERE ord IN ({qs})", tuple(ords))]
    if not sids:
        return None

    # sid -> node -> the graph's own chainage. `down_m`/`up_m` are metres along the blue line,
    # so they are what makes a stretch a stretch rather than a bag of pieces.
    handle_of_sid: dict[str, int] = {}
    nodes = []
    for sid in sids:
        nid = ids.get(sid)
        n = graph.nodes.get(nid) if nid else None
        # `blk` is the blue line a piece sits on. A lake has none — it is not on a line — so
        # only a MISSING node disqualifies here, never a missing blk.
        if n is None:
            continue
        handle_of_sid[nid] = sid
        nodes.append((nid, n))
    if not nodes:
        return None

    # THE MAINSTEM IS THE LONGEST BLUE LINE, and the rest is side channel. A named river is
    # often several blks — a braid, a side channel, a canal — and drawing them as one
    # continuous stretch would put a rule on water it never covered.
    by_blk: dict[str, list] = defaultdict(list)
    for nid, n in nodes:
        by_blk[str(n.blk)].append((nid, n))

    # THE MAINSTEM COMES FROM THE ITEM THE SCREEN IS NAMED FOR, not from whichever co-item is
    # longest. "ATNARKO/BELLA COOLA RIVERS" is one synopsis row over two rivers, and the Atnarko
    # is the longer of them — so choosing the longest blue line across both drew the ATNARKO on
    # the screen titled Bella Coola River, at the Atnarko's length, with the Atnarko's stretches.
    # Two screens, two names, one river.
    #
    # The co-items still contribute their SECTIONS, which is what the expansion is for: it is how
    # the Vedder's rules reach the Chilliwack's screen. They just cannot take the line.
    own_sids = {r[0] for r in db.execute(
        "SELECT sid FROM item_section WHERE ord = ?", (ord_,))}
    own_blks = {str(n.blk) for nid, n in nodes if handle_of_sid.get(nid) in own_sids and n.blk}
    pick = {b: v for b, v in by_blk.items() if b in own_blks} or by_blk
    main_blk = max(pick, key=lambda b: sum(x[1].up_m - x[1].down_m for x in pick[b]))
    main = sorted(by_blk[main_blk], key=lambda x: x[1].down_m)

    set_of = dict(db.execute(
        f"SELECT sid, set_id FROM section_ruleset WHERE sid IN ({','.join('?' * len(sids))})",
        sids))
    handle_of = {ids[s]: s for s in sids if s in ids}

    base_m = main[0][1].down_m
    runs: list[dict] = []
    for nid, n in main:
        sid = handle_of.get(nid)
        set_id = set_of.get(sid)
        km0 = round((n.down_m - base_m) / 1000.0, 1)
        km1 = round((n.up_m - base_m) / 1000.0, 1)
        g = geoms.get(nid)
        pts = [_pts(g, to_lonlat)] if g is not None else []
        oob = bool(getattr(n, "out_of_bc", False)) or (
            (n.up_m - n.down_m) >= 250.0 and (g is None or g.length < 250.0))
        if runs and runs[-1]["set"] == set_id and runs[-1]["oob"] == oob:
            runs[-1]["to"] = km1
            runs[-1]["pts"].extend(pts)
            runs[-1]["n"] += 1
            # A run is several sections merged, so its geography is the UNION of theirs — the
            # same way `mus` is gathered below. Without this a stretch reported only the
            # region of whichever section happened to open it.
            for key, pref in (("regions", "area:region:"), ("mu_groups", "area:mu_group:")):
                got = {a.split(":")[-1] for a in (getattr(n, "in_areas", None) or ())
                       if a.startswith(pref)}
                runs[-1][key] = sorted(set(runs[-1].get(key) or ()) | got)
            runs[-1]["mus"] = sorted(set(runs[-1]["mus"])
                                     | {m for m in (getattr(n, "mus", None) or ())})
        else:
            runs.append({"set": set_id, "from": km0, "to": km1, "pts": pts, "n": 1,
                         "mus": sorted({m for m in (getattr(n, "mus", None) or ())}),
                         # THE MU NUMBER IS NOT THE REGION, and the page had no way to know it.
                         #
                         # It derived one by splitting the MU id at the dash — "6-13" -> Region
                         # 6 — and used that to drop a zone rule belonging to another region.
                         # Haida Gwaii is MUs 6-12 and 6-13 and is administered as REGION 1, so
                         # on the Yakoun that test threw away all 25 Region 1 rules: the bait
                         # ban, the barbless-hook rule, and the Haida Gwaii quota table itself.
                         # The screen showed neither region's quotas.
                         #
                         # The atlas already knows. Every section's `in_areas` carries the
                         # region polygon it falls inside — 1,956,214 of 1,957,890 sections
                         # carry exactly one — so the answer is READ here rather than inferred
                         # from a string that was never a region id.
                         "regions": sorted({a.split(":")[-1]
                                            for a in (getattr(n, "in_areas", None) or ())
                                            if a.startswith("area:region:")}),
                         # The MU GROUPS it falls in, for the same reason: "Management Units
                         # 1-1 to 1-6" is a geography a regulation is written for, and a rule
                         # scoped to it is not a regional default.
                         "mu_groups": sorted({a.split(":")[-1]
                                              for a in (getattr(n, "in_areas", None) or ())
                                              if a.startswith("area:mu_group:")}),
                         "km": 0.0, "joins": [], "label": _bound_label(n.lower_bound),
                         # WHAT KIND OF THING BOUNDS IT. An AREA names both of its edges — the
                         # Chilliwack Ecological Reserve labels the stretch entering it AND the
                         # stretch leaving it — so the page needs the kind to say which end
                         # this is. A 1.8 km reserve reading as the foot of a 22 km stretch is
                         # what happens without it.
                         "bkind": (getattr(getattr(n, "lower_bound", None), "kind", None).value
                                   if getattr(getattr(n, "lower_bound", None), "kind", None)
                                   is not None else "point"),
                         # OUTSIDE BRITISH COLUMBIA. The Kootenay leaves the province below
                         # Creston and comes back; the graph has always known (`out_of_bc`,
                         # set by `mark_out_of_bc`) but the bundle carries no such column, so
                         # the ladder drew 160 km of Idaho as though B.C. rules ran there.
                         #
                         # MARKED, NOT DROPPED. Dropping it at graph build would cut the river
                         # in two and restart the chainage, and the Kootenay above the border
                         # is genuinely the same river as the Kootenay below it.
                         #
                         # The same test `mark_out_of_bc` now makes, made here as well, because
                         # the flag on a pickled graph is only as new as the last atlas build.
                         # A piece claiming kilometres it cannot draw has a route running on
                         # where the province has no line — the Chilliwack above the ecological
                         # reserve reads down=60407 up=82832 with a zero-length geometry, and
                         # that water is in Washington.
                         "oob": oob})
    # ---- water this book does not govern is not on the ladder ---------------------
    #
    # Two different things get called "out of B.C." and both come off.
    #
    # The CHILLIWACK's top piece reads down=60407 up=82832 — a claim on 22.4 km — against a
    # geometry of zero length: FWA's route measure belongs to the whole blue line, and the river
    # above the ecological reserve is in Washington. There was never anything there to draw. It
    # was also making B.C.'s 60 km of Chilliwack read as an 83 km river.
    #
    # The KOOTENAY's is real water — it leaves the province below Creston and returns 263 km
    # later, and we hold its line. Dropping it leaves a jump from km 169 to km 432, which was
    # the argument for keeping it as a marked gap. Overruled, and rightly: a reader who sees the
    # kilometres jump understands a river left the province, and a rung that answers "nothing
    # here applies" is a tap that leads nowhere.
    #
    # So the ladder holds only water this synopsis governs.
    runs = [r for r in runs if not r["oob"]]

    # ---- WHICH CROSSING OF THE SAME BOUNDARY THIS IS -------------------------------
    #
    # An AREA is a polygon: it cuts the river where the river enters and again where it leaves,
    # and both cuts carry its name. Two stretches then read identically — the Fraser printed
    # "From Region 5 – Region 7A boundary / To Region 5 – Region 7A boundary", which looks like
    # no distance at all rather than the 29 km it is.
    #
    # The SPLITS ARE ALREADY IN THE RIGHT PLACES. Downstream of the first cut the river is in
    # one region; upstream of the last it is in the other; between them it is in both, and the
    # MUs say so (5-13 and 7-8 on the same reach). What was missing is only which crossing each
    # cut is, so the piece between them can be named as the piece between them. Where a boundary
    # is crossed once, there is one cut and no middle — nothing here fires.
    at = defaultdict(list)
    for i, r in enumerate(runs):
        if r.get("bkind") == "area" and r.get("label"):
            at[r["label"]].append(i)
    for lab, idx in at.items():
        if len(idx) < 2:
            continue
        for n, i in enumerate(idx):
            runs[i]["bside"] = "down" if n == 0 else ("up" if n == len(idx) - 1 else str(n + 1))

    # A LAKE IS DRAWN AS ITS SHORELINE, not as the river running under it.
    area_km2 = None
    if water_kind == "lake" and len(runs) == 1:
        got = _lake_outline(item_id, to_lonlat)
        if got:
            ring, area_km2 = got
            runs[0]["pts"] = [ring]
            runs[0]["ring"] = True

    # ...AND A LAKE THE BOOK WRITES AS SEVERAL WATERS IS A LADDER OF THEM, like a river.
    #
    # Each piece is its own registry item, so left alone they open as separate screens with
    # nothing to say one is part of the other. A river with different rules along it gets one
    # screen and a ladder of stretches; this is the same question and gets the same answer.
    #
    # A part becomes a RUN carrying its own `set`, which is all the machinery below needs: the
    # rules are collected from every set on the screen and their spans come from which runs
    # carry which set, so a rule that binds only the netted-off corner spans only that rung.
    parts = _lake_parts(db, item_id) if water_kind == "lake" else []
    if parts and runs:
        base = runs[0]
        lake_runs = []
        i = 0
        # IS THE PARENT STILL A WATER? Ask whether any synopsis row is about it.
        #
        # Shannon Lake, Kootenay Lake and Williston Lake all have `entries = 0` now: every row
        # that once named them was repointed to a part, because the parts ARE the lake — the
        # polygons partition it and the ingest carved every fid out. Drawing the parent as a
        # rung put a water on the ladder that the book has nothing to say about, and made
        # Shannon three rungs where the book has two.
        #
        # They still carry rules — 77, 80 and 116 of them — but those are the regional and
        # provincial defaults that reach any water sitting in that region, not rules written
        # about this one. An entry of its own is what makes a rung worth showing.
        #
        # A lake split only PART of the way keeps its parent rung, because a row still names
        # it. The test is the same either way.
        own_entries = db.execute("SELECT COUNT(*) FROM entry WHERE item_id = ?",
                                 (item_id,)).fetchone()[0]
        if own_entries:
            lake_runs.append(dict(base, label=name_of_item(db, item_id) or name,
                                  **{"from": 0.0, "to": 1.0}))
            i = 1
        for child_id, child_name in parts:
            # `part_of` comes from the bundle, which refuses a part its registry lacks, so the
            # child is always an item here.
            row = db.execute("SELECT ord FROM item WHERE item_id = ?", (child_id,)).fetchone()
            csids = [r[0] for r in db.execute(
                "SELECT sid FROM item_section WHERE ord = ?", (row[0],))]
            cset = None
            for csid in csids:
                got2 = db.execute("SELECT set_id FROM section_ruleset WHERE sid = ?",
                                  (csid,)).fetchone()
                if got2:
                    cset = got2[0]
                    break
            # A PART HAS AN AREA, NOT A LENGTH. `from`/`to` are ordinals that put the rungs in
            # order, so their difference is 1 and the rung would read "1 KM" — meaningless for
            # still water, and wrong about a 389 km² lake body.
            got2 = _part_ring(build, child_id, to_lonlat) if build is not None else None
            ring2, area2 = got2 if got2 else (None, None)
            lake_runs.append({**base, "set": cset, "from": float(i), "to": float(i + 1),
                              "pts": [ring2] if ring2 else [], "ring": bool(ring2),
                              "label": child_name, "n": 1, "joins": [], "bkind": "part",
                              "area": area2})
            i += 1
        if len(lake_runs) > 1:
            runs = lake_runs
            print(f"    {len(runs)} parts: " + " · ".join(str(r["label"]) for r in runs))

    for r in runs:
        r["km"] = round(r["to"] - r["from"], 1)

    # THE CUT-POINTS, with where they are. A stretch is bounded by named things — a dam, a
    # confluence, a bridge, boundary signs — and "km 704" tells a reader nothing they can find
    # on the ground. These were emitted as an empty list, so the ladder had only numbers.
    splits: list[dict] = []
    seen_ref: set[tuple] = set()
    for nid, nd in main:
        for end in (nd.lower_bound, nd.upper_bound):
            lab = _bound_label(end)
            ref = getattr(end, "boundary_id", None) if end is not None else None
            m = getattr(end, "route_measure", None)
            if not lab or not ref or m is None:
                continue
            # KEYED ON THE BOUNDARY *AND* THE PLACE. An area cuts the river where it enters and
            # again where it leaves, and both cuts carry the same boundary id — so keying on the
            # id alone kept only the first, and the stretch between the two crossings had no tick
            # at its upper end and nothing to number it with.
            key = (ref, round(m, 1))
            if key in seen_ref:
                continue
            seen_ref.add(key)
            km = round((m - base_m) / 1000.0, 1)
            if km < -0.5 or km > (main[-1][1].up_m - base_m) / 1000.0 + 0.5:
                continue
            splits.append({"km": km, "label": lab,
                           "kind": (getattr(end, "kind", None).value
                                    if getattr(end, "kind", None) is not None else "point"),
                           "lon": None, "lat": None})
    splits.sort(key=lambda x: x["km"])

    # ONLY THE CUT-POINTS THAT CUT ANYTHING.
    #
    # A river carries every boundary the atlas knows — gauges, lake edges, MU lines, every
    # curated split — and most of them separate water with IDENTICAL rules. Showing them all
    # made the ladder a list of places rather than a list of answers, and the "also" line under
    # a chosen stretch named every boundary on the river instead of the ones bounding it.
    #
    # A stretch is defined by its RULES, so the only cut-points worth drawing are the ones a
    # run actually starts or ends at: given splits a, b, c, d where rules change only at b, the
    # functional stretches are a-b and b-d, and c is not a boundary of anything.
    edges = {r["from"] for r in runs} | {r["to"] for r in runs}
    splits = [x for x in splits if any(abs(x["km"] - e) < 0.15 for e in edges)]

    # PUT THE MARKER WHERE THE CUT-POINT IS. It took the FIRST VERTEX of whichever section the
    # boundary was found on, which is the start of that piece and not the boundary at all — the
    # ticks landed off the drawn river, one of them in the next valley. The boundary knows its
    # chainage, and the runs carry the line the map draws, so the honest position is the point
    # at that chainage ON that line.
    for x in splits:
        at = _at_km(runs, x["km"])
        if at:
            x["lon"], x["lat"] = at

    side = [{"pts": _pts(geoms[nid], to_lonlat), "set": set_of.get(handle_of.get(nid))}
            for b, xs in by_blk.items() if b != main_blk
            for nid, n in xs if nid in geoms]

    # --- the rules those sets point at -------------------------------------------------
    #
    # FROM EVERY SECTION OF THE WATER, NOT JUST THE MAINSTEM.
    #
    # The rules list used to be built from the RUNS, which are the longest blue line only. Six
    # of the Chilliwack's nine own rules bind on the Vedder — the same synopsis row covers both
    # — so they were dropped, and a river with nine rules of its own showed three, the rest of
    # the page being regional defaults. A rule that reaches this water belongs on the page even
    # when the piece it reaches is not the one the ladder draws.
    #
    # It still gets its SPANS from the runs, because that is what the ladder can show. A rule
    # that touches no run keeps `spans: []` rather than being given the whole river, which
    # would be a claim about extent that nothing in the data supports.
    span_of: dict[tuple[str, str, str], list] = defaultdict(list)
    for r in runs:
        if r["set"] is None:
            continue
        for (eid, rid, via) in db.execute(
                "SELECT entry_id, rule_id, via FROM ruleset WHERE set_id = ?", (r["set"],)):
            span_of[(eid, rid, via)].append([r["from"], r["to"]])
    everywhere: set[tuple[str, str, str]] = set()
    # EVERY SET ON THE SCREEN, not only the ones the parent item's sections carry.
    #
    # `set_of` is built from this water's own sids, which for a cut lake is the parent — and the
    # parent is exactly the piece that no longer holds anything. So the arms' rulesets were never
    # collected: the rungs had the right `set` for their spans, and the rules those spans pointed
    # at were absent. Kootenay Lake showed three rungs, 77 regional rules, and not one of the
    # kokanee closures or arm quotas the whole split exists to separate.
    all_sets = sorted({v for v in set_of.values() if v is not None}
                      | {r["set"] for r in runs if r.get("set") is not None})
    for chunk in range(0, len(all_sets), 400):
        part = all_sets[chunk:chunk + 400]
        for row in db.execute(
                f"SELECT entry_id, rule_id, via FROM ruleset WHERE set_id IN"
                f" ({','.join('?' * len(part))})", part):
            everywhere.add(tuple(row))

    rules: list[dict] = []
    entries: dict[str, dict] = {}
    if everywhere:
        for (eid, rid, via) in sorted(everywhere):
            spans = span_of.get((eid, rid, via), [])
            cur = db.execute("SELECT * FROM rule WHERE entry_id=? AND rule_id=?", (eid, rid))
            src = cur.fetchone()
            if src is None:
                continue
            d = dict(zip([c[0] for c in cur.description], src))
            cond = json.loads(d["conditions"] or "{}")
            # `while` is a bundle column of its own now; put it back where this page reads it.
            if d.get("while_"):
                cond["while"] = json.loads(d["while_"])
            # So is `exempts` — resolved, [{default_id | target, entry_id, note?}]. Left in its
            # column, every lift silently vanished from this file.
            if d.get("exempts"):
                cond["exempts"] = json.loads(d["exempts"])
            if d.get("standing"):
                cond["standing"] = True
            _sb, _tier = _set_by(eid, db, eareas, via, name)
            rules.append({
                "entry": eid, "rule": rid,
                # WHO SET IT AND HOW NARROWLY — from the entry, not from the id prefix.
                **({"setby": _sb} if _sb else {}),
                **({"tier": _tier} if _tier else {}),
                # THE CATALOGUE'S OWN WORDS. `kind`/`details` are gone: `type` is one of
                # fifteen, `family` is the section a reader sees it under, and `label` is
                # GENERATED, so this document cannot word a rule differently from the app.
                "type": d["type"], "family": d["family"], "dimension": d["dimension"],
                "label": d["label"],
                "when": json.loads(d["when_"] or "{}"),
                "species": json.loads(d["species"] or "[]"),
                # THE FISH A RULE CARVES OUT is part of what it says — "a salmon of any legal
                # size or species (other than kokanee)", "all game fish other than burbot". It
                # is a bundle column and was simply not copied, so every renderer that answers
                # from fields rather than from the generated label lost the exception.
                "species_except": json.loads(d["species_except"] or "[]"),
                "take": d["take"], "may_target": d["may_target"],
                # The retention fields the QUOTA TABLE needs, at the top level rather than
                # buried in `conditions` — a table that has to parse a JSON blob per cell is a
                # table nobody will keep working. `conditions` still carries everything else.
                **{k: cond[k] for k in ("unlimited", "lengths", "period", "water",
                                        "origin", "within", "record_retention",
                                        # `while` decides whether a closure shuts the WATER or
                                        # only one way of fishing it — see `narrows` in the page.
                                        "while", "closed_to", "when_targeting")
                   if k in cond},
                "conditions": cond,
                "uncertain": d["uncertain"], "scope": d["scope"], "via": via,
                "verbatim": d["verbatim"], "extent_text": d["extent_text"],
                # The book names a part of this water; this rule covers all of it. See `_widened`.
                **({"widened": True} if (eid, rid) in (wide or ()) else {}),
                "spans": spans,
                "km": round(sum(b - a for a, b in spans), 1),
            })
            if eid not in entries:
                e = db.execute("SELECT name, full_name, verbatim, symbols, mus FROM entry"
                               " WHERE entry_id = ?", (eid,)).fetchone()
                if e:
                    entries[eid] = {"name": e[0], "full": e[1], "verbatim": e[2],
                                    "symbols": json.loads(e[3] or "[]"),
                                    "mus": json.loads(e[4] or "[]"),
                                    "areas": (eareas or {}).get(eid, [])}

    # ---- rules this water HAS but the atlas could not place ----------------------
    #
    # 137 of the 202 rules that name a sub-extent bind to NO section anywhere. They are in the
    # bundle and reachable from nothing, so the app renders the water as if they did not exist.
    # Some are closures — "No fishing, Dec 1-May 31 — in any tributaries" on the Campbell.
    #
    # The atlas is RIGHT to refuse them: `classify` will not widen a location it could not
    # resolve, because turning "500 m upstream of Causeway Road" into the whole lake applies a
    # 500 m closure to kilometres of water. But dropping has the opposite failure and it is the
    # worse one — silence reads as permission.
    #
    # So they are neither placed nor dropped: carried on the water, with the book's own words
    # for where they apply, and rendered apart from the ladder because nothing here knows which
    # part of the river they are. The reader is told a rule exists and told to go read the sign.
    placed = {(eid, rid) for (eid, rid, _v) in everywhere}
    unplaced: list[dict] = []
    if kin:
        for eid, in db.execute(
                f"SELECT entry_id FROM entry WHERE item_id IN ({','.join('?' * len(kin))})",
                tuple(sorted(kin))):
            cur = db.execute("SELECT * FROM rule WHERE entry_id = ?", (eid,))
            cols = [c[0] for c in cur.description]
            for src in cur.fetchall():
                d = dict(zip(cols, src))
                if (eid, d["rule_id"]) in placed:
                    continue
                cond = json.loads(d["conditions"] or "{}")
                if d.get("while_"):
                    cond["while"] = json.loads(d["while_"])
                if d.get("exempts"):
                    cond["exempts"] = json.loads(d["exempts"])
                if d.get("standing"):
                    cond["standing"] = True
                unplaced.append({
                    "entry": eid, "rule": d["rule_id"], "type": d["type"],
                    "family": d["family"], "dimension": d["dimension"], "label": d["label"],
                    "when": json.loads(d["when_"] or "{}"),
                    "species": json.loads(d["species"] or "[]"),
                    "take": d["take"], "may_target": d["may_target"],
                    "conditions": cond, "verbatim": d["verbatim"],
                    # WHY it could not be placed, in the book's words. This is the whole point
                    # of showing the row: "on parts", "within 60 m of shore", "Thelwood Creek".
                    "extent_text": d["extent_text"],
                    "scope": d["scope"], "uncertain": d["uncertain"], "spans": [], "km": 0.0,
                })
                if eid not in entries:
                    e = db.execute("SELECT name, full_name, verbatim, symbols, mus FROM entry"
                                   " WHERE entry_id = ?", (eid,)).fetchone()
                    if e:
                        entries[eid] = {"name": e[0], "full": e[1], "verbatim": e[2],
                                        "symbols": json.loads(e[3] or "[]"),
                                        "mus": json.loads(e[4] or "[]"),
                                        "areas": (eareas or {}).get(eid, [])}
        unplaced.sort(key=lambda r: (r["entry"], r["rule"]))

    # `place_water.ckm` IS NOT CHAINAGE. It is centikm from the place TO the water, capped at
    # 25 km — how far off the river the town is, not how far along it. Reading it as a position
    # put Vedder Crossing at km 37 of an 83 km river and Keyhole Canyon past the head. The
    # position has to be measured, so it is: nearest point on the drawn line, and the km that
    # point sits at.
    landmarks = []
    for nm, ckm, lon, lat, pop in db.execute(
            "SELECT p.name, pw.ckm, p.lon, p.lat, p.pop FROM place_water pw"
            " JOIN place p ON p.place_id = pw.place_id WHERE pw.ord = ?"
            " ORDER BY pw.ckm", (ord_,)):
        # A LANDMARK IS SOMETHING ON THE RIVER. `ckm` is how far the place sits OFF the water,
        # so it is the right filter: without it, Harrison Mills and Sts'ailes — 16 and 22 km
        # away, on the Fraser — were pinned to the Chilliwack's km 0, and 27 of the 45 places
        # piled up at the mouth. Two kilometres is the width of a valley bottom.
        if (ckm or 0) > 200:
            continue
        km = _km_of(runs, lon, lat)
        if km is None:
            continue
        landmarks.append({"name": nm, "km": km, "lon": lon, "lat": lat,
                          "off": round((ckm or 0) / 100.0, 2), "pop": pop or 0})
    landmarks.sort(key=lambda x: x["km"])

    # THE RIVER IS AS LONG AS THE STRETCHES THAT SURVIVED. Measuring to the last node's `up_m`
    # measures the blue line, which on a border river runs on past the province — it made the
    # Chilliwack an 83 km river when B.C. holds 60 km of it.
    total = round(runs[-1]["to"], 1) if runs else round((main[-1][1].up_m - base_m) / 1000.0, 1)
    primary = next(iter(entries.values()), None)
    return {"name": name, "item": item_id, "kind": water_kind, "area": area_km2,
            "runs": runs, "rules": rules, "side": side,
            "unplaced": unplaced, "landmarks": landmarks, "splits": splits, "total": total,
            "entry": primary, "entries": entries}


def _refuse_stale(db) -> None:
    """The bundle this reads must carry `entry.matched`, `item.part_of`, `rule.exempts`, and no
    retired rule field."""
    from pipeline.tools.export_ui_rules import RETIRED_ON_RULE

    if "matched" not in {r[1] for r in db.execute("PRAGMA table_info(entry)")}:
        raise SystemExit("build_section_data: the bundle has no `entry.matched` — rebuild it "
                         "with a rules.py that writes every matched item")
    if "part_of" not in {r[1] for r in db.execute("PRAGMA table_info(item)")}:
        raise SystemExit("build_section_data: the bundle has no `item.part_of` — a lake's parts "
                         "would open as unrelated waters; rebuild it")
    if "exempts" not in {r[1] for r in db.execute("PRAGMA table_info(rule)")}:
        raise SystemExit("build_section_data: the bundle has no `rule.exempts` column — its "
                         "lifts would be dropped; rebuild it")
    stale = sorted({k for (c,) in db.execute("SELECT conditions FROM rule")
                    for k in json.loads(c or "{}") if k in RETIRED_ON_RULE})
    if stale:
        raise SystemExit(f"build_section_data: the bundle ships retired rule field(s) {stale} — "
                         f"rebuild it from the current catalogue")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--build", type=Path, default=None)
    ap.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    ap.add_argument("--out", type=Path,
                    default=REPO_ROOT / "data" / "generated" / "regs" / "sections.json")
    ap.add_argument("--print", dest="to_stdout", action="store_true",
                    help="write the JSON to stdout instead of to the data file")
    a = ap.parse_args()
    build = a.build or GENERATED.require_build()

    from pipeline.common.io.serialize import read_artifact
    from pipeline.common.section_handles import read as read_handles

    log(f"reading {a.bundle}")
    db = sqlite3.connect(a.bundle)
    _refuse_stale(db)
    handles = read_handles(build)                       # node id per handle, index == handle
    log(f"  handles {len(handles[0]):,}")

    log("reading graph + geometries (large)")
    graph = read_artifact(str(build / "graph.pkl"))
    geoms = read_artifact(str(build / "geometries.pkl"))
    to_lonlat = _albers_to_lonlat()

    eareas = _entry_areas(db)
    wide = _widened(db)
    log(f"  {len(eareas)} entries declare an area")

    out: dict[str, object] = {}
    for entry in WATERS:
        name, why = entry[0], entry[1]
        kind = entry[2] if len(entry) > 2 else "stream"
        want = entry[3] if len(entry) > 3 else ""
        got = _one_water(db, graph, geoms, handles, to_lonlat, name, kind,
                         eareas, want, wide, build)
        if got is None:
            log(f"  ✗ {name}: no stream item of that name — SKIPPED")
            continue
        got["why"] = why
        out[name] = got
        log(f"  ✓ {name}: {len(got['runs'])} run(s), {len(got['rules'])} rule(s), "
            f"{got['total']} km")

    out["_bundle"] = {k: v for k, v in db.execute(
        "SELECT k, v FROM meta WHERE k IN ('version', 'reach_run', 'reach_digest', "
        "'section_handles') ORDER BY k")}
    out["_species"] = _species_names()
    out["_groups"] = _species_groups()
    out["_members"] = _group_members()
    blob = json.dumps(out, separators=(",", ":"), ensure_ascii=False)

    if a.to_stdout:
        print(blob)
        return 0
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(blob, encoding="utf-8")
    log(f"\nwrote {len(blob):,} bytes to {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
