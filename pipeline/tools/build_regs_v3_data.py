"""Regenerate the dataset embedded in `app/design/regs-v3.html`.

The prototype in that file — "what applies here", two stages, where on the river then what
applies there — runs on REAL build output, and its data used to be produced by hand. So when
the corpus moved to the rule catalogue the file kept showing prose rules (`kind`, `details`)
for entries that no longer exist: of the 110 entry ids it referenced, 16 survived. A design
document that disagrees with the build is worse than none, because it is still persuasive.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.build_regs_v3_data          # in place
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.build_regs_v3_data --print  # to stdout

WHAT IT READS, and why each one:

    bundle.sqlite      the rules, the interned rulesets, the entries, the places. The shipped
                       artifact, so the prototype cannot show a rule the app would not.
    section_handles    the integer the bundle calls a section -> the node id everything else
                       calls it. Never derived; the file is the owner (see that module).
    graph.pkl          chainage. `down_m`/`up_m` are metres along the blue line, which is what
                       turns a set of sections into a STRETCH with a start and an end.
    geometries.pkl     the line to draw, in BC Albers, reprojected here and nowhere else.

The waters are named below rather than discovered. They are chosen to exercise the cases the
document is about, and a water that stops exercising its case should be replaced, not kept
because it is already in the list.
"""

from __future__ import annotations

import argparse
import json
import re
import pathlib
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

from pipeline.common.curated import GENERATED, REPO_ROOT

#: (display name, why it is here). The `why` is not decoration — it is the test for whether a
#: water still belongs, and the reason the list is not just "big rivers".
WATERS: list[tuple[str, str]] = [
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
    ("Yakoun River",     "Haida Gwaii: an island system on its own, with a seasonal steelhead "
                         "stamp and none of the mainland's regional furniture"),
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
    xs, ys = zip(*list(geom.coords))
    lon, lat = to_lonlat(list(xs), list(ys))
    return [[round(a, ndigits), round(b, ndigits)] for a, b in zip(lon, lat)]


def _co_items(item_id: str) -> set[str]:
    """The OTHER registry items a synopsis row covers alongside this one.

    From the curated entries' `matched`, which is the only place that fact lives — the bundle's
    `entry.item_id` keeps just the first match, by design (see the note in rules.py)."""
    global _CO
    if _CO is None:
        import glob
        _CO = {}
        for f in glob.glob("data/curated/regulations/entries/catalogue/region-*.json"):
            for e in json.loads(pathlib.Path(f).read_text(encoding="utf-8")).get("entries", []):
                m = [x for x in (e.get("matched") or []) if x]
                for a in m:
                    _CO.setdefault(a, set()).update(x for x in m if x != a)
    return _CO.get(item_id, set())


_CO = None


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
    return [{"id": code.lower(), "name": _SPECIES_WORDS[code],
             "codes": [code, *kin.get(code, ()), *SPECIES_GROUPS[code]]}
            for code in ("TROUT_CHAR", "SALMON", "WHITEFISH", "BASS", "ALL_GAME_FISH")]


def _rules_by_id(db: sqlite3.Connection) -> dict[tuple[str, str], dict]:
    cols = [r[1] for r in db.execute("PRAGMA table_info(rule)")]
    out = {}
    for row in db.execute(f"SELECT {', '.join(cols)} FROM rule"):
        r = dict(zip(cols, row))
        out[(r["entry_id"], r["rule_id"])] = r
    return out


def _one_water(db, graph, geoms, handles, to_lonlat, name: str):
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
    row = db.execute("SELECT ord, item_id FROM item WHERE name = ? AND kind = 'stream'"
                     " ORDER BY ord LIMIT 1", (name,)).fetchone()
    if row is None:
        return None
    ord_, item_id = row

    # EVERY ITEM THE SAME SYNOPSIS ROW COVERS, and no more. "CHILLIWACK / VEDDER RIVERS" is
    # ONE row over two registry items, and taking only the item that carries the name dropped
    # every rule bound on the Vedder.
    #
    # THE TEST IS `matched`, NOT "an entry that binds here". Following any binding entry pulled
    # the Elk River wholesale into the Fording (they share an inherited rule) and the Skeena
    # into the Babine — a 77 km river reported as 214 km, drawn as its receiving water.
    ords = {ord_}
    kin = {item_id} | _co_items(item_id)
    for co in _co_items(item_id):
        for (o,) in db.execute("SELECT ord FROM item WHERE item_id = ? AND kind = 'stream'", (co,)):
            ords.add(o)
    qs = ",".join("?" * len(ords))
    sids = [r[0] for r in db.execute(
        f"SELECT sid FROM item_section WHERE ord IN ({qs})", tuple(ords))]
    if not sids:
        return None

    # sid -> node -> the graph's own chainage. `down_m`/`up_m` are metres along the blue line,
    # so they are what makes a stretch a stretch rather than a bag of pieces.
    nodes = []
    for sid in sids:
        nid = ids.get(sid)
        n = graph.nodes.get(nid) if nid else None
        if n is None or getattr(n, "blk", None) is None:
            continue
        nodes.append((nid, n))
    if not nodes:
        return None

    # THE MAINSTEM IS THE LONGEST BLUE LINE, and the rest is side channel. A named river is
    # often several blks — a braid, a side channel, a canal — and drawing them as one
    # continuous stretch would put a rule on water it never covered.
    by_blk: dict[str, list] = defaultdict(list)
    for nid, n in nodes:
        by_blk[str(n.blk)].append((nid, n))
    main_blk = max(by_blk, key=lambda b: sum(x[1].up_m - x[1].down_m for x in by_blk[b]))
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
        else:
            runs.append({"set": set_id, "from": km0, "to": km1, "pts": pts, "n": 1,
                         "mus": sorted({m for m in (getattr(n, "mus", None) or ())}),
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

    for r in runs:
        r["km"] = round(r["to"] - r["from"], 1)

    # THE CUT-POINTS, with where they are. A stretch is bounded by named things — a dam, a
    # confluence, a bridge, boundary signs — and "km 704" tells a reader nothing they can find
    # on the ground. These were emitted as an empty list, so the ladder had only numbers.
    splits: list[dict] = []
    seen_ref: set[str] = set()
    for nid, nd in main:
        for end in (nd.lower_bound, nd.upper_bound):
            lab = _bound_label(end)
            ref = getattr(end, "boundary_id", None) if end is not None else None
            if not lab or not ref or ref in seen_ref:
                continue
            seen_ref.add(ref)
            m = getattr(end, "route_measure", None)
            if m is None:
                continue
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
    all_sets = sorted({v for v in set_of.values() if v is not None})
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
            rules.append({
                "entry": eid, "rule": rid,
                # THE CATALOGUE'S OWN WORDS. `kind`/`details` are gone: `type` is one of
                # fifteen, `family` is the section a reader sees it under, and `label` is
                # GENERATED, so this document cannot word a rule differently from the app.
                "type": d["type"], "family": d["family"], "dimension": d["dimension"],
                "label": d["label"],
                "windows": json.loads(d["windows"] or "[]"),
                "species": json.loads(d["species"] or "[]"),
                "take": d["take"], "may_target": d["may_target"],
                # The retention fields the QUOTA TABLE needs, at the top level rather than
                # buried in `conditions` — a table that has to parse a JSON blob per cell is a
                # table nobody will keep working. `conditions` still carries everything else.
                **{k: cond[k] for k in ("unlimited", "over_cm", "under_cm", "period", "water",
                                        "origin", "combined", "band", "within", "record_retention",
                                        # `method` decides whether a closure shuts the WATER or
                                        # only one way of fishing it — see `narrows` in the page.
                                        "method", "angler_class", "when_targeting", "permitted",
                                        # the licence table's own columns
                                        "on_retention", "when_open", "water_class", "document",
                                        "required")
                   if k in cond},
                "conditions": cond,
                "uncertain": d["uncertain"], "scope": d["scope"], "via": via,
                "verbatim": d["verbatim"], "extent_text": d["extent_text"],
                "spans": spans,
                "km": round(sum(b - a for a, b in spans), 1),
            })
            if eid not in entries:
                e = db.execute("SELECT name, full_name, verbatim, symbols, mus FROM entry"
                               " WHERE entry_id = ?", (eid,)).fetchone()
                if e:
                    entries[eid] = {"name": e[0], "full": e[1], "verbatim": e[2],
                                    "symbols": json.loads(e[3] or "[]"),
                                    "mus": json.loads(e[4] or "[]")}

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
                unplaced.append({
                    "entry": eid, "rule": d["rule_id"], "type": d["type"],
                    "family": d["family"], "dimension": d["dimension"], "label": d["label"],
                    "windows": json.loads(d["windows"] or "[]"),
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
                                        "mus": json.loads(e[4] or "[]")}
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
    return {"name": name, "item": item_id, "runs": runs, "rules": rules, "side": side,
            "unplaced": unplaced, "landmarks": landmarks, "splits": splits, "total": total,
            "entry": primary, "entries": entries}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--build", type=Path, default=None)
    ap.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    ap.add_argument("--html", type=Path,
                    default=REPO_ROOT / "app" / "design" / "regs-v3.html")
    ap.add_argument("--print", dest="to_stdout", action="store_true",
                    help="write the JSON to stdout instead of into the html")
    a = ap.parse_args()
    build = a.build or GENERATED.require_build()

    from pipeline.common.io.serialize import read_artifact
    from pipeline.common.section_handles import read as read_handles

    log(f"reading {a.bundle}")
    db = sqlite3.connect(a.bundle)
    handles = read_handles(build)                       # node id per handle, index == handle
    log(f"  handles {len(handles[0]):,}")

    log("reading graph + geometries (large)")
    graph = read_artifact(str(build / "graph.pkl"))
    geoms = read_artifact(str(build / "geometries.pkl"))
    to_lonlat = _albers_to_lonlat()

    out: dict[str, object] = {}
    for name, why in WATERS:
        got = _one_water(db, graph, geoms, handles, to_lonlat, name)
        if got is None:
            log(f"  ✗ {name}: no stream item of that name — SKIPPED")
            continue
        got["why"] = why
        out[name] = got
        log(f"  ✓ {name}: {len(got['runs'])} run(s), {len(got['rules'])} rule(s), "
            f"{got['total']} km")

    out["_species"] = _species_names()
    out["_groups"] = _species_groups()
    blob = json.dumps(out, separators=(",", ":"), ensure_ascii=False)

    if a.to_stdout:
        print(blob)
        return 0
    html = a.html.read_text(encoding="utf-8")
    new, n = re.subn(r'(<script id="d" type="application/json">).*?(</script>)',
                     lambda m: m.group(1) + blob + m.group(2), html, count=1, flags=re.S)
    if not n:
        log("✗ no <script id=\"d\"> block in the html — nothing written")
        return 1
    a.html.write_text(new, encoding="utf-8")
    log(f"\nwrote {len(blob):,} bytes into {a.html.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
