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


def _species_names() -> dict[str, str]:
    from pipeline.regs.parsing.species import SPECIES
    return {c: r.common_name for c, r in SPECIES.items()}


def _species_groups() -> list[dict]:
    """The groups the SYNOPSIS prints, from the catalogue — not a hand list.

    The old file carried `TRT` and a hand-made trout set. The catalogue's groups are the
    words the page actually uses, and `ALL_GAME_FISH` is the closed list from definitions.md.
    """
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS, _SPECIES_WORDS
    return [{"id": code.lower(), "name": _SPECIES_WORDS[code],
             "codes": [code, *SPECIES_GROUPS[code]]}
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

    sids = [r[0] for r in db.execute("SELECT sid FROM item_section WHERE ord = ?", (ord_,))]
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
        if runs and runs[-1]["set"] == set_id:
            runs[-1]["to"] = km1
            runs[-1]["pts"].extend(pts)
            runs[-1]["n"] += 1
        else:
            runs.append({"set": set_id, "from": km0, "to": km1, "pts": pts, "n": 1,
                         "mus": sorted({m for m in (getattr(n, "mus", None) or ())}),
                         "km": 0.0, "joins": [], "label": None})
    for r in runs:
        r["km"] = round(r["to"] - r["from"], 1)

    side = [{"pts": _pts(geoms[nid], to_lonlat), "set": set_of.get(handle_of.get(nid))}
            for b, xs in by_blk.items() if b != main_blk
            for nid, n in xs if nid in geoms]

    # --- the rules those sets point at -------------------------------------------------
    sets = sorted({r["set"] for r in runs if r["set"] is not None})
    rules: list[dict] = []
    entries: dict[str, dict] = {}
    if sets:
        span_of: dict[tuple[str, str], list] = defaultdict(list)
        for r in runs:
            for (eid, rid, via) in db.execute(
                    "SELECT entry_id, rule_id, via FROM ruleset WHERE set_id = ?", (r["set"],)):
                span_of[(eid, rid, via)].append([r["from"], r["to"]])
        for (eid, rid, via), spans in sorted(span_of.items()):
            cur = db.execute("SELECT * FROM rule WHERE entry_id=? AND rule_id=?", (eid, rid))
            src = cur.fetchone()
            if src is None:
                continue
            d = dict(zip([c[0] for c in cur.description], src))
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
                "conditions": json.loads(d["conditions"] or "{}"),
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

    landmarks = [{"name": nm, "km": round(ckm, 1), "lon": lon, "lat": lat, "pop": pop or 0}
                 for nm, ckm, lon, lat, pop in db.execute(
                     "SELECT p.name, pw.ckm, p.lon, p.lat, p.pop FROM place_water pw"
                     " JOIN place p ON p.place_id = pw.place_id WHERE pw.ord = ?"
                     " ORDER BY pw.ckm", (ord_,))]

    total = round((main[-1][1].up_m - base_m) / 1000.0, 1)
    primary = next(iter(entries.values()), None)
    return {"name": name, "item": item_id, "runs": runs, "rules": rules, "side": side,
            "landmarks": landmarks, "splits": [], "total": total,
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
