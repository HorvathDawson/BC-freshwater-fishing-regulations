"""T226: compile the curated in-season matches into the file the app reads beside the feed.

    python -m pipeline.regs.inseason.publish [--out DIR] [--feed index.json]

writes `<out>/matches.json` (default `GENERATED.inseason / "feed"`, which the local tile server serves as
`/feeds/inseason/`; production: upload it to R2 `feeds/inseason/matches.json`, beside the T225 worker's index.json).
`--feed` copies a captured feed `index.json` there too (dev: the app then reads both from the tile server).

FORMAT `inseason-links/1` — no section id ever (AGENTS 5): an extent names its water by `item` (durable) and its cuts
by the split's LABEL, the name the release's `part_mark.label` carries where two parts of the water meet; the app
resolves the parts between them in its own release (`@app/core` `inSeasonParts`) and, when a label is not a part
boundary there, shows the banner on the whole water saying so — never guessing a part.

    {"format": "inseason-links/1", "digest": <sha of the curated file>,
     "matches": {"is-…": {"extents": [{"item": "gnis:…", "op": "between", "at": ["Tamihi Rapids Bridge", "…"]}],
                          "from"?: "YYYY-MM-DD", "to"?: "YYYY-MM-DD"}}}

PLACES (owner 2026-10-29: lake inlets, confluences, region / MU crossings, tributaries): with the atlas and the
shipped release at hand (`--release`, default `data/release/LATEST`; refused unless its `section_handles` digest is
the atlas's), every extent is resolved on the atlas (`points.resolve`: the regs' resolver, the measure-cut
pick-points, the tributary walk) and handed to the app as the PARTS of each water it touches, in THAT release:

    "release": "r-…", "matches": {"is-…": {…, "places": {"gnis:…": {"parts": [0, 2], "some": [2], "all": false}}}}

`parts` are part indexes of the water in that release (never a section id, AGENTS 5); `some` the parts only partly
covered; `all` the whole water. The app reads `places` only on the release it names (rule 6: never across a
version) and otherwise falls back to `extents` (labels). `--no-places` skips it (no atlas load).

Deterministic: no clock in the output.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

from pipeline.regs.inseason import matches as M

FORMAT = "inseason-links/1"


def compile_links(doc: M.Matches, labels: dict[str, str], digest: str) -> dict:
    out: dict = {}
    missing: list[str] = []
    for rid in sorted(doc.matches):
        m = doc.matches[rid]
        exts = []
        for e in m.extents:
            at = []
            for s in e.splits:
                lab = labels.get(s, "")
                if not lab and M.place_id_shape(s):
                    lab = s          # a derived pick-point: no part mark carries it — the app reads `places`
                if not lab:
                    missing.append(f"{rid}: split {s!r} has no label")
                at.append(lab)
            exts.append({"item": e.item_id, "op": e.op, **({"at": at} if at else {}),
                         **({"tributaries": e.tributaries} if e.tributaries else {})})
        row: dict = {"extents": exts}
        if m.effective and m.effective.from_:
            row["from"] = m.effective.from_
        if m.effective and m.effective.to:
            row["to"] = m.effective.to
        out[rid] = row
    if missing:
        raise ValueError("; ".join(missing))
    return {"format": FORMAT, "digest": digest, "matches": out}


def release_dir(arg: Path | None) -> Path:
    from pipeline.common.curated import GENERATED
    if arg:
        return Path(arg)
    root = Path(GENERATED.release)
    return root / (root / "LATEST").read_text(encoding="utf-8").strip()


def _sqlite(gz: Path, tmp: Path):
    import gzip
    import sqlite3
    out = tmp / gz.name.replace(".gz", "")
    out.write_bytes(gzip.decompress(gz.read_bytes()))
    return sqlite3.connect(str(out))


def compile_places(doc: M.Matches, rel: Path, log=print) -> tuple[str, dict[str, dict]] | None:
    """{match id: {item_id: {parts, some, all}}} on release `rel`, or None (atlas / release disagree)."""
    import json as _json
    import tempfile
    from pipeline.atlas.reach import position
    from pipeline.atlas.registry import load_registry, regions
    from pipeline.common import section_handles as H
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    from pipeline.regs.inseason import points as P
    build = GENERATED.require_build()
    manifest = _json.loads((rel / "manifest.json").read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory() as t:
        core = _sqlite(next(rel.glob("core-*.sqlite.gz")), Path(t))
        water = _sqlite(next(rel.glob("water-*.sqlite.gz")), Path(t))
        meta = dict(core.execute("SELECT k, v FROM meta").fetchall())
        if meta.get("section_handles") != H.digest_for(build):
            log(f"places skipped: release {rel.name} handles {meta.get('section_handles')} != atlas {H.digest_for(build)}")
            return None
        reg = load_registry(str(build / "registry.json"))
        g = read_artifact(str(build / "graph.pkl"))
        regions.attach_from(build, g, reg)
        position.attach(g, build)
        handle = H.read(build)[1]
        ord_of = dict(core.execute("SELECT item_id, ord FROM item").fetchall())
        id_of = {o: i for i, o in ord_of.items()}
        part_of: dict[int, dict[int, int]] = {}
        for o, sid, ix in water.execute("SELECT ord, sid, part_ix FROM part_section"):
            part_of.setdefault(o, {})[sid] = ix
        out: dict[str, dict] = {}
        for rid in sorted(doc.matches):
            secs: set[int] = set()
            for e in doc.matches[rid].extents:
                try:
                    got = P.resolve(reg, g, e.model_dump(exclude_none=True))
                except P.Unresolved as ex:
                    log(f"{rid}: {e.op} {e.item_id} unresolved on the atlas: {ex}")
                    continue
                secs |= {handle[s] for s in got["sections"] if s in handle}
            places: dict[str, dict] = {}
            for o, members in part_of.items():
                hit = {sid for sid in members if sid in secs}
                if not hit:
                    continue
                per: dict[int, list[int]] = {}
                for sid, ix in members.items():
                    per.setdefault(ix, [0, 0])[0] += 1
                    if sid in hit:
                        per[ix][1] += 1
                parts = sorted(ix for ix, (n, h) in per.items() if h)
                some = sorted(ix for ix, (n, h) in per.items() if 0 < h < n)
                places[id_of[o]] = {"parts": parts, **({"some": some} if some else {}),
                                    "all": len(hit) == len(members)}
            out[rid] = {k: places[k] for k in sorted(places)}
        return str(manifest.get("data_release") or rel.name), out


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--matches", type=Path, default=None, help="the curated file (default CURATED.inseason.matches)")
    ap.add_argument("--feed", type=Path, default=None, help="a captured feed index.json to copy beside it (dev)")
    ap.add_argument("--release", type=Path, default=None, help="the shipped release to name parts in (default LATEST)")
    ap.add_argument("--no-places", action="store_true", help="labels only: no atlas load, no parts")
    a = ap.parse_args(argv)
    src = a.matches or M.default_path()
    text = Path(src).read_text(encoding="utf-8")
    doc = M.load(src)
    links = compile_links(doc, M.split_labels(), hashlib.sha256(text.encode("utf-8")).hexdigest()[:16])
    if not a.no_places:
        got = compile_places(doc, release_dir(a.release))
        if got:
            links["release"] = got[0]
            for rid, places in got[1].items():
                if places and rid in links["matches"]:
                    links["matches"][rid]["places"] = places
    out = a.out or (GENERATED.inseason / "feed")
    out.mkdir(parents=True, exist_ok=True)
    (out / "matches.json").write_text(json.dumps(links, ensure_ascii=False, separators=(",", ":")) + "\n",
                                      encoding="utf-8")
    if a.feed:
        shutil.copyfile(a.feed, out / "index.json")
    print(f"in-season links: {len(links['matches'])} matched row(s) -> {out / 'matches.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
