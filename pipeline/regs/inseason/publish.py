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
                if not lab:
                    missing.append(f"{rid}: split {s!r} has no label")
                at.append(lab)
            exts.append({"item": e.item_id, "op": e.op, **({"at": at} if at else {})})
        row: dict = {"extents": exts}
        if m.effective and m.effective.from_:
            row["from"] = m.effective.from_
        if m.effective and m.effective.to:
            row["to"] = m.effective.to
        out[rid] = row
    if missing:
        raise ValueError("; ".join(missing))
    return {"format": FORMAT, "digest": digest, "matches": out}


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--matches", type=Path, default=None, help="the curated file (default CURATED.inseason.matches)")
    ap.add_argument("--feed", type=Path, default=None, help="a captured feed index.json to copy beside it (dev)")
    a = ap.parse_args(argv)
    src = a.matches or M.default_path()
    text = Path(src).read_text(encoding="utf-8")
    doc = M.load(src)
    links = compile_links(doc, M.split_labels(), hashlib.sha256(text.encode("utf-8")).hexdigest()[:16])
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
