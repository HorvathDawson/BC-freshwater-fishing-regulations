"""WHAT WAITS ON A REBUILD (T541): every review entry and split whose build is not the one on disk, why, and
the command(s) the human runs — the review app's pending-build panel (`GET /api/build`), readable with no server.

    python -m pipeline.regs.review.pending [--json]

ONE implementation: `pending` is the decision (it asks `vintage.entry_state` / `vintage.split_state`, nowhere
else); the review app's `reuse.pending_build` hands it the app's cached reads, this CLI reads the live tree
(`on_disk`). Reads only; never runs a build.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable, Iterable, Mapping, Optional


def anchors_now(e: Mapping, ctx) -> dict[str, str]:
    """split id -> its anchor digest in splits.json NOW, for every split the entry's binding reads
    (`fingerprint.entry_split_ids`). A named id splits.json does not have is the gate's to refuse."""
    from pipeline.atlas.splits.splits import anchor_digest
    from pipeline.regs.review import fingerprint as F
    idx = ctx.split_index
    return {sid: anchor_digest(idx[sid]) for sid in sorted(F.entry_split_ids(e, ctx)) if sid in idx}


def pending(entries: Iterable[tuple[str, Mapping]], ctx, built, target,
            *, anchors_for: Optional[Callable[[Mapping], Mapping[str, str]]] = None) -> dict:
    """THE PENDING BUILD: {built, entries, splits, splits_checked, commands} — empty lists when the vintage on disk
    is the one the corpus and splits.json describe. `entries`: (region, entry) as `io.read_entryfile` reads them;
    `ctx`: a `fingerprint.Context` (splits.json + registry); `built`: ONE `vintage.Built` every row is decided on
    (a second read could see a rebuild that landed mid-call); `target`: the `vintage.Target` the commands rebuild;
    `anchors_for`: the caller's memoised `anchors_now` (the app's), default computed here."""
    from pipeline.atlas.splits.splits import anchor_digest
    from pipeline.regs.parsing import io
    from pipeline.regs.review import vintage as V
    anchors_for = anchors_for or (lambda e: anchors_now(e, ctx))
    rows, reasons = [], set()
    for region, e in entries:
        st = V.entry_state(e["entry_id"], io.entry_digest(e), anchors_for(e), built, target)
        if st["state"] != V.CURRENT:
            rows.append({"entry_id": e["entry_id"], "region": region, "name": e.get("name", ""),
                         "reasons": st["reasons"], "splits": st["splits"]})
            reasons.update(st["reasons"])
    splits = []
    idx = ctx.split_index
    for sid in sorted(idx):
        st = V.split_state(sid, anchor_digest(idx[sid]), built, target)
        if st["state"] != V.CURRENT:
            splits.append({"split_id": sid, "reasons": st["reasons"]})
            reasons.update(st["reasons"])
    return {"built": built.summary(), "entries": rows, "splits": splits,
            "splits_checked": "anchors" if built.anchors is not None else "ids",
            "commands": V.rebuild_commands(reasons, target)}


def pending_on_disk(target=None) -> dict:
    """`pending` over the live tree (default `vintage.Target.canonical()`, the paths the review app serves when no
    `CURATION_*` copy is set): the catalogue region files read with the served registry, splits.json, and the
    vintage on disk."""
    from pipeline.atlas.registry import load_registry
    from pipeline.regs.parsing import io
    from pipeline.regs.review import fingerprint as F
    from pipeline.regs.review import vintage as V
    t = target or V.Target.canonical()
    registry = load_registry(Path(t.build) / "registry.json")
    entries = [(r, e) for r in io.region_ids(Path(t.entries))
               for e in io.read_entryfile(Path(t.entries) / f"region-{r}.json", registry=registry).values()]
    ctx = F.Context(splits=json.loads(Path(t.splits).read_text(encoding="utf-8")), registry=registry)
    built = V.read_built(Path(t.build), Path(t.reach), Path(t.bundle), Path(t.release))
    return pending(entries, ctx, built, t)


def summary_line(got: Mapping) -> str:
    """One line: how many entries and splits wait on a rebuild."""
    ne, ns = len(got["entries"]), len(got["splits"])
    if not ne and not ns:
        return "nothing waits on a rebuild: the build on disk is the corpus and splits.json as they are"
    return f"{ne} review entr{'y' if ne == 1 else 'ies'} and {ns} split(s) wait on a rebuild"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.pending", description=__doc__.split("\n")[0])
    ap.add_argument("--json", action="store_true", help="print the whole result as JSON")
    a = ap.parse_args(argv)
    got = pending_on_disk()
    if a.json:
        json.dump(got, sys.stdout, indent=1, ensure_ascii=False)
        print()
        return 0
    print(summary_line(got))
    for e in got["entries"]:
        print(f"  entry {e['entry_id']}  (region {e['region']}, {e['name']}): {'; '.join(e['reasons'])}"
              + (f"  [splits {', '.join(e['splits'])}]" if e["splits"] else ""))
    for s in got["splits"]:
        print(f"  split {s['split_id']}: {'; '.join(s['reasons'])}")
    if got["commands"]:
        print("\nthe rebuild (the human runs it; this prints, never runs):")
        for c in got["commands"]:
            print(f"  {c}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
