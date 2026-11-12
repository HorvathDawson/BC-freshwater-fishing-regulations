"""ITEM IDS GONE FROM THE LIVE RELEASE WITH NO ALIAS (T555 item 5: the release check).

    python -m pipeline.deliver.release.gone_ids data/release/<before> data/release/<after>

A reader keeps item ids on the device: a spot's water, a gauge alert's water, a shared link. The app follows
`core.item_alias` (old id -> the id it became), so an id a build ABSORBED still opens its water. An id that is simply
gone — in the release before, not an item of the new one, and no alias row naming it — opens nothing: the spot keeps
its pin and name but loses its water. This lists every such id, with the name the release before gave it, so the list
is read before the release ships. It LISTS and never refuses: ids do go between builds (AGENTS 5: 99.88 % durable),
and whether one should have had an alias is a judgement about that water. The fix for one that should is where the id
was absorbed (the registry's `aliases`), then a rebuild.

Reads only the two release folders (each one's manifest names its core file). A core cut before `item_alias` has no
aliases: every id gone from it is listed.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
import tempfile
from pathlib import Path


def core_ids(release_dir: Path) -> tuple[dict[str, str], dict[str, str]]:
    """(item_id -> name, alias -> item_id) of a release folder's core file. Raises naming the file when it is not there."""
    release_dir = Path(release_dir)
    mpath = release_dir / "manifest.json"
    if not mpath.is_file():
        raise FileNotFoundError(f"{mpath} is missing: {release_dir} is not a release folder")
    core = json.loads(mpath.read_text(encoding="utf-8"))["files"].get("core")
    if not core:
        raise ValueError(f"{mpath} names no core file")
    gz = release_dir.parent / core["url"]
    if not gz.is_file():
        raise FileNotFoundError(f"{gz} is missing (named by {mpath})")
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "core.sqlite"
        db.write_bytes(gzip.decompress(gz.read_bytes()))
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            items = {str(i): str(n or "") for i, n in con.execute("SELECT item_id, name FROM item")}
            has = con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'item_alias'").fetchone()
            alias = {str(a): str(i) for a, i in con.execute("SELECT alias, item_id FROM item_alias")} if has else {}
        finally:
            con.close()
    return items, alias


def gone(before: tuple[dict[str, str], dict[str, str]], after: tuple[dict[str, str], dict[str, str]]) -> dict:
    """What happened to every item id of `before` in `after`: kept, followed by an alias to an item `after` holds, or gone."""
    b_items, _ = before
    a_items, a_alias = after
    missing = sorted(i for i in b_items if i not in a_items)
    followed = {i: a_alias[i] for i in missing if a_alias.get(i) in a_items}
    return {"before": len(b_items), "kept": len(b_items) - len(missing),
            "aliased": [{"id": i, "now": followed[i], "name": b_items[i]} for i in missing if i in followed],
            "gone": [{"id": i, "name": b_items[i],
                      **({"alias_to_missing": a_alias[i]} if i in a_alias else {})} for i in missing if i not in followed]}


def summary(rep: dict, before: str, after: str) -> str:
    head = (f"item ids, {before} -> {after}: {rep['kept']} of {rep['before']} kept, {len(rep['aliased'])} absorbed "
            f"(followed by item_alias), {len(rep['gone'])} gone with no alias")
    rows = [f"  gone  {g['id']}  {g['name'] or '(unnamed)'}"
            + (f"  (item_alias names {g['alias_to_missing']}, which is not an item)" if "alias_to_missing" in g else "")
            for g in rep["gone"]]
    return "\n".join([head, *rows])


def compare(before_dir: Path, after_dir: Path) -> dict:
    return gone(core_ids(before_dir), core_ids(after_dir))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("before", type=Path)
    ap.add_argument("after", type=Path)
    ap.add_argument("--json", type=Path, help="also write the full report here")
    a = ap.parse_args(argv)
    rep = compare(a.before, a.after)
    if a.json:
        a.json.write_text(json.dumps(rep, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(summary(rep, a.before.name, a.after.name))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
