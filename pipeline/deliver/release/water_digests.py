"""(T1030) THE PER-WATER DIGEST LIST, AND THE CHANGED-WATERS COUNT THE MANIFEST STATES FROM IT.

    python -m pipeline.deliver.release.water_digests <release dir> --out FILE           # a release's list, to FILE
    python -m pipeline.deliver.release.water_digests --count <before list> <after list>  # the count between two lists

(The build writes a release's own list into its folder and names it in its manifest, `attach`; this command never
writes into a release folder — that would move the folder's fingerprint, `pipeline.release.fingerprint`.)

The notifications inbox (T867) says "N waters changed" on a data release. N is `manifest.changed_waters`: the number of
waters whose resolved rules differ from the release this one replaces. A build cannot rely on an older release being on
disk (they are pruned to live + previous), and decoding an older release with newer code is not always possible, so
EVERY RELEASE WRITES ITS OWN LIST and the next build counts against its predecessor's:

    <release>/water-digests.json.gz    gzip (mtime 0) of canonical JSON (sorted keys, no clock: AGENTS 19)
        {"format": "water-digests/1", "basis": {"scheme": SCHEME, "regs": {<section>: <version>}, "salmon": <format>},
         "waters": {"<item_id>": "<16 hex>", ...}}

KEYED ON THE DURABLE `item_id` (AGENTS 5) — never a section id or a handle, which do not survive a rebuild (AGENTS 6) — and
read from the SHIPPED files only (the regs store put back together from the release, `store.release.province_store`, and
the salmon shards, through `answers_diff.Release`, the reference decoder the changed-answers list uses), so the list is
a pure function of the release's files.

WHAT A WATER'S DIGEST COVERS (`digests_of`): every water the app shows (`display.waters`), part by part in order:
  * where its parts split: each part's end tokens, whether it is closed all year, the rules asking for a paper licence
    (rule ids) — never its length or its sections (geometry; a re-cut with the same answers is not a change);
  * the part's regs answer, every day: its answer key's steelhead / kind / tidal facts, its day segments and moments, and
    each segment's frames (ladder, rows, gear, licence, display, the trout table) decoded with their rule and licensing
    refs as ids (`answers_diff.net_value`, its derived year strips left out); adjacent segments with the same frames are
    one;
  * the DFO salmon on its sections: the distinct chains, each as its per-species verdicts by day, the place answering
    them (a DFO location id, or `book:<rule id>`), its gear lines and its words;
  * the water's unresolved licensing (licensing ids).
So "changed" = ANY of that differs, wording included, or the water is new or gone (`changed`). Words change rarely
between releases; a count that hid a wording change would under-say what an angler reads.

THE COUNT IS UNKNOWN — the manifest states NO `changed_waters`, never a guessed 0 — when the predecessor wrote no list
(the first release with this file, or a predecessor folder that is gone), or when the two lists' `basis` differ (a codec
version or this file's scheme changed: the same water would digest differently). The build log says which.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

FORMAT = "water-digests/1"
FILENAME = "water-digests.json.gz"
#: the trout cell's keys whose every int is a rule ref (`trout.table`: a row's and its sheet's `src`)
TROUT_RULE_KEYS = frozenset({"src"})
#: bump when what `digests_of` covers changes: two lists of different schemes are never compared (`count`)
SCHEME = 1


class DigestError(SystemExit):
    """A list that exists but cannot be read: refused, never read as "no list" (W46)."""


def _canon(v) -> str:
    return json.dumps(v, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _h(v) -> str:
    return hashlib.sha256(_canon(v).encode("utf-8")).hexdigest()[:16]


def law(v, text_keys: frozenset):
    """`v` without its words: every dict key in `text_keys` (the changed-answers list's `TEXT_KEYS`, the leaves it
    calls "wording only") left out, at any depth."""
    if isinstance(v, dict):
        return {k: law(x, text_keys) for k, x in v.items() if k not in text_keys}
    if isinstance(v, list):
        return [law(x, text_keys) for x in v]
    return v


def water_digests(release_dir: Path) -> dict:
    """The list of the release at `release_dir` (module doc), from its shipped files."""
    from pipeline.tools.audit import answers_diff as AD
    rel = AD.Release(Path(release_dir))
    return digests_of(rel, part_sections(rel))


def part_sections(rel) -> Dict[Tuple[str, int], list]:
    """{(item_id, part_ix): [section handle, ...]} from the release's water file (`part_section`), every membership
    (a section two waters share is in both)."""
    import sqlite3
    import tempfile
    from pipeline.tools.audit.answers_diff import _file_bytes
    out: Dict[Tuple[str, int], list] = {}
    raw = _file_bytes(rel.dir, rel.manifest, "water")
    with tempfile.TemporaryDirectory(prefix="water-digests-") as tmp:
        p = Path(tmp) / "water.sqlite"
        p.write_bytes(raw)
        con = sqlite3.connect(str(p))
        try:
            for o, sid, pix in con.execute("SELECT ord, sid, part_ix FROM part_section ORDER BY ord, part_ix, sid"):
                out.setdefault((rel.item_id[o], pix), []).append(sid)
        finally:
            con.close()
    return out


def digests_of(rel, sections_of: Dict[Tuple[str, int], list]) -> dict:
    """The list from a decoded release (`answers_diff.Release`) and each part's sections."""
    from pipeline.tools.audit import answers_diff as AD
    wire = rel.wire
    shown = wire["sections"]["display"]["waters"]
    frame_memo: Dict[tuple, str] = {}
    key_memo: Dict[int, str] = {}
    chain_memo: Dict[int, str] = {}

    def lic(v):                                         # licensing refs -> licensing ids
        return AD._map_refs(v, rel.lid)

    def rules(v):                                       # rule refs -> rule ids (every int: a list of rules)
        return AD._all_refs(v, rel.rid)

    def frame(name: str, ref) -> str:
        hit = frame_memo.get((name, ref))
        if hit is None:
            if name == "trout":
                # the trout cell's rule refs are its rows' `src` lists (its other ints are days and row indexes)
                v = None if ref is None else AD.resolve(
                    rel.codecs["trout"].decode_frame(rel.sections["trout"], ref, rel.rule_ids, rel.fish),
                    frozenset(), frozenset(), rel.rid, None, all_refs=TROUT_RULE_KEYS)
            else:
                v = AD.net_value(rel, name, ref, True)
            hit = frame_memo[(name, ref)] = _h(law(v, AD.TEXT_KEYS))
        return hit

    def key(k: int) -> str:
        hit = key_memo.get(k)
        if hit is None:
            meta = wire["keys"][k]
            starts, moms = rel.key_meta(k)
            n = len(rel.sections["ladder"]["at"][k])
            cells = [[frame(name, ref) for name, ref in zip((*AD.SECTION_NAMES, "trout"), rel.refs(k, s))]
                     for s in range(n)]
            if moms is None and len(cells) == len(starts):
                # one segment per start: adjacent segments with the same frames are one answer (merged)
                segs = []
                for d, c in zip(starts, cells):
                    if not segs or segs[-1][1] != c:
                        segs.append((d, c))
                segs = [list(x) for x in segs]
            else:
                segs = {"starts": starts, "moments": moms, "cells": cells}
            hit = key_memo[k] = _h({"facts": meta[2:9], "days": segs})
        return hit

    def chain(c: int) -> str:
        hit = chain_memo.get(c)
        if hit is None:
            sal = rel.salmon
            ch = sal["chains"][c]
            v, g = sal["verdicts"], sal["gears"]
            sp = [[[r[i], v[r[i + 1]]] for i in range(0, len(r), 2)] for r in ch["sp"]]
            src = [[[r[i], AD._src(ch, k, r[i])] for i in range(0, len(r), 2)] for k, r in enumerate(ch.get("a") or [])]
            gear = [[ch["g"][i], g[ch["g"][i + 1]] if ch["g"][i + 1] >= 0 else []] for i in range(0, len(ch["g"]), 2)]
            extra = {x: y for x, y in ch.items() if x not in AD.CHAIN_DERIVED}
            hit = chain_memo[c] = _h(law({"sp": sp, "src": src, "gear": gear, "extra": extra}, AD.TEXT_KEYS))
        return hit

    out: Dict[str, str] = {}
    for wid, w in sorted(shown.items()):
        akeys = wire["parts"].get(wid, [])
        parts = []
        for pix, p in enumerate(w["parts"]):
            k = akeys[pix] if pix < len(akeys) else None
            chains = sorted({chain(rel.chain_of.get(h, rel.default_chain)) for h in sections_of.get((wid, pix), ())})
            parts.append(None if p is None else {
                "lo": p["lo"], "hi": p["hi"], "closed_all_year": p["closed_all_year"],
                "paper_licence": rules(p["paper_licence"]), "regs": None if k is None else key(k), "salmon": chains})
        out[wid] = _h({"parts": parts, "unresolved_licensing": lic(w["unresolved_licensing"])})
    return {"format": FORMAT, "basis": basis(rel), "waters": out}


def basis(rel) -> dict:
    """What two lists must share to be compared: this file's scheme, the regs codec versions, the salmon format."""
    return {"scheme": SCHEME, "regs": dict(sorted(rel.versions.items())),
            "salmon": rel.manifest["files"]["salmon"]["format"]}


def raw_bytes(lst: dict) -> bytes:
    return (_canon(lst) + "\n").encode("utf-8")


def encode(lst: dict) -> bytes:
    return gzip.compress(raw_bytes(lst), compresslevel=9, mtime=0)


def write(lst: dict, path: Path) -> Path:
    path = Path(path)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_bytes(encode(lst))
    tmp.replace(path)
    return path


def _parse(raw: bytes, where) -> dict:
    try:
        lst = json.loads(raw.decode("utf-8"))
    except ValueError as e:
        raise DigestError(f"water digests: {where} is not a JSON list ({e})")
    if not isinstance(lst, dict) or lst.get("format") != FORMAT or not isinstance(lst.get("waters"), dict):
        raise DigestError(f"water digests: {where} is not a {FORMAT} list")
    return lst


def read(path: Path) -> Optional[dict]:
    """The list at `path` (a file `write` wrote), or None when there is no file there. A file that is not a list of this
    format is refused."""
    path = Path(path)
    if not path.is_file():
        return None
    try:
        raw = gzip.decompress(path.read_bytes())
    except OSError as e:
        raise DigestError(f"water digests: {path} is not gzip ({e})")
    return _parse(raw, path)


def release_list(release_dir: Path) -> Optional[dict]:
    """The list a release folder's manifest names (`water_digests`), held to its sha256; None when it names none (a
    release built before T1030). Named but missing or not those bytes: refused (W46), never read as "no list"."""
    d = Path(release_dir)
    m = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
    e = m.get(MANIFEST_KEY)
    if e is None:
        return None
    p = d.parent / e["url"]
    fix = "rebuild that release (its build writes its list: python -m pipeline.deliver all)"
    if not p.is_file():
        raise DigestError(f"water digests: {d.name}'s manifest names {e['url']}, which is missing — {fix}")
    try:
        raw = gzip.decompress(p.read_bytes())
    except OSError as e2:
        raise DigestError(f"water digests: {p} is not gzip ({e2}) — {fix}")
    if hashlib.sha256(raw).hexdigest() != e["sha256"]:
        raise DigestError(f"water digests: {p} is not the list {d.name}'s manifest names (sha256) — {fix}")
    return _parse(raw, p)


#: the manifest's block naming the list, and the count (both outside the release id: `rebase.said_of` names neither)
MANIFEST_KEY = "water_digests"
COUNT_KEY = "changed_waters"


def previous_state(previous: Optional[Path]) -> dict:
    """What the build needs of the release it replaces, read BEFORE the build writes anything: its id, its list (None:
    `why`), and its own count (kept when the build turns out to be that same release again)."""
    if previous is None:
        return {"name": None, "id": None, "list": None, "count": None,
                "why": "no previous release was named (deliver all names the one LATEST names)"}
    d = Path(previous)
    if not (d / "manifest.json").is_file():
        return {"name": d.name, "id": None, "list": None, "count": None,
                "why": f"the previous release {d.name} is not on disk ({d})"}
    m = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
    lst = release_list(d)
    return {"name": d.name, "id": m.get("data_release"), "list": lst, "count": m.get(COUNT_KEY),
            "why": None if lst is not None else f"the previous release {d.name} wrote no list (built before T1030)"}


def attach(final: Path, before: dict, say: Callable[[str], None] = print) -> Optional[int]:
    """Write `final`'s own list beside its files, and name it and the count (`count`) in its manifest; return the count.

    The release id is not touched: it was minted from the files and what `rebase.said_of` reads, and these two keys are
    neither (a count depends on the predecessor, so the same content cut after two different releases is one id). A
    rebuild of the release `before` IS (the same id: LATEST naming the release being built again) keeps that release's
    own count — against itself every water would read unchanged."""
    final = Path(final)
    mp = final / "manifest.json"
    m = json.loads(mp.read_text(encoding="utf-8"))
    rid = m["data_release"]
    lst = water_digests(final)
    raw = raw_bytes(lst)
    gz = gzip.compress(raw, compresslevel=9, mtime=0)
    (final / FILENAME).write_bytes(gz)
    m[MANIFEST_KEY] = {"url": f"{rid}/{FILENAME}", "bytes": len(raw), "gz": len(gz),
                       "sha256": hashlib.sha256(raw).hexdigest(), "format": FORMAT, "waters": len(lst["waters"])}
    if before.get("id") == rid:
        n = before.get("count")
        say(f"changed_waters: {rid} is the release it replaces, built again — its own count is kept "
            f"({'none' if n is None else n})")
    else:
        n = count(before.get("list"), lst, say, before_name=before.get("name") or "the previous release",
                  why=before.get("why"))
    if n is None:
        m.pop(COUNT_KEY, None)
    else:
        m[COUNT_KEY] = n
    mp.write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")       # the build's own format (build.ship)
    return n


def changed(before: dict, after: dict) -> list:
    """The item ids whose digest differs, or that are in one list only (a water added or removed), sorted."""
    a, b = before["waters"], after["waters"]
    return sorted(w for w in set(a) | set(b) if a.get(w) != b.get(w))


def count(before: Optional[dict], after: dict, say: Callable[[str], None] = print,
          before_name: str = "the previous release", why: Optional[str] = None) -> Optional[int]:
    """`manifest.changed_waters`: the number of `changed` waters, or None (UNKNOWN, said through `say`) when there is
    no list before (`why`) or the two lists' `basis` differ."""
    if before is None:
        say(f"changed_waters: unknown — {why or f'{before_name} has no list'}; the manifest states no count")
        return None
    if before.get("basis") != after.get("basis"):
        say(f"changed_waters: unknown — {before_name}'s list is of another basis ({before.get('basis')} vs "
            f"{after.get('basis')}); the manifest states no count")
        return None
    n = len(changed(before, after))
    say(f"changed_waters: {n} of {len(after['waters'])} waters differ from {before_name}")
    return n


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.deliver.release.water_digests",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("release", type=Path, nargs="?", help="a release folder: write its list")
    ap.add_argument("--out", type=Path, default=None, help="where to write the release's list (never into a release)")
    ap.add_argument("--count", type=Path, nargs=2, metavar=("BEFORE", "AFTER"), help="count between two lists")
    ap.add_argument("--list", action="store_true", help="with --count: print the changed item ids too")
    a = ap.parse_args(argv)
    if a.count:
        before, after = read(a.count[0]), read(a.count[1])
        if after is None:
            raise DigestError(f"water digests: no list at {a.count[1]}")
        n = count(before, after, before_name=str(a.count[0]))
        if a.list and n is not None:
            print("\n".join(changed(before, after)))
        return 0
    if a.release is None or a.out is None:
        ap.error("name a release folder and --out FILE, or --count BEFORE AFTER")
    if (a.out.resolve().parent / "manifest.json").is_file():
        ap.error(f"--out {a.out} is inside a release folder: its build writes its own list (module doc)")
    out = write(water_digests(a.release), a.out)
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
