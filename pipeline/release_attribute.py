"""A BINDING DIFF THE BATCH MEANT (T993 item 8, T992): is every change in a release's binding diff explained by a
binding input the batch changed since the live release was built?

    python -m pipeline.release_attribute <binding_diff.json> <since commit>

0.8.5 stopped on "Lost Lagoon +1": T981 had added that water's local-closure entry on purpose, and the only way on was
reading the diff and `--accept-diff`. `pipeline release` now asks this first: when EVERY change is explained the diff
passes with a printed note naming each change's input, file and commits; anything else still stops, as before (W46:
the gate is not loosened for an unexplained change). CONSERVATIVE BY DESIGN (two reviews, 2026-11-21):

  * NOTHING is auto-accepted when the binding CODE moved (a file the reach stage imported, `build-manifest.json`
    `stages.reach.code_files`, or anything under pipeline/atlas/) or the ATLAS did (the two runs' `handles` /
    `registry_digest` differ) since the live release: `release.binding_moved` says why and the diff stops.
  * A change is explained only by ITS OWN binding input, and only in the direction that input implies (a change that
    both gains and loses sections is never explained):
      - the rule (or licensing record) itself added (gains only) or removed (loses only);
      - its BINDING fields edited (`RULE_BINDING_KEYS`): extents only added (gains only) or only removed (loses only).
        Wording, dates, limits are not binding: such an edit explains nothing;
      - its entry's `matched` waters only added (gains only) or only removed (loses only);
      - a row of a NAME / MATCH file the reach reads (`binding_row_files`: name variants, found names, continuations,
        unmapped waters, added streams, the book-name override links) naming one of those matched waters, only added
        (gains only) or only removed (loses only). A file that merely mentions the water explains nothing; a cut moves
        sections both ways and is the atlas (rebuilt, so the atlas gate above stops it).
  * A DFO place (`dfo` key = its location id) only by a changed row of the curated DFO places naming it, and only when
    its outcome stayed and it moved one way. Unresolved, diagnostic and tidal / outside changes are never explained.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from pipeline.common.curated import CURATED, REPO_ROOT

#: the fields of a rule that decide where it binds (everything else — words, dates, limits, species — does not)
RULE_BINDING_KEYS = ("extents", "includes_tributaries", "water", "scope")


def _rel(p) -> str:
    return str(Path(p).resolve().relative_to(Path(REPO_ROOT).resolve()))


def binding_row_files() -> set[str]:
    """(review 2026-11-21) the NAME / MATCH files the reach reads whose rows decide which water an entry binds."""
    w, r = CURATED.waters, CURATED.regulations
    return {_rel(p) for p in (w.name_variants, w.found_names, w.continuations, w.unmapped_waters, w.added_streams,
                              r.book_name_links)}


def dfo_places_root() -> str:
    return _rel(CURATED.dfo_salmon.places)


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout


def _at(rev: str, path: str):
    """The JSON of `path` at `rev`; None when it is not there (added or removed in between)."""
    got = subprocess.run(["git", "show", f"{rev}:{path}"], cwd=REPO_ROOT, capture_output=True, text=True)
    if got.returncode:
        return None
    return json.loads(got.stdout)


def curated_changes(since: str, rev: str = "HEAD") -> list[str]:
    """Every curated JSON path a commit after `since` (up to `rev`) touched, repo-relative, sorted."""
    base = str(Path(CURATED.base).resolve().relative_to(Path(REPO_ROOT).resolve()))
    return sorted(p for p in _git("diff", "--name-only", since, rev, "--", base).splitlines() if p.endswith(".json"))


def _strings(o, out: set[str]) -> set[str]:
    if isinstance(o, str):
        out.add(o)
    elif isinstance(o, dict):
        for k, v in o.items():
            out.add(k)
            _strings(v, out)
    elif isinstance(o, list):
        for v in o:
            _strings(v, out)
    return out


def _holds_rows(v) -> bool:
    return isinstance(v, dict) or (isinstance(v, list) and any(isinstance(x, (dict, list)) for x in v))


def _rows(o, key: str = "", top: bool = True) -> list[tuple[str, str]]:
    """(the key it sits under, canonical JSON) for every ROW of a curated file: the DEEPEST dicts (a dict none of whose
    values holds a dict or a list of them: one cut, one variant, one call), each under the dict key it sits at, if any.
    A flat top-level mapping is one row per key. So a changed row names only what IT carries — never every water of
    the file (a container is never a row)."""
    out = []
    if isinstance(o, dict):
        if not any(_holds_rows(v) for v in o.values()):
            if top:
                return [(k, json.dumps({k: v}, sort_keys=True)) for k, v in o.items()]
            return [(key, json.dumps(o, sort_keys=True))]
        for k, v in o.items():
            out += _rows(v, k, False)
    elif isinstance(o, list):
        for v in o:
            out += _rows(v, key, False)
    return out


def _entries(doc) -> dict[str, dict] | None:
    if isinstance(doc, dict) and isinstance(doc.get("entries"), list):
        return {e["entry_id"]: e for e in doc["entries"] if isinstance(e, dict) and e.get("entry_id")}
    return None




def _entry_files(rev: str) -> list[str]:
    """Every curated `region-*.json` at `rev` (the entry sources live in such files)."""
    base = str(Path(CURATED.base).resolve().relative_to(Path(REPO_ROOT).resolve()))
    return sorted(p for p in _git("ls-tree", "-r", "--name-only", rev, "--", base).splitlines()
                  if Path(p).name.startswith("region-") and p.endswith(".json"))


def changed_rows(since: str, rev: str = "HEAD") -> dict:
    """What the curated changes between `since` and `rev` are, for `attribute`:
      entries  {entry_id: (file, the entry at `since` or None, at `rev` or None)} — every entry whose content differs
      rows     [(file, the names the row carries, "+" added / "-" removed)] — rows of the binding NAME / MATCH files
               (`binding_row_files`) and of the DFO places only; a row of any other file explains nothing
      files    {file: [the commits that touched it]}
      heads    {entry_id: the entry at `rev`} — every entry of every entry source"""
    entries: dict[str, tuple] = {}
    rows: list[tuple[str, frozenset, str]] = []
    files: dict[str, list[str]] = {}
    binding, dfo = binding_row_files(), dfo_places_root()
    for p in curated_changes(since, rev):
        before, after = _at(since, p), _at(rev, p)
        files[p] = _git("log", "--format=%h", f"{since}..{rev}", "--", p).split()
        eb, ea = _entries(before), _entries(after)
        if eb is not None or ea is not None:
            eb, ea = eb or {}, ea or {}
            for eid in sorted(set(eb) | set(ea)):
                if _canon(eb.get(eid)) != _canon(ea.get(eid)):
                    entries[eid] = (p, eb.get(eid), ea.get(eid))
            if not p.startswith(dfo.rstrip("/") + "/"):
                continue
        if p not in binding and not p.startswith(dfo.rstrip("/") + "/"):
            continue
        rb, ra = set(_rows(before) if before is not None else []), set(_rows(after) if after is not None else [])
        for sign, got in (("+", ra - rb), ("-", rb - ra)):
            for where, row in sorted(got):
                rows.append((p, frozenset(_strings(json.loads(row), {where} if where else set())), sign))
    heads: dict[str, dict] = {}
    for p in _entry_files(rev):
        heads.update(_entries(_at(rev, p)) or {})
    return {"entries": entries, "rows": rows, "files": files, "heads": heads}


def _canon(o) -> str:
    return json.dumps(o, sort_keys=True)


def _own(entry: dict | None, own: str):
    """The rule (`rule_id`) or licensing record (`record_id`) `own` of an entry, wherever it sits in it; None."""
    if not isinstance(entry, (dict, list)):
        return None
    stack = [entry]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            if own in (o.get("rule_id"), o.get("record_id")):
                return o
            stack.extend(v for v in o.values() if isinstance(v, (dict, list)))
        elif isinstance(o, list):
            stack.extend(v for v in o if isinstance(v, (dict, list)))
    return None


def _one_way(grew: bool, shrank: bool) -> str | None:
    """"+" when an input only grew, "-" when it only shrank, None when it moved both ways (direction unknowable)."""
    return "+" if grew and not shrank else "-" if shrank and not grew else None


def _direction_ok(way: str | None, c: dict) -> tuple[bool, str]:
    gained, lost = int(c.get("gained") or 0), int(c.get("lost") or 0)
    if "gained" not in c or "lost" not in c:
        return False, "the change has no sections to check a direction on"
    if gained and lost:
        return False, f"its binding moved both ways (+{gained} -{lost})"
    if way is None:
        return False, "its input moved both ways, so no direction can be checked"
    if (way == "+" and lost) or (way == "-" and gained):
        return False, (f"its input {'only grew' if way == '+' else 'only shrank'}, yet the binding "
                       f"{'LOST' if lost else 'GAINED'} {lost or gained} section(s)")
    return True, ""


def _input_change(e: str, own: str, ch: dict) -> tuple[str, str | None, str] | None:
    """The rule's own binding input that changed in its entry: (what, its direction, file) or None. The rule itself
    added / removed, its binding fields (`RULE_BINDING_KEYS`) edited, or its entry's matched waters."""
    if e not in ch["entries"]:
        return None
    f, b, a = ch["entries"][e]
    ob, oa = _own(b, own), _own(a, own)
    if ob is None and oa is not None:
        return f"rule {own} added", "+", f
    if oa is None and ob is not None:
        return f"rule {own} removed", "-", f
    if ob is not None:
        pb = {k: ob.get(k) for k in RULE_BINDING_KEYS}
        pa = {k: oa.get(k) for k in RULE_BINDING_KEYS}
        if pb != pa:
            if any(pb[k] != pa[k] for k in RULE_BINDING_KEYS if k != "extents"):
                return f"rule {own}'s binding fields edited", None, f
            xb = {_canon(x) for x in pb["extents"] or ()}
            xa = {_canon(x) for x in pa["extents"] or ()}
            return f"rule {own}'s extents edited", _one_way(bool(xa - xb), bool(xb - xa)), f
    mb = set((b or {}).get("matched") or ())
    ma = set((a or {}).get("matched") or ())
    if mb != ma:
        return f"entry {e}'s matched waters changed ({', '.join(sorted(mb ^ ma)[:3])})", _one_way(bool(ma - mb),
                                                                                                bool(mb - ma)), f
    return None


def attribute(rep: dict, since: str, rev: str = "HEAD", changes: dict | None = None) -> tuple[bool, list[str], list[str]]:
    """(every change explained?, one line per explained change, one line per change not explained) — the rules in the
    module's docstring. The code / atlas gate is the caller's (`release.binding_moved`), asked first. `changes`
    defaults to `changed_rows`."""
    ch = changed_rows(since, rev) if changes is None else changes

    def commits(f: str) -> str:
        return ", ".join(ch["files"].get(f, [])[:3]) or "commit ?"

    ok, bad = [], []
    for kind, key in [("unresolved", k) for k in rep.get("unresolved") or {}] \
            + [("diagnostic", k) for k in rep.get("diagnostics") or {}]:
        bad.append(f"{kind} {key}: an unresolved / diagnostic change is never auto-accepted")
    for kind, key, c in [("rule", k, v) for k, v in (rep.get("rules") or {}).items()] \
            + [("licensing", k, v) for k, v in (rep.get("licensing") or {}).items()]:
        c = c if isinstance(c, dict) else {}
        e, _, rest = key.partition("::")
        own = rest.split(" ", 1)[0]
        got = _input_change(e, own, ch)
        if got:
            what, way, f = got
            fine, why = _direction_ok(way, c)
            (ok if fine else bad).append(f"{kind} {key}: {what} in {f} ({commits(f)})" if fine
                                         else f"{kind} {key}: {what} in {f}, but {why}")
            continue
        head = ch["heads"].get(e) or (ch["entries"].get(e) or (None, None, None))[2] or {}
        waters = {m for m in head.get("matched") or () if isinstance(m, str)}
        hits = [(f, sorted(waters & names)[0], sign) for f, names, sign in ch["rows"]
                if waters & names and not f.startswith(dfo_root(ch))]
        if not hits:
            bad.append(f"{kind} {key}: none of its own binding inputs (the rule's binding fields, its entry's matched "
                       f"waters, a name / match row naming them) changed since {since[:12]}")
            continue
        signs = {s for _f, _w, s in hits}
        fine, why = _direction_ok(_one_way("+" in signs, "-" in signs), c)
        f, water, _s = hits[0]
        (ok if fine else bad).append(f"{kind} {key}: a row of {f} naming {water} changed ({commits(f)})" if fine
                                     else f"{kind} {key}: a row of {f} naming {water} changed, but {why}")
    for loc, c in (rep.get("dfo") or {}).items():
        hits = [s for f, names, s in ch["rows"] if loc in names and f.startswith(dfo_root(ch))]
        outcome = (c or {}).get("outcome") or [None, None]
        if not hits:
            bad.append(f"dfo {loc}: no curated DFO place row changed since {since[:12]} names it")
        elif outcome[0] != outcome[1]:
            bad.append(f"dfo {loc}: its outcome changed ({outcome[0]} -> {outcome[1]}): never auto-accepted")
        else:
            fine, why = _direction_ok(_one_way("+" in hits, "-" in hits), c or {})
            (ok if fine else bad).append(f"dfo {loc}: a curated DFO place row naming it changed" if fine
                                         else f"dfo {loc}: a DFO place row naming it changed, but {why}")
    for name in rep.get("side") or {}:
        bad.append(f"{name}: the tidal / outside sets moved (never attributable to a row)")
    return not bad, ok, bad


def dfo_root(ch: dict) -> str:
    return ch.get("dfo_root") or (dfo_places_root().rstrip("/") + "/")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("diff", type=Path)
    ap.add_argument("since")
    a = ap.parse_args(argv)
    all_ok, ok, bad = attribute(json.loads(a.diff.read_text(encoding="utf-8")), a.since)
    for x in ok:
        print(f"  explained: {x}")
    for x in bad:
        print(f"  NOT explained: {x}", file=sys.stderr)
    print(f"{len(ok)} explained, {len(bad)} not (the code / atlas gate is `pipeline release`'s, not asked here)")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
