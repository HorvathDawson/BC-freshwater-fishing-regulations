"""A BINDING DIFF THE BATCH MEANT (T993 item 8, T992): is every change in a release's binding diff explained by a curated
row changed in the commits since the live release was built?

    python -m pipeline.release_attribute <binding_diff.json> <since commit>

0.8.5 stopped on "Lost Lagoon +1": T981 had added that water's local-closure entry on purpose, and the only way on was
reading the diff and `--accept-diff`. `pipeline release` now asks this first: when EVERY change is attributable the
diff passes with a printed note naming each change's row, file and commits; anything else — a change no committed row
explains — still stops, as before (W46: the gate is not loosened for an unexplained change).

What explains a change (nothing is inferred from a name or a count):
  * a rule's, licensing record's or unresolved row's ENTRY (`<entry_id>::<rule>`) whose content differs between the two
    commits in any entry source (every curated `region-*.json` with an `entries` list: the synopsis, local closures, …),
    added and removed entries included;
  * a changed ROW of any other curated JSON file (an element of a list, or a top-level key's value) that NAMES the
    entry id, or one of the entry's `matched` ids (an added lake, a name variant), as one of its string values;
  * a DFO place (`dfo` key = its location id) named by a changed curated row the same way.
The tidal / outside side tables are never attributable (they move with the atlas, not a row).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from pipeline.common.curated import CURATED, REPO_ROOT


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


def changed_rows(since: str, rev: str = "HEAD") -> dict:
    """{"entries": {entry_id: file}, "rows": [(file, the names a changed row carries)], "files": {file: [commits]},
    "entry_matched": {entry_id: [matched ids]}} for the curated changes between `since` and `rev`."""
    entries: dict[str, str] = {}
    rows: list[tuple[str, frozenset]] = []
    matched: dict[str, list[str]] = {}
    files: dict[str, list[str]] = {}
    for p in curated_changes(since, rev):
        before, after = _at(since, p), _at(rev, p)
        files[p] = _git("log", "--format=%h", f"{since}..{rev}", "--", p).split()
        eb, ea = _entries(before), _entries(after)
        if eb is not None or ea is not None:
            eb, ea = eb or {}, ea or {}
            for eid in sorted(set(eb) | set(ea)):
                if json.dumps(eb.get(eid), sort_keys=True) != json.dumps(ea.get(eid), sort_keys=True):
                    entries[eid] = p
            for eid, e in ea.items():
                matched[eid] = [m for m in e.get("matched") or () if isinstance(m, str)]
            continue
        rb, ra = _rows(before) if before is not None else [], _rows(after) if after is not None else []
        for where, row in sorted(set(rb) ^ set(ra)):
            rows.append((p, frozenset(_strings(json.loads(row), {where} if where else set()))))
    return {"entries": entries, "rows": rows, "files": files, "entry_matched": matched}


def _entry_of(key: str) -> str:
    return key.split("::", 1)[0].split(" ", 1)[0]


def attribute(rep: dict, since: str, rev: str = "HEAD", changes: dict | None = None,
              matched_of=None) -> tuple[bool, list[str], list[str]]:
    """(every change explained?, one line per explained change, one line per change nothing explains). `matched_of`
    (entry id -> its matched ids) defaults to the entries as they are at `rev`; `changes` to `changed_rows`."""
    ch = changed_rows(since, rev) if changes is None else changes
    matched = ch.get("entry_matched", {}) if matched_of is None else matched_of

    def commits(f: str) -> str:
        return ", ".join(ch["files"].get(f, [])[:3]) or "commit ?"

    ok, bad = [], []
    keyed = [("rule", k) for k in rep.get("rules") or {}] + [("unresolved", k) for k in rep.get("unresolved") or {}] \
        + [("licensing", k) for k in rep.get("licensing") or {}] \
        + [("diagnostic", k) for k in rep.get("diagnostics") or {} if not k.startswith("dfo:")]
    for kind, key in keyed:
        e = _entry_of(key)
        if e in ch["entries"]:
            f = ch["entries"][e]
            ok.append(f"{kind} {key}: entry {e} changed in {f} ({commits(f)})")
            continue
        names = {e, *matched.get(e, ())}
        hit = next(((f, sorted(names & carried)[0]) for f, carried in ch["rows"] if names & carried), None)
        if hit:
            ok.append(f"{kind} {key}: a row of {hit[0]} naming {hit[1]} changed ({commits(hit[0])})")
        else:
            bad.append(f"{kind} {key}: no curated row changed since {since[:12]} names entry {e}")
    dfo_keys = list(rep.get("dfo") or {}) + [k.split(" ", 1)[0][len("dfo:"):] for k in rep.get("diagnostics") or {}
                                              if k.startswith("dfo:")]
    for loc in dfo_keys:
        hit = next(((f, loc) for f, carried in ch["rows"] if loc in carried), None)
        if hit:
            ok.append(f"dfo {loc}: a row of {hit[0]} naming it changed ({commits(hit[0])})")
        else:
            bad.append(f"dfo {loc}: no curated row changed since {since[:12]} names it")
    for name in rep.get("side") or {}:
        bad.append(f"{name}: the tidal / outside sets moved (never attributable to a row)")
    return not bad, ok, bad


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
    print(f"{len(ok)} explained, {len(bad)} not")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
