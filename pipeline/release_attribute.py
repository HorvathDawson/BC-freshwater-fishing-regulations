"""A BINDING DIFF THE BATCH MEANT (T993 item 8, T992): is every change in a release's binding diff explained by a curated
row changed in the commits since the live release was built?

    python -m pipeline.release_attribute <binding_diff.json> <since commit>

0.8.5 stopped on "Lost Lagoon +1": T981 had added that water's local-closure entry on purpose, and the only way on was
reading the diff and `--accept-diff`. `pipeline release` now asks this first: when EVERY change is attributable the
diff passes with a printed note naming each change's row, file and commits; anything else — a change no committed row
explains — still stops, as before (W46: the gate is not loosened for an unexplained change).

What explains a change — ONLY that change's own curated input (review 2026-11-21: an entry-wide or name-wide match let
a code or atlas move through):
  * the rule's (or licensing record's) OWN object (`<entry_id>::<rule>`) added, removed or edited in any entry source
    (every curated `region-*.json` with an `entries` list), with the direction checked: an added rule only gains
    sections, a removed one only loses them. A sibling rule's edit in the same entry explains nothing;
  * the entry's `matched` waters changed (what every rule of it binds through), direction checked the same way;
  * a changed leaf ROW of any other curated JSON file naming one of the entry's matched ids or a cut id the rule
    itself names (its `split*` values) — never one naming only the entry id;
  * a DFO place (`dfo` key = its location id) named by a changed curated row.
The tidal / outside side tables are never attributable (they move with the atlas, not a row). When in doubt: a stop.
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


def _entry_files(rev: str) -> list[str]:
    """Every curated `region-*.json` at `rev` (the entry sources and the DFO places live in such files)."""
    base = str(Path(CURATED.base).resolve().relative_to(Path(REPO_ROOT).resolve()))
    return sorted(p for p in _git("ls-tree", "-r", "--name-only", rev, "--", base).splitlines()
                  if Path(p).name.startswith("region-") and p.endswith(".json"))


def changed_rows(since: str, rev: str = "HEAD") -> dict:
    """What the curated changes between `since` and `rev` are, for `attribute`:
      entries  {entry_id: (file, the entry at `since` or None, at `rev` or None)} — every entry whose content differs
      rows     [(file, the names a changed non-entry ROW carries)]
      files    {file: [the commits that touched it]}
      heads    {entry_id: the entry at `rev`} — every entry of every entry source (its matched ids and the cuts its
               rules name: what a changed row must name to explain one of its rules)"""
    entries: dict[str, tuple] = {}
    rows: list[tuple[str, frozenset]] = []
    files: dict[str, list[str]] = {}
    for p in curated_changes(since, rev):
        before, after = _at(since, p), _at(rev, p)
        files[p] = _git("log", "--format=%h", f"{since}..{rev}", "--", p).split()
        eb, ea = _entries(before), _entries(after)
        if eb is not None or ea is not None:
            eb, ea = eb or {}, ea or {}
            for eid in sorted(set(eb) | set(ea)):
                if _canon(eb.get(eid)) != _canon(ea.get(eid)):
                    entries[eid] = (p, eb.get(eid), ea.get(eid))
            continue
        rb, ra = _rows(before) if before is not None else [], _rows(after) if after is not None else []
        for where, row in sorted(set(rb) ^ set(ra)):
            rows.append((p, frozenset(_strings(json.loads(row), {where} if where else set()))))
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


def _cut_ids(o, out: set[str]) -> set[str]:
    """Every cut id a rule names (the values under any `split*` key: `splits`, `split_id`, …)."""
    if isinstance(o, dict):
        for k, v in o.items():
            if "split" in k:
                out |= {v} if isinstance(v, str) else {x for x in v if isinstance(x, str)} if isinstance(v, list) else set()
            _cut_ids(v, out)
    elif isinstance(o, list):
        for v in o:
            _cut_ids(v, out)
    return out


def _by_entry(e: str, own: str, c: dict, ch: dict) -> tuple[bool | None, str]:
    """Is the change explained by the rule's OWN entry edit? (True, why) / (False, why: an edit that contradicts the
    binding's direction) / (None, ""): this rule's own curated input did not change there. Only THIS rule's own
    object (added, removed or edited) or the entry's `matched` waters (what every rule of it binds through) count —
    an edit to a sibling rule of the same entry never explains another rule's move."""
    if e not in ch["entries"]:
        return None, ""
    f, b, a = ch["entries"][e]
    gained, lost = int(c.get("gained") or 0), int(c.get("lost") or 0)
    ob, oa = _own(b, own), _own(a, own)
    if _canon(ob) != _canon(oa):
        if ob is None and lost:
            return False, f"rule {own} was added in {f}, yet its binding LOST {lost} section(s)"
        if oa is None and gained:
            return False, f"rule {own} was removed in {f}, yet its binding GAINED {gained} section(s)"
        return True, f"rule {own} {'added' if ob is None else 'removed' if oa is None else 'edited'} in {f}"
    mb = set((b or {}).get("matched") or ())
    ma = set((a or {}).get("matched") or ())
    if mb != ma:
        if ma > mb and lost:
            return False, f"entry {e} only gained matched waters in {f}, yet the binding LOST {lost} section(s)"
        if ma < mb and gained:
            return False, f"entry {e} only lost matched waters in {f}, yet the binding GAINED {gained} section(s)"
        return True, f"entry {e}'s matched waters changed in {f} ({', '.join(sorted(mb ^ ma)[:3])})"
    return None, ""


def attribute(rep: dict, since: str, rev: str = "HEAD", changes: dict | None = None) -> tuple[bool, list[str], list[str]]:
    """(every change explained?, one line per explained change, one line per change nothing explains). A change is
    explained only by ITS OWN curated input changed since `since`: the rule's own object in its entry (added / removed
    / edited, with the binding's direction checked where it can be: an added rule only gains, a removed one only
    loses), the entry's matched waters (same check), or a changed row of another curated file naming one of those
    waters or a cut the rule names. Nothing else — a sibling rule's edit, a row naming only the entry, a code or atlas
    move — explains it: when in doubt it stops. `changes` defaults to `changed_rows`."""
    ch = changed_rows(since, rev) if changes is None else changes

    def commits(f: str) -> str:
        return ", ".join(ch["files"].get(f, [])[:3]) or "commit ?"

    ok, bad = [], []
    keyed = [("rule", k, v) for k, v in (rep.get("rules") or {}).items()] \
        + [("unresolved", k, {}) for k in rep.get("unresolved") or {}] \
        + [("licensing", k, v) for k, v in (rep.get("licensing") or {}).items()] \
        + [("diagnostic", k, {}) for k in rep.get("diagnostics") or {} if not k.startswith("dfo:")]
    for kind, key, c in keyed:
        e, _, rest = key.partition("::")
        own = rest.split(" ", 1)[0]
        got, why = _by_entry(e, own, c if isinstance(c, dict) else {}, ch)
        if got:
            ok.append(f"{kind} {key}: {why} ({commits(ch['entries'][e][0])})")
            continue
        if got is False:
            bad.append(f"{kind} {key}: {why}")
            continue
        head = ch["heads"].get(e) or (ch["entries"].get(e) or (None, None, None))[2] or {}
        names = {m for m in head.get("matched") or () if isinstance(m, str)} | _cut_ids(_own(head, own), set())
        hit = next(((f, sorted(names & carried)[0]) for f, carried in ch["rows"] if names & carried), None)
        if hit:
            ok.append(f"{kind} {key}: a row of {hit[0]} naming {hit[1]} (a water or cut it binds through) changed "
                      f"({commits(hit[0])})")
        else:
            bad.append(f"{kind} {key}: none of its own curated inputs (the rule, its entry's matched waters, the cuts "
                       f"it names) changed since {since[:12]}")
    dfo_keys = list(rep.get("dfo") or {}) + [k.split(" ", 1)[0][len("dfo:"):] for k in rep.get("diagnostics") or {}
                                              if k.startswith("dfo:")]
    for loc in dfo_keys:
        hit = next((f for f, carried in ch["rows"] if loc in carried), None)
        if hit:
            ok.append(f"dfo {loc}: a row of {hit} naming it changed ({commits(hit)})")
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
