"""Which rule bindings differ between two reach runs ON DISK — streamed, so a 19 GB run fits.

    python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full data/generated/reaches/full_new
    python -m pipeline.atlas.reach.binding_diff BEFORE AFTER --json /tmp/binding_diff.json

The gate before promoting a reach rerun (memory: "split fixes change the trib walk" — diff the rule
bindings, not just atlas parity). `reach.cli --against` answers the same question but reads the old
run whole into memory (`io.read_run`), which a 19 GB `rule_section.jsonl` does not survive; this
walks both files side by side instead, holding one row of each at a time. Either run may be in the
T73 form (`rule_section.jsonl.gz`) or the old one: an old-vs-converted diff is the T73 proof.

Compared, per (entry, rule) or (entry, record):
  * `rule_section`        — sections gained / lost, and sections whose `scope` changed (reach ↔ trib)
  * `rule_unresolved`     — a rule that became, or stopped being, unresolved (or changed its reason)
  * `licensing_section`   — the same as `rule_section`, for the licensing records
  * `dfo_salmon.jsonl.gz` — each DFO location's sections and outcome (T546 item 8: placed by the same
                            `build_reach`, outside the rules' digest, so nothing else saw a moved place)
  * `tidal.jsonl`, `outside_bc.jsonl` — the sections taken out of every binding
  * the diagnostics a curator must see: each `unclassified` piece (and its reason) and each `ambiguous_cut`
    pick, from `rule_diagnostic.jsonl` and `dfo_salmon_diagnostic.jsonl`
And STANDING (the after run's report, not a change): pieces withheld (`unplaced`), rows whose scope does not
resolve, curated override ids no registry item carries — printed every time, so a known gap is never quiet.
A side table one run lacks (a baseline from before it existed) is listed under `not_compared`, never skipped
unseen.

Both runs are written by `io.write_run`, sorted by (entry_id, rule_id|record_id, section_id); a file
out of that order is refused rather than misread. Exit status: 0 when nothing differs, 1 when
something does, 2 on a bad input.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

#: How many example section ids each changed rule keeps (the counts are always exact).
SAMPLE = 5


@dataclass
class Change:
    gained: int = 0
    lost: int = 0
    rescoped: int = 0
    sample_gained: list[str] = field(default_factory=list)
    sample_lost: list[str] = field(default_factory=list)

    @property
    def blast(self) -> int:
        return self.gained + self.lost + self.rescoped

    def as_dict(self) -> dict:
        return {"gained": self.gained, "lost": self.lost, "rescoped": self.rescoped,
                "sample_gained": self.sample_gained, "sample_lost": self.sample_lost}


def _rows(path: Path, owner: str) -> Iterator[tuple[tuple[str, str, str], str]]:
    """((entry_id, owner_id, section_id), scope) for each row, checking the sort order. The rule
    bindings are read through `io.iter_rule_sections`, so either form (T73's gzip, or a pre-T73 run's
    plain JSONL) diffs against either."""
    if owner == "rule_id":
        from pipeline.atlas.reach.io import iter_rule_sections
        for e, r, s, scope in iter_rule_sections(path):
            yield (e, r, s), scope
        return
    last = None
    with path.open(encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            if not line.strip():
                continue
            r = json.loads(line)
            key = (r["entry_id"], r[owner], r["section_id"])
            if last is not None and key <= last:
                raise ValueError(f"{path}:{n} is out of order ({key} after {last}) — not a "
                                 f"`write_run` table")
            last = key
            yield key, r.get("scope", "")


def diff_sections(before: Path, after: Path, owner: str = "rule_id") -> dict[tuple[str, str], Change]:
    """Merge-walk two sorted section tables; return the changes keyed by (entry_id, owner_id)."""
    out: dict[tuple[str, str], Change] = {}
    a, b = _rows(before, owner), _rows(after, owner)
    ra, rb = next(a, None), next(b, None)
    while ra is not None or rb is not None:
        if rb is None or (ra is not None and ra[0] < rb[0]):
            c = out.setdefault(ra[0][:2], Change())
            c.lost += 1
            if len(c.sample_lost) < SAMPLE:
                c.sample_lost.append(ra[0][2])
            ra = next(a, None)
        elif ra is None or rb[0] < ra[0]:
            c = out.setdefault(rb[0][:2], Change())
            c.gained += 1
            if len(c.sample_gained) < SAMPLE:
                c.sample_gained.append(rb[0][2])
            rb = next(b, None)
        else:
            if ra[1] != rb[1]:
                out.setdefault(ra[0][:2], Change()).rescoped += 1
            ra, rb = next(a, None), next(b, None)
    return out


def _unresolved(path: Path) -> dict[tuple[str, str], str]:
    out: dict[tuple[str, str], str] = {}
    if not path.exists():
        return out
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                r = json.loads(line)
                out[(r["entry_id"], r["rule_id"])] = r.get("reason") or ""
    return out


#: the side tables compared whole (small): name -> (file, key fields, value field or None)
SIDE_TABLES = {"tidal": ("tidal.jsonl", ("section_id",), "entry_id"),
               "outside_bc": ("outside_bc.jsonl", ("section_id",), None)}
DFO_FILE, DFO_DIAG_FILE, RULE_DIAG_FILE = "dfo_salmon.jsonl.gz", "dfo_salmon_diagnostic.jsonl", "rule_diagnostic.jsonl"


def _jsonl(path: Path) -> Iterator[dict]:
    import gzip
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def _dfo(path: Path) -> dict[str, tuple[str, frozenset]]:
    return {r["location_id"]: (r.get("outcome") or "", frozenset(r.get("sections") or ())) for r in _jsonl(path)}


def _diag_keys(path: Path) -> dict[str, str]:
    """`unclassified` pieces and `ambiguous_cut` picks as {key: value}: what changing them would change for a curator."""
    out: dict[str, str] = {}
    for r in _jsonl(path):
        pay = r.get("payload") or {}
        if isinstance(pay, str):
            pay = json.loads(pay)
        who = (f"dfo:{r['location_id']}" if r.get("location_id")
               else f"{r.get('entry_id')}::{r.get('rule_id') or '-'}")
        if r.get("kind") in ("unclassified", "scope_unclassified"):
            for piece in pay.get("pieces") or ():
                out[f"{who} unplaced {piece}"] = str((pay.get("reasons") or {}).get(piece, "unknown"))
        elif r.get("kind") == "ambiguous_cut":
            out[f"{who} ambiguous_cut {pay.get('split_id')}@{pay.get('extent')}"] = str(pay.get("used"))
    return out


def _changed(b: dict, a: dict) -> dict[str, dict]:
    return {k: {"before": b.get(k), "after": a.get(k)} for k in sorted(set(b) | set(a)) if b.get(k) != a.get(k)}


def _standing(run: Path) -> dict:
    rep = run / "report.json"
    if not rep.exists():
        return {}
    r = json.loads(rep.read_text(encoding="utf-8"))
    return {"unplaced": len(r.get("unplaced") or ()),
            "scope_unresolved": list(r.get("scope_unresolved") or ()),
            "override_id_gaps": [f"{x['name']} {'/'.join(x.get('mu') or ())}: {len(x['unresolved'])} id(s), "
                                 f"{x['resolved']} resolved" for x in r.get("override_id_gaps") or ()]}


def _side_diffs(before: Path, after: Path) -> dict:
    """T546 item 8: DFO placements, the tidal / outside sets and the curator's diagnostics."""
    out: dict = {"dfo": {}, "side": {}, "diagnostics": {}, "not_compared": []}
    if (before / DFO_FILE).exists() and (after / DFO_FILE).exists():
        db, da = _dfo(before / DFO_FILE), _dfo(after / DFO_FILE)
        for loc in sorted(set(db) | set(da)):
            (ob, sb), (oa, sa) = db.get(loc, (None, frozenset())), da.get(loc, (None, frozenset()))
            if (ob, sb) != (oa, sa):
                out["dfo"][loc] = {"outcome": [ob, oa], "gained": len(sa - sb), "lost": len(sb - sa),
                                   "sample_gained": sorted(sa - sb)[:SAMPLE], "sample_lost": sorted(sb - sa)[:SAMPLE]}
    else:
        out["not_compared"].append(DFO_FILE)
    for name, (fn, keys, val) in SIDE_TABLES.items():
        if (before / fn).exists() and (after / fn).exists():
            rows = [{"|".join(str(r[k]) for k in keys): str(r.get(val)) if val else "" for r in _jsonl(d / fn)}
                    for d in (before, after)]
            ch = _changed(*rows)
            if ch:
                out["side"][name] = {"changed": len(ch), "sample": dict(list(ch.items())[:SAMPLE])}
        else:
            out["not_compared"].append(fn)
    for fn in (RULE_DIAG_FILE, DFO_DIAG_FILE):
        if (before / fn).exists() and (after / fn).exists():
            out["diagnostics"].update(_changed(_diag_keys(before / fn), _diag_keys(after / fn)))
        else:
            out["not_compared"].append(fn)
    return out


def differs(rep: dict) -> bool:
    """Does this diff hold a change the release must stop on (`release.stage_checks`, the CLI's exit status)?"""
    return bool(rep["rules_changed"] or rep["unresolved_changed"] or rep["licensing_changed"]
                or rep.get("dfo_changed") or rep.get("side_changed") or rep.get("diagnostics_changed"))


def diff_runs(before: str | Path, after: str | Path) -> dict:
    """The whole comparison, as a JSON-able dict."""
    from pipeline.atlas.reach.io import rule_section_file
    before, after = Path(before), Path(after)
    files = []
    for d in (before, after):
        try:
            files.append(rule_section_file(d))
        except SystemExit as e:
            raise FileNotFoundError(f"{d}: not a reach run ({e})") from None
    rules = diff_sections(*files)
    ub, ua = _unresolved(before / "rule_unresolved.jsonl"), _unresolved(after / "rule_unresolved.jsonl")
    unresolved = {f"{e}::{r}": {"before": ub.get((e, r)), "after": ua.get((e, r))}
                  for (e, r) in sorted(set(ub) | set(ua)) if ub.get((e, r)) != ua.get((e, r))}
    lic: dict = {}
    lb, la = before / "licensing_section.jsonl", after / "licensing_section.jsonl"
    if lb.exists() and la.exists():
        lic = diff_sections(lb, la, owner="record_id")

    def _ranked(changes):
        return {f"{e}::{r}": c.as_dict()
                for (e, r), c in sorted(changes.items(), key=lambda kv: (-kv[1].blast, kv[0]))}
    side = _side_diffs(before, after)
    return {"before": str(before), "after": str(after),
            "rules_changed": len(rules), "unresolved_changed": len(unresolved),
            "licensing_changed": len(lic),
            "dfo_changed": len(side["dfo"]), "side_changed": len(side["side"]),
            "diagnostics_changed": len(side["diagnostics"]),
            "rules": _ranked(rules), "unresolved": unresolved, "licensing": _ranked(lic),
            "dfo": side["dfo"], "side": side["side"], "diagnostics": side["diagnostics"],
            "not_compared": side["not_compared"], "standing": _standing(after)}


def _standing_lines(rep: dict) -> list[str]:
    st, out = rep.get("standing") or {}, []
    if rep.get("not_compared"):
        out.append(f"  NOT COMPARED (absent from a run): {', '.join(rep['not_compared'])}")
    if st.get("unplaced"):
        out.append(f"  standing: {st['unplaced']} piece(s) withheld from their rule (report.json `unplaced`)")
    if st.get("scope_unresolved"):
        out.append(f"  standing: scope unresolved: {', '.join(st['scope_unresolved'])}")
    for x in st.get("override_id_gaps") or ():
        out.append(f"  standing: override ids in no registry item: {x}")
    return out


def summary(rep: dict, top: int = 40) -> str:
    if not differs(rep):
        return "\n".join([f"no binding changed ({rep['before']} → {rep['after']})", *_standing_lines(rep)])
    lines = [f"{rep['rules_changed']:,} rule(s) bind different sections · "
             f"{rep['unresolved_changed']:,} unresolved change(s) · "
             f"{rep['licensing_changed']:,} licensing record(s) moved · "
             f"{rep.get('dfo_changed', 0):,} DFO place(s) moved · "
             f"{rep.get('side_changed', 0):,} tidal/outside table(s) changed · "
             f"{rep.get('diagnostics_changed', 0):,} diagnostic(s) changed", *_standing_lines(rep), ""]
    for k, c in list(rep["rules"].items())[:top]:
        lines.append(f"  {k}  +{c['gained']:,} -{c['lost']:,}"
                     + (f" rescoped {c['rescoped']:,}" if c["rescoped"] else ""))
    if len(rep["rules"]) > top:
        lines.append(f"  … {len(rep['rules']) - top:,} more")
    for k, v in rep["unresolved"].items():
        lines.append(f"  {k}  unresolved: {v['before']} → {v['after']}")
    for k, c in list(rep["licensing"].items())[:top]:
        lines.append(f"  licensing {k}  +{c['gained']:,} -{c['lost']:,}")
    for k, c in list((rep.get("dfo") or {}).items())[:top]:
        lines.append(f"  dfo {k}  +{c['gained']:,} -{c['lost']:,}"
                     + (f" outcome {c['outcome'][0]} → {c['outcome'][1]}" if c["outcome"][0] != c["outcome"][1] else ""))
    for k, c in (rep.get("side") or {}).items():
        lines.append(f"  {k}: {c['changed']:,} section(s) changed")
    for k, v in list((rep.get("diagnostics") or {}).items())[:top]:
        lines.append(f"  {k}: {v['before']} → {v['after']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("before", help="the promoted reach run (e.g. data/generated/reaches/full)")
    ap.add_argument("after", help="the candidate run")
    ap.add_argument("--json", help="write the full comparison here")
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args(argv)
    try:
        rep = diff_runs(args.before, args.after)
    except (FileNotFoundError, ValueError) as e:
        print(f"binding_diff: {e}", file=sys.stderr)
        return 2
    print(summary(rep, args.top))
    if args.json:
        Path(args.json).write_text(json.dumps(rep, indent=1) + "\n", encoding="utf-8")
    return 1 if differs(rep) else 0


if __name__ == "__main__":
    sys.exit(main())
