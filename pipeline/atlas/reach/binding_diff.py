"""Which rule bindings differ between two reach runs ON DISK — streamed, so a 19 GB run fits.

    python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full data/generated/reaches/full_new
    python -m pipeline.atlas.reach.binding_diff BEFORE AFTER --json /tmp/binding_diff.json

The gate before promoting a reach rerun (memory: "split fixes change the trib walk" — diff the rule
bindings, not just atlas parity). `reach.cli --against` answers the same question but reads the old
run whole into memory (`io.read_run`), which a 19 GB `rule_section.jsonl` does not survive; this
walks both files side by side instead, holding one row of each at a time.

Compared, per (entry, rule) or (entry, record):
  * `rule_section`        — sections gained / lost, and sections whose `scope` changed (reach ↔ trib)
  * `rule_unresolved`     — a rule that became, or stopped being, unresolved (or changed its reason)
  * `licensing_section`   — the same as `rule_section`, for the licensing records

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
    """((entry_id, owner_id, section_id), scope) for each row, checking the sort order."""
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


def diff_runs(before: str | Path, after: str | Path) -> dict:
    """The whole comparison, as a JSON-able dict."""
    before, after = Path(before), Path(after)
    for d in (before, after):
        if not (d / "rule_section.jsonl").exists():
            raise FileNotFoundError(f"{d} has no rule_section.jsonl — not a reach run")
    rules = diff_sections(before / "rule_section.jsonl", after / "rule_section.jsonl")
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
    return {"before": str(before), "after": str(after),
            "rules_changed": len(rules), "unresolved_changed": len(unresolved),
            "licensing_changed": len(lic),
            "rules": _ranked(rules), "unresolved": unresolved, "licensing": _ranked(lic)}


def summary(rep: dict, top: int = 40) -> str:
    if not (rep["rules_changed"] or rep["unresolved_changed"] or rep["licensing_changed"]):
        return f"no binding changed ({rep['before']} → {rep['after']})"
    lines = [f"{rep['rules_changed']:,} rule(s) bind different sections · "
             f"{rep['unresolved_changed']:,} unresolved change(s) · "
             f"{rep['licensing_changed']:,} licensing record(s) moved", ""]
    for k, c in list(rep["rules"].items())[:top]:
        lines.append(f"  {k}  +{c['gained']:,} -{c['lost']:,}"
                     + (f" rescoped {c['rescoped']:,}" if c["rescoped"] else ""))
    if len(rep["rules"]) > top:
        lines.append(f"  … {len(rep['rules']) - top:,} more")
    for k, v in rep["unresolved"].items():
        lines.append(f"  {k}  unresolved: {v['before']} → {v['after']}")
    for k, c in list(rep["licensing"].items())[:top]:
        lines.append(f"  licensing {k}  +{c['gained']:,} -{c['lost']:,}")
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
    return 1 if (rep["rules_changed"] or rep["unresolved_changed"] or rep["licensing_changed"]) else 0


if __name__ == "__main__":
    sys.exit(main())
