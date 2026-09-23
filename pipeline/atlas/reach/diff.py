"""What changed between two reach-builder runs.

The question this exists to answer is not "which items moved" — it is **"which rules now
mean something different?"** One routine rebuild changed content on 2,950 registry items;
without this, a rebuild is unreviewable.

Ranked by blast radius. (A change inside a `locked` entry used to be listed first; catalogue
entries have no `locked` — a re-parse is guarded by `ingested.json` instead — so that ranking
read a field no entry carries and is gone.)
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pipeline.atlas.reach.io import read_run


@dataclass
class RuleChange:
    entry_id: str
    rule_id: str
    kind: str                  # gained | lost | rebound | outcome
    before: str = ""
    after: str = ""
    added: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()

    @property
    def blast(self) -> int:
        return len(self.added) + len(self.removed)


@dataclass
class DiffReport:
    changes: list[RuleChange] = field(default_factory=list)
    n_before: int = 0
    n_after: int = 0

    def summary(self) -> str:
        if not self.changes:
            return f"no rule changed ({self.n_after:,} rules)"
        head = f"{len(self.changes):,} of {self.n_after:,} rules changed"
        lines = [head, ""]
        for c in self.changes[:40]:
            mark = "   "
            if c.kind == "outcome":
                lines.append(f"{mark}{c.entry_id}::{c.rule_id}  {c.before} -> {c.after}")
            else:
                bits = []
                if c.added:
                    bits.append(f"+{len(c.added)}")
                if c.removed:
                    bits.append(f"-{len(c.removed)}")
                lines.append(f"{mark}{c.entry_id}::{c.rule_id}  sections {' '.join(bits)}")
        if len(self.changes) > 40:
            lines.append(f"   … {len(self.changes) - 40:,} more")
        return "\n".join(lines)


def diff_runs(before_dir, after_result) -> DiffReport:
    """Compare a previous run on disk against a fresh in-memory result."""
    before = read_run(before_dir)

    after: dict[tuple[str, str], dict] = {}
    for b in after_result.bindings:
        after[(b.entry_id, b.rule_id)] = {
            "outcome": b.outcome.value, "sections": set(b.sections),
            "reason": b.reason.value if b.reason else None,
        }

    rep = DiffReport(n_before=len(before), n_after=len(after))
    for key in sorted(set(before) | set(after)):
        entry_id, rule_id = key
        was, now = before.get(key), after.get(key)

        if was is None:
            rep.changes.append(RuleChange(entry_id, rule_id, "outcome", "(absent)",
                                          now["outcome"]))
            continue
        if now is None:
            rep.changes.append(RuleChange(entry_id, rule_id, "outcome", was["outcome"],
                                          "(absent)"))
            continue
        if was["outcome"] != now["outcome"]:
            rep.changes.append(RuleChange(
                entry_id, rule_id, "outcome",
                f"{was['outcome']}({was['reason'] or ''})".rstrip("()"),
                f"{now['outcome']}({now['reason'] or ''})".rstrip("()"),
                added=tuple(sorted(now["sections"] - was["sections"])),
                removed=tuple(sorted(was["sections"] - now["sections"]))))
            continue
        added = now["sections"] - was["sections"]
        removed = was["sections"] - now["sections"]
        if added or removed:
            kind = "rebound" if (added and removed) else ("gained" if added else "lost")
            rep.changes.append(RuleChange(entry_id, rule_id, kind,
                                          added=tuple(sorted(added)),
                                          removed=tuple(sorted(removed))))

    rep.changes.sort(key=lambda c: (-c.blast, c.entry_id, c.rule_id))
    return rep
