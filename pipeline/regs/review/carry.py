"""`carry_marks`: how a migration keeps a reviewer's work it did not change.

A normalisation pass, a schema migration or a split rename rewrites entries without changing what
they mean. The fingerprints cannot always see that (a renamed split id IS a different binding to
them), so the migration DECLARES what it left unchanged, and `carry_marks` re-stamps exactly
those marks with the fingerprints of the migrated form — appended as copies carrying
`carried_by`, never edited in place.

It re-stamps a mark only when ALL of these hold, and the mutation tests pin each one:
  * the migration declared the mark's target unchanged (`unchanged` / `renamed_splits`);
  * the mark HELD on the target before the migration (a stale mark stays stale — a migration
    never launders a change nobody reviewed);
  * the target still exists after it (a rule mark: its rule id too).
A mark whose fingerprints are already equal after the migration needs no copy. Everything else is
left alone, and the fingerprints decide what goes stale and why.
"""
from __future__ import annotations

from typing import Iterable, Mapping

from pipeline.regs.review import fingerprint as F
from pipeline.regs.review.marks import Mark, Prints, latest, stale_reasons


def prints_of(mark: Mark, entries: Mapping[str, dict], ctx: F.Context,
              target: str | None = None) -> Prints | None:
    """The prints of `mark`'s target (or `target`, a renamed one) in `entries` under `ctx`; None
    when it is not there."""
    tgt = target or mark.target
    try:
        if mark.scope == "split":
            return Prints.model_validate(F.split_fingerprints(tgt, ctx))
        e = entries.get(tgt)
        if e is None:
            return None
        return Prints.model_validate(F.entry_fingerprints(e, ctx, rule_id=mark.rule_id))
    except KeyError:
        return None


def carry_marks(before: Mapping[str, dict], after: Mapping[str, dict], *,
                marks: Iterable[Mark], unchanged: Iterable[str], by: str,
                ctx_before: F.Context, ctx_after: F.Context | None = None,
                renamed_splits: Mapping[str, str] | None = None) -> list[Mark]:
    """The re-stamped copies to APPEND for a migration from `before` to `after` (entry_id ->
    entry). `unchanged` is the entry ids (and split ids) the migration declares it left meaning
    the same; `renamed_splits` maps a renamed split's old id to its new one (its split mark moves
    with it). `by` names the migration. `ctx_after` defaults to `ctx_before` (a migration that
    did not touch splits.json, the overrides or the bundle)."""
    ctx_after = ctx_after or ctx_before
    renamed = dict(renamed_splits or {})
    declared = set(unchanged) | set(renamed)
    out: list[Mark] = []
    for m in latest(marks).values():
        if m.target not in declared:
            continue
        was = prints_of(m, before, ctx_before)
        if was is None or stale_reasons(m, was):
            continue                                     # a stale mark stays stale
        new_target = renamed.get(m.target, m.target) if m.scope == "split" else m.target
        now = prints_of(m, after, ctx_after, target=new_target)
        if now is None:
            continue
        if now == m.prints and new_target == m.target:
            continue                                     # nothing to re-stamp
        out.append(m.model_copy(update={"prints": now, "target": new_target, "carried_by": by}))
    return out
