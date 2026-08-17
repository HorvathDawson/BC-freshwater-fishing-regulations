"""Parse-context builder — the CONSTRAINED SELECTION menu handed to the parser for one waterbody.

The rebuilt parser never extracts geometry from prose. For each synopsis row it is given exactly one
registry item and the closed set of bindable cut-points on it (`item.boundaries`), and it must express
every rule's reach by *selecting* from that set (op + boundary ids) — or, when no boundary fits, record
the unbindable phrase in `unresolved_locators` and flag `needs_review`. This module turns a
`RegistryItem` into that menu and renders the per-entry user message; the stable instructions + worked
examples live in `PARSE_PROMPT.md`.

`within(area)` scopes (Garibaldi/reserve closures) are NOT auto-bound by the parser — they're a curation
step (DECISION 2026-08-16): an area reg is flagged `needs_review` and a curator adds the extent (matched
area for a blanket closure, or a `within(area)` extent for a system-scoped one), pointing at a catalog
`area_id`. So the parse menu carries only the item's own boundaries, never an area list.

    ctx = build_parse_context(item, raw_regs, region="5")
    allowed = ctx.bindable_ids                      # -> validate_entry_splits(entry, allowed)
    user_msg = render_user_message(ctx)             # embed after the PARSE_PROMPT.md system text
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pipeline.models import RegistryItem
from pipeline.parsing.species import prompt_menu

_PROMPT = Path(__file__).resolve().parent / "prompts" / "PARSE_PROMPT.md"


@dataclass(frozen=True)
class ParseContext:
    entry_id: str
    row_index: int = -1                               # synopsis global row index (the {index,entry} key)
    name: str = ""
    region: str = ""
    mus: tuple[str, ...] = ()
    item_id: str = ""
    item_kind: str = ""
    variants: tuple[str, ...] = ()
    boundaries: tuple[tuple[str, str, str], ...] = ()    # (id, label, kind) — the bindable cut-points
    raw_regs: str = ""

    @property
    def bindable_ids(self) -> set[str]:
        """The closed set an Extent.splits may reference — exactly the item's boundary ids. Pass to
        `validate_entry_splits(entry, ctx.bindable_ids)` at ingest."""
        return {bid for bid, _, _ in self.boundaries}


def build_parse_context(item: RegistryItem, raw_regs: str = "", entry_id: str = "",
                        region: str = "", row_index: int = -1) -> ParseContext:
    """Assemble the constrained menu for one item: its bindable boundaries + identity. Area `within`
    targets are intentionally excluded — area scoping is a curation step (see module docstring)."""
    return ParseContext(
        entry_id=entry_id or item.id,
        row_index=row_index,
        name=item.name,
        region=region,
        mus=item.mus,
        item_id=item.id,
        item_kind=item.kind,
        variants=item.variants,
        boundaries=tuple((b.id, b.label, b.kind) for b in item.boundaries),
        raw_regs=raw_regs,
    )


def load_system_prompt() -> str:
    """The stable parser instructions + worked examples (PARSE_PROMPT.md)."""
    return _PROMPT.read_text(encoding="utf-8")


def render_user_message(ctx: ParseContext) -> str:
    """The per-entry payload: identity, the closed boundary menu, `within` targets, species menu, and
    the verbatim regs. Deliberately terse — the parser selects from these, it does not invent ids."""
    lines: list[str] = []
    lines.append(f"## Waterbody: {ctx.name or '(unnamed)'}  [{ctx.item_id}, kind={ctx.item_kind}]")
    if ctx.region or ctx.mus:
        lines.append(f"Region {ctx.region or '?'} · MUs: {', '.join(ctx.mus) or '?'}")
    if ctx.variants:
        lines.append(f"Also known as: {', '.join(ctx.variants)}")
    lines.append("")

    lines.append("### Bindable boundaries (the ONLY ids an extent.splits may use)")
    if ctx.boundaries:
        for bid, label, kind in ctx.boundaries:
            lines.append(f"- `{bid}`  — {label}  [{kind}]")
    else:
        lines.append("- (none) — this item has no cut-points; only op:whole is bindable.")
    lines.append("")

    lines.append("### Species codes (leave rule.species empty = ALL species; else pick from these)")
    lines.append(prompt_menu())
    lines.append("")

    lines.append("### Regulations (regs_verbatim — copy exactly; every rule_text must be a substring)")
    lines.append(ctx.raw_regs.strip())
    return "\n".join(lines)


_BATCH_ENVELOPE = """\
---

# OUTPUT (STRICT)

Return ONLY a JSON array — one object per ITEM above, in this exact shape:

    [ { "index": <the item's index, unchanged>, "entry": { ...Entry... } }, ... ]

- Copy each item's `index` verbatim; it maps your result back to the row. Never renumber.
- `entry` is the full Entry object (see the schema + examples above), bound to THAT item's boundaries.
- Return one object for every item. Do not wrap in Markdown fences or add prose.

# YOU MAY RUN THE PROJECT'S PYTHON HELPERS

This is a coding-agent parse — use the repo's own functions to get it right instead of guessing:

    PYTHONPATH="$PWD" .venv/bin/python -c \\
      "from pipeline.parsing.species import resolve_species_phrase as r; print(r('char'))"   # -> ['SLV']

    # self-check ONE candidate entry against its item BEFORE you submit (validates Entry + split ids):
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.validate <batch.json> <candidate.json>

`resolve_species_phrase(text)` maps a species word to codes; `pipeline.parsing.validate` runs the exact
Entry validators + split-id check the ingest step will run, so you can iterate until it passes.
"""


def render_batch_prompt(contexts: list[ParseContext]) -> str:
    """A self-contained batch prompt: the stable rules/examples (PARSE_PROMPT.md), each item's
    constrained menu, then the `{index, entry}` output envelope + the note that the agent may run the
    repo's Python helpers (species resolution, self-validation)."""
    parts = [load_system_prompt(), "\n\n---\n\n# BATCH — parse EACH item below into its own Entry\n"]
    for ctx in contexts:
        parts.append(f"\n---\n## ITEM index={ctx.row_index}\n\n{render_user_message(ctx)}\n")
    parts.append(_BATCH_ENVELOPE)
    return "\n".join(parts)
