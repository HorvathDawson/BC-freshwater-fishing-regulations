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
from pipeline.parsing.rows import symbols_include_tributaries
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
    also_item_ids: tuple[str, ...] = ()               # a combined override's OTHER items (see below)
    item_kind: str = ""
    variants: tuple[str, ...] = ()
    boundaries: tuple[tuple[str, str, str], ...] = ()    # (id, label, kind) — the bindable cut-points
    boundaries_by_item: tuple[tuple[str, tuple[str, ...]], ...] = ()   # item_id -> its own cut-point ids
    raw_regs: str = ""
    symbols: tuple[str, ...] = ()                        # synopsis row symbols (e.g. 'Incl. Tribs')
    review_hints: tuple[str, ...] = ()                    # prior reviewer findings (a REPASS re-parse)
    no_registry: bool = False                            # True: no registry match — split rules, bind nothing
    registry_note: str = ""                              # why (matcher status + reason), when no_registry

    @property
    def bindable_ids(self) -> set[str]:
        """The closed set an Extent.splits may reference — exactly the item's boundary ids. Pass to
        `validate_entry_splits(entry, ctx.bindable_ids)` at ingest."""
        return {bid for bid, _, _ in self.boundaries}


def build_parse_context(item: RegistryItem, raw_regs: str = "", entry_id: str = "",
                        region: str = "", row_index: int = -1, name: str = "",
                        symbols: tuple[str, ...] = (), review_hints: tuple[str, ...] = (),
                        also_items: tuple[RegistryItem, ...] = ()) -> ParseContext:
    """Assemble the constrained menu for one item: its bindable boundaries + identity. Area `within`
    targets are intentionally excluded — area scoping is a curation step (see module docstring).

    `name` overrides the displayed identity name — used when several synopsis rows share one registry
    item (reach splits like "Elk River (downstream of Elko Dam)"): each row becomes its OWN entry that
    keeps its reach-qualified name, so the reach is visible and can scope the whole entry.

    `also_items` = the OTHER items a combined override pinned (`MatchResult.also`). One synopsis row can
    name several registry items — "CHILLIWACK / VEDDER RIVERS" is one row over Chilliwack River + Vedder
    River + Vedder Canal, and "FRASER RIVER (upstream of the CPR Bridge at Mission)" is the mainstem plus
    twelve named channels and sloughs. The curator already spelled those out in `gnis_ids`, but only the
    first pin used to survive, so the entry covered the Chilliwack without the Vedder and the Fraser
    without its channels. They are merged here: the entry stays keyed on the primary item, and every
    pinned item contributes its boundaries to the menu, its MUs, and its names."""
    items = (item, *also_items)
    bmap: dict[str, tuple[str, str, str]] = {}
    for it in items:                                   # union, primary first (its ids win a collision)
        for b in it.boundaries:
            bmap.setdefault(b.id, (b.id, b.label, b.kind))
    by_item = tuple((it.id, tuple(b.id for b in it.boundaries)) for it in items)
    return ParseContext(
        entry_id=entry_id or item.id,
        row_index=row_index,
        name=name or item.name,
        region=region,
        mus=tuple(sorted({m for it in items for m in it.mus})),
        item_id=item.id,
        also_item_ids=tuple(it.id for it in also_items),
        item_kind=item.kind,
        variants=tuple(sorted({v for it in items for v in it.variants} | {it.name for it in items if it.name})),
        boundaries=tuple(bmap.values()),
        boundaries_by_item=by_item,
        raw_regs=raw_regs,
        symbols=tuple(symbols),
        review_hints=tuple(review_hints),
    )


def build_no_registry_context(*, entry_id: str, name: str, raw_regs: str, registry_note: str,
                              region: str = "", mus: tuple[str, ...] = (), row_index: int = -1,
                              symbols: tuple[str, ...] = ()) -> ParseContext:
    """A parse menu for a row with NO registry match. The reg text is still split into rules, but there
    are no boundaries to bind — every rule goes to review and no extents are invented. Identity comes
    from the synopsis row (not a registry item)."""
    return ParseContext(
        entry_id=entry_id,
        row_index=row_index,
        name=name,
        region=region,
        mus=mus,
        item_id="",
        item_kind="",
        variants=(),
        boundaries=(),
        raw_regs=raw_regs,
        symbols=tuple(symbols),
        no_registry=True,
        registry_note=registry_note,
    )


def load_system_prompt() -> str:
    """The stable parser instructions + worked examples (PARSE_PROMPT.md)."""
    return _PROMPT.read_text(encoding="utf-8")


def render_boundary_menu(boundaries, owners=None) -> list[str]:
    """The bindable-boundary menu lines shared by the PARSE and REVIEW prompts: `id — label [kind]`.
    `boundaries` is an iterable of (id, label, kind) (tuples from ParseContext, or lists from a batch
    item). Empty -> a single 'no cut-points' line.

    `owners` ((item_id, (split ids,)) pairs) annotates each line with the water it belongs to — only
    meaningful for a COMBINED entry, where an `item_id`-scoped extent must bind a cut-point on the
    item it names."""
    if not boundaries:
        return ["- (none) — this item has no cut-points; only op:whole is bindable."]
    owner = {bid: iid for iid, ids in (owners or ()) for bid in ids}
    return [f"- `{bid}`  — {label}  [{kind}]" + (f"  (on {owner[bid]})" if bid in owner else "")
            for bid, label, kind in boundaries]


def render_user_message(ctx: ParseContext) -> str:
    """The per-entry payload: identity, the closed boundary menu, `within` targets, species menu, and
    the verbatim regs. Deliberately terse — the parser selects from these, it does not invent ids."""
    lines: list[str] = []
    header = f"## Waterbody: {ctx.name or '(unnamed)'}"
    header += f"  [NO REGISTRY MATCH]" if ctx.no_registry else f"  [{ctx.item_id}, kind={ctx.item_kind}]"
    lines.append(header)
    if ctx.also_item_ids:
        lines.append(f"Combined entry — these regs cover {len(ctx.also_item_ids) + 1} registry items: "
                     f"{ctx.item_id}, {', '.join(ctx.also_item_ids)}. The boundary menu below is their "
                     f"union; bind each rule to whichever cut-point its text names, or scope it to one "
                     f"water with `item_id` (see 'Combined entries' in the instructions).")
    if ctx.region or ctx.mus:
        lines.append(f"Region {ctx.region or '?'} · MUs: {', '.join(ctx.mus) or '?'}")
    if ctx.variants:
        lines.append(f"Also known as: {', '.join(ctx.variants)}")
    if symbols_include_tributaries(ctx.symbols):
        lines.append("Synopsis symbol **[Includes Tributaries]** → set entry `tributaries.included = true`.")
    lines.append("")

    if ctx.review_hints:
        lines.append("### ⚠ A prior review flagged this parse — FIX these before re-emitting:")
        for h in ctx.review_hints:
            lines.append(f"- {h}")
        lines.append("")

    if ctx.no_registry:
        lines.append("### ⚠ NO REGISTRY MATCH — content-only parse")
        lines.append(f"Reason: {ctx.registry_note or 'unmatched'}")
        lines.append("This row has NO registry item, so there are NO boundaries to bind. Still do the "
                     "real work: split `regs_verbatim` into rules with restriction_type / details / "
                     "dates / species / display_location. For EVERY rule set `extents: []`, "
                     "`needs_review: true`, and a `review_reason` (e.g. \"no registry match — attach an "
                     "item and bind extents\"). Do NOT invent split ids or op:whole. Leave "
                     "`registry_status`/`registry_note` unset (ingest fills them).")
        lines.append("")
    else:
        lines.append("### Bindable boundaries (the ONLY ids an extent.splits may use)")
        # owners only for a combined entry — a single-item menu has nothing to disambiguate
        lines.extend(render_boundary_menu(
            ctx.boundaries, ctx.boundaries_by_item if ctx.also_item_ids else None))
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
- Pick `species` codes only from the menu shown with each item; leave it empty for ALL species.
- Bind `extents.splits` only to that item's listed boundary ids. If none fit, record the phrase in
  `unresolved_locators` and set `needs_review` — never invent an id.

This is a single-shot parse: emit the JSON directly. Your output is validated (Entry schema + split-id
check) after you submit, and any batch that fails is re-run — so get each entry right in one pass.
"""


def render_batch_prompt(contexts: list[ParseContext]) -> str:
    """A self-contained batch prompt: the stable rules/examples (PARSE_PROMPT.md), each item's
    constrained menu, then the `{index, entry}` output envelope. Single-shot — the agent emits JSON
    directly (no tools); validation happens downstream at ingest."""
    parts = [load_system_prompt(), "\n\n---\n\n# BATCH — parse EACH item below into its own Entry\n"]
    for ctx in contexts:
        parts.append(f"\n---\n## ITEM index={ctx.row_index}\n\n{render_user_message(ctx)}\n")
    parts.append(_BATCH_ENVELOPE)
    return "\n".join(parts)
