"""Parse-context builder — the CONSTRAINED SELECTION menu handed to the parser for one waterbody.

The rebuilt parser never extracts geometry from prose. For each synopsis row it is given exactly one
registry item and the closed set of bindable cut-points on it (`item.boundaries`), and it must express
every rule's reach by *selecting* from that set (op + boundary ids) — or, when no boundary fits, record
the unbindable phrase in `unresolved_locators` and give a `review_reason`. This module turns a
`RegistryItem` into that menu and renders the per-entry user message; the stable instructions + worked
examples live in `prompts/CATALOGUE_PARSE_PROMPT.md`.

`within(area)` scopes (Garibaldi/reserve closures) are NOT auto-bound by the parser — they're a curation
step (DECISION 2026-08-16): an area reg is given a `review_reason` and a curator adds the extent (matched
area for a blanket closure, or a `within(area)` extent for a system-scoped one), pointing at a catalog
`area_id`. So the parse menu carries only the item's own boundaries, never an area list.

    ctx = build_parse_context(item, raw_regs, region="5")
    allowed = ctx.bindable_ids                      # the closed set `validate_catalogue` checks
    user_msg = render_user_message(ctx)             # embed after the CATALOGUE_PARSE_PROMPT.md text
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pipeline.common.models import RegistryItem
from pipeline.regs.parsing.rows import symbols_include_tributaries
from pipeline.regs.parsing.catalogue import species_menu

#: The catalogue format: a rule is a TYPE plus named CONDITIONS, and the label is generated.
_CATALOGUE_PROMPT = Path(__file__).resolve().parent / "prompts" / "CATALOGUE_PARSE_PROMPT.md"


@dataclass(frozen=True)
class ParseContext:
    entry_id: str
    row_index: int = -1                               # synopsis global row index (the {index,entry} key)
    name: str = ""
    #: What the registry calls the matched item. Kept beside `name`, never merged into it.
    display_name: str = ""
    region: str = ""
    #: Every MU the matched registry ITEM touches — a union, shown to the parser as orientation.
    mus: tuple[str, ...] = ()
    #: The MUs of the synopsis ROW itself — the heading this regulation is printed under, and what
    #: `entry_id` is built from. NOT the same as `mus`: the West Road ("Blackwater") River is one
    #: item spanning 5-12, 5-13, 6-1, 7-8 and 7-10, but its region-5 row is printed under 5-13
    #: alone. `entry_id` is built from this one, or an entry claims MUs its row never named.
    row_mus: tuple[str, ...] = ()
    item_id: str = ""
    also_item_ids: tuple[str, ...] = ()               # a combined override's OTHER items (see below)
    item_kind: str = ""
    variants: tuple[str, ...] = ()
    #: (id, label, kind, aliases) — the bindable cut-points. `aliases` are the OTHER ids this same
    #: physical point answers to, and they are bindable too. A cut-point often carries two authored
    #: names: the curated split and a gauge that resolved to the identical measure ("McIntyre Dam"
    #: and `gauge__08NM247` are one point), or a confluence named from either bank ("Thompson River
    #: → Fraser River" appears on both rivers). `_pickup` keeps whichever was applied last and demotes
    #: the other to an alias, so which id survives as `id` is an accident of ordering — and 20 curated
    #: splits the regulations actually name lost that coin-toss. The reach resolver has always
    #: accepted either id; only this menu didn't show them, so the parser could not name them.
    boundaries: tuple[tuple[str, str, str, tuple[str, ...]], ...] = ()
    boundaries_by_item: tuple[tuple[str, tuple[str, ...]], ...] = ()   # item_id -> its own cut-point ids
    raw_regs: str = ""
    symbols: tuple[str, ...] = ()                        # synopsis row symbols (e.g. 'Incl. Tribs')
    pages: tuple[int, ...] = ()                          # synopsis page(s) the row is printed on
    row_image: str = ""                                  # cropped image of the printed row
    review_hints: tuple[str, ...] = ()                    # prior reviewer findings (a REPASS re-parse)
    no_registry: bool = False                            # True: no registry match — split rules, bind nothing
    registry_note: str = ""                              # why (matcher status + reason), when no_registry

    @property
    def bindable_ids(self) -> set[str]:
        """The closed set an Extent.splits may reference: each boundary id AND every alias of it.
        Aliases belong here because `extent.py` resolves them (`b.id == bid or (aliases & want)`) —
        excluding them would refuse a binding that works."""
        out: set[str] = set()
        for bid, _, _, aliases in self.boundaries:
            out.add(bid)
            out.update(aliases)
        return out


def build_parse_context(item: RegistryItem, raw_regs: str = "", entry_id: str = "",
                        region: str = "", row_index: int = -1, name: str = "",
                        symbols: tuple[str, ...] = (), review_hints: tuple[str, ...] = (),
                        also_items: tuple[RegistryItem, ...] = (),
                        row_mus: tuple[str, ...] = (),
                        pages: tuple[int, ...] = (), row_image: str = "") -> ParseContext:
    """Assemble the constrained menu for one item: its bindable boundaries + identity. Area `within`
    targets are intentionally excluded — area scoping is a curation step (see module docstring).

    `name` is the SYNOPSIS row's own name and is carried through verbatim. It used to fall back to
    `item.name` when a caller passed nothing, which silently replaced the synopsis name with the
    registry's — and where several differently-named rows resolve to items sharing one collective
    name, that made them look like duplicates of each other. INDATA, TCHENTLO, TSAYTA and CHUCHI
    LAKE all became "Nation Lakes"; HAYNES/HYDRAULIC/MINNOW became "McCulloch Reservoir". The
    registry's name is now carried separately as `display_name`, so both survive.

    `also_items` = the OTHER items a combined override pinned (`MatchResult.also`). One synopsis row can
    name several registry items — "CHILLIWACK / VEDDER RIVERS" is one row over Chilliwack River + Vedder
    River + Vedder Canal, and "FRASER RIVER (upstream of the CPR Bridge at Mission)" is the mainstem plus
    twelve named channels and sloughs. The curator already spelled those out in `gnis_ids`, but only the
    first pin used to survive, so the entry covered the Chilliwack without the Vedder and the Fraser
    without its channels. They are merged here: the entry stays keyed on the primary item, and every
    pinned item contributes its boundaries to the menu, its MUs, and its names."""
    items = (item, *also_items)
    bmap: dict[str, tuple[str, str, str, tuple[str, ...]]] = {}
    for it in items:                                   # union, primary first (its ids win a collision)
        for b in it.boundaries:
            # `split:` is the graph's ref prefix; the parser binds bare ids, so strip it here rather
            # than asking the model to know about the prefix.
            aliases = tuple(a[len("split:"):] if a.startswith("split:") else a
                            for a in (b.aliases or ()))
            bmap.setdefault(b.id, (b.id, b.label, b.kind, aliases))
    by_item = tuple((it.id, tuple(x for b in it.boundaries
                                  for x in (b.id, *(a[len("split:"):] if a.startswith("split:") else a
                                                    for a in (b.aliases or ())))))
                    for it in items)
    return ParseContext(
        entry_id=entry_id or item.id,
        row_index=row_index,
        name=name,                     # the synopsis's own words — never item.name
        display_name=item.name,        # what the registry calls what it resolved to
        region=region,
        mus=tuple(sorted({m for it in items for m in it.mus})),
        row_mus=tuple(row_mus),
        item_id=item.id,
        also_item_ids=tuple(it.id for it in also_items),
        item_kind=item.kind,
        variants=tuple(sorted({v for it in items for v in it.variants} | {it.name for it in items if it.name})),
        boundaries=tuple(bmap.values()),
        boundaries_by_item=by_item,
        raw_regs=raw_regs,
        symbols=tuple(symbols),
        pages=tuple(pages),
        row_image=row_image,
        review_hints=tuple(review_hints),
    )


def build_no_registry_context(*, entry_id: str, name: str, raw_regs: str, registry_note: str,
                              region: str = "", mus: tuple[str, ...] = (), row_index: int = -1,
                              symbols: tuple[str, ...] = (),
                              pages: tuple[int, ...] = (),
                              row_image: str = "") -> ParseContext:
    """A parse menu for a row with NO registry match. The reg text is still split into rules, but there
    are no boundaries to bind — every rule goes to review and no extents are invented. Identity comes
    from the synopsis row (not a registry item)."""
    return ParseContext(
        entry_id=entry_id,
        row_index=row_index,
        name=name,
        region=region,
        mus=mus,
        row_mus=tuple(mus),                # no registry item: the row's MUs are the only MUs
        item_id="",
        item_kind="",
        variants=(),
        boundaries=(),
        raw_regs=raw_regs,
        symbols=tuple(symbols),
        pages=tuple(pages),
        row_image=row_image,
        no_registry=True,
        registry_note=registry_note,
    )


def load_system_prompt(catalogue: bool = True) -> str:
    """The parser instructions — CATALOGUE_PARSE_PROMPT.md, the only format there is.

    PARSE_PROMPT.md and RULE_STANDARDS.md are gone. They taught an agent how to PHRASE a label
    (80 distinct quota grammars, three ways to write one motor limit, "single barbless hook"
    written seven ways). Nothing is phrased by hand now — a rule is a TYPE plus named CONDITIONS
    and the line the reader sees is generated — so that variance cannot occur and the instructions
    that policed it have nothing to police.

    The one part of RULE_STANDARDS worth keeping was §3, "type by what the rule DOES, not by the
    words it uses", and it lives in the catalogue prompt with its table updated.
    """
    return _CATALOGUE_PROMPT.read_text(encoding="utf-8")


def render_boundary_menu(boundaries, owners=None) -> list[str]:
    """The bindable-boundary menu lines shared by the PARSE and REVIEW prompts: `id — label [kind]`.
    `boundaries` is an iterable of (id, label, kind[, aliases]) (tuples from ParseContext, or lists
    from a batch item). Empty -> a single 'no cut-points' line.

    An alias is printed as "also written `other_id`", because it is the SAME physical cut-point
    under another authored name. Without this line the parser was shown `gauge__08NM247` where the
    regulation says "McIntyre Dam" and had no way to connect the two. The menu tells it to bind the
    CANONICAL id (the one before the dash); an alias is accepted and rewritten at ingest, so the
    stored extent names one id per cut-point no matter which name the page used.

    `owners` ((item_id, (split ids,)) pairs) annotates each line with the water it belongs to — only
    meaningful for a COMBINED entry, where an `item_id`-scoped extent must bind a cut-point on the
    item it names."""
    if not boundaries:
        return ["- (none) — this item has no cut-points; only op:whole is bindable — a rule "
                "about a PART of this water gets `extent_text` + `review_reason` and NO extents."]
    owner = {bid: iid for iid, ids in (owners or ()) for bid in ids}
    out = []
    for b in boundaries:
        bid, label, kind = b[0], b[1], b[2]
        aliases = tuple(b[3]) if len(b) > 3 else ()
        line = f"- `{bid}`  — {label}  [{kind}]"
        if bid in owner:
            line += f"  (on {owner[bid]})"
        if aliases:
            line += ("  — also written " + " / ".join(f"`{a}`" for a in aliases)
                     + "; BIND THE ID ABOVE")
        out.append(line)
    return out


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
                     f"union; bind each rule to whichever cut-point its text names, or scope an "
                     f"extent to one of these waters with its `item_id`.")
    if ctx.region or ctx.mus:
        lines.append(f"Region {ctx.region or '?'} · MUs: {', '.join(ctx.mus) or '?'}")
    if ctx.variants:
        lines.append(f"Also known as: {', '.join(ctx.variants)}")
    if symbols_include_tributaries(ctx.symbols):
        lines.append("Synopsis symbol **[Includes Tributaries]** → set the entry's "
                     "`includes_tributaries: true`.")
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
                     "real work: split `regs_verbatim` into catalogue rules — `type`, its required "
                     "conditions, `species`, `when`, and a `verbatim` that is a substring of the "
                     "regs below. Write each rule's `extents` as the prompt teaches — "
                     "`[{\"op\": \"whole\"}]` for a rule about the whole row, `undrawn_part` beside "
                     "`whole` for a part of it, `extent_text` + `unresolved_locators` for a place "
                     "nothing can draw — and on EVERY rule a `review_reason` (e.g. \"no registry "
                     "match — attach an item and bind extents\"). A `whole` here binds nothing until "
                     "a curator attaches the item. Do NOT invent split ids.")
        lines.append("")
    else:
        lines.append("### Bindable boundaries (the ONLY ids an extent.splits may use)")
        # owners only for a combined entry — a single-item menu has nothing to disambiguate
        lines.extend(render_boundary_menu(
            ctx.boundaries, ctx.boundaries_by_item if ctx.also_item_ids else None))
        lines.append("")

    lines.append("### Species — the ONLY values `species` / `species_except` may take")
    lines.append(species_menu())
    lines.append("")

    lines.append("### Regulations (regs_verbatim — copy exactly; every rule's `verbatim` must be a "
                 "substring)")
    lines.append(ctx.raw_regs.strip())
    return "\n".join(lines)


_BATCH_ENVELOPE = """\
---

# OUTPUT (STRICT)

Return ONLY a JSON array — one object per ITEM above, in this exact shape:

    [ { "index": <the item's index, unchanged>, "entry": { ...Entry... } }, ... ]

- Copy each item's `index` verbatim; it maps your result back to the row. Never renumber.
- `entry` is the full catalogue entry (see "Output" in the instructions above), bound to THAT
  item's boundaries.
- Return one object for every item. Do not wrap in Markdown fences or add prose.
- Pick `species` from the menu shown with each item — the GROUP the regulation's own words use
  (`TROUT_CHAR`, `ALL_GAME_FISH`) unless the sentence names one fish. Empty is not "all".
- Bind `extents.splits` only to that item's listed boundary ids. If none fit, record the phrase in
  `unresolved_locators` and give a `review_reason` — never invent an id.

This is a single-shot parse: emit the JSON directly. Your output is validated (catalogue schema + split-id
check) after you submit, and any batch that fails is re-run — so get each entry right in one pass.
"""


def render_batch_prompt(contexts: list[ParseContext]) -> str:
    """A self-contained batch prompt: the stable rules/examples (CATALOGUE_PARSE_PROMPT.md), each item's
    constrained menu, then the `{index, entry}` output envelope. Single-shot — the agent emits JSON
    directly (no tools); validation happens downstream at ingest."""
    parts = [load_system_prompt(),
             "\n\n---\n\n# BATCH — parse EACH item below into its own entry\n"]
    for ctx in contexts:
        parts.append(f"\n---\n## ITEM index={ctx.row_index}\n\n{render_user_message(ctx)}\n")
    parts.append(_BATCH_ENVELOPE)
    return "\n".join(parts)
