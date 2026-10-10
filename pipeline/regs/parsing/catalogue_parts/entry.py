"""`CatalogueEntry` and `CatalogueFile` — the row, its entry-level validators, and the file.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .vocab import RuleType
from .text import EXTRACTION_MARKUP, squash
from .dates import _days
from .species import (
    PROTECTED_FISH, protected_fish_printed, source_artefact_problems, species_problems,
    steelhead_scope_problems, trout_scope_problems
)
from .checks import LIST_MARKER, _dates_lost
from .licensing import Alternative, Designation, LicensingRecord, licensing_verbatims
from .see import See, _NOT_A_WATER, _POINTER_WORDS
from .rule import CatalogueRule


# --------------------------------------------------------------------------------------- #
# Entries
# --------------------------------------------------------------------------------------- #

#: The book's "includes tributaries" glyph, as the extractor writes it into `symbols`
#: (`pipeline.regs.parsing.rows.TRIBUTARIES_SYMBOL`). `CatalogueEntry._glyph_walks` holds the row to it.
GLYPH_INCLUDES_TRIBUTARIES = "Incl. Tribs"


def _extent_errors(entry: "CatalogueEntry") -> List[str]:
    """Every extent the entry, its rules and its licensing records carry, read through `Extent`.

    The catalogue stores extents as plain dicts, and nothing read them through the model: an arity
    error or a misspelt key reached the reach builder, which ignores what it does not know. Now
    every one is validated (4,900 passed unchanged when this was added).

    A WATERSHED PART MAY NOT WALK. `Extent.watershed` already selects every lake and stream of the
    river's basin on its side of the cut; a tributary walk from it would climb out of the
    downstream part into the upstream one — the side the book excluded. So a rule or record with
    one is refused `includes_tributaries: true`, its own or its entry's. `tributaries_only` stays:
    on a watershed it means the part without the river itself."""
    from pipeline.regs.parsing.entry_models import Extent

    e: List[str] = []

    def check(where: str, extents) -> bool:
        wet = False
        for i, x in enumerate(extents or []):
            if not isinstance(x, dict):
                e.append(f"{where}: extent {i} is not an object")
                continue
            try:
                wet = Extent.model_validate(x).watershed or wet
            except ValueError as err:
                e.append(f"{where}: extent {i}: {squash(str(err))[:200]}")
        return wet

    check("entry extents", entry.extents)
    holders = [(f"{r.rule_id}", r) for r in entry.rules] + \
              [(f"{x.kind} {x.id}", x) for x in entry.licensing]
    # `walk_past` says how a CARVE-OUT is walked; on a place it would mean nothing.
    for where, xs in [("entry extents", entry.extents)] + \
            [(w, getattr(h, "extents", None)) for w, h in holders]:
        if any(isinstance(x, dict) and x.get("walk_past") for x in xs or []):
            e.append(f"{where}: walk_past belongs on a tributary_excludes carve-out, not on a place")
    for where, h in holders:
        wet = check(where, getattr(h, "extents", None))
        check(f"{where} tributary_excludes", getattr(h, "tributary_excludes", None))
        own = getattr(h, "includes_tributaries", None)
        walks = own if own is not None else entry.includes_tributaries
        if wet and walks:
            e.append(f"{where}: a `watershed` extent already holds the tributaries of its part — "
                     f"includes_tributaries{' (inherited from the entry)' if own is None else ''} "
                     f"would walk out of it into the other side; drop it")
    return e


class CatalogueEntry(BaseModel):
    """One row of the synopsis — a water, a zone, or the province — and its typed rules.

    `regs_verbatim` is the whole printed passage; every rule's `verbatim` must be a contiguous
    substring of it. That chain of custody is what caught an invented 50 cm sub-limit on the first
    sample, and what refused a Classified Waters entry whose text mentioned Kootenay Class II
    waters that no rule covered.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    entry_id: str
    name: str
    display_name: str = ""
    region: str = ""
    scope_note: str = ""
    regs_verbatim: str = Field(..., min_length=1)
    source_pages: List[int] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    matched: List[str] = Field(default_factory=list)
    extents: List[dict] = Field(default_factory=list)
    includes_tributaries: Optional[bool] = None
    rules: List[CatalogueRule] = Field(default_factory=list)
    #: WHAT YOU MUST HOLD, AND WHAT THIS WATER IS — see the licensing section above. Not rules:
    #: licensing never competes and never votes on open/closed, and it depends on who the angler
    #: is and what they are doing, which no rule takes.
    licensing: List[LicensingRecord] = Field(default_factory=list)
    #: POINTERS — "See Lonzo Creek" (see `See`). Not rules: they bind nothing. A row whose ONLY
    #: content is a pointer has no rules at all, and says so by having only this.
    see: List[See] = Field(default_factory=list)
    #: ANADROMOUS RAINBOW TROUT ARE FOUND IN THIS ROW'S WATER, so the book's definition holds here
    #: (p.80: "steelhead: a rainbow trout longer than 50 cm in waters where anadromous rainbow trout
    #: are found" — `DEFINITIONAL_SIZE`): a rainbow over 50 cm IS a steelhead, governed by the
    #: steelhead rules, and a rainbow rule speaks only for rainbow of 50 cm or less. The book states
    #: the definition for every such water but lists none, so it is set per row where the fact is
    #: known (Chilliwack/Vedder, user ruling 2026-09-25), never inferred. The bundle marks the row's
    #: waters (`steelhead_water`); `read.effective_rules` reads a rainbow there by it.
    anadromous_rainbow: bool = False
    #: THE BOOK SAYS THIS WATER IS TIDAL: "Nitinat Lake is tidal water; tidal regulations apply and a
    #: (federal) Tidal Waters Sport Fishing Licence is required" (p.17). No provincial regulation
    #: holds there — not the zone's base, not a park closure, not a licence — so the reach builder
    #: takes the row's waters out of every OTHER row's binding (`reach.outside.tidal_sections`), the
    #: way it takes out water past the border. The row itself carries only its note: every rule is an
    #: `advisory` and it has no licensing (user ruling 2026-09-29).
    tidal: bool = False

    @property
    def pointer_only(self) -> bool:
        """A row that states no regulation of its own — only where to look (`see`)."""
        return bool(self.see) and not self.rules and not self.licensing

    @model_validator(mode="after")
    def _book_species_only(self) -> "CatalogueEntry":
        """A SYNOPSIS ROW NAMES ONLY THE BOOK'S SPECIES (p.80). A bare `CatalogueRule` also
        accepts the federal salmon codes the DFO feed types (`FEDERAL_SALMON`); a row of the
        book does not — its salmon rules name `SALMON`."""
        bad = []
        for r in self.rules:
            bad += species_problems(set(r.species) | set(r.species_except),
                                    f"{r.rule_id}.species")
            bad += species_problems(set(r.when_targeting), f"{r.rule_id}.when_targeting")
            for c in r.gear:
                if c.when is not None:
                    bad += species_problems(set(c.when.targeting),
                                            f"{r.rule_id}.gear.when.targeting")
        if bad:
            raise ValueError("; ".join(bad))
        return self

    @model_validator(mode="after")
    def _protected_fish_printed(self) -> "CatalogueEntry":
        """A PROTECTED-SPECIES RULE NAMES EXACTLY THE PROTECTED FISH ITS ROW PRINTS (`PROTECTED_FISH`,
        2026-10-03). The list is the book's, so it is read off the row's own words both ways: a fish
        named and not printed is invented, a fish printed and not named is the empty group again.
        A protected fish never shares a rule with a game fish or a group — the list is said as one
        ("Protected species: …"), and a game fish beside it would be closed by a superior rule."""
        bad = []
        printed = protected_fish_printed(self.regs_verbatim)
        for r in self.rules:
            named = [c for c in r.species if c in PROTECTED_FISH]
            if not named:
                continue
            if len(named) != len(r.species) or r.species_except:
                bad.append(f"{r.rule_id}: protected fish {named} share the rule with "
                           f"{[c for c in r.species if c not in PROTECTED_FISH]} / except "
                           f"{list(r.species_except)} — a protected-species rule names only them")
            if set(named) != set(printed) or len(set(named)) != len(named):
                bad.append(f"{r.rule_id}: names protected fish {named}, its row prints {printed} "
                           f"— name exactly the fish the row prints, each once")
        if bad:
            raise ValueError("; ".join(bad))
        return self

    @model_validator(mode="after")
    def _glyph_walks(self) -> "CatalogueEntry":
        """THE BOOK'S "INCLUDES TRIBUTARIES" GLYPH ON THE ROW MEANS THE ROW WALKS (p.4 legend: "when
        all regulations cited apply to both the named body of water and its tributaries, an asterisk
        is placed in the first column"). The reach builder never reads `symbols`, so a row carrying
        the glyph with no `includes_tributaries` bound none of its tributaries — the Eve, Honna,
        Kitsumkalum, Insect Creek, Burnt River and Bella Coola rows did (2026-09-29). Refused unless
        the entry says `includes_tributaries: true`, or every rule that stays on the water says why
        in a `review_reason`."""
        if GLYPH_INCLUDES_TRIBUTARIES not in self.symbols or self.includes_tributaries is True:
            return self
        silent = [r.rule_id for r in self.rules
                  if not (r.includes_tributaries or r.tributaries_only or r.review_reason)]
        if silent or not self.rules:
            raise ValueError(
                f"{self.entry_id}: the row prints the includes-tributaries glyph "
                f"({GLYPH_INCLUDES_TRIBUTARIES!r} in symbols) but includes_tributaries is "
                f"{self.includes_tributaries} — set it true, or give each rule that does not walk "
                f"a review_reason saying why: {silent or 'the row has no rules'}")
        return self

    @model_validator(mode="after")
    def _tidal_is_a_note(self) -> "CatalogueEntry":
        """A TIDAL ROW CARRIES ONLY ITS NOTE (`tidal`): the provincial regulations do not hold on
        tidal water, so neither can a rule or licensing record of the row itself."""
        if not self.tidal:
            return self
        bad = [r.rule_id for r in self.rules if r.type is not RuleType.advisory]
        if bad or self.licensing or not self.matched:
            raise ValueError(
                f"{self.entry_id}: a tidal row carries only advisory notes on its matched water — "
                f"non-advisory rules {bad}, licensing records {len(self.licensing)}, "
                f"matched {self.matched}")
        return self

    @model_validator(mode="after")
    def _no_source_artefacts(self) -> "CatalogueEntry":
        """A KNOWN SOURCE ARTEFACT IS NOT A RULE (`SOURCE_ARTEFACTS`): Goat River's "Leadville
        Creek Cameron Creek" is two map labels printed into the row (user ruling 2026-09-30)."""
        bad = source_artefact_problems(self.entry_id, self.rules)
        if bad:
            raise ValueError(f"{self.entry_id}: " + "; ".join(bad))
        return self

    @model_validator(mode="after")
    def _trout_scope(self) -> "CatalogueEntry":
        """"TROUT" INCLUDES CHAR UNLESS THE ROW MENTIONS CHAR (user ruling 2026-09-28) — see
        `trout_scope_problems`. The row is this entry: a water's row, or a zone table."""
        bad = trout_scope_problems(self.entry_id, self.regs_verbatim, self.rules)
        if bad:
            raise ValueError(f"{self.entry_id}: " + "; ".join(bad))
        return self

    @model_validator(mode="after")
    def _steelhead_scope(self) -> "CatalogueEntry":
        """See `steelhead_scope_problems` (user ruling 2026-10-08)."""
        bad = steelhead_scope_problems(self.entry_id, self.rules)
        if bad:
            raise ValueError(f"{self.entry_id}: " + "; ".join(bad))
        return self

    @model_validator(mode="after")
    def _complements(self) -> "CatalogueEntry":
        """"OTHER PARTS" IS THE REST OF THE WATER AFTER NAMED SIBLINGS (`Extent` op `rest`).

        Bull River's "Other parts: trout/char daily quota = 1" was held as an undrawn part, so the
        everyday quota for most of the river never decided anything. As `rest` it binds the
        rule's water minus the sections its `siblings` bind (`reach.build.build_reach`). So each
        sibling must be a rule of THIS entry that draws its place: extents, none of them `rest`
        (a complement of a complement has no order), no undrawn part and no unbound locator (the
        rest would swallow the part nobody drew). And `rest` stands alone on a rule — a union with
        another place is a different shape — and only on a rule: an entry's scope, a licensing
        record or a carve-out has no siblings to be the rest of."""
        from pipeline.regs.parsing.entry_models import Op
        e: List[str] = []
        rest = Op.REST.value

        def is_rest(x) -> bool:
            return isinstance(x, dict) and x.get("op") == rest

        if any(is_rest(x) for x in self.extents):
            e.append("entry extents: op rest is a rule's complement of its siblings — an entry's "
                     "scope has none")
        for x in self.licensing:
            if any(is_rest(y) for y in (getattr(x, "extents", None) or [])) or \
                    any(is_rest(y) for y in (getattr(x, "tributary_excludes", None) or [])):
                e.append(f"{x.kind} {x.id}: op rest belongs to rules — a licensing record is "
                         f"placed on its water, never the rest of other rules")
        by_id = {r.rule_id: r for r in self.rules}
        for r in self.rules:
            if any(is_rest(y) for y in r.tributary_excludes):
                e.append(f"{r.rule_id}: a carve-out (tributary_excludes) cannot be op rest")
            mine = [x for x in (r.extents or []) if is_rest(x)]
            if not mine:
                continue
            if len(r.extents or []) != 1:
                e.append(f"{r.rule_id}: op rest must be the rule's only extent — it is the rest "
                         f"of the water, not a place to union with another")
            if r.undrawn_part.strip():
                e.append(f"{r.rule_id}: op rest DRAWS the other parts — drop undrawn_part")
            for sib in mine[0].get("siblings") or []:
                o = by_id.get(sib)
                if o is None or sib == r.rule_id:
                    e.append(f"{r.rule_id}: rest sibling {sib!r} names no other rule in this "
                             f"entry")
                    continue
                if not o.extents:
                    e.append(f"{r.rule_id}: rest sibling {sib} has no extents — the rest of an "
                             f"undrawn place is unknown")
                elif any(is_rest(y) for y in o.extents):
                    e.append(f"{r.rule_id}: rest sibling {sib} is itself op rest — a complement "
                             f"of a complement has no order")
                if o.undrawn_part.strip() or o.unresolved_locators or o.standing:
                    e.append(f"{r.rule_id}: rest sibling {sib} holds in a part nothing draws — "
                             f"the rest of the water around it is unknown")
        if e:
            raise ValueError(f"{self.entry_id}: " + "; ".join(e))
        return self

    @model_validator(mode="after")
    def _steelhead_waters(self) -> "CatalogueEntry":
        """THE KNOWN STEELHEAD WATERS (`Extent` op `steelhead_waters`, user ruling 2026-10-02) are
        the reach builder's set, so only a zone/provincial row's RULE or LICENSING RECORD may bind
        them — a water row names its own water — and only as its one extent (a union with another
        place is a different shape). Each sibling is another rule (or, on a record, another record)
        of this entry that draws its place: its sections are what the twin leaves to it."""
        from pipeline.regs.parsing.entry_models import Op
        op = Op.STEELHEAD_WATERS.value
        e: List[str] = []

        def has(xs) -> bool:
            return any(isinstance(x, dict) and x.get("op") == op for x in xs or [])

        if has(self.extents):
            e.append("entry extents: op steelhead_waters belongs to a rule or a licensing record")
        zone = self.entry_id.startswith("z")
        rules = {r.rule_id: r for r in self.rules}
        recs = {x.id: x for x in self.licensing}
        for kind, rid, exts, excl, pool in (
                [("rule", r.rule_id, r.extents, r.tributary_excludes, rules) for r in self.rules]
                + [(x.kind, x.id, getattr(x, "extents", None), getattr(x, "tributary_excludes", None),
                    recs) for x in self.licensing]):
            if has(excl):
                e.append(f"{kind} {rid}: a carve-out cannot be op steelhead_waters")
            if not has(exts):
                continue
            if not zone:
                e.append(f"{kind} {rid}: op steelhead_waters on a water row — a water row names "
                         f"its own water")
            if len(exts or []) != 1:
                e.append(f"{kind} {rid}: op steelhead_waters must be the only extent")
            for sib in (exts[0].get("siblings") or []):
                o = pool.get(sib)
                if o is None or sib == rid:
                    e.append(f"{kind} {rid}: steelhead_waters sibling {sib!r} names no other "
                             f"{'rule' if kind == 'rule' else 'licensing record'} of this entry")
                elif not getattr(o, "extents", None) or has(o.extents):
                    e.append(f"{kind} {rid}: steelhead_waters sibling {sib} does not draw its "
                             f"own place")
        if e:
            raise ValueError(f"{self.entry_id}: " + "; ".join(e))
        return self

    @model_validator(mode="after")
    def _chain_of_custody(self) -> "CatalogueEntry":
        e: List[str] = []
        seen: set[str] = set()
        haystack = squash(self.regs_verbatim)
        # A POINTER QUOTES THE ROW and never points at the row itself. Whether its targets exist
        # is a question about the corpus (the bundle build and `test_see_pointers` ask it).
        for s in self.see:
            if squash(s.verbatim) not in haystack:
                e.append(f"see {s.verbatim[:40]!r}: not a contiguous substring of regs_verbatim")
            if self.entry_id in s.entry_ids:
                e.append(f"see {s.verbatim[:40]!r}: points at its own entry")
        # A POINTER IS NOT A RULE. An information rule whose words only say where else to look
        # ("See Lonzo Creek", "A tributary of Slocan River. See Slocan River") is a `see` edge;
        # written as an advisory it binds the water and shows the pointer as a regulation.
        for r in self.rules:
            if r.type is RuleType.advisory and _POINTER_WORDS.search(squash(r.verbatim)) \
                    and not _NOT_A_WATER.search(squash(r.verbatim)):
                e.append(f"{r.rule_id}: {squash(r.verbatim)[:50]!r} is a pointer to another "
                         f"row — write it as `see` (with the entry it names), not as a rule")
        for r in self.rules:
            if r.rule_id in seen:
                e.append(f"duplicate rule_id {r.rule_id!r}")
            seen.add(r.rule_id)
            needle = squash(r.verbatim)
            if needle not in haystack:
                e.append(f"{r.rule_id}: verbatim is not a contiguous substring of regs_verbatim")
        # A LIST NUMBER IS LAYOUT, NOT THE SENTENCE. "3. Within 23 m downstream of …" is item 3 of
        # the book's list of no-fishing places; quoted with its number the rule reads "3. Within
        # 23 m …" on every water in the province. A verbatim starts at the sentence.
        quoted = [(r.rule_id, r.verbatim) for r in self.rules] + \
            [(f"licensing {x.id}", x.verbatim) for x in self.licensing] + \
            [("see", s.verbatim) for s in self.see]
        for who, v in quoted:
            if EXTRACTION_MARKUP.search(v or ""):
                e.append(f"{who}: verbatim carries the extraction's markup ('**' / '[Includes "
                         f"Tributaries]') — quote the printed sentence (`clean_verbatim`)")
            m = LIST_MARKER.match(v or "")
            if m:
                e.append(f"{who}: verbatim starts with the list marker {m.group(0).strip()!r} — "
                         f"quote the sentence, not its number")
        ids = {r.rule_id for r in self.rules}
        for r in self.rules:
            # A TARGET NAMES A RULE THAT EXISTS. One in this entry is checked here; one in another
            # entry (`entry_id`) by the file, and across files by the bundle build.
            for x in r.exempts:
                if x.entry_id == self.entry_id:
                    e.append(f"{r.rule_id}: exempts names its own entry in entry_id — leave it "
                             f"out; a target with no entry_id is this entry's")
                elif x.target and not x.entry_id and (x.target not in ids
                                                      or x.target == r.rule_id):
                    e.append(f"{r.rule_id}: exempts target {x.target!r} names no other rule in "
                             f"this entry")
            if r.suspended_while and (r.suspended_while not in ids
                                      or r.suspended_while == r.rule_id):
                e.append(f"{r.rule_id}: suspended_while={r.suspended_while!r} names no other rule "
                         f"in this entry")
        # A CLAUSE IS IN FORCE ONLY WHILE ITS QUOTA IS. "Bull trout daily quota = 1 (none under
        # 80 cm), Jul 1-30 and Nov 1-Dec 31" is one sentence; stored as a quota with the dates
        # and a `within` clause without them, the clause read as ALL YEAR (an absent `when` is all
        # year) and showed "none under 80 cm" on Aug 15 beside the release. A readers' `within`
        # does not carry time, so the clause states it: its days must lie inside its parent's.
        parents = {r.rule_id: r for r in self.rules}
        for r in self.rules:
            p = parents.get(r.within) if r.within else None
            if p is None or p.when is None or p.when.is_empty():
                continue
            mine = r.when
            if mine is None or mine.is_empty():
                e.append(f"{r.rule_id}: a clause `within` {p.rule_id}, which holds only on its "
                         f"own `when`, has none — it would read as all year; give it the "
                         f"parent's `when`")
            elif mine.dates and p.when.dates and not _days(mine.dates) <= _days(p.when.dates):
                e.append(f"{r.rule_id}: its `when` holds on days its parent {p.rule_id} does not")
        # EVERY SEASON THE ROW PRINTS REACHES A RULE — see `_dates_lost`.
        e += _dates_lost(self)
        # EVERY QUOTE A LICENSING RECORD CARRIES is chain of custody too — per printed clause, so
        # a stamp period or a suspension note cannot be paraphrased under a real designation.
        lic_ids: set = set()
        units: set = set()
        by_rule = {r.rule_id: r for r in self.rules}
        region = self.entry_id.split(":", 1)[0]
        for x in self.licensing:
            if x.id in lic_ids:
                e.append(f"duplicate licensing id {x.id!r}")
            lic_ids.add(x.id)
            for q in licensing_verbatims(x):
                if squash(q) not in haystack:
                    e.append(f"{x.kind} {x.id}: {q[:50]!r} is not a contiguous substring of "
                             f"regs_verbatim")
            # AN ALTERNATIVE ON A ROW THAT IS NO WATER must name its place itself. On a
            # provincial or zone row (`zp:basic_licence` is the one it would be written on)
            # `whole` or a bare cut has no water to be the whole OF, so the record would be
            # accepted nowhere or — once someone "fixes" that — everywhere.
            if isinstance(x, Alternative) and not self.matched:
                vague = [i for i, ex in enumerate(x.extents)
                         if not (ex.get("item_id") or ex.get("item_ids") or ex.get("area_id")
                                 or ex.get("area_kind") not in (None, "", "region"))]
                if vague:
                    e.append(f"alternative {x.id}: extent(s) {vague} name no place, and this "
                             f"row is not a water — name the item or area it is accepted on")
            if not isinstance(x, Designation):
                continue
            # "two separate Class II waters … require separate licences": one entry, two units.
            if x.unit in units:
                e.append(f"two designations in one entry share unit {x.unit!r}")
            units.add(x.unit)
            for sw in x.suspended_while:
                c = by_rule.get(sw.rule_id)
                if c is None:
                    e.append(f"designation {x.id}: suspended_while names no rule "
                             f"{sw.rule_id!r} in this entry")
                elif not (c.type is RuleType.retention_limit and c.take == 0
                          and c.may_target is False):
                    e.append(f"designation {x.id}: suspended_while {sw.rule_id!r} is not a "
                             f"closure — a designation sleeps under a closure, never a quota")
            # STEELHEAD COUNTRY PRINTS THE STAMP. Outside the Kootenay (Region 4), where Class II
            # waters print no stamp, a designation that says nothing about it has lost a clause.
            if region != "r4" and x.steelhead_stamp_during is None \
                    and x.steelhead_stamp_waived is None and not x.review_reason:
                e.append(f"designation {x.id}: no steelhead_stamp_during or _waived in "
                         f"steelhead country — record the printed clause, or say why in "
                         f"review_reason")
        e += _extent_errors(self)
        # EVERY RULE SAYS WHERE IT IS. Nothing inherits the entry's extents (see
        # `CatalogueRule.extents`), so a rule with none and no words for its place would have a
        # reach nobody stated — which a reader could only fill by guessing.
        for r in self.rules:
            if not r.extents and not r.extent_text.strip() and not r.unresolved_locators:
                e.append(f"{r.rule_id}: says nothing about where it applies — give it `extents` "
                         f"(the entry's reach, written out, if it covers the whole row), or name "
                         f"the place it cannot bind in `extent_text` / `unresolved_locators`")
        if not self.rules and not self.licensing and not self.see:
            e.append("an entry with no rules, no licensing and no `see` says nothing")
        if e:
            raise ValueError(f"{self.entry_id}: " + "; ".join(e))
        return self


class CatalogueFile(BaseModel):
    model_config = ConfigDict(frozen=True)
    region: str
    entries: List[CatalogueEntry]

    @model_validator(mode="after")
    def _unique_ids(self) -> "CatalogueFile":
        ids = [x.entry_id for x in self.entries]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ValueError(f"duplicate entry_id(s): {sorted(dupes)}")
        return self

    @model_validator(mode="after")
    def _exempts_name_real_rules(self) -> "CatalogueFile":
        """A target in ANOTHER entry of this file must be one of its rules. (A target in another
        file cannot be seen here; the bundle build refuses it, and a test runs over the corpus.)"""
        rules = {x.entry_id: {r.rule_id for r in x.rules} for x in self.entries}
        bad = [f"{x.entry_id}/{r.rule_id} -> {t.entry_id}#{t.target}"
               for x in self.entries for r in x.rules for t in r.exempts
               if t.entry_id in rules and t.target not in rules[t.entry_id]]
        if bad:
            raise ValueError(f"exempts target(s) name no rule of that entry: {bad}")
        return self
