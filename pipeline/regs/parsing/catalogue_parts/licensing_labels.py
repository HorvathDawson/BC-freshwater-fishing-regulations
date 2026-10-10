"""Licensing labels — the per-record lines (`licensing_parts`, `compose_licensing`).

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from typing import Optional

from .species import expand_species
from .gear import CONDUCT_ACTS
from .checks import part_words
from .licensing import (
    Alternative, Designation, Doing, Exemption, LicenceTerms, NotClassified, Path, Requirement,
    WHO_AXES, Who
)
from .labels import _DOC_WORDS, _EMPHASIS, _size, label, species_words


# --------------------------------------------------------------------------------------- #
# Licensing labels — generated from the record's fields, like every other label. The verbatim
# stays on the record underneath. A per-angler SENTENCE ("as a non-resident on Skeena River 2
# today you need …") is composed by the reader, where a test can pin the whole string; these are
# the per-record lines it is composed from.
# --------------------------------------------------------------------------------------- #

def _docs(ds, joiner: str = "and") -> str:
    names = [_DOC_WORDS.get(getattr(d, "value", d), str(getattr(d, "value", d)).replace("_", " "))
             for d in ds]
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + f" {joiner} " + names[-1]


def _a(doc) -> str:
    """One document with its article: "a Classified Waters Licence", "an angling guide licence",
    and none for a permission, which is not a thing you hold one of."""
    w = _docs([doc])
    if w.startswith("permission"):
        return w
    return ("an " if w[:1].lower() in "aeiou" else "a ") + w


def _cap(s: str) -> str:
    return s[:1].upper() + s[1:]


def unit_words(unit: str, units: Optional[dict] = None) -> str:
    """A licence unit as a reader knows it: the name a designation prints for it. A unit no
    designation names yet (Skookumchuck Creek, whose water is not in the catalogue) reads as its
    words, capitalised — never as the slug."""
    got = (units or {}).get(unit)
    return got or " ".join(w.capitalize() for w in unit.split("_"))


def _path_words(p: "Path") -> str:
    """One way to satisfy a requirement, as an object of "need": "a basic angling licence and a
    Classified Waters Licence"."""
    if p.hold:
        return " and ".join(_a(d) for d in p.hold)
    if p.accompanied_by is not None:
        who = p.accompanied_by.who
        # "anglers 16 and over who hold" read as a crowd; the book means one companion.
        companion = ("someone 16 or over" if who == Who(age=["16_plus"])
                     else f"one of the {who.words()}")
        out = (f"be accompanied by {companion} who holds the licences and stamps this fishing "
               f"requires")
    else:
        out = f"hold what {p.as_.words()} must hold"
    out += {"counts_to_companion": " (any fish you keep count toward your companion's limit)",
            "own": " (you keep your own quota)"}[p.quota]
    return out


def _doing_words(d: "Doing") -> str:
    sp = species_words(expand_species(d.species) if len(d.species) == 1
                       and d.species[0] == "CHAR" else d.species,
                       d.species_except).lower()
    # KOKANEE IS A SALMON TO A BIOLOGIST AND NOT TO THIS GROUP. The book says "(other than
    # kokanee)" and the group already leaves it out, so the label says what the group means
    # rather than letting "salmon" read as every salmon.
    if "SALMON" in d.species and "KO" not in expand_species(list(d.species)):
        sp += " (not kokanee)"
    if d.origin is not None and sp:
        sp = f"{d.origin.value} {sp}"
    if d.act == "fishing":
        return "to fish"
    if d.act == "targeting":
        return f"to fish for {sp}"
    if d.act == "retaining":
        size = ""
        if d.lengths:
            b = d.lengths[0]
            size = (f" {b.min_cm}–{b.max_cm} cm" if b.min_cm is not None and b.max_cm is not None
                    else f" over {b.min_cm} cm" if b.min_cm is not None
                    else f" under {b.max_cm} cm")
        return f"to keep {sp}{size}"
    if d.act == "retaining_recorded":
        return "when keeping a fish whose retention you must record on your licence"
    return "to guide anglers"


#: THE PARTS OF A LICENSING RECORD'S LINE, as for a rule (`LABEL_PARTS`): each generated from the
#: record's fields, absent when it has nothing to say, never the verbatim. `compose_licensing`
#: joins them; the kind decides which appear.
#:
#:   who         the anglers it is about — "Anglers 16 and over", "Non-residents"
#:   what        the fact itself: "Class II Classified Water", "Not a Classified Water", the
#:               document sold ("Annual Classified Waters Licence for non-residents"), what an
#:               alternative accepts ("A Yukon angling licence is also accepted here")
#:   need        what must be held — "a basic angling licence or …"; "no basic angling licence"
#:               on an exemption
#:   must        a duty — "carry your paper licence", "produce your licence on request"
#:   way         a way to satisfy it that is not a document — "be accompanied by someone 16 or
#:               over who holds …"
#:   doing       the activity that triggers it — "to fish", "to keep steelhead"
#:   where       "on a classified stream during its classified period", "on streams"
#:   when        the dates, days and hours it holds
#:   unit        the licence unit(s), as the page names them
#:   stamp       the classified-water Steelhead Stamp: its period here, or its waiver (an
#:               OUTRIGHT waiver says no steelhead stamp of any kind is needed)
#:   waived      where a requirement does not hold: on a Classified Water whose row waives the
#:               Steelhead Stamp outright, while it is in force (`Requirement.waived_where`)
#:   terms       how a document is sold — "sold by the day; at most 8 days per licence year"
#:   instead     what an alternative stands in for — "in place of a basic angling licence"
#:   except      anglers taken out of `who`
#:   suspended   "not in force while “<closure>” applies"
#:   note        "provincial licences are not valid here" (a superior authority)
#:   records     the fish whose retention must be recorded — on the "carry your paper licence"
#:               duty (`retaining_recorded`), DERIVED from the corpus's `record_retention` rules
#:               (`recorded_fish`), never typed: "hatchery steelhead, adult chinook, …"
#:   in_part     the undrawn part a requirement holds in (`Requirement.undrawn_part`): a note on
#:               the water, never required of all of it
LICENSING_PARTS = ("who", "what", "need", "must", "way", "doing", "records", "where", "when",
                   "unit", "stamp", "waived", "terms", "instead", "except", "suspended", "note",
                   "in_part")


def recorded_fish(entries) -> list[tuple[str, list[str]]]:
    """THE FISH WHOSE RETENTION MUST BE RECORDED ON THE LICENCE, derived from the corpus (UI
    consumer's report, 2026-10-03): every `record_retention` rule, said as the fish it is about —
    life stage, origin, species, size, and the water when the rule names one ("rainbow trout over
    50 cm (Main body of Kootenay Lake)"). Rules saying the same thing (`zp:steelhead` r4 and its
    twin r4b) are one entry. Returns [(words, ["entry_id::rule_id", …])], in corpus order.

    This is what "paper licences are required when retaining hatchery steelhead, chinook, Shuswap
    Lake char or rainbow trout, or Kootenay Lake rainbow trout" (p.6) refers to; the list is never
    typed, so a record duty added to the corpus reaches the paper-licence line by itself."""
    out: dict[str, list[str]] = {}
    for ce in entries:
        for r in ce.rules:
            if not r.record_retention:
                continue
            sp = species_words(list(r.species), list(r.species_except))
            sp = sp[:1].lower() + sp[1:]          # "lake trout and Dolly Varden/bull trout"
            if r.life_stage is not None:
                sp = f"{r.life_stage.value} {sp}"
            if r.origin is not None:
                sp = f"{r.origin.value} {sp}"
            size = _size(r).strip().strip("()")
            words = f"{sp} {size}".strip() if size else sp
            if any(isinstance(x, dict) and x.get("item_id") for x in r.extents or []):
                words += f" ({ce.display_name or ce.name})"
            out.setdefault(words, []).append(f"{ce.entry_id}::{r.rule_id}")
    return [(w, ids) for w, ids in out.items()]


def licensing_parts(rec, siblings: Optional[dict] = None, *, units: Optional[dict] = None,
                    refs: Optional[dict] = None, recorded: Optional[list] = None) -> dict:
    """One licensing record's line, as PARTS — see `LICENSING_PARTS`; `compose_licensing` joins.

    Context a record cannot carry itself, all optional:
      siblings  {rule_id: CatalogueRule} of the record's entry, so a suspension names its
                closure in the closure's own words;
      units     {unit: unit_name} across the corpus, so terms name a licence unit the way the
                page prints it ("Dean River Class I - Main Section"), never as a slug;
      refs      {(entry_id, id): record} across the corpus, so an alternative says WHICH
                licence it stands in for, never an id;
      recorded  the words of `recorded_fish(entries)`, so the "carry your paper licence" duty
                (`retaining_recorded`) says WHICH fish it is about.
    """
    p: dict = {}
    if isinstance(rec, Designation):
        p["what"] = f"Class {rec.classified} Classified Water"
        if rec.when and not rec.when.is_empty():
            p["when"] = rec.when.words()
        p["unit"] = rec.unit_name
        if rec.steelhead_stamp_during is not None:
            w = rec.steelhead_stamp_during.when
            p["stamp"] = ("Steelhead Stamp required whatever you fish for"
                          + (f", {w.words()}" if not w.is_empty() else ""))
        elif rec.waives_every_stamp:
            # user ruling 2026-10-02: no steelhead stamp of any kind — the Classified Waters
            # Licence is still required (it is still Class II water)
            p["stamp"] = ("No Steelhead Stamp required here"
                          + (f", {rec.when.words()}" if rec.when and not rec.when.is_empty()
                             else "")
                          + " — neither the classified-water stamp nor the stamp to fish for "
                            "steelhead")
        elif rec.steelhead_stamp_waived is not None:
            p["stamp"] = "Steelhead Stamp not required here unless you fish for steelhead"
        said = []
        for s in rec.suspended_while:
            other = (siblings or {}).get(s.rule_id)
            said.append(label(other) if other is not None else "its closure")
        if said:
            p["suspended"] = "; ".join(f"not in force while “{x}” applies" for x in said)
    elif isinstance(rec, NotClassified):
        p["what"] = "Not a Classified Water"
    elif isinstance(rec, Requirement):
        if rec.who is not None:
            p["who"] = _cap(rec.who.words())
        water = rec.water.value if rec.water is not None else None
        if rec.on is not None:
            period = {"classified_period": "its classified period",
                      "steelhead_period": "its Steelhead Stamp period"}[rec.on]
            p["where"] = f"on a classified {water or 'water'} during {period}"
        elif water:
            p["where"] = f"on {water}s"
        if rec.when and not rec.when.is_empty():
            p["when"] = rec.when.words()
        # "to fish" adds nothing to a duty you have only while fishing.
        if not (rec.conduct and rec.doing.act == "fishing"):
            p["doing"] = _doing_words(rec.doing)
        if rec.conduct:
            # A duty is an instruction: "Anglers 16 and over: carry your paper licence when …".
            p["must"] = "; ".join(CONDUCT_ACTS[a] for a in rec.conduct)
        elif all(q.hold for q in rec.satisfied_by):
            p["need"] = " or ".join(_path_words(q) for q in rec.satisfied_by)
        else:
            # A path that is not a document is an instruction: "…: to fish, be accompanied by …".
            p["way"] = ", or ".join(_path_words(q) for q in rec.satisfied_by)
        if rec.doing.act == "retaining_recorded" and recorded:
            p["records"] = (", ".join(recorded[:-1]) + " or " + recorded[-1]
                            if len(recorded) > 1 else recorded[0])
        if rec.undrawn_part.strip():
            p["in_part"] = part_words(rec.undrawn_part)
        if rec.waived_where is not None:
            p["waived"] = ("not on a Classified Water whose row says “Steelhead Stamp not "
                           "required”, while it is in force")
        if rec.who_except is not None:
            p["except"] = f"except {rec.who_except.words()}"
        if rec.authority == "superior":
            p["note"] = "provincial licences are not valid here"
    elif isinstance(rec, LicenceTerms):
        doc = _docs([rec.document])
        if rec.classified:
            doc = f"Class {rec.classified} {doc}"
        if rec.sold == "per_licence_year":
            doc = "annual " + doc
        if rec.name:
            # THE CLASS AS THE TABLE PRINTS IT ("One Day Angling Licence") — the document it is
            # follows, so a stamp's row ("Steelhead") still says what it is.
            plain = _docs([rec.document])
            plain = plain[2:] if plain.startswith("a ") else plain[3:] if plain.startswith(
                "an ") else plain
            doc = rec.name if plain.lower() in rec.name.lower() else f"{rec.name} ({plain})"
        p["what"] = _cap(doc) + (f" for {rec.who.words()}" if rec.who is not None else "")
        if rec.units:
            # Semicolons, because a printed unit name may carry its own comma ("Dean River
            # Class I, signs 100 m below the canyon to tidal boundary").
            p["unit"] = "; ".join(unit_words(u, units) for u in rec.units)
        bits = []
        if rec.sold == "per_day":
            bits.append("sold by the day")
        if rec.covers:
            bits.append({"every_unit": "valid on every classified water",
                         "one_unit": "valid only on the one water it names"}[rec.covers])
        if rec.max_consecutive_days:
            bits.append(f"at most {rec.max_consecutive_days} consecutive days per licence")
        if rec.max_days_per_licence_year:
            bits.append(f"at most {rec.max_days_per_licence_year} days per licence year")
        if rec.unlimited_days:
            bits.append("no limit on the number of days")
        if rec.max_per_licence_year:
            n = rec.max_per_licence_year
            bits.append(f"at most {n} {'licence' if n == 1 else 'licences'} per licence year")
        if rec.max_units_per_licence_year:
            n = rec.max_units_per_licence_year
            bits.append(f"at most {n} of these waters per licence year" if len(rec.units) > 1
                        else f"at most {n} {'water' if n == 1 else 'waters'} per licence year")
        if rec.allocation:
            bits.append({"open": "on open sale", "booking": "by first-come-first-served booking",
                         "draw": "by annual limited-entry draw"}[rec.allocation])
        if rec.needs:
            bits.append("needs your angling guide's number")
        if rec.name and rec.sold == "per_licence_year":
            bits.append("valid for the licence year (April 1 to March 31)")
        if rec.valid_days is not None:
            bits.append("valid for 1 day" if rec.valid_days == 1
                        else f"valid for {rec.valid_days} consecutive days")
        if rec.fee_cad is not None:
            bits.append(f"reduced fee ${rec.fee_cad:.2f}")
        if rec.fees_cad:
            res = {"resident": "B.C. residents", "non_resident": "non-residents",
                   "non_resident_alien": "non-resident aliens"}
            bits.append("fee " + ", ".join(f"${rec.fees_cad[r]:.2f} ({res[r]})"
                                           for r in WHO_AXES["residency"] if r in rec.fees_cad)
                        + " as printed for 2025-2027, before tax")
        p["terms"] = "; ".join(bits)
    elif isinstance(rec, Exemption):
        p["who"] = _cap(rec.who.words())
        p["need"] = f"no {_docs(rec.documents, 'or')}"
    elif isinstance(rec, Alternative):
        target = (refs or {}).get((rec.alternative_to.entry_id, rec.alternative_to.id))
        if isinstance(target, Requirement) and target.satisfied_by:
            instead = " or ".join(_path_words(q) for q in target.satisfied_by)
        else:
            instead = "the licence otherwise required"
        p["what"] = (_cap(" or ".join(_path_words(q) for q in rec.satisfied_by))
                     + " is also accepted here")
        p["instead"] = f"in place of {instead}"
    else:
        raise TypeError(f"not a licensing record: {type(rec).__name__}")
    return {k: re.sub(r"\s+", " ", _EMPHASIS.sub("", p[k])).strip()
            for k in LICENSING_PARTS if (p.get(k) or "").strip()}


def compose_licensing(parts: dict) -> str:
    """ONE sentence from a licensing record's parts — the one composer (see `compose`)."""
    g = parts.get
    tail = ((f": {g('records')}" if g("records") else "")
            + (f" {g('where')}" if g("where") else "") + (f", {g('when')}" if g("when") else "")
            + (f" ({g('waived')})" if g("waived") else "")
            + (f" — in part: {g('in_part')}" if g("in_part") else ""))
    if g("need") is not None:
        out = f"{g('who') or 'You'} need {g('need')}" + (f" {g('doing')}" if g("doing") else "")
        out += tail
    elif g("must") is not None or g("way") is not None:
        body = (g("must") + (f" {g('doing')}" if g("doing") else "") + tail if g("must")
                else (g("doing") or "") + tail + ", " + g("way"))
        out = f"{g('who')}: {body[:1].lower() + body[1:]}" if g("who") else _cap(body)
    else:
        out = g("what") or ""
        if g("when"):
            out += f", {g('when')}"
        if g("unit"):
            out += (f" ({g('unit')})" if g("terms") is not None
                    else f" (licence unit: {g('unit')})")
        if g("terms") is not None:
            out += f": {g('terms')}"
        if g("instead"):
            out += f", {g('instead')}"
        if g("stamp"):
            out += f". {g('stamp')}"
        if g("suspended"):
            out += ". " + ". ".join(_cap(x) for x in g("suspended").split("; "))
    if g("except"):
        out += f" ({g('except')})"
    if g("note"):
        out += f"; {g('note')}"
    return out + "."


def licensing_label(rec, siblings: Optional[dict] = None, *, units: Optional[dict] = None,
                    refs: Optional[dict] = None) -> str:
    """The composed sentence — `compose_licensing(licensing_parts(...))`; a preview only."""
    return compose_licensing(licensing_parts(rec, siblings, units=units, refs=refs))
