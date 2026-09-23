"""Self-check a candidate catalogue parse — the SAME gate ingest runs.

Designed for the parsing agent to run on itself until clean, so a bad candidate never reaches a
human. Needs only the batch (which carries each item's printed text) and the candidate JSON.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.validate_catalogue \
        batch_000.json candidate.json

Exit 0 = every entry valid. 1 = at least one failed, with the reason printed.

Three layers, and the third is the one that matters:

  1. the MODEL — types, conditions, enums, arithmetic  (CatalogueRule)
  2. the ENTRY — each rule's verbatim inside its own regs_verbatim  (CatalogueEntry)
  3. THE SOURCE — regs_verbatim against the text the agent was GIVEN

and, where the batch item is available, the REACH: every split id must be a cut-point on that
water (an invented one otherwise reaches the corpus and only surfaces later as a rule that
silently selects nothing), with alias ids rewritten to the canonical spelling.

Layer 2 alone is not chain of custody: the author writes both sides, so an invented sentence passes.
Two did, in the first authored pass. Layer 3 is what closes it.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Iterable

from pydantic import ValidationError

from pipeline.regs.parsing.catalogue import CatalogueEntry, squash, RuleType, label, \
    licensing_label

# `squash` is the ONE normaliser and lives in catalogue.py, beside the model validator that
# also needs it — two copies drifted once and cost a whole parse.
def boundary_ids(item: dict) -> tuple[set[str], dict[str, str]]:
    """(every bindable id, {alias: canonical id}) for one batch item.

    A cut-point can answer to several authored ids — "McIntyre Dam" and the gauge that resolved to
    the identical measure are one point, and a confluence is named from either bank. Both BIND,
    because `extent.py` resolves either. Only one is STORED."""
    allowed: set[str] = set(item.get("bindable_ids") or ())
    canon: dict[str, str] = {}
    for b in (item.get("boundaries") or ()):
        if not b:
            continue
        allowed.add(b[0])
        for a in (b[3] if len(b) > 3 else ()):
            allowed.add(a)
            canon[a] = b[0]
    return allowed, canon


def default_extents(data: dict) -> int:
    """Give every rule that says nothing about location the whole water, IN PLACE.

    The parse prompt states this default — "Every rule needs `extents`. The default is the
    whole water" — and the model mostly did not write it: it put the reach on the ENTRY and
    left the rules bare. 1,957 of 2,397 rules arrived with no extents, and a rule with no
    extents binds to NOTHING, so 73% of the corpus resolved to no water at all.

    Written here rather than defaulted at resolution time so the corpus states its own reach
    and the resolver has one less way to disagree with it.

    NOT applied to a rule that DESCRIBES a place it could not bind — `extent_text` or
    `unresolved_locators`. "500 m upstream and downstream of Causeway Road" with no boundary
    to bind to is a real, specific location, and widening it to the whole water applies a
    500 m closure to kilometres of it. Measured on the first full parse: of 1,957 bare rules,
    1,752 say nothing about location and 205 do.

    Returns how many rules were given a default.
    """
    # WHAT `whole` MEANS is "every section of every item this entry matched", so on an entry
    # that matched NOTHING it means nothing at all. That is the shape of a regional rule:
    # `z1:bait_ban_streams` names no water, and its reach lives on the ENTRY as
    # `within area:region:1, feature_types: [stream]`. Giving its rule `whole` left 95 of the
    # 117 zone entries — every regional bait ban, hook rule and quota in the province —
    # binding to no section, which is the backbone of "what applies here" missing entirely.
    #
    # So the default is the ENTRY'S OWN REACH when there is no matched item to take one from.
    fallback = [{"op": "whole"}]
    if not (data.get("matched") or []) and (data.get("extents") or []):
        fallback = [dict(x) for x in data["extents"] if isinstance(x, dict)]

    n = 0
    for r in (data.get("rules") or ()):
        if not isinstance(r, dict) or r.get("extents"):
            continue
        if (r.get("unresolved_locators") or []) or (r.get("extent_text") or "").strip():
            continue
        r["extents"] = [dict(x) for x in fallback]
        n += 1
    return n


def canonicalise_splits(data: dict, item: dict) -> list[str]:
    """Rewrite alias split ids to their canonical id, IN PLACE, and report ids this water cannot
    bind. Returns error strings (empty = clean).

    This lives in the validator, not only in ingest, because the agent is told to run the validator
    on itself until clean — a check it cannot run is a defect it cannot fix, and an invented
    cut-point is otherwise invisible until it silently selects no sections at reach resolution."""
    if item.get("no_registry"):
        return []                    # nothing to bind; the parser is told to emit extents: []
    allowed, canon = boundary_ids(item)
    if not allowed:
        return []                    # a batch written before boundaries were exported
    errors: list[str] = []
    # A COMBINED ENTRY'S MENU IS A UNION over several waters, so the union check alone accepts a
    # reach scoped to the Atnarko but bounded by a confluence that only exists on the Bella Coola.
    # An extent that names its water(s) must bind a cut-point ON one of them. A multi-item scope
    # allows a cut on ANY named item — that is its point: the reach's two ends are on different
    # waters.
    by_item = {i: {canon.get(x, x) for x in ids}
               for i, ids in (item.get("bindable_by_item") or {}).items()}

    def visit(extents, where: str) -> None:
        for ex in extents or ():
            if not isinstance(ex, dict):
                continue
            ids = [ex["item_id"]] if ex.get("item_id") else list(ex.get("item_ids") or ())
            scoped = set().union(*(by_item.get(i, set()) for i in ids)) if ids and by_item else None
            fixed = []
            for sid in (ex.get("splits") or ()):
                if sid in canon:
                    fixed.append(canon[sid])        # an alias — store the canonical spelling
                elif sid in allowed:
                    fixed.append(sid)
                else:
                    errors.append(f"{where}: split id {sid!r} is not a cut-point on this water "
                                  f"— it was invented or belongs to another item")
                    fixed.append(sid)
                    continue
                if scoped is not None and fixed[-1] not in scoped:
                    errors.append(f"{where}: extent is scoped to {', '.join(ids)!r} but {sid!r} "
                                  f"is not a cut-point on it — drop the scope, add the item that "
                                  f"carries {sid!r}, or bind a cut-point on that water")
            if fixed:
                ex["splits"] = fixed

    visit(data.get("extents"), "entry scope")
    for i, r in enumerate(data.get("rules") or ()):
        if isinstance(r, dict):
            visit(r.get("extents"), f"rule {i + 1}")
    # A LICENSING RECORD BINDS BY THE SAME CUT-POINTS a rule does — a designation's reach, a
    # requirement's place, a carve-out from its tributaries — so the same menu governs it.
    for i, x in enumerate(data.get("licensing") or ()):
        if isinstance(x, dict):
            visit(x.get("extents"), f"licensing {i + 1}")
            visit(x.get("tributary_excludes"), f"licensing {i + 1} tributary_excludes")
    return errors




#: An exemption names the zone entry it lifts. The parser writes the name the BOOK uses —
#: "exempt from spring closure" — and the entry is called `spring_stream_closure`. The two
#: never had to agree, because nothing checked: `liftsIn` looks the id up, finds nothing, and
#: lifts nothing. The rule still renders, still says "exempt", and the closure it exempts you
#: from goes on closing the water.
#:
#: 37 rules were in this state across four regions, including "Mainstem open all year" on the
#: Fraser — 1,250 km shown shut for the six months the spring closure runs. Every one of the
#: three spellings is an unambiguous alias of a real entry, confirmed by the rule's own verbatim
#: and by the target existing in that rule's region:
#:
#:   spring_closure       -> spring_stream_closure       "Exempt from spring closure",
#:                                                       "EXEMPT from Apr 1-June 14 closure"
#:   trout_char_release   -> trout_char_winter_release   "EXEMPT from the regional Nov 1-Mar 31
#:                                                        trout/char catch and release"
#:   bait_ban             -> bait_ban_streams            "also EXEMPT from bait ban upstream of
#:                                                        Cottonwood River"
#:
#: Aliases only. An id that is NOT in this map and matches no zone entry is left alone and
#: reported by `unresolved_exempt_ids` — a silent rename is how this got here.
EXEMPT_ALIASES = {
    "spring_closure": "spring_stream_closure",
    "trout_char_release": "trout_char_winter_release",
    "bait_ban": "bait_ban_streams",
    "summer_closure": "summer_stream_closure",
    # "exempt from regional Nov 1-Mar 31 BULL TROUT catch and release". The regional entry is
    # `trout_char_winter_release`, one rule, TROUT_CHAR, Nov 1-Mar 31 — the same rule, named by
    # the fish it is being lifted for. Safe because the exempting rule carries `species: [BT]`,
    # so `fullLift` is false and it NARROWS rather than striking the whole regional release.
    "bull_trout_release": "trout_char_winter_release",
}

#: Some exemptions name a single RULE inside a zone entry, not the entry. Lifting the entry
#: would reach every rule in it, which is the over-application direction.
#:
#: "EXEMPT from the regional kokanee 'none from streams' daily quota" is one line of Region 4's
#: `species_quotas`, which carries eleven — bass, burbot, crayfish, pike, sturgeon, walleye and
#: the rest. Pointed at the entry it would touch all of them; pointed at `species_quotas.r5`,
#: which IS "none from streams" for kokanee, it touches exactly the line the book names.
EXEMPT_RULE_TARGETS = {
    "kokanee_stream_quota": "species_quotas.r5",
}


def resolve_exempt_ids(data: dict) -> int:
    """Rewrite a known alias in `exempts.default_id` to the entry id it means. Returns the count."""
    n = 0
    for rule in data.get("rules") or []:
        if not isinstance(rule, dict):
            continue
        for ex in rule.get("exempts") or []:
            if not isinstance(ex, dict):
                continue
            key = str(ex.get("default_id") or "")
            want = EXEMPT_ALIASES.get(key)
            if want:
                ex["default_id"] = want
                n += 1
                continue
            rule_target = EXEMPT_RULE_TARGETS.get(key)
            if rule_target:
                ex.pop("default_id", None)
                ex["target"] = rule_target
                n += 1
    return n


def coerce_shapes(data: dict) -> int:
    """Rewrite the shapes a model reaches for when the schema spells a field differently.

    These are NOT guesses about meaning. Each one is a value the model already carried, written
    in a form the schema does not accept, and the rewrite is mechanical and reversible. Anything
    where the meaning would have to be INFERRED is left to fail — a rejected entry is cheap, and
    a silently wrong one is not.

    Two shapes, both seen in one 34-entry run. (A third, `windows` written as objects, went with
    `windows` itself: seasons are `when`, and the old field is refused.)

    `exempts` as a bare string. The schema takes a list of exemptions, each naming what it lifts;
    the model writes the name on its own — ``"spring closure"``. Same value, no list around it.

    `electric_only: true`, which is not a field at all. It is `aspect: propulsion` with
    `level: electric_only`, and the model invents it because that is what the book calls the rule.

    NOT coerced, deliberately: a `bait_restriction` with no `gear`, and a propulsion rule with
    no `level`. "No powered boats" and "No vessels" are both a refusal, and they are different
    rules — deriving one from the other means reading the sentence, which is the one thing this
    file exists to avoid. Those entries fail, and the prompt now documents the fields.

    Returns the number of rules changed.
    """
    n = 0
    for rule in data.get("rules") or []:
        if not isinstance(rule, dict):
            continue

        ex = rule.get("exempts")
        if ex is not None and not isinstance(ex, list):
            if isinstance(ex, str) and ex.strip():
                rule["exempts"] = [{"default_id": ex.strip(), "note": ""}]
            elif isinstance(ex, dict):
                rule["exempts"] = [ex]
            else:
                rule["exempts"] = []
            n += 1

        if "electric_only" in rule:
            on = bool(rule.pop("electric_only"))
            if on:
                rule.setdefault("aspect", "propulsion")
                rule.setdefault("level", "electric_only")
            n += 1
    return n


def exemptions_stay_inside_what_they_lift(entry_data: dict) -> list[str]:
    """A LIFT MUST SIT INSIDE THE RULE IT LIFTS.

    `zp:bait.r2` permits dead fin fish "when set lining in lakes of Region 6 or in lakes of Zone A
    of Region 7", lifting the PROVINCE-WIDE fin fish ban. It resolved instead against Zone B's fin
    fish ban, and the corpus still carries that in its own `review_reason`. Checking the SUBJECT
    would not have caught it — Zone B bans fin fish too, so "does the target ban what this
    permits?" answers yes for the wrong rule. Checking the PLACE does: Region 6 lakes are not
    inside Zone B, so the pairing is impossible.

    Only same-entry targets are checked here. A `default_id` names a standing rule in another
    entry whose extents this function cannot see, and guessing at them would be worse than
    silence — `resolve_exempt_ids` above owns that half.
    """
    out: list[str] = []
    rules = {r.get("rule_id"): r for r in (entry_data.get("rules") or []) if isinstance(r, dict)}
    entry_ex = entry_data.get("extents") or []

    def places(rule: dict) -> set:
        got = set()
        for ex in (rule.get("extents") or entry_ex or []):
            if not isinstance(ex, dict):
                continue
            a = ex.get("area_id") or ex.get("area_kind")
            if a:
                got.add(str(a))
        return got

    for rid, rule in rules.items():
        for ex in (rule.get("exempts") or []):
            target = (ex or {}).get("target")
            if not target or target not in rules:
                continue                       # a default_id, or a target in another entry
            mine, theirs = places(rule), places(rules[target])
            if not mine or not theirs or mine & theirs:
                continue                       # unplaced, or overlapping — nothing provable
            out.append(
                f"{rid}: exempts {target}, but they share no ground — this lift binds "
                f"{sorted(mine)} and the rule it lifts binds {sorted(theirs)}. A lift that sits "
                f"outside what it lifts either names the wrong rule or reaches further than the "
                f"rule it is an exception to.")
    return out


def check_entry(entry_data: dict, source_text: str,
                item: dict | None = None) -> tuple[CatalogueEntry | None, list[str]]:
    """Returns (entry, errors). `source_text` is the printed row the agent was given.

    `item` is that row's batch item. Given one, extents are checked against its boundary menu and
    alias ids are rewritten to canonical — so pass it whenever it is available."""
    errors: list[str] = []
    # Before anything else: a field written in the wrong SHAPE carries the right value, and
    # rejecting it costs a re-parse of the whole entry. See `coerce_shapes`.
    coerce_shapes(entry_data)
    # An exemption that names a zone entry by the BOOK's wording lifts nothing.
    resolve_exempt_ids(entry_data)
    errors += exemptions_stay_inside_what_they_lift(entry_data)
    default_extents(entry_data)
    if item is not None:
        # Before model validation: this rewrites aliases, and the rewritten value is what the
        # returned entry must carry.
        errors += canonicalise_splits(entry_data, item)
    try:
        entry = CatalogueEntry.model_validate(entry_data)
    except ValidationError as exc:
        for e in exc.errors():
            loc = ".".join(str(x) for x in e["loc"])
            errors.append(f"{loc}: {e['msg']}")
        return None, errors

    # LAYER 3 — the passage must be the text we handed over, not a rewrite of it.
    if source_text:
        if squash(entry.regs_verbatim) not in squash(source_text):
            errors.append(
                "regs_verbatim is not a contiguous run of the source row — it has been reworded, "
                "reordered or stitched. Quote the printed text.")

    for rule in entry.rules:
        # Every number must be in the rule's OWN sentence. This is what stops a limit being
        # attributed to a rule whose text never stated it.
        numbers = [(f, getattr(rule, f)) for f in
                   ("take", "max_kmh", "max_power_kw", "per_daily")]
        # THE SIZES ARE INSIDE `lengths` NOW, and they are exactly the numbers this check exists
        # for: a bound the sentence never stated is a size limit invented by the parser.
        for i, b in enumerate(rule.lengths or []):
            numbers += [(f"lengths[{i}].min_cm", b.min_cm), (f"lengths[{i}].max_cm", b.max_cm)]
        # A MEASURED gear bound is a printed number too. It is stored in the unit its slot names,
        # and the book may print it in another ("3 cm" is `hook_gap_mm: 30`, "1 m" is 1000), so
        # any of the unit's spellings will do. Counts are not checked: "single" is a 1 in words.
        spell = {}
        for i, c in enumerate(rule.gear):
            for k in ("max", "min"):
                v = getattr(c, k)
                if v is not None and c.slot.value.endswith(("_mm", "_kg")):
                    f = f"gear[{i}].{k}"
                    numbers.append((f, v))
                    spell[f] = [v, v / 10, v / 1000] if c.slot.value.endswith("_mm") else [v]
        for field, value in numbers:
            if value in (None, 0):
                continue
            printed = f"{value:g}" if isinstance(value, float) else str(value)
            if field in spell and any(f"{x:g}" in rule.verbatim for x in spell[field]):
                continue
            if printed not in rule.verbatim:
                errors.append(
                    f"{rule.rule_id}: {field}={printed} does not appear in its own verbatim "
                    f"({rule.verbatim[:70]!r})")

        if rule.within and rule.within not in {r.rule_id for r in entry.rules}:
            errors.append(f"{rule.rule_id}: within={rule.within!r} names no rule in this entry")

        if not label(rule).strip():
            errors.append(f"{rule.rule_id}: generates an empty label")

    # A SIZE ON A LICENCE is a printed number like any other: "to keep rainbow trout over 50 cm".
    for x in entry.licensing:
        doing = getattr(x, "doing", None)
        for i, b in enumerate((doing.lengths if doing else None) or []):
            for f, v in (("min_cm", b.min_cm), ("max_cm", b.max_cm)):
                if v and str(v) not in x.verbatim:
                    errors.append(f"licensing {x.id}: doing.lengths[{i}].{f}={v} does not appear "
                                  f"in its own verbatim ({x.verbatim[:70]!r})")
        if not licensing_label(x).strip():
            errors.append(f"licensing {x.id}: generates an empty label")

    by_id = {r.rule_id: r for r in entry.rules}
    for rule in entry.rules:
        if rule.within and rule.take is not None:
            parent = by_id.get(rule.within)
            if parent and parent.take is not None and not parent.unlimited \
                    and rule.take > parent.take:
                errors.append(
                    f"{rule.rule_id}: takes {rule.take} inside a parent of {parent.take} — a "
                    f"sub-limit cannot exceed what it sits in. If it genuinely does, it REPLACES "
                    f"the parent for its species and is not a sub-limit.")

    # A row whose printed text is long but which produced one short rule has almost certainly
    # dropped something. Advisory, not fatal — some rows really are one sentence.
    if source_text and len(squash(source_text)) > 200 and len(entry.rules) == 1:
        errors.append(
            f"ADVISORY: {len(squash(source_text))} characters of source produced a single rule — "
            f"check for a restriction you have not carried across")

    return entry, errors


def _source_for(item: dict) -> str:
    for key in ("raw_regs", "regs_verbatim", "text", "source_text"):
        if item.get(key):
            return str(item[key])
    return ""


def run(batch_path: str, candidate_path: str) -> int:
    batch = json.loads(Path(batch_path).read_text(encoding="utf-8"))
    items = batch.get("items", batch) if isinstance(batch, dict) else batch
    by_id = {i.get("entry_id") or i.get("id"): i for i in items}

    candidate = json.loads(Path(candidate_path).read_text(encoding="utf-8"))
    entries = candidate.get("entries", candidate) if isinstance(candidate, dict) else candidate

    failed = 0
    for data in entries:
        eid = data.get("entry_id", "<no entry_id>")
        source = _source_for(by_id.get(eid, {}))
        if eid not in by_id:
            print(f"FAIL {eid}: not in the batch — an entry_id was invented or altered")
            failed += 1
            continue
        _, errors = check_entry(data, source, by_id.get(eid))
        fatal = [e for e in errors if not e.startswith("ADVISORY")]
        for e in errors:
            print(f"{'WARN' if e.startswith('ADVISORY') else 'FAIL'} {eid}: {e}")
        if fatal:
            failed += 1

    total = len(entries)
    print(f"\n{total - failed}/{total} entries valid")
    return 1 if failed else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("batch")
    ap.add_argument("candidate")
    sys.exit(run(**vars(ap.parse_args())))


if __name__ == "__main__":
    main()
