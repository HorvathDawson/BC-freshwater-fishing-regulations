"""THE GENERATOR for gear: rules in, a settled `MethodTable` out — in two stages.

  STAGE 1  `base(rules, kind, here)` — the region's standing gear table for this kind of water:
           every region-wide gear rule a section carries (`Source.is_base`), settled on its own.
           `region_base(region, kind)` is the same thing computed from the whole corpus for one
           region, whether or not a section in it has shipped — the browsable table the printed
           synopsis lets a person check.

  STAGE 2  `table(rules, kind, here, label)` — the base, with the section's overrides laid on
           top: the water's own rules, area rules, inherited rules, and the lifts among them.
           And the quota ledger's closures, so a water shut to fishing reads as shut here.

WHAT THE RAW FIELDS BECOME
    permitted: True/False            a PERMIT / BAN term on `method`
    type: bait_/tackle_restriction   a RIG term — topic from `dimension`, content from the
                                     fields it sets; `allowed: True` makes it an allowance
    take / may_target with `method`  a KEEP counter — an `Allowance` in a per-method ledger
    exempts                          a LIFT — whole, or of the fish it names
    method: other                    chumming is chumming; "no gear in the water during a No
                                     Fishing period" is the WHILE-CLOSED rule, shown on the
                                     closure, not as a method of its own
"""
from __future__ import annotations
import re
import sys
from dataclasses import replace
from functools import lru_cache
from typing import Dict, FrozenSet, Iterable, List, Optional, Tuple

from pipeline.regs.table.applies import applies_of, Applies, ALWAYS
from pipeline.regs.table.authority import source_of, Source, Authority, Scope
from pipeline.regs.table.corpus import rid, rule_part, rules as all_rules, section_rules as corpus_section
from pipeline.regs.table.ledger import Allowance, Ledger, LIFTED, shuts_the_water
from pipeline.regs.table.method import (Term, MethodTable, MethodRow, METHODS, RIG_TYPES,
                                        RIG_TOPIC, HOOK_AND_LINE, default_term)
from pipeline.regs.table.outcome import outcome_of
from pipeline.regs.table.size import size_of
from pipeline.regs.table.subject import Subject, Origin, Water
from pipeline.regs.table.where import parse_where, cut_for_this


def is_gear(x: dict) -> bool:
    return bool(x.get("method")) or str(x.get("type") or "") in RIG_TYPES


def reaches_kind(x: dict, water_kind: str) -> bool:
    """Does this rule speak about this kind of water? TWO SPELLINGS OF ONE FACT: the rule's
    `water` field, and `feature_types` on its typed extents — which the atlas resolver reads
    (`pipeline/atlas/reach/extent.py`) and the page builder honours. Reading only `water` put
    the province's three set-line companion rules (typed `feature_types: [lake]`, no `water`)
    on every stream of Regions 6 and 7A, and the reference base disagreed with every shipped
    section on the Skeena — a fault in the reference, not the data."""
    wk = x.get("water")
    if wk and wk != water_kind:
        return False
    kinds = {str(t).lower() for e in (x.get("extents") or []) for t in (e.get("feature_types") or [])}
    return not kinds or water_kind in kinds


def method_of(x: dict) -> str:
    """The schema's `other` holds two real prohibitions: chumming, and the no-gear-while-closed
    rule. Neither is a way of fishing called "other"."""
    m = str(x.get("method") or "")
    if m != "other":
        return m
    words = f"{x.get('label') or ''} {x.get('verbatim') or ''} {x.get('reason') or ''}".lower()
    if "chumming" in words:
        return "chumming"
    if "no fishing period" in words:
        return "__while_closed__"
    return "other"


# -- the extent: a place, a region list, or a condition somebody filed as a place ---------
_SCOPE_NOISE = {"lakes", "lake", "streams", "stream", "rivers", "river", "of", "and", "or", "in",
                "the", "region", "regions", "zone", "a", "b", "all", "waters", "only"}


def explained_by_scope(text: str, regions: FrozenSet[str]) -> bool:
    """"lakes of Region 6 and Zone A of Region 7" says nothing the typed extents (Regions 6 and
    7A) and the water kind do not already say; "Fraser, Lower Pitt and Lower Harrison Rivers,
    Region 2" names three rivers the typed extent (Region 2) does not."""
    if not regions:
        return False
    words = [w for w in re.split(r"[^a-z0-9]+", (text or "").lower())
             if w and w not in _SCOPE_NOISE and not re.fullmatch(r"\d+[ab]?", w)]
    return not words


def _applies(x: dict, label: str, regions: FrozenSet[str], kind: str) -> Tuple[Applies, str]:
    """(when it applies, a condition the catalogue filed as a place). A permit's extent that
    is not a place — "huts must be removed before spring breakup" — is a condition of the
    permit, kept as words and REPORTED as a curation defect; a ban's or rig rule's is a place
    inside the water nobody can draw, and stays a caveat."""
    ext = x.get("extent_text") or ""
    prose = ""
    if ext and (explained_by_scope(ext, regions) or parse_where(ext).kind == "regions"):
        ext = ""
    if ext and kind == "permit":
        prose, ext = ext, ""
    ap = applies_of(x.get("windows"), ext or None, all_year=not x.get("windows"),
                    section_label=label, from_time=x.get("from_time"), to_time=x.get("to_time"),
                    weekdays=x.get("weekdays"), unless=str(x.get("windows_are") or "") == "excepts")
    return ap, prose


def _says(x: dict) -> Tuple[Tuple[str, str], ...]:
    """The content a rig rule sets, as comparable pairs."""
    out = []
    for k in ("barbless", "hook_count", "max_gap_mm", "min_gap_cm", "lure", "max_lines",
              "max_flies", "max_weight_kg"):
        v = x.get(k)
        if v is not None and v != "" and v is not False:
            out.append((k, str(v)))
    if x.get("bait"):
        out.append(("bait", str(x["bait"])))
    if x.get("water"):
        out.append(("water", str(x["water"])))
    return tuple(sorted(out))


def _only_when(x: dict) -> str:
    parts = []
    if x.get("when_targeting"):
        from pipeline.regs.table.build import name
        parts.append("when fishing for " + ", ".join(name(c).lower() for c in x["when_targeting"]))
    if x.get("when_open"):
        parts.append("where open to fishing")
    if x.get("reason") and str(x.get("type")) in RIG_TYPES and x.get("method") is None \
            and "downrigger" not in str(x["reason"]):
        parts.append(str(x["reason"]))
    return "; ".join(parts)


def _lift_targets(x: dict, ids: Iterable[str]) -> FrozenSet[str]:
    ids = list(ids)
    out = set()
    for ex in (x.get("exempts") or []):
        if ex.get("target"):
            out |= {i for i in ids if rule_part(i) == ex["target"]}
        if ex.get("default_id"):
            out |= {i for i in ids if rule_part(i).split(".")[0] == ex["default_id"]}
    out.discard(rid(x))                  # a rule cannot lift itself
    return frozenset(out)


def terms_of(rules: List[dict], water_kind: str, here: FrozenSet[str] = frozenset(),
             label: str = "") -> Tuple[List[Term], Dict[str, Ledger], Dict[str, list], List[dict]]:
    """Raw rules -> (terms, keep ledgers by method, lifted fish by method, unplaceable lifts).
    Nothing is settled here beyond the lifts a keep ledger needs; that is the table's job."""
    ids = [rid(x) for x in rules if is_gear(x)]
    terms: List[Term] = []
    keeps: Dict[str, List[Allowance]] = {}
    keep_lifted: Dict[str, Dict[str, str]] = {}
    lifted_fish: Dict[str, list] = {}
    unplaceable: List[dict] = []
    by_key = {rid(x): x for x in rules}
    for x in rules:
        if not is_gear(x):
            continue
        if not reaches_kind(x, water_kind):
            continue
        src = source_of(x)
        if not _bites(src, here):
            continue
        m = method_of(x)
        rigk = str(x.get("type") or "") in RIG_TYPES
        perm = x.get("permitted")
        kind = ("while_closed" if m == "__while_closed__" else
                "permit" if perm is True else "ban" if perm is False else
                "rig" if rigk else "")
        ap, prose = _applies(x, label, src.regions, kind)
        lifts = _lift_targets(x, ids)
        base = dict(method="" if (rigk and not x.get("method")) else m, source=src, applies=ap,
                    regions=src.regions, text=(x.get("label") or x.get("verbatim") or "").strip(),
                    only_when=_only_when(x), prose_extent=prose)
        if prose:
            base["only_when"] = "; ".join(p for p in (base["only_when"], prose) if p)
        # WHAT YOU MAY KEEP BY IT: a counter, in the quota table's own vocabulary.
        o = outcome_of(x.get("take"), x.get("may_target"), x.get("unlimited"), x.get("period"), False)
        if o is not None and x.get("method"):
            subj = Subject(frozenset(x.get("species") or []),
                           Origin(x["origin"]) if x.get("origin") else Origin.both,
                           size_of(x.get("over_cm"), x.get("under_cm"), take=x.get("take"),
                                   within=x.get("within"), band=bool(x.get("band")),
                                   period=x.get("period") or "daily"),
                           Water.any, m, frozenset(x.get("species_except") or []))
            keeps.setdefault(m, []).append(Allowance(subj, o, src, ap))
            terms.append(Term(kind="keep", keep=keeps[m][-1], **base))
            continue
        if lifts:
            sp = frozenset(x.get("species") or [])
            terms.append(Term(kind="lift", lifts=lifts, lifts_fish=sp, **base))
            if ap.kind == "somewhere":
                unplaceable.append({"lifter": rid(x), "note": base["text"], "where": ap.detail})
            # A lift that names fish narrows a KEEP counter — "except burbot" — and the fish it
            # frees is shown on the row as free, citing the sentence.
            if sp and ap.kind != "somewhere":
                for t in lifts:
                    tgt = by_key.get(t)
                    if tgt is not None and tgt.get("method"):
                        lifted_fish.setdefault(method_of(tgt), []).append((sp, terms[-1]))
            elif not sp and ap.kind != "somewhere":
                for t in lifts:
                    tgt = by_key.get(t)
                    if tgt is not None and tgt.get("method") and tgt.get("take") is not None:
                        keep_lifted.setdefault(method_of(tgt), {})[t] = LIFTED
            if not (x.get("allowed") or kind in ("permit", "ban")):
                continue                     # the lift is all this rule is
        if kind == "rig":
            dim = str(x.get("dimension") or "")
            head = dim.split(":")[0]
            key = ("bait:" + (dim.split(":", 1)[1].split("/")[0] if ":" in dim else "any")
                   if head == "bait" else head)
            terms.append(Term(kind="rig", topic=RIG_TOPIC.get(head, "Also"), key=key,
                              says=_says(x), allows=bool(x.get("allowed")), **base))
        elif kind in ("permit", "ban", "while_closed"):
            terms.append(Term(kind=kind, says=_says(x), **base))
        else:
            # A method rule with neither permission nor content: a note on the method.
            terms.append(Term(kind="rig", topic="Also", key="unspecified", says=_says(x), **base))
    # THE KEEP LEDGERS: one per method, settled by the quota table's own rules, with the fish a
    # lift takes out of a counter subtracted from its subject before it settles.
    ledgers: Dict[str, Ledger] = {}
    for m, alws in keeps.items():
        narrow: Dict[str, set] = {}
        for sp, lifter in lifted_fish.get(m, []):
            for t in lifter.lifts:
                narrow.setdefault(t, set()).update(sp)
        fixed = []
        for a in alws:
            if a.rule_id in narrow:
                s = a.scope
                whole = Subject(frozenset(narrow[a.rule_id])).covers(Subject(s.fish, excepts=s.excepts))
                if whole:
                    keep_lifted.setdefault(m, {})[a.rule_id] = LIFTED
                else:
                    a = replace(a, scope=replace(s, excepts=s.excepts | frozenset(narrow[a.rule_id])))
            fixed.append(a)
        ledgers[m] = Ledger(fixed, lifted=keep_lifted.get(m, {}), water_kind=water_kind, label=label)
    return terms, ledgers, lifted_fish, unplaceable


def _bites(src: Source, here: FrozenSet[str]) -> bool:
    if not src.regions or not here:
        return True
    return any(h == r or h.startswith(r) for h in here for r in src.regions)


# -- the province's permits, for the sentence a method with no rule here deserves ---------
@lru_cache(maxsize=None)
def provincial_permits() -> Dict[str, List[Term]]:
    out: Dict[str, List[Term]] = {}
    for x in all_rules():
        if not is_gear(x) or x.get("permitted") is not True or not x["entry"].startswith("zp:"):
            continue
        src = source_of(x)
        t = Term(method_of(x), "permit", src, ALWAYS, src.regions, x.get("label") or "",
                 says=_says(x))
        out.setdefault(t.method, []).append(t)
    return out


# -- closures, from the quota ledger ------------------------------------------------------
def closures_of(rules: List[dict], water_kind: str, here: FrozenSet[str], label: str) -> List[Allowance]:
    """Every "No Fishing" that shuts this water, as the quota table settled it — in force,
    not a place nobody can draw, not an hour of the day."""
    from pipeline.regs.table.build import ledger
    L = ledger(rules, water_kind, here, label)
    return [a for a in L.allowances
            if a.kind == "closed" and shuts_the_water(a.scope) and L.in_force(a)
            and not a.applies.within_day and a.derived_from is None]


# -- the two stages -----------------------------------------------------------------------
def base_rules(rules: List[dict]) -> List[dict]:
    return [x for x in rules if is_gear(x) and source_of(x).is_base]


def _key(rules: List[dict]) -> Tuple[str, ...]:
    return tuple(sorted(rid(x) for x in rules))


@lru_cache(maxsize=None)
def _base(key: Tuple[str, ...], water_kind: str, here: FrozenSet[str],
          province: bool = False) -> MethodTable:
    want = set(key)
    rs = [x for x in all_rules() if rid(x) in want]
    terms, ledgers, lifted_fish, unplaceable = terms_of(rs, water_kind, here)
    return MethodTable(terms, water_kind=water_kind, here=here, keep_ledgers=ledgers,
                       elsewhere=provincial_permits(), lifted_fish=lifted_fish,
                       province=province)


def base(rules: List[dict], water_kind: str, here: FrozenSet[str] = frozenset()) -> MethodTable:
    """STAGE 1 for a section: the standing table its region-wide gear rules make."""
    return _base(_key(base_rules(rules)), water_kind, frozenset(here))


def region_base(region: str, water_kind: str, extra: List[dict] = ()) -> MethodTable:
    """STAGE 1 for a REGION, from the whole corpus: the province's region-wide gear rules that
    reach this region, and the region's own. What the printed synopsis lets a person check.

    HAIDA GWAII IS AN AREA, NOT A CHAPTER. Its rules live in Region 1's chapter under `hg_`
    and are area-scoped, so both halves of the old filter missed them: `is_base` is false and
    `"z" + region` is `"z1hg"`, a prefix no entry has. Haida Gwaii's gear table was therefore
    the province's rules and nothing else, and its own bait ban — all streams in Management
    Units 6-12 and 6-13, Nov 1 – Apr 30 — was on no table in the corpus. The page told a
    reader "Roe may be used" for the half of the year the book bans bait there. The quota side
    has always read `z1:hg_` specially; this now does the same.

    `extra` lays one named AREA's gear rules on top of the region's. They settle by rank like
    any other term, and an area is closer than a region, so a bait ban inside it wins."""
    here = frozenset({region})
    if region == "1hg":
        rs = [x for x in all_rules() if is_gear(x) and
              (x["entry"].startswith("z1:hg_") or
               (x["entry"].startswith("zp:") and source_of(x).is_base and _bites(source_of(x), here)))]
    else:
        rs = [x for x in all_rules() if is_gear(x) and source_of(x).is_base
              and (x["entry"].startswith("zp:") or x["entry"].split(":")[0] == "z" + region)
              and _bites(source_of(x), here)]
    have = {rid(x) for x in rs}
    rs = rs + [x for x in extra if is_gear(x) and rid(x) not in have]
    return _base(_key(rs), water_kind, here)


def provincial_base(water_kind: str) -> MethodTable:
    """Every province-wide gear rule — INCLUDING the ones the book limits to named regions.

    Those used to be dropped here (`not source_of(x).regions`), and the cost was severe: every
    rule restricting the spear carries a region extent — only non-game fish, burbot in Regions
    3/5/6/7/8, none at all in Regions 1/2/4 — so the only survivor was the rule that merely
    DESCRIBES the method ("propelled by a spring, an elastic band, compressed air, a bow, or by
    hand"), and the province's row read "Spear fishing is permitted", with nothing further, for
    the most restricted method in the book. Set lining lost all four of its conditions the same
    way and read as simply not allowed.

    They are kept out of `standing` instead (see `MethodTable.province`), so a rule for Regions
    6 and 7A neither vanishes nor speaks for the whole province: it is shown, with its regions.
    """
    rs = [x for x in all_rules() if is_gear(x) and source_of(x).is_base and x["entry"].startswith("zp:")]
    return _base(_key(rs), water_kind, frozenset(), province=True)


def table(rules: List[dict], water_kind: str = "stream", here: FrozenSet[str] = frozenset(),
          label: str = "") -> MethodTable:
    """STAGE 2. The base, with this section's overrides and the quota ledger's closures."""
    here = frozenset(here)
    b = base(rules, water_kind, here)
    terms, ledgers, lifted_fish, unplaceable = terms_of(rules, water_kind, here, label)
    over = [t for t in terms if not t.is_base]
    lf = {m: list(v) for m, v in b.lifted_fish.items()}
    for m, v in lifted_fish.items():
        lf.setdefault(m, []).extend(x for x in v if x not in lf[m])
    return b.overlay(over, here=here, label=label, keep_ledgers=ledgers, lifted_fish=lf,
                     closures=closures_of(rules, water_kind, here, label))


# -- compatibility with the quota test-suite's one gear test ------------------------------
def rungs_for(method: str, rules: List[dict], here=frozenset(), water_kind: str = "stream") -> List[Term]:
    """The terms of one method on one section. Kept because `test_regs_table.py` calls it."""
    T = table(rules, water_kind, frozenset(here))
    return [t for t in T.terms if t.method == method or (not t.method and method in HOOK_AND_LINE)]


def resolve_method(method: str, terms: List[Term], here=frozenset()) -> Optional[MethodRow]:
    """One way of fishing, answered. `constraints` are the rig terms binding here."""
    T = MethodTable(terms, here=frozenset(here), elsewhere=provincial_permits())
    row = MethodRow(T, method)
    row.constraints = [t for t in T._for(method, "rig") if T.in_force(method, t)]     # type: ignore[attr-defined]
    return row


# -- a terminal view ----------------------------------------------------------------------
def show(water: str, run: int = 0, on: Optional[Tuple[int, int]] = None):
    from pipeline.regs.table.build import D, section_kind, section_label, name
    from pipeline.regs.table.rows import rows
    rules, here = corpus_section(water, run)
    kind = section_kind(water)
    T = table(rules, kind, here, section_label(water, run))
    print(f"\n{'═'*84}\n  {water} · stretch {run + 1} · {kind} · Region {', '.join(sorted(here))}\n{'═'*84}")
    c = T.shut(on)
    if c is not None:
        print(f"  CLOSED — no fishing here: {c.source.words()} “{c.source.verbatim[:70]}”")
    for r in T.rows():
        s = r.standing(on)
        print(f"\n  ┌ {r.name.upper():28s} {r.verdict_word(on).upper()}   ({s.source.words()})")
        print(f"  │   “{(s.source.verbatim or s.text)[:76]}”")
        for topic, ts in r.rig(on).items():
            print(f"  │ {topic}")
            for t in ts:
                ex = "; ".join(f"except {c.plain()} ({c.source.who})" for c in r.carves(t))
                print(f"  │    {t.plain()[:48]:48s} {t.source.words()[:30]:30s}"
                      f"{('  ' + t.applies.detail) if t.applies.detail else ''}{('  ' + ex) if ex else ''}")
        L = r.keep()
        if L is not None:
            for kr in rows(L, name):
                h = kr.headline()
                if h: print(f"  │ keep: {kr.heading(name):30s} {h.word():8s} ({h.source.who})")
        for fish, lifter in T.lifted_fish.get(r.method, []):
            print(f"  │ keep: {', '.join(name(f) for f in fish):30s} free here — {lifter.source.who}")
        for t, st in r.folded():
            print(f"  │  ~ {t.plain()[:40]:40s} {st[:44]}")
        print("  └")


if __name__ == "__main__":
    show(sys.argv[1] if len(sys.argv) > 1 else "Fraser River",
         int(sys.argv[2]) if len(sys.argv) > 2 else 0,
         tuple(int(x) for x in sys.argv[3].split("-")) if len(sys.argv) > 3 else None)
