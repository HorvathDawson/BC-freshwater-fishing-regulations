"""Builders: every rule (`build_rules`) and every part (`produce_parts`), with the checks on the words they
say (`when_problems`, `size_problems`), the closers that open a water, and the paper-licence duty."""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence, Tuple
from pipeline.deliver.answers.common import AnswersError, when_words
from pipeline.deliver.answers.display.rule import _num, cut_points, rule_facts
from pipeline.deliver.answers.display.part import (
    Water, part_km, part_label, part_labels, part_place, picker, runs_label
)


def build_rules(B) -> List[dict]:
    """The per-rule facts, in the export's `rules` order (`common.rule_index`)."""
    order = sorted(B.index, key=B.index.__getitem__)
    out = [rule_facts(B.rules[k], B.rules) for k in order]
    bad = [p for k, f in zip(order, out)
           for p in when_problems(f"{k[0]}::{k[1]}", B.rules[k], f) + size_problems(f"{k[0]}::{k[1]}", B.rules[k], f)]
    if bad:
        raise AnswersError(f"display: {len(bad)} rule title/plain words contradict the rule — " + "; ".join(bad[:10]))
    return out


def when_problems(rid: str, x: dict, facts: dict) -> List[str]:
    """T104 GATE: a rule held some HOURS or on some WEEKDAYS names them in its title and in its plain sentence
    (where it has one) — in any of `when_words`' styles (a title from the label says "on Saturdays and Sundays")."""
    w = x.get("when") or {}
    out = []
    for part in ("hours", "weekdays"):
        if not w.get(part):
            continue
        said = sorted({when_words({part: w[part]}, s) for s in ("title", "plain", "label")})
        for f in ("title", "plain"):
            text = facts.get(f)
            if text is not None and not any(s in text for s in said):
                out.append(f"{rid}: the {f} {text!r} does not name its {part} ({' / '.join(said)})")
    return out


_CM_SAID = re.compile(r"(\d+(?:\.\d+)?) cm \(\d+ in\)")
_LONGER_NONE_OVER = re.compile(r"(\d+(?:\.\d+)?) cm \(\d+ in\) or longer, none over (\d+(?:\.\d+)?) cm")
#: (Q63) the must-be style's kept range, "must be 20–30 cm (8–12 in)"
_MUST_BE_RANGE = re.compile(r"must be (\d+(?:\.\d+)?)–(\d+(?:\.\d+)?) cm")


def size_problems(rid: str, x: dict, facts: dict) -> List[str]:
    """T103 GATE: every size a rule's title and plain sentence name is one of the cut points its `bands` cut at,
    and no "X or longer, none over Y" (nor "must be X–Y cm", Q63) has Y at or below X (a size rule nobody can meet)."""
    if not x.get("lengths"):
        return []
    cuts = {float(c) for c in cut_points(x["lengths"])}
    out = []
    for f in ("title", "plain"):
        text = facts.get(f) or ""
        for m in _CM_SAID.finditer(text):
            if float(m.group(1)) not in cuts:
                out.append(f"{rid}: the {f} {text!r} names {m.group(1)} cm, not one of the cut points "
                           f"{[_num(c) for c in sorted(cuts)]}")
        for m in _LONGER_NONE_OVER.finditer(text):
            if float(m.group(2)) <= float(m.group(1)):
                out.append(f"{rid}: the {f} {text!r} says none over {m.group(2)} cm, below its floor of "
                           f"{m.group(1)} cm")
        for m in _MUST_BE_RANGE.finditer(text):
            if float(m.group(2)) <= float(m.group(1)):
                out.append(f"{rid}: the {f} {text!r} says must be {m.group(1)}–{m.group(2)} cm, its top at or "
                           f"below its bottom")
    return out


def unless_opened_here(closing: Dict[str, List[str]], words: Dict[str, Optional[str]],
                       superior: Sequence[str] = ()) -> List[str]:
    """Z12/Q41: of the closures that close the water at a segment (`closing`, {rule: [fish]}), those
    whose "unless opened" proviso (`words`, `unless_opened`) is the answer here — every one, unless
    a closure of the SAME (superior) authority printed with NO proviso closes every fish it closes
    beside it: a national park RESERVE's own closure ("All fresh waters within Pacific Rim … are
    closed to fishing") leaves the park closure's "unless opened by Parks Canada" unsaid, plainly
    closed. A provincial closure beside it (Region 4's spring stream closure in Kootenay or Yoho)
    does not: both are shown, each in its own words."""
    plain_shut = [set(fs) for r, fs in closing.items() if not words.get(r) and r in superior]
    return sorted(r for r, fs in closing.items()
                  if words.get(r) and not any(set(fs) <= p for p in plain_shut))


def closers_of(closing: Sequence[Tuple[int, Sequence[str]]]) -> List[int]:
    """RULINGS P46: THE CLOSURES THAT CLOSE THE WATER on a closed day — of the decided
    `closing` ([[rule, [game fish]]], in its order), page v36 closersAt's choice, decided here and only read by the
    app: those closing the most game fish; and, when no single closure closes every game fish any of them closes,
    each that is not wholly inside a closure of more fish. A one-fish closure beside a blanket one (the Province's
    white sturgeon closure under Region 2's spring closure) closes nothing more and is not one of them."""
    if not closing:
        return []
    top = max(len(fs) for _r, fs in closing)
    every = len({f for _r, fs in closing for f in fs})
    return [r for r, fs in closing
            if len(fs) == top or (top < every and not any(
                len(gs) > len(fs) and set(fs) <= set(gs) for _g, gs in closing))]


def closed_all_year(store, key_ix: int) -> bool:
    """Closed to every game fish on all 366 days: every reading of the rule key is closed
    (`reading.closed`, the verdicts' one predicate — the status index's own answer)."""
    return all(r.closed for r in store.readings(key_ix))


def paper_licence(B, key, kind: Optional[str], store=None) -> List[str]:
    """The record duties that reach this part (7.7 step 7, "Keep a hatchery steelhead? Carry your
    paper licence"): its set's rules with `record_retention` for this kind of water — the
    `records` of `zp:licence_administration#carry_paper_licence` that bind here. RULINGS P45 (the
    line named hatchery steelhead on the Ape and the Bella Coola, closed to steelhead): with the
    verdicts, only a record rule whose fish may be KEPT here on some day (`record_kept`) — a duty to
    record a fish nobody may keep here is no duty here."""
    out = []
    for e, r, _ in B.sets[key.set_id]:
        x = B.rules[(e, r)]
        if not x.get("record_retention"):
            continue
        if kind and x.get("water") and x["water"] != kind:
            # M3: a rule's `water` is enforced by its binding; one bound to the other kind of
            # water is a binding defect, never filtered here
            raise AnswersError(f"display: {e}::{r} is for {x['water']}s, bound to a {kind}")
        out.append(f"{e}::{r}")
    if store is not None:
        out = [rid for rid in out if record_kept(B, store, B.key_ix[key], rid)]
    return out


def record_kept(B, store, key_ix: int, rid: str) -> bool:
    """The record rule's fish may be KEPT here on some day: in some stored verdict of the key, for one of the rule's
    fish (and its origin, where the rule names one), a keeping quota SPEAKS — a rule that keeps a fish (`rules.keeps`, the
    one predicate the reader's competition asks) and is no method's or circumstance's (`while` / `caught`). The record duty itself always speaks (it is no keeper and
    competes with nothing), so it cannot say this; the closure that shuts a fish displaces its keepers in the reader
    (RU-7, step 4c), so a speaking quota is a fish someone may keep. Where no keeping quota of the book takes part for
    the fish at all (adult chinook: DFO sets it), the book cannot say it is shut: the duty stays."""
    from pipeline.deliver.bundle.rules import keeps
    x = B.rules[tuple(rid.split("::", 1))]
    fish = set(x.get("species") or [])
    origin = x.get("origin") or None
    ids = store.rule_ids
    from pipeline.deliver import types as T
    took_part = False
    for (_rd, f, o), v in sorted(store.frames(key_ix).items()):
        if f not in fish or (origin is not None and o != origin):
            continue
        for r, st, *_rest in store.rows(v):
            if ids[r] == rid:
                continue
            y = B.rules.get(tuple(ids[r].split("::", 1))) or {}
            if keeps(y) and not y.get("while") and not y.get("caught"):
                if st in T.SPEAKER_CODES:
                    return True
                took_part = True
    # no keeping quota of the book's takes part for the fish at all (adult chinook: the book sets no chinook quota, DFO
    # does): the book cannot say it is shut here, so the duty stays — only a fish whose keepers ALL lost is dropped
    return not took_part


def _by_grp(item_id: str, w: dict, doc: dict):
    """T517: the water as its RULE-SET GROUPS (what a part was before T517): one pseudo-part per `grp`, its parts'
    runs together (mouth-upward km order kept as `compose_runs` lists them: upstream first) and sections summed —
    `(Water over them, {grp: [export part index]})`. The picker, the full label, the place and the hint read it."""
    members: Dict[int, List[int]] = {}
    for ex, q in enumerate(w["parts"]):
        members.setdefault(q["grp"], []).append(ex)
    if sorted(members) != list(range(len(members))):
        raise AnswersError(f"display: {item_id}'s part groups are not 0..n-1: {sorted(members)}")
    merged = []
    for g in range(len(members)):
        qs = [w["parts"][ex] for ex in members[g]]
        tagged = [(k, dict(r)) for k, q in enumerate(qs) for r in q.get("runs") or []]
        tagged.sort(key=lambda t: (t[1].get("km_from") is None, -(t[1].get("km_from") or 0)))
        # pieces of ONE run (the salmon chain cut it, T517) meet end to end where one PART ends and the next begins:
        # said as the one run they were before T517. Two runs of one part stay two (a border, a lake between).
        joined: List[tuple] = []
        stem: Optional[int] = None                      # the index in `joined` of the last main-stem run
        for k, r in tagged:
            last = joined[stem] if stem is not None else None
            if last and last[0] != k and not r.get("branch") and last[1].get("to") \
                    and last[1].get("to") == r.get("from") and last[1].get("km_to") == r.get("km_from") \
                    and set(last[1]) == set(r):
                last[1]["to"], last[1]["km_to"] = r.get("to"), r.get("km_to")
                joined[stem] = (k, last[1])
                continue
            joined.append((k, r))
            if not r.get("branch"):
                stem = len(joined) - 1
        merged.append({**qs[0], "runs": [r for _, r in joined], "sections": sum(q["sections"] for q in qs)})
    return Water(item_id, {**w, "parts": merged}, doc), members


def _crossings(path: str) -> Dict[tuple, list]:
    """{(item id, crossing token): [(km, the token of what stands there)]} — the bundle's `crossing_near` — and
    {(item id, "part", part_ix): near token} — its `part_near`."""
    from pipeline.deliver.answers.common import connect
    db = connect(path)
    try:
        out: Dict[tuple, list] = {}
        for iid, t, km, near in db.execute("SELECT i.item_id, c.token, c.km, c.near FROM crossing_near c "
                                           "JOIN item i ON i.ord = c.ord ORDER BY i.item_id, c.token, c.km"):
            out.setdefault((iid, t), []).append((km, near))
        for iid, p, near in db.execute("SELECT i.item_id, n.part_ix, n.near FROM part_near n JOIN item i "
                                       "ON i.ord = n.ord ORDER BY i.item_id, n.part_ix"):
            out[(iid, "part", p)] = near
        return out
    finally:
        db.close()


def _water_ends_km(w: dict) -> Tuple[Optional[float], Optional[float]]:
    """(T697) The km of a water's own lower and upper end: the lowest `km_to` and the highest `km_from` of the main-stem
    runs of ALL its parts (outside B.C. too) — where its stem begins and ends, cut or not. (None, None): no measured
    main-stem run."""
    from pipeline.deliver.answers.display.part import _js
    main = [r for p in w.get("parts") or [] for r in p.get("runs") or []
            if not r.get("branch") and not _js(r.get("polygon"))]
    los = [float(r["km_to"]) for r in main if r.get("km_to") is not None]
    his = [float(r["km_from"]) for r in main if r.get("km_from") is not None]
    return (min(los) if los else None, max(his) if his else None)


def _names(W: "Water", crossings: Dict[tuple, List[Tuple[float, str]]]) -> List[dict]:
    """T517 (owner 2026-11-09) every part's name facts: its two END TOKENS (`lo`, `hi`; the app composes "<lower> →
    <upper>" from `display.ends`, `spanName`). A part with NO MAIN STEM (a lake, side channels only) ships none: the
    app says its water's name — unless the water has several such parts, which then carry their side channels' ends
    (`branch_end_tokens`). NO TWO PARTS OF ONE WATER SAY THE SAME NAME, none is numbered and none says "Within": an
    end that is a CROSSING (`topology.is_crossing`: an area's boundary, the B.C. border, a region line, a river
    polygon, an unnamed lake) is said NEAR what stands there (`topology.crossing_near`: the nearest named water
    feature, at the border the nearest B.C. town) — else at its km — wherever the part's two ends say the same words
    or its name is another part's; the part across that place says the same token (the mark there names it).
    Anything still said twice stops the build (`name_check`)."""
    from pipeline.deliver.answers.display.part import NEAR, PLACE, at_place, branch_end_tokens, end_name_of, \
        end_tokens, end_words
    from pipeline.deliver.bundle.topology import is_crossing
    toks = [end_tokens(W.doc, p) for p in W.parts]
    # a part with no main stem says its water's name — unless the water has several such parts, or it is a piece of a
    # run the salmon chain cuts (its joined name runs from the first piece's lower end to the last's upper end)
    run_n: Dict[tuple, int] = {}
    for p in W.parts:
        run_n[(p["grp"], p["run"])] = run_n.get((p["grp"], p["run"]), 0) + 1
    several = sum(1 for t in toks if t is None) > 1
    toks = [t if t is not None or run_n[(p["grp"], p["run"])] < 2 else branch_end_tokens(p)
            for p, t in zip(W.parts, toks)]
    ends: List[Optional[list]] = [[t[0], t[1]] if t else None for t in toks]
    if several:
        # several parts with no main stem: each said "<water> near <what it lies nearest>" (`topology.part_near`;
        # lo = hi = "near:<token>"), the one thing that tells two side channels of unmeasured ends apart
        for i, (p, t) in enumerate(zip(W.parts, toks)):
            if t is None:
                near = crossings.get((W.id, "part", W.export_ix[i]))
                if near is None:
                    raise AnswersError(f"display: {W.id} part {W.export_ix[i]}: one of several parts with no main "
                                       f"stem, and no named water or B.C. town near it to tell it apart")
                ends[i] = [f"near:{near}", f"near:{near}"]
    water = W.w["name"]

    def name(i: int) -> str:
        if not ends[i]:
            return water
        if ends[i][0] == ends[i][1] and ends[i][0].startswith("near:"):
            w = end_words(W.doc, ends[i][0], "from")
            return f"{water} {w[:1].lower() + w[1:]}"
        return end_name_of(W.doc, *ends[i])

    def base(e: str) -> str:
        return e.split(NEAR)[0].split(PLACE)[0]

    def place(tok: str, km: float) -> str:
        if NEAR in tok:
            return at_place(base(tok), km)
        got = [n for k, n in crossings.get((W.id, tok), ()) if abs(k - km) <= 0.05]
        return f"{tok}{NEAR}{got[0]}" if got else at_place(tok, km)

    def requalify(i: int, side: int, km: float) -> None:
        """Say part i's end `side` at its place — and the same end of every part that meets it there."""
        new = place(ends[i][side], km)
        b = base(ends[i][side])
        for j, t in enumerate(toks):
            if ends[j] is None:
                continue
            for s2, k2 in ((0, t[2]), (1, t[3])):
                if base(ends[j][s2]) == b and k2 is not None and abs(k2 - km) <= 0.05:
                    ends[j][s2] = new
    # (T697, owner 2026-11-12: "it is mouth to cut the cut to head") an UNNAMED LAKE at the water's own top or bottom is
    # no cut: the water's own end, said as every water's is ("Source", "Mouth"). Anywhere else it is a place two parts
    # meet with no name of its own: always said near what stands there ("Unnamed lake near X"), else at its km.
    # GUARDED LAST RESORT (owner 2026-11-12 "Yes, read the real boundary", RULINGS P55 follow-up): the bundle already names the
    # real boundary at a lake edge — the cut aliased there, the region line, the B.C. border (`bundle.spans.end_token`,
    # `lines_at_lake_edges`) — so on the export no part end says "Unnamed lake …"; `test_part_names_unnamed_lake.py`
    # proves zero province-wide. This only keeps a future bare lake end said at its place, never "a lake".
    lo_w, hi_w = _water_ends_km(W.w)
    for i, t in enumerate(toks):
        if t and ends[i]:
            if ends[i][1] == "lake_outlet" and hi_w is not None and t[3] is not None and abs(t[3] - hi_w) <= 0.05:
                ends[i][1] = "source"
            if ends[i][0] == "lake_inlet" and lo_w is not None and t[2] is not None and abs(t[2] - lo_w) <= 0.05:
                ends[i][0] = "mouth"
    for i, t in enumerate(toks):                      # a river polygon has no name of its own: always said near
        if t:
            for side, km in ((0, t[2]), (1, t[3])):
                if ends[i][side] in ("polygon", "lake_inlet", "lake_outlet"):
                    requalify(i, side, km)
    for _ in range(3):
        said = [name(i) for i in range(len(W.parts))]
        clash = [i for i in range(len(W.parts)) if ends[i] and not ends[i][0].startswith("near:") and (
                 said.count(said[i]) > 1 or end_words(W.doc, ends[i][0], "to") == end_words(W.doc, ends[i][1], "from"))]
        if not clash:
            break
        for i in clash:
            t = toks[i]
            for side, km in ((0, t[2]), (1, t[3])):
                if is_crossing(base(ends[i][side])) and PLACE not in ends[i][side]:
                    requalify(i, side, km)
    return [{"lo": ends[i][0] if ends[i] else None, "hi": ends[i][1] if ends[i] else None}
            for i in range(len(W.parts))]


def produce_parts(B, doc: dict, keys: Sequence[tuple], parts: Dict[str, list],
                  rule_ref=None, lic_ref=None, log=print, store=None) -> Dict[str, dict]:
    """PURE: every named water's part facts, `{item_id: {"parts": [facts | null per EXPORT part],
    "picker": {...}, "unresolved_licensing": [record]}}`, a part's facts {order, label, runs,
    place, km, closed_all_year, paper_licence}; null for a part outside B.C. The part keys
    are the keying module's (`common.part_keys`); the picker names parts by export index."""
    from pipeline.deliver.answers.common import rule_key
    rule_ref = rule_ref or (lambda k: k)
    lic_ref = lic_ref or (lambda k: k)
    closed_memo: Dict = {}
    if store is None:
        raise AnswersError("display: the part facts need the verdicts (`closed_all_year`)")
    out: Dict[str, dict] = {}
    unresolved: Dict[str, List[str]] = {}
    for k, x in (doc.get("licensing") or {}).items():
        if x.get("placement") == "unresolved":
            unresolved.setdefault(x["entry_id"], []).append(k)
    crossings = _crossings(B.path)
    for item_id in sorted(parts):
        w = doc["waters"][item_id]
        W = Water(item_id, w, doc)
        if not W.parts:
            continue
        # T517: a part is one connected run; the PICKER, the full label, the place and the hint stay per rule-set
        # group (`grp`, what a part was before T517: its runs together), so the picker's length is unchanged
        W, Wg, members = W, *_by_grp(item_id, w, doc)
        labels_g = part_labels(Wg)
        gi = {g: k for k, g in enumerate(Wg.export_ix)}
        labels = [labels_g[gi[p["grp"]]] for p in W.parts]
        # T517 (owner 2026-11-09): a part's name is its two end tokens; the app composes the words of any span shown
        names = _names(W, crossings)
        closed = []
        facts: List[Optional[dict]] = [None] * len(w["parts"])
        for i, (ex, p) in enumerate(zip(W.export_ix, W.parts)):
            rk = rule_key(keys[parts[item_id][ex]])
            if rk not in closed_memo:
                closed_memo[rk] = closed_all_year(store, B.key_ix[rk])
            closed.append(closed_memo[rk])
            k = gi[p["grp"]]
            facts[ex] = {
                "order": i, "label": labels[i], **names[i], "runs": runs_label(doc, p),
                "place": part_place(Wg, k) or None,          # null: no entry heading
                "km": part_km(p),
                "closed_all_year": closed[-1],
                "paper_licence": [rule_ref(r) for r in paper_licence(B, rk, w.get("kind"), store=store)]}
        shut = {p["grp"]: c for p, c in zip(W.parts, closed)}
        pk = picker(Wg, labels_g, [shut[g] for g in Wg.export_ix])
        for c in pk["choices"]:
            c["parts"] = sorted(x for g in c["parts"] for x in members[g])
        out[item_id] = {"parts": facts, "picker": pk,
                        "unresolved_licensing": [lic_ref(k) for k in sorted(
                            k for e in w.get("entries") or [] for k in unresolved.get(e, []))]}
    log(f"  parts: {sum(1 for v in out.values() for f in v['parts'] if f)} parts of {len(out)} "
        f"waters; {sum(closed_memo.values())} of {len(closed_memo)} rule keys closed all year")
    return out
