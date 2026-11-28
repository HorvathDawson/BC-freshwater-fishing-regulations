"""Builders: every rule (`build_rules`) and every part (`produce_parts`), with the checks on the words they
say (`when_problems`, `size_problems`), the closers that open a water, and the paper-licence duty."""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence, Tuple
from pipeline.deliver.answers.common import AnswersError, when_words
from pipeline.deliver.answers.display.rule import _num, cut_points, rule_facts
from pipeline.deliver.answers.display.part import (
    AREA_HELD, SIDE_APART, SIDE_AREA, STEM_LICENSING, Water, own_end, part_km, part_label, part_labels, part_place, picker,
    runs_label, side_only, stem_licensing, water_ends_km
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
    # (T679 #17) the group's runs are the export's (`grp_runs`: pieces of one run the salmon chain cut, joined once
    # there, `export.read.group_runs`); a one-part group's are its part's. A group of 2+ parts with none is refused
    grp_runs = w.get("grp_runs")
    merged = []
    for g in range(len(members)):
        qs = [w["parts"][ex] for ex in members[g]]
        if len(qs) > 1 and grp_runs is None:
            raise AnswersError(f"display: {item_id}'s group {g} has {len(qs)} parts and the export joined no runs "
                               "(`grp_runs`): rebuild the export")
        runs = [dict(r) for r in (grp_runs[g] if grp_runs is not None else qs[0].get("runs") or [])]
        merged.append({**qs[0], "runs": runs, "sections": sum(q["sections"] for q in qs),
                       # (T801 item 5(1)) each member's licensing set where it runs on the main stem (`part_place`)
                       STEM_LICENSING: stem_licensing(qs),
                       # (T1172) a group of side-channel parts: apart if any member is; its area only if all share one
                       SIDE_APART: any(q.get(SIDE_APART) for q in qs),
                       SIDE_AREA: qs[0].get(SIDE_AREA) if all(q.get(SIDE_AREA) == qs[0].get(SIDE_AREA) for q in qs)
                       else None,
                       # (T1199 PR 2) the areas its members carry: a group is one rule set, so a stretch of it between
                       # two crossings of an area is the member's that carries it (Blaney Creek: one member in each of
                       # two restricted lands, one rule set)
                       AREA_HELD: sorted(set().union(*(set(q.get(AREA_HELD) or ()) for q in qs)))
                       if all(AREA_HELD in q for q in qs) else None})
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
        # (T1140) the area a part with no main stem lies inside (`bundle.side_channels.inside_rows`): (area, how, whether
        # the atlas cuts at it)
        for iid, p, area, how, cut in db.execute("SELECT i.item_id, n.part_ix, n.area, n.how, n.cut FROM part_inside n "
                                                 "JOIN item i ON i.ord = n.ord ORDER BY i.item_id, n.part_ix"):
            out[(iid, "inside", p)] = (area, how, bool(cut))
        # (T1140 proposal) the own name of a part of added city lines (`bundle.side_channels.own_name_rows`)
        for iid, p, nm, lb in db.execute("SELECT i.item_id, n.part_ix, n.name, n.label FROM part_own_name n JOIN item i "
                                         "ON i.ord = n.ord ORDER BY i.item_id, n.part_ix"):
            out[(iid, "own", p)] = (nm, lb)
        # (T1199 PR 2) the cut areas whose rule each part carries (`bundle.side_channels.area_held_rows`)
        for iid, p, area in db.execute("SELECT i.item_id, h.part_ix, h.area FROM part_area_held h JOIN item i "
                                       "ON i.ord = h.ord ORDER BY i.item_id, h.part_ix, h.area"):
            out.setdefault((iid, "held", p), set()).add(area)
        from pipeline.deliver.answers.display.part import area_key
        out[("cut_areas",)] = {area_key(a) for (a,) in db.execute("SELECT area FROM cut_area")}
        out.update(_flows_into(db))
        return out
    finally:
        db.close()


def _flows_into(db) -> Dict[tuple, str]:
    """(T1140) {(item id, "flows_into", part_ix): the end token of the named water a part of side channels only flows into}
    where its flow pointer (`section_down`) leaves the water for ONE named lake (`lake_inlet:<item>`) or stream
    (`confluence:<item>`): Theodosia River's channel into Olsen Lake. A part reaching none, or two, has no row."""
    secs: Dict[tuple, List[int]] = {}
    for iid, p, sid in db.execute("SELECT i.item_id, ps.part_ix, ps.sid FROM part_section ps JOIN item i ON i.ord = ps.ord "
                                  "JOIN section_span sp ON sp.sid = ps.sid WHERE sp.off_stem = 1 "
                                  "ORDER BY i.item_id, ps.part_ix, ps.sid"):
        secs.setdefault((iid, p), []).append(sid)
    stemful = {(iid, p) for iid, p in db.execute(
        "SELECT DISTINCT i.item_id, ps.part_ix FROM part_section ps JOIN item i ON i.ord = ps.ord "
        "JOIN section_span sp ON sp.sid = ps.sid WHERE sp.off_stem = 0")}
    down = dict(db.execute("SELECT sid, down_sid FROM section_down"))
    owner: Dict[int, List[tuple]] = {}
    for sid, iid, kind in db.execute("SELECT s.sid, i.item_id, i.kind FROM item_section s JOIN item i ON i.ord = s.ord "
                                     "ORDER BY s.sid, i.item_id"):
        owner.setdefault(sid, []).append((iid, kind))
    out: Dict[tuple, str] = {}
    for (iid, p), ss in secs.items():
        if (iid, p) in stemful:
            continue
        mine = set(ss)
        got = {(w, k) for s in ss if s in down and down[s] not in mine for w, k in owner.get(down[s], ()) if w != iid}
        if len(got) == 1:
            w, k = next(iter(got))
            out[(iid, "flows_into", p)] = (f"lake_inlet:{w}" if k in ("lake", "reservoir", "wetland")
                                           else f"confluence:{w}")
    return out


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
    import json as _json
    from pipeline.deliver.answers.display.part import AREA_HEAD, NEAR, OWN, PLACE, REVIEWED_INSIDE, SIDE, at_place, \
        branch_end_tokens, end_name_of, end_tokens, end_words, side_channels, side_words
    from pipeline.deliver.bundle.topology import is_crossing
    toks = [end_tokens(W.doc, p) for p in W.parts]
    # a part with no main stem says its water's name — unless the water has several such parts, or it is a piece of a
    # run the salmon chain cuts (its joined name runs from the first piece's lower end to the last's upper end)
    run_n: Dict[tuple, int] = {}
    for p in W.parts:
        run_n[(p["grp"], p["run"])] = run_n.get((p["grp"], p["run"]), 0) + 1
    # (T1140, owner 2026-11-25: "itshould have the real name … it will say eco reserve here") a part with no main stem
    # inside one area (the bundle's `part_inside`: in a clean-cut area EXACTLY when the cut puts all its lines there,
    # T1199 E35; elsewhere at least 95 % on its shapes) is said by it: "Squamish River inside Baynes Island Ecological Reserve" (the token `part.INSIDE`,
    # the same as both its ends)
    several = sum(1 for t in toks if t is None) > 1
    # (T1172) the NAME says only a reviewed area (`part.REVIEWED_INSIDE`): an unreviewed one the atlas cuts at stops the
    # build below; an unreviewed membership-only one is passed over here (the part's subtitle still says it,
    # `runs_label`; `produce_parts` lists it)
    inside = [crossings.get((W.id, "inside", W.export_ix[i])) if t is None else None for i, t in enumerate(toks)]
    inside = [a if a is None or (W.id, a[0], a[1]) in REVIEWED_INSIDE or a[2] else None for a in inside]
    toks = [t if t is not None or a is not None or run_n[(p["grp"], p["run"])] < 2 else branch_end_tokens(p)
            for p, t, a in zip(W.parts, toks, inside)]
    ends: List[Optional[list]] = [[t[0], t[1]] if t else [f"{AREA_HEAD[a[1]]}area:{a[0]}"] * 2 if a else None
                                  for t, a in zip(toks, inside)]
    # (T1140 PROPOSAL, coordinator 2026-11-25, under the owner's "do whatever makes our current state decent until this is
    # fixed in hardening") a part of added city lines with its own name: "Noons Creek — Turner Creek (City of Port Moody
    # ESA streams layer)" (`part.OWN`, both ends; Turner Creek itself is its own water since T1246)
    for i, t in enumerate(toks):
        own = crossings.get((W.id, "own", W.export_ix[i])) if t is None and ends[i] is None else None
        if own:
            ends[i] = [OWN + _json.dumps(list(own), separators=(",", ":"), ensure_ascii=False)] * 2
    if not several:
        # (T1140, owner 2026-11-25: "Named by its ends") the ONE part of side channels only of a water, where EVERY channel
        # has a named end (T1172 round 4): said by them ("Side channel from the Exchamsiks River confluence up to the Skeena
        # River Ecological Reserve boundary"; `part.SIDE`); one whose run names only its own mouth takes the named water its
        # flow reaches. Else its label, the plain count
        for i, (p, t) in enumerate(zip(W.parts, toks)):
            if t is None and ends[i] is None:
                chans = side_channels(p, crossings.get((W.id, "flows_into", W.export_ix[i])))
                if chans:
                    tok = SIDE + _json.dumps(chans, separators=(",", ":"))
                    try:
                        side_words(W.doc, chans)
                    except AnswersError as e:
                        raise AnswersError(f"display: {W.id} part {W.export_ix[i]}: {e} ({chans})") from None
                    ends[i] = [tok, tok]
    if several:
        # several parts with no main stem: each said "<water> near <what it lies nearest>" (`topology.part_near`;
        # lo = hi = "near:<token>"), the one thing that tells two side channels of unmeasured ends apart
        for i, (p, t) in enumerate(zip(W.parts, toks)):
            if t is None and ends[i] is None:
                near = crossings.get((W.id, "part", W.export_ix[i]))
                if near is None:
                    raise AnswersError(f"display: {W.id} part {W.export_ix[i]}: one of several parts with no main "
                                       f"stem, and no named water or B.C. town near it to tell it apart")
                ends[i] = [f"near:{near}", f"near:{near}"]
    for i, (p, t, a) in enumerate(zip(W.parts, toks, inside)):
        if a is not None and (W.id, a[0], a[1]) not in REVIEWED_INSIDE:
            raise AnswersError(f"display: {W.id} part {W.export_ix[i]} would be said {a[1]} {a[0]!r}, a pair no reviewer has "
                               f"looked at: check its picture, then add it to `part.REVIEWED_INSIDE` (T1140)")
        # (T1172, owner 2026-11-26: "Distance only to tell parts apart") a part said by its label ("2 side channels") is the
        # ONE such part of its water, so its label says no distance and needs none: no stop for a channel with no join
    water = W.w["name"]

    def name(i: int) -> str:
        if not ends[i]:
            return water
        if ends[i][0] == ends[i][1] and ends[i][0].startswith("near:"):
            w = end_words(W.doc, ends[i][0], "from")
            from pipeline.deliver.answers.display.name_check import near_name
            return near_name(water, w)
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
            if ends[j] is None or t is None:          # (T1140) an inside part has no ends of its own to meet
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
    # ONE rule for the name and the header (T711): `part.own_end`
    w_km = water_ends_km(W.w)
    for i, t in enumerate(toks):
        if t and ends[i]:
            ends[i][1] = own_end(ends[i][1], t[3], w_km)
            ends[i][0] = own_end(ends[i][0], t[2], w_km)
    # (T708 (b), owner 2026-11-13) a mouth the book places by signs is said with them: "Mouth (at the signs)"
    # (`part.MOUTH_WORDS`; the token `mouth:<item_id>`, words only)
    from pipeline.deliver.answers.display.part import MOUTH_OF, MOUTH_WORDS
    if W.id in MOUTH_WORDS:
        for i in range(len(ends)):
            if ends[i] and ends[i][0] == "mouth":
                ends[i][0] = f"{MOUTH_OF}{W.id}"
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


def held_facts(item_id: str, w: dict, crossings: Dict[tuple, object]) -> dict:
    """(T1199 PR 2) the water with the cut areas whose rule each part carries set on EVERY part (`part.AREA_HELD`,
    read by `runs_label`: a stretch between two crossings of an area is "within" it only when it carries its rule)."""
    from pipeline.deliver.answers.display.part import area_key, area_of_token
    parts = [dict(p, **{AREA_HELD: sorted(crossings.get((item_id, "held", i), ()))}) if p is not None else p
             for i, p in enumerate(w.get("parts") or [])]
    # (review) an area a stretch between two crossings names must be a cut area the bundle knows, by the one key
    # (`part.area_key`): a name that drifted, or an area not cut, would read every stretch of it "Outside" — never silent
    named = sorted({area_of_token(r["from"]) for p in parts if p for r in p.get("runs") or []
                    if str(r.get("from") or "").startswith("area:") and r["from"] == r.get("to")})
    if named:
        known = crossings.get(("cut_areas",))
        if known is None:
            raise AnswersError(f"display: {item_id}: the bundle names no cut areas (`cut_area`), so whether its "
                               f"stretches between crossings of {named} lie inside cannot be said")
        unknown = [a for a in named if area_key(a) not in known]
        if unknown:
            raise AnswersError(f"display: {item_id}: a stretch between two crossings of {unknown}: no cut area of that "
                               f"name in the bundle (`cut_area`), so its words cannot be said")
    return {**w, "parts": parts}


def within_disagreements(item_id: str, parts: Sequence[dict], labels: Sequence[str], doc: dict) -> List[str]:
    """(T1199 PR 2, RULINGS E36) every part whose words say it lies "within" an area whose rule it does not carry, or
    "outside … (between its boundaries)" one whose rule it carries — read back from the words shipped, so a later
    composer cannot drift from the rule unseen."""
    from pipeline.deliver.answers.display.part import OUTSIDE_BETWEEN, end_name
    out = []
    for i, (p, lab) in enumerate(zip(parts, labels)):
        if p is None or not lab:
            continue
        held = set(p.get(AREA_HELD) or ())
        for r in p.get("runs") or []:
            t = str(r.get("from") or "")
            if not (t.startswith("area:") and t == r.get("to")):
                continue
            area = re.sub(r" boundary$", "", end_name(doc, t))
            low = lab.lower()
            inside = t[len("area:"):].split("@")[0] in held
            said_in = f"within {area}".lower() in low
            said_out = OUTSIDE_BETWEEN.format(area=area).lower() in low
            if (said_in and not inside) or (said_out and inside):
                out.append(f"{item_id} part {i}: says {'within' if said_in else 'outside'} {area!r} but "
                           f"{'carries' if inside else 'does not carry'} its rule")
    return out


def side_facts(item_id: str, w: dict, crossings: Dict[tuple, object]) -> dict:
    """(T1172, owner 2026-11-26) the water with what its parts of side channels only say beside their channels, set on
    each such part (`part.SIDE_APART`, `part.SIDE_AREA`, read by `runs_label`), ONE rule for every water: a distance
    only where the water has more than one such part in B.C. ("Distance only to tell parts apart"); the area the bundle
    measures it inside or at, every one, reviewed or not ("The area shows in the app SUBTITLE, universally")."""
    ix = [i for i, p in enumerate(w.get("parts") or []) if p.get("ruleset") is not None and side_only(p)]
    if not ix:
        return w
    parts = list(w["parts"])
    for i in ix:
        a = crossings.get((item_id, "inside", i))
        parts[i] = {**parts[i], SIDE_APART: len(ix) > 1, SIDE_AREA: [a[0], a[1], a[2]] if a else None}
    return {**w, "parts": parts}


def unmeasured_side_parts(W: "Water", names: List[dict]) -> List[str]:
    """(T1172 round 4, owner 2026-11-26: "make it LOUD") the parts said by their label, the one such part of their water,
    with a channel no measure places: no stop (the label says no distance), but listed — such a part sorts last among its
    water's parts (`part_km`)."""
    out = []
    for i, (p, n) in enumerate(zip(W.parts, names)):
        if side_only(p) and n["lo"] is None and not p.get(SIDE_APART):
            bare = [r for r in p.get("runs") or [] if r.get("branch") and r.get("km_from") is None]
            if bare:
                out.append(f"{W.id} part {W.export_ix[i]}: {len(bare)} of "
                           f"{sum(1 for r in p.get('runs') or [] if r.get('branch'))} channel(s) with no measure")
    return out


def untold_side_parts(W: "Water") -> List[str]:
    """(T1172, review of rounds 3-4) the parts whose label NEEDS its distance — a part of side channels only on a water
    with more than one such part (`part.SIDE_APART`: the distance tells them apart) — with a channel no measure places:
    the build stops on them, named (none today: no water has two such parts)."""
    out = []
    for i, p in enumerate(W.parts):
        if side_only(p) and p.get(SIDE_APART):
            bare = [r for r in p.get("runs") or [] if r.get("branch") and r.get("km_from") is None]
            if bare:
                out.append(f"display: {W.id} part {W.export_ix[i]}: one of its water's several parts of side channels "
                           f"only, told apart by its distance, with {len(bare)} channel(s) no measure places (T1172)")
    return out


def inside_candidates(item_id: str, w: dict, crossings: Dict[tuple, object]) -> List[str]:
    """(T1172) the parts the bundle measures inside a membership-only area no reviewer has looked at
    (`part.REVIEWED_INSIDE`): their subtitles say the area, their names wait for the look — listed, never silent."""
    from pipeline.deliver.answers.display.part import REVIEWED_INSIDE
    out = []
    for i, p in enumerate(w.get("parts") or []):
        a = crossings.get((item_id, "inside", i))
        if a and not a[2] and (item_id, a[0], a[1]) not in REVIEWED_INSIDE:
            out.append(f"{item_id} part {i} {a[1]} {a[0]!r}")
    return out


def reviewed_still_said(out: Dict[str, dict]) -> None:
    """(T1140) every reviewed "inside" / "at" part of the waters built is still said so (`part.REVIEWED_INSIDE`): one that
    stops being inside is looked at again too — the tripwire's other way."""
    from pipeline.deliver.answers.display.part import AREA_HEAD, REVIEWED_INSIDE
    said_in = {(i, str(f["lo"])[len(head) + len("area:"):], how) for i, v in out.items() for f in v["parts"] if f
               for how, head in AREA_HEAD.items() if str(f.get("lo") or "").startswith(head + "area:")}
    gone = sorted({r for r in REVIEWED_INSIDE if r[0] in out} - said_in)
    if gone:
        raise AnswersError(f"display: reviewed inside parts no longer said inside: {gone} (T1140: look at them again, "
                           f"then update `part.REVIEWED_INSIDE`)")


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
    stops: List[str] = []
    candidates: List[str] = []
    for item_id in sorted(parts):
        # (T1172) a part of side channels only carries its distance rule and its area (`side_facts`)
        w = held_facts(item_id, side_facts(item_id, doc["waters"][item_id], crossings), crossings)
        candidates += inside_candidates(item_id, w, crossings)
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
        try:
            names = _names(W, crossings)
        except AnswersError as e:              # (T1140) every part the rules cannot name, together, then stop
            stops.append(str(e))
            continue
        stops += untold_side_parts(W)
        for u in unmeasured_side_parts(W, names):
            log(f"  side part said by its label with no measure (no stop; it sorts last among its water's parts): {u}")
        closed = []
        facts: List[Optional[dict]] = [None] * len(w["parts"])
        for i, (ex, p) in enumerate(zip(W.export_ix, W.parts)):
            rk = rule_key(keys[parts[item_id][ex]])
            if rk not in closed_memo:
                closed_memo[rk] = closed_all_year(store, B.key_ix[rk])
            closed.append(closed_memo[rk])
            k = gi[p["grp"]]
            facts[ex] = {
                "order": i, "label": labels[i], **names[i],
                "runs": runs_label(doc, p, water_ends_km(w)),
                "place": part_place(Wg, k) or None,          # null: no entry heading
                "km": part_km(p),
                "closed_all_year": closed[-1],
                "paper_licence": [rule_ref(r) for r in paper_licence(B, rk, w.get("kind"), store=store)]}
        # (T1199 PR 2) no part says it lies within an area whose rule it does not carry, nor outside one it carries
        stops += within_disagreements(item_id, W.parts, [f"{facts[ex]['runs']} | {facts[ex]['label']}"
                                                         for ex in W.export_ix], doc)
        shut = {p["grp"]: c for p, c in zip(W.parts, closed)}
        pk = picker(Wg, labels_g, [shut[g] for g in Wg.export_ix])
        for c in pk["choices"]:
            c["parts"] = sorted(x for g in c["parts"] for x in members[g])
        out[item_id] = {"parts": facts, "picker": pk,
                        "unresolved_licensing": [lic_ref(k) for k in sorted(
                            k for e in w.get("entries") or [] for k in unresolved.get(e, []))]}
    if stops:
        raise AnswersError("display: parts the names cannot say —\n  " + "\n  ".join(stops))
    reviewed_still_said(out)
    for c in candidates:
        log(f"  INSIDE candidate (its subtitle says the area; its name waits for a reviewer, `part.REVIEWED_INSIDE`): {c}")
    log(f"  parts: {sum(1 for v in out.values() for f in v['parts'] if f)} parts of {len(out)} "
        f"waters; {sum(closed_memo.values())} of {len(closed_memo)} rule keys closed all year")
    return out
