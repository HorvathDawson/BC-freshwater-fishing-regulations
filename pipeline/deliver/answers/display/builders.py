"""Builders: every rule (`build_rules`) and every part (`produce_parts`), with the checks on the words they
say (`when_problems`, `size_problems`), the closers that open a water, and the paper-licence duty."""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence, Tuple
from pipeline.deliver.answers.common import AnswersError, when_words
from pipeline.deliver.answers.display.rule import _num, cut_points, rule_facts
from pipeline.deliver.answers.display.part import (
    Water, part_km, part_label, part_labels, part_place, picker, runs_label, short_labels
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


def _run_words(ex: int, multi: Dict[int, List[int]], ends_of: Dict[int, Optional[tuple]]) -> dict:
    """T517 `lo` / `hi` (this piece's own two ends) and `run_name` (the whole run, its lowest piece's lower end to its
    highest piece's upper end) for a piece of a run the salmon chain cuts; all None for a part that is its run."""
    if ex not in multi:
        return {"lo": None, "hi": None, "run_name": None}
    ends = [ends_of[x] for x in multi[ex] if ends_of[x]]
    mine = ends_of[ex]
    return {"lo": mine[0] if mine else None, "hi": mine[1] if mine else None,
            "run_name": f"{ends[0][0]} → {ends[-1][1]}" if ends else None}


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
        runs = [r for q in qs for r in q.get("runs") or []]
        runs.sort(key=lambda r: (r.get("km_from") is None, -(r.get("km_from") or 0)))
        merged.append({**qs[0], "runs": runs, "sections": sum(q["sections"] for q in qs)})
    return Water(item_id, {**w, "parts": merged}, doc), members


def _shorts(W: "Water", labels: Sequence[str]) -> List[str]:
    """Every part's name (`short_label`, its own two ends), two parts the same words would name told apart by their
    full labels' distinguishing tail, else numbered — `short_labels` over the parts, the labels per group."""
    from pipeline.deliver.answers.display.part import short_label
    out = [short_label(W.doc, p, labels[i]) for i, p in enumerate(W.parts)]
    for s_ in set(out):
        ix = [i for i, t in enumerate(out) if t == s_]
        if len(ix) < 2:
            continue
        for n, i in enumerate(ix, 1):
            h = re.split(r" — | · ", labels[i], maxsplit=1)
            tail = h[1] if len(h) > 1 and h[1] else ""
            out[i] = f"{s_} · {tail}" if tail and len({labels[j] for j in ix}) > 1 else f"{s_} ({n})"
    return out


def produce_parts(B, doc: dict, keys: Sequence[tuple], parts: Dict[str, list],
                  rule_ref=None, lic_ref=None, log=print, store=None) -> Dict[str, dict]:
    """PURE: every named water's part facts, `{item_id: {"parts": [facts | null per EXPORT part],
    "picker": {...}, "unresolved_licensing": [record]}}`, a part's facts {order, label, runs,
    place, hint, km, closed_all_year, paper_licence}; null for a part outside B.C. The part keys
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
        shorts = _shorts(W, labels)
        # T517: a run the DFO salmon chain cuts into several parts (same grp, run) carries its pieces' own two ends
        # and the whole run's name, so the map joins them and says them without composing prose
        from pipeline.deliver.answers.display.part import run_ends
        pieces: Dict[tuple, List[int]] = {}
        for ex in W.export_ix:
            q = w["parts"][ex]
            pieces.setdefault((q["grp"], q["run"]), []).append(ex)
        multi = {ex: sorted(v) for v in pieces.values() if len(v) > 1 for ex in v}
        ends_of = {ex: run_ends(doc, w["parts"][ex]) for ex in multi}
        closed = []
        facts: List[Optional[dict]] = [None] * len(w["parts"])
        for i, (ex, p) in enumerate(zip(W.export_ix, W.parts)):
            rk = rule_key(keys[parts[item_id][ex]])
            if rk not in closed_memo:
                closed_memo[rk] = closed_all_year(store, B.key_ix[rk])
            closed.append(closed_memo[rk])
            k = gi[p["grp"]]
            facts[ex] = {
                "order": i, "label": labels[i], "short": shorts[i],
                **_run_words(ex, multi, ends_of), "runs": runs_label(doc, p),
                "place": part_place(Wg, k) or None,          # null: no entry heading
                "hint": part_label(Wg, k), "km": part_km(p),
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
