"""THE PROOF (trout table plan, T810 ticket J; contract 2.10, guarantee 2, the fight list's item 9): every trout and char
table the release ships says the same law as today's answers, for every water and every day.

    check_model(model, bundle, data) -> {tables, cells, problems}      (every key, every segment)

It runs inside the card gate (`card_check --all`, `pipeline.deliver card_check`) whenever the answers hold the trout
section, so a flagged side release and the cutover build are stopped by it; its count goes into the gate's stamp, and the
store refuses answers whose stamp checked fewer tables than the file holds (`card_check.stamp_problems`: the floor, so a
proof that silently checks nothing cannot pass).

A SEPARATE DECODER. It imports nothing from the builder (`trout/table.py`, `sheets.py`, `timeline.py`; a test holds the
imports). It reads the DECODED table (the shipped words) with G's grammar (`trout/words.py`: every text has its reader;
a text none reads fails here, never skipped as free text), and the law from today's answers: the post-pass rows frame
(per fish and origin the decided state, the day's number and the size lines), the display frame (the whole-water
closure, Q-D3) and the licence frame (the stamps), and from the keeping engine over the bundle's rules and the reader's
verdicts (`engine.Engine`, as `card_check.check_caps` uses it). It never compares the table with the data the builder
read to word it (the group tree): the "checks seeded from input launder" lesson.

WHAT IT PROVES, per (answer key, segment):
  null       the cell is NULL exactly per Q-D3 (the display says closed or tidal, or no trout or char fish answers)
  covers     `covers` is exactly the rows frame's trout rows, each mixed row counted as its two halves (A0 amendment),
             and every trout and char fish the frame answers is in the table exactly once (a dropped fish fails)
  state      each fish's box is its state (keep / release / closed); the origin words: a plain row = the same law for
             both origins, "Hatchery only" = only hatchery fish keep, twin rows = each origin its own law
  alone      per fish, origin and length (every cut any rule or table text draws, a length inside each piece) the most
             the table lets you keep of that fish alone = the rows frame's decided count (`count_at`)
  joint      the table's joint limits (box totals, shared bands, own bands, row caps, strip size and wild limits,
             `together`) accept EXACTLY the baskets the engine accepts: every single fish, seeded random baskets, and
             each limit filled to its number and one past it (the engine's own rule bands too); a released or closed
             fish keeps none (`card_check.check_caps`' method over a new reading)
  possession a row (or its box's strip) says a possession line <=> the fish's own possession differs from the page's
             lead (F0, `card.page_possession`), with the rules' numbers (`card_check.possession_problems`, re-aimed)
  origin     T793 re-aimed: a rows-frame keep row with an origin condition holding a table fish says an origin there;
             T805 re-aimed: a one-origin limit two rows share (`tree.across_rows`) is said once, across them
  top, fold  read back; their numbers are the engine's most (a plain "Up to N": all the table's fish together here)
  dates      every DateSet text against the following segments (spec 3.3): the first later start whose answer differs,
             "rule changes" only for a looser-only change, a tighter change worded with exactly what tightens, "until"
             the last day, a reopening only on something closed today; no change left undated, none invented; the
             sheet's coming-change sentence the same change; a one-day segment (Adams) shows the day before's stricter
             row and its sheet says the day
  sheet      the head, and every "Here" line read back and held to the row it opens from (size, band, box, possession,
             stop duty, the licence line's stamps from the post-pass licence answer and its duties from the decided
             lines, any other line a phrase the row, strip, `together` or a quiet line says)
  year bar   shown exactly when every fish shares one state (release or closed) on an open water; its runs the page's
             year rule over the table's fish (`year.stamp_years`)
  src        every rule a table cites exists; each fish's deciding rule is in its row's `src`
  elsewhere  each kept-elsewhere line read back; its "Keep N more here" proved by the engine after the fish kept there;
             (T810 L) each covered keep row's kept-elsewhere SENTENCE on the table (whose limit, the row's own number),
             none standing for a row the table does not cover (`elsewhere_said_problems`)

AND OVER EVERY ROWS FRAME OF EVERY KEY, a table or not (T810 L, the rows 34 cutover):
  possession F0's `page_possession` as shipped = derived again from the frame's possession roles and the bundle's rules
             (the lead, its sentence and value words, every also-line: `page_possession_problems`)
  cut        a covered row carries no `group` and no `elsewhere`; every other keep row keeps its kept-elsewhere line, a
             NULL cell's too (`cut_problems`)

Each check has a mutation test (`test_trout_check.py`): a wrong number, a dropped fish, a shifted date, an unparseable
sentence, a size edge, an origin, a scope, a possession line, a shared band's span: each fails.
"""
from __future__ import annotations

import json
import math
import random
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver import calendar as K
from pipeline.deliver.answers import common
from pipeline.deliver.answers.trout import words as W

INF = math.inf
TC = common.trout_char()
#: random baskets per table the joint proof keeps (and each limit filled to its number and one past it)
BASKETS = 32
SEED = 20261116

# Mutation pins (False = the check is off; each has a test that fails with it off):
#: the joint limits through the engine's basket proof
A_JOINT_PROOF = True
#: possession said <=> it differs from the page's lead (F0), with the rules' numbers
A_POSSESSION_PROOF = True
#: the dates against the following segments
A_DATE_PROOF = True


class _Fail(Exception):
    pass


# ============================================================================================
# The law of today's answers
# ============================================================================================

_STATE = {"keep": "keep", "no_limit": "keep", "release": "release", "closed": "closed"}
_SIZE_LINES = ("rel", "cap", "outersize")


def law(dec: Optional[dict]) -> Optional[tuple]:
    """One origin's decided answer -> (state, daily | None, ((t, from, to | None, take | None), …)); None: no rule
    speaks for that origin."""
    if dec is None:
        return None
    st = _STATE.get(dec["status"])
    if st is None:
        raise _Fail(f"unknown decided status {dec['status']!r}")
    if st != "keep":
        return (st, None, ())
    daily = None if dec["status"] == "no_limit" else dec.get("daily")
    return ("keep", daily, tuple(sorted((x["t"], x["a"], x.get("b"), x.get("take")) for x in dec.get("lines") or ()
                                        if x["t"] in _SIZE_LINES)))


def count_at(lw: Optional[tuple], L: float) -> float:
    """How many fish of length `L` the law keeps today (INF: no limit)."""
    if lw is None or lw[0] != "keep":
        return 0
    n = INF if lw[1] is None else lw[1]
    for t, a, b, take in lw[2]:
        if (a or 0) <= L < (INF if b is None else b):
            n = 0 if t == "rel" else n if take is None else min(n, take)
    return n


def law_cuts(*laws) -> set:
    out = set()
    for lw in laws:
        for _t, a, b, _k in (lw[2] if lw else ()):
            out.add(float(a or 0))
            if b is not None:
                out.add(float(b))
    return out


def mids(cuts: Iterable[float]) -> List[float]:
    pts = sorted({0.0, *[c for c in cuts if c != INF]})
    return [(x + y) / 2 for x, y in zip(pts, pts[1:])] + [pts[-1] + 5]


def lead_of(fa: dict) -> Optional[str]:
    return common.lead_name(fa.get("hatchery"), fa.get("wild"))


def asked(frame: dict) -> Dict[str, str]:
    """Every trout and char fish the rows frame answers -> its state (its leading origin's)."""
    out = {}
    for S, fa in (frame.get("fish") or {}).items():
        if S in TC:
            ld = lead_of(fa)
            if ld is not None:
                out[S] = _STATE[fa[ld]["status"]]
    return out


def keeps(lw) -> bool:
    return lw is not None and lw[0] == "keep" and any(count_at(lw, L) > 0 for L in mids(law_cuts(lw)))


# ============================================================================================
# The decoded table
# ============================================================================================

class Row:
    """One table row as read back: its fish, the origins it speaks for, the lengths it keeps, its lines read."""

    def __init__(self, b: int, i: int, box: dict, r: dict):
        self.b, self.i, self.box, self.r = b, i, box, r
        self.key, self.fish, self.origin = r["key"], r["fish"], r["origin"]
        self.kind = box["kind"]
        self.label = bool(r["label"] or box["label"])
        self.ranges: Optional[Tuple[Tuple[float, float], ...]] = None
        self.wild_ranges: Optional[Tuple[Tuple[float, float], ...]] = None   # "… if wild": the wild fish's own size
        self.lines = [(t, W.parse_row_line(t)) for t in r["lines"]]
        self.quiet = [(t, W.parse_quiet(t)) for t in r["quiet"]]
        self.band: Optional[tuple] = None          # (kind, n, m, from, to)
        self.poss: Optional[tuple] = None          # the possession line read, or the band's (k from the quiet line)
        self.poss_text: Optional[str] = None
        self.daily_said: Optional[float] = None    # a possession band's day number (its quiet line)
        if r["sub"] is not None:
            m = W.re.fullmatch(rf"a rainbow over {W._CM}", r["sub"])
            if not m or self.fish != "ST":
                raise _Fail(f"row {self.key}: cannot read the sub {r['sub']!r}")
            self.ranges = ((W._cm(m[1], m[2]), INF),)
        for t, x in self.lines:
            if x[0] == "size":
                if self.ranges is not None:
                    raise _Fail(f"row {self.key}: two sizes")
                self.ranges = x[1]
            elif x[0] == "size_wild":
                if self.wild_ranges is not None or self.origin is not None or self.label:
                    raise _Fail(f"row {self.key}: 'if wild' on a row of one origin, or twice")
                self.wild_ranges = x[1]
        if bool(r.get("split")) != (self.wild_ranges is not None):
            # (trout 5) the merged hatchery and wild row is marked, and only it
            raise _Fail(f"row {self.key}: split {r.get('split')} but its lines {'say' if self.wild_ranges else 'say no'} "
                        "'if wild'")

    def origins(self) -> Tuple[str, ...]:
        if self.origin:
            return (self.origin,)
        if self.label:
            return ("hatchery",)
        return common.ORIGINS


class Table:
    """One cell's TroutFrame read back with G's grammar (every text; a text G cannot read raises WordsError)."""

    def __init__(self, cell: dict):
        self.cell = cell
        t = cell["table"]
        self.t = t
        self.top = W.parse_top_full(W.join_top(t["top"]["lead"], t["top"].get("sub")))
        self.fold = W.parse_fold(t["fold"]["value"])
        self.together = W.parse_together(t["together"]["text"]) if t["together"] else None
        self.together_text = t["together"]["text"] if t["together"] else None
        if t["top"]["second"] is not None:
            second = W.parse_together(t["top"]["second"])
            if self.together is None:
                # T810 G2: the lakes' own count said once, under the top line (Echoes)
                if second[0] != "lakes":
                    raise _Fail(f"top.second {t['top']['second']!r} without the line under the table")
                self.together, self.together_text = second, t["top"]["second"]
        # T810 L: the covered rows' kept-elsewhere sentences lead the list, then the worked lines; each read back
        self.elsewhere = [(e, W.parse_elsewhere(e)) for e in t["elsewhere"] if W.is_elsewhere_line(e)]
        self.elsewhere_says = [(e, W.parse_elsewhere_says(e)) for e in t["elsewhere"] if not W.is_elsewhere_line(e)]
        self.rows: List[Row] = []
        self.boxes = []
        for bi, b in enumerate(t["boxes"]):
            kind, n = W.parse_head(b["head"])
            if kind != b["kind"]:
                raise _Fail(f"box {bi} is {b['kind']} and its head says {b['head']!r}")
            strip = W.parse_strip(b["strip"]) if b["strip"] else ()
            if b.get("possession"):
                # T810 G2 Q4: the box's possession on its own line ("In your possession: …")
                if not b["possession"].startswith("In your possession: "):
                    raise W.WordsError(f"trout words: cannot read the box's possession {b['possession']!r}")
                strip = strip + (("possession", W.parse_possession("i" + b["possession"][1:])),)
            if b["tag"] is not None and not W.re.fullmatch(r"[A-Z][a-z]+’s rule", b["tag"]):
                raise W.WordsError(f"trout words: cannot read the tag {b['tag']!r}")
            if b["label"] is not None and b["label"] != W.HATCHERY_ONLY:
                raise W.WordsError(f"trout words: cannot read the label {b['label']!r}")
            rows = [Row(bi, i, b, r) for i, r in enumerate(b["rows"])]
            for r in rows:
                if r.r["label"] is not None and r.r["label"] != W.HATCHERY_ONLY:
                    raise W.WordsError(f"trout words: cannot read the label {r.r['label']!r}")
            for x in b["bands"]:
                kind_, k, m = W.parse_band(x["text"])
                if kind_ != x["kind"]:
                    raise _Fail(f"box {bi}: band {x['text']!r} is marked {x['kind']}")
                total = W.band_total(x["text"])
                if kind_ in ("shared", "own") and total is not None:
                    # (owner 00:52) the overall limit the band names first: the box's, or a raised box's first number
                    ex = W.parse_head_except(b["head"])
                    if total != n and not (ex is not None and total == ex[0]):
                        raise _Fail(f"box {bi}: band {x['text']!r} names {total} in a box of {n}")
                if kind_ == "shared" and (m != x["to"] - x["from"] + 1 if m is not None else x["to"] == x["from"]):
                    raise _Fail(f"box {bi}: band {x['text']!r} spans rows {x['from']}..{x['to']}")
                if kind_ == "size" and (m != x["to"] - x["from"] + 1 if m is not None else x["to"] - x["from"] + 1 < 3):
                    raise _Fail(f"box {bi}: band {x['text']!r} spans rows {x['from']}..{x['to']}")
                if kind_ == "shared" and m is None and W.band_wild_n(x["text"]) is not None and \
                        any(r.origin != "wild" for r in rows[x["from"]:x["to"] + 1]):
                    raise _Fail(f"box {bi}: band {x['text']!r} over a row that is not wild")
                if kind_ == "shared" and m is None and W.band_wild_n(x["text"]) is None and \
                        x["to"] - x["from"] + 1 < 3:
                    raise _Fail(f"box {bi}: band {x['text']!r} over two rows says 'between them'")
                for r in rows[x["from"]:x["to"] + 1]:
                    r.band = (kind_, k, m, x["from"], x["to"])
            inner = []
            for x in b.get("inner") or ():
                # (trout 4, R4) a count nested in one band, its cap's words only
                span = x["to"] - x["from"] + 1
                if x["kind"] == "shared":
                    kind_, k, m = W.parse_band(x["text"])
                    if kind_ != "shared" or W.band_total(x["text"]) is not None or (m is not None and m != span) \
                            or (m is None and span < 3):
                        raise _Fail(f"box {bi}: nested band {x['text']!r} over rows {x['from']}..{x['to']}")
                    inner.append((x["from"], x["to"], k, None))
                else:
                    got = W.parse_size_part(x["text"])
                    if got is None or (got[2] is not None and got[2] != span) or (got[2] is None and span < 3):
                        raise _Fail(f"box {bi}: nested band {x['text']!r} over rows {x['from']}..{x['to']}")
                    inner.append((x["from"], x["to"], got[0], got[1]))
            self.boxes.append({"kind": b["kind"], "n": n, "strip": strip, "rows": rows, "raw": b, "inner": inner,
                               "except": W.parse_head_except(b["head"])})
            self.rows += rows
        for r in self.rows:
            for t, x in r.lines:
                if x[0] == "possession":
                    r.poss, r.poss_text = x[1], t
            if r.poss is None:
                for p in self.boxes[r.b]["strip"]:
                    if p[0] == "possession":
                        r.poss = p[1]
                        r.poss_text = next((s for s in (r.box["strip"] or "").split("; ")
                                            if s.startswith("in your possession: ")), None) or \
                            "i" + r.box["possession"][1:]
            if r.band and r.band[0] == "possession":
                q = [x for _t, x in r.quiet if x[0] == "possession"]
                if len(q) != 1 or q[0][2] != r.band[1] or q[0][3] != (r.fish,):
                    raise _Fail(f"row {r.key}: a possession band {r.band[1]} without its quiet line")
                r.daily_said = q[0][1]
                if r.poss is None:
                    # T810 G2 Q10: the quiet line carries the possession's size clause ("…, and only 1 of them over …")
                    r.poss = ("count", r.band[1], (r.fish,), q[0][4])
                    r.poss_text = f"in your possession: no more than {W._n(r.band[1])} " \
                        f"{common.lc_names([r.fish])}" + "".join(
                            f", only {W._n(k)} over {common.cm_in(rng[0][0])}" for k, rng in q[0][4])
        self.keep_rows = [r for r in self.rows if r.kind == "keep"]

    # -- the row a fish (species, origin, length) is kept under, if any ---------------------------------
    def row_for(self, S: str, o: str, L: float) -> Optional[Row]:
        for r in self.keep_rows:
            if r.fish != S or o not in r.origins():
                continue
            rng = r.ranges if r.ranges is not None else ((0, INF),)
            if o == "wild" and r.wild_ranges is not None:
                rng = r.wild_ranges
            if any(a < L < b for a, b in rng):
                return r
        return None

    def cuts(self) -> set:
        out = set()
        for r in self.keep_rows:
            for a, b in (r.ranges or ()):
                out |= {float(a), float(b)}
            for _t, x in r.lines:
                for v in _cms(x):
                    out.add(float(v))
        for b in self.boxes:
            for p in b["strip"]:
                for v in _cms(p):
                    out.add(float(v))
        if self.together:
            for v in _cms(self.together):
                out.add(float(v))
        return out

    # -- the joint limits: (name, predicate over a fish, limit) ------------------------------------------
    def limits(self) -> List[Tuple[str, Callable, float]]:
        out: List[Tuple[str, Callable, float]] = []
        keep_fish = {r.fish for r in self.keep_rows}

        def of_rows(rows):
            ids = {id(x) for x in rows}
            return lambda f, rf: rf is not None and id(rf) in ids

        def fishes(codes, over=None, origin=None):
            cs = set(codes)
            return lambda f, rf: rf is not None and f[0] in cs and (over is None or f[2] > over) and \
                (origin is None or f[1] == origin)
        for bi, b in enumerate(self.boxes):
            if b["kind"] != "keep":
                continue
            rows = b["rows"]
            out.append((f"box {bi} total {b['n']}", of_rows(rows), b["n"]))
            if b.get("except") is not None:
                # T834 (owner 13:15): "Keep 5, brook trout up to 20" — the first number is what the OTHER fish come to
                # here (the exception's number is the box's day most, above)
                first, exc, _most = b["except"]
                out.append((f"box {bi} head {first} besides {list(exc)}",
                            of_rows([r for r in rows if r.fish not in set(exc)]), first))
            seen_bands = set()
            for r in rows:
                if r.band is None:
                    raise _Fail(f"row {r.key} has no band")
                kind, k, _m, fr, to = r.band
                if (fr, to) in seen_bands:
                    continue
                seen_bands.add((fr, to))
                if kind in ("own", "shared"):
                    out.append((f"box {bi} band {fr}..{to} {k}", of_rows(rows[fr:to + 1]), k))
                elif kind == "possession":
                    out.append((f"row {r.key} a day {r.daily_said}", of_rows([r]), r.daily_said))
                elif kind == "size":
                    # (T810 L) "counts toward the 2 · only 1 over 50 cm (20 in) between them": the box's number (its
                    # total, above), and these rows over the size: at most the band's (a raised box: its first number)
                    if k != b["n"] and not (b.get("except") is not None and k == b["except"][0]):
                        raise _Fail(f"row {r.key}: a size band of the {k} in a box of {b['n']}")
                    _bn, n_, cm_, _m = W.parse_band_size(b["raw"]["bands"][next(
                        i for i, x in enumerate(b["raw"]["bands"]) if x["from"] == fr)]["text"])
                    out.append((f"box {bi} band {fr}..{to} over {cm_}: {n_}",
                                lambda f, rf, R=of_rows(rows[fr:to + 1]), cm=cm_: R(f, rf) and f[2] > cm, n_))
                elif kind == "upto" and k != b["n"] and not (b.get("except") is not None and k == b["except"][0]
                                                               and r.fish not in set(b["except"][1])):
                    raise _Fail(f"row {r.key}: 'counts toward the {k}' in a box of {b['n']}")
            for fr, to, k, cm in b.get("inner") or ():
                # a nested band: these rows (over its size, a size band): at most its number
                out.append((f"box {bi} nested {fr}..{to}: {k}" + (f" over {cm}" if cm is not None else ""),
                            (lambda f, rf, R=of_rows(rows[fr:to + 1]), cm=cm: R(f, rf) and (cm is None or f[2] > cm)), k))
            for p in b["strip"]:
                if p[0] == "size":
                    n, cm, extra = p[1], p[2], p[3]
                    if extra is None:
                        out.append((f"box {bi} over {cm}: {n}", lambda f, rf, R=of_rows(rows), cm=cm: R(f, rf) and f[2] > cm, n))
                    else:
                        out.append((f"box {bi} over {cm}: {n} besides a hatchery steelhead",
                                    lambda f, rf, R=of_rows(rows), cm=cm: R(f, rf) and f[2] > cm
                                    and not (f[0] == "ST" and f[1] == "hatchery"), n))
                        out.append((f"box {bi} over {cm}: {extra} with one", lambda f, rf, R=of_rows(rows), cm=cm:
                                    R(f, rf) and f[2] > cm, extra))
                elif p[0] == "size_except":
                    out.append((f"box {bi} over {p[2]}: {p[1]}, not counting {list(p[3])}",
                                lambda f, rf, R=of_rows(rows), cm=p[2], ex=set(p[3]): R(f, rf) and f[2] > cm
                                and f[0] not in ex, p[1]))
                elif p[0] == "wild":
                    out.append((f"box {bi} wild {p[1]}", lambda f, rf, R=of_rows(rows): R(f, rf) and f[1] == "wild",
                                p[1]))
            for r in rows:
                me = of_rows([r])
                for t, x in r.lines:
                    k = x[0]
                    if k in ("only", "uses", "stop"):
                        # ("stop": "after keeping 2, stop fishing this river today" — at most 2 of this row's fish)
                        out.append((f"row {r.key} {t!r}", me, x[1]))
                    elif k == "count":
                        out.append((f"row {r.key} {t!r}", lambda f, rf, me=me, o=set(x[2]) & keep_fish:
                                    me(f, rf) or (rf is not None and f[0] in o), x[1]))
                    elif k == "size_count":
                        out.append((f"row {r.key} {t!r}", lambda f, rf, me=me, o=set(x[3]), cm=x[2]:
                                    (me(f, rf) or (rf is not None and f[0] in o)) and f[2] > cm, x[1]))
                    elif k == "wild":
                        out.append((f"row {r.key} {t!r}", lambda f, rf, me=me: me(f, rf) and f[1] == "wild", x[1]))
                    elif k == "also":
                        a = x[1]
                        if a[0] == "size":
                            _s, n, cm, mine, others, then = a
                            who = set(mine or (r.fish,)) | set(others)
                            out.append((f"row {r.key} {t!r}", fishes(who, cm), n))
                            for n2, cm2 in then:
                                out.append((f"row {r.key} {t!r} then", fishes(who, cm2), n2))
                        elif a[0] == "count":
                            _c, n, mine, others = a
                            out.append((f"row {r.key} {t!r}", fishes(set(mine or (r.fish,)) | set(others)), n))
                        elif a[0] == "across":
                            _c, n, o, others = a
                            out.append((f"row {r.key} {t!r}", fishes({r.fish} | set(others), origin=o), n))
        if self.together is not None:
            if self.together[0] == "origin":
                out.append((f"together {self.together_text!r}",
                            lambda f, rf, o=self.together[2]: rf is not None and f[1] == o, self.together[1]))
            else:
                out.append((f"together {self.together_text!r}", lambda f, rf: rf is not None,
                            self.together[1]))
        return out

    def accepts(self, seq: Sequence[tuple], limits=None) -> bool:
        limits = self.limits() if limits is None else limits
        rows = []
        for f in seq:
            rf = self.row_for(f[0], f[1], f[2]) if f[3] == "here" else None
            if rf is None:
                return False
            rows.append(rf)
        for _name, pred, n in limits:
            if sum(1 for f, rf in zip(seq, rows) if pred(f, rf)) > n:
                return False
        return True

    def alone(self, S: str, o: str, L: float, limits) -> float:
        f = (S, o, L, "here")
        rf = self.row_for(S, o, L)
        if rf is None:
            return 0
        got = [n for _name, pred, n in limits if pred(f, rf)]
        return min(got) if got else INF


def _cms(x) -> List[float]:
    """Every length a read text names (its cuts)."""
    out: List[float] = []

    def walk(v):
        if isinstance(v, tuple):
            for y in v:
                walk(y)
    if isinstance(x, tuple):
        k = x[0]
        if k in ("size", "size_wild") and len(x) == 2:
            for a, b in x[1]:
                out.extend([a, b])
        elif k in ("uses",):
            out.append(x[2])
        elif k == "size_count":
            out.append(x[2])
        elif k in ("size", "size_except") and len(x) == 4:          # a strip size part
            out.append(x[2])
        elif k in ("wild",) and x[2]:
            for a, b in x[2]:
                out.extend([a, b])
        elif k == "also":
            a = x[1]
            if a[0] == "size":
                out.append(a[2])
                out.extend(cm for _n, cm in a[5])
        elif k == "origin" and x[3]:
            for a, b in x[3]:
                out.extend([a, b])
    return [v for v in out if v not in (0, INF)]


# ============================================================================================
# One cell
# ============================================================================================

class Cell:
    """What one (key, segment) proof reads: the decoded cell, the rows / display / licence frames, the rule lookup
    (`rule(ref)` -> the export doc's rule fields) and, for the engine's proofs, the reader's Part (None: no bundle)."""

    def __init__(self, cell, frame, display, licence, rule: Callable, P=None, steelhead_water=False,
                 doc_names: Optional[dict] = None, ids=None):
        self.cell, self.frame, self.display, self.licence = cell, frame, display, licence
        self.rule, self.P, self.st_water = rule, P, steelhead_water
        self.doc_names = doc_names or {}
        self.ids = ids
        #: the segment's own rows frame (`frame` is the day before's on a one-day segment's table)
        self.today = frame


def covers_of(frame: dict) -> List[int]:
    """The rows-frame rows the table covers, a row mixing trout or char with other fish counted as its two halves
    (the trout half first: contract A0 amendment)."""
    out, at = [], 0
    for r in frame.get("rows") or ():
        fish = set(r.get("all_members") or ()) | set(r.get("members") or ())
        t, o = fish & TC, fish - TC
        if t:
            out.append(at)
        at += 2 if (t and o) else 1
    return out


def null_problems(frame: dict, display: dict, cell) -> List[str]:
    want = display.get("status") in ("closed", "tidal") or not asked(frame)
    if (cell is None) != want:
        return [f"the cell is {'NULL' if cell is None else 'a table'}; Q-D3 says {'NULL' if want else 'a table'} "
                f"(display {display.get('status')!r}, fish {sorted(asked(frame))})"]
    return []


def _st_skip(st_water: bool, S: str, L: float) -> bool:
    """A rainbow over 50 cm IS a steelhead where the definition holds (p.80): no rainbow row speaks for it, and no
    steelhead row for a rainbow under it."""
    if not st_water:
        return False
    from pipeline.deliver.bundle import read
    st = read._steelhead_min_cm()
    return (S == "RB" and L > st) or (S == "ST" and L < st)


#: T810 L (the sweep's builder fault): a Keep row never carries a 0 — a band "only 0" or a line "only 0" (a fish kept
#: nowhere is released); and a fish answered keep that the engine keeps none of at any length may stand under Release.
#: False = the mutation pins (each check off)
A_NO_ZERO_KEEP_ROW = True
A_NONE_KEPT_IS_RELEASE = True
ZERO_MARK = "a Keep row carries a 0"


def _engine_keeps(c: Cell, S: str) -> bool:
    """The engine keeps some fish `S` here, alone, at some length, of either origin."""
    from pipeline.deliver.answers.engine import CAP
    E = _engine(c.P)
    return any(E.most([(S, o, L, "here")], cap=CAP) > 0 for o in common.ORIGINS for L in E.lengths() or (30.0,))


def zero_problems(T: Table) -> List[str]:
    """No Keep row carries a 0 (`A_NO_ZERO_KEEP_ROW`)."""
    if not A_NO_ZERO_KEEP_ROW:
        return []
    probs = []
    for r in T.keep_rows:
        if r.band is not None and r.band[0] in ("own", "shared", "possession") and r.band[1] == 0:
            probs.append(f"row {r.key}: {ZERO_MARK} band")
        if any(x[0] in ("only", "count", "size_count", "wild") and x[1] == 0 for _t, x in r.lines):
            probs.append(f"row {r.key}: {ZERO_MARK} line")
    return probs


def law_problems(c: Cell, T: Table) -> List[str]:
    """State, origins, coverage and each fish's count alone at every length, against the rows frame's decided answers."""
    probs: List[str] = zero_problems(T)
    fa_all = c.frame.get("fish") or {}
    want = asked(c.frame)
    said: Dict[str, int] = {}
    for r in T.rows:
        said.setdefault(r.fish, r.b)
        if said[r.fish] != r.b:
            probs.append(f"{r.fish} sits in two boxes")
    for S in sorted(set(want) | set(said)):
        if S not in want:
            probs.append(f"the table says {S}, which the rows frame does not answer")
            continue
        if S not in said:
            probs.append(f"{S} ({want[S]}) is answered by the rows frame and said nowhere in the table")
            continue
        rows = [r for r in T.rows if r.fish == S]
        kind = rows[0].kind
        if kind == "release" and want[S] == "keep" and A_NONE_KEPT_IS_RELEASE and c.P is not None \
                and not _engine_keeps(c, S):
            continue                # (T810 L) answered keep, but its group holds it to 0: the engine keeps none
        if kind != want[S]:
            probs.append(f"{S} sits in the {kind} box; its answer is {want[S]}")
            continue
        fa = fa_all[S]
        lw = {o: law(fa.get(o)) for o in common.ORIGINS}
        if kind != "keep":
            for o, x in lw.items():
                if keeps(x):
                    probs.append(f"{S} is under {kind}, but its {o} fish keep")
            continue
        # the origin words (owner 15:48, 16:00)
        if len(rows) == 2:
            if {r.origin for r in rows} != {"hatchery", "wild"}:
                probs.append(f"{S}: two rows that are not a wild and hatchery pair")
            elif not (keeps(lw["hatchery"]) and keeps(lw["wild"])):
                probs.append(f"{S}: twin rows, but only one origin keeps")
        elif len(rows) == 1:
            r = rows[0]
            if r.origin is not None:
                probs.append(f"{S}: one row with an origin word")
            elif r.label:
                if not keeps(lw["hatchery"]) or lw["wild"] is None or keeps(lw["wild"]):
                    probs.append(f"{S}: 'Hatchery only', but hatchery {lw['hatchery'] and lw['hatchery'][0]}, wild "
                                 f"{lw['wild'] and lw['wild'][0]}")
            elif r.wild_ranges is None:
                if lw["hatchery"] is not None and lw["wild"] is not None and \
                        any(count_at(lw["hatchery"], L) != count_at(lw["wild"], L)
                            for L in mids(law_cuts(lw["hatchery"], lw["wild"]))):
                    probs.append(f"{S}: one row with no origin word, but wild and hatchery fish have different laws")
        else:
            probs.append(f"{S}: {len(rows)} rows")
    # each fish alone, every length: the engine's most of that fish alone here (its rules and the reader's verdicts;
    # the decided answer alone misses a row's group limits, which the engine holds)
    limits = T.limits()
    E = _engine(c.P) if c.P is not None else None
    if E is None:
        return probs
    from pipeline.deliver.answers.engine import CAP
    Ls = sorted(set(E.lengths()) | set(mids(T.cuts())))
    for S in sorted(said):
        if S not in want or want[S] != "keep":
            continue
        for o in common.ORIGINS:
            for L in Ls:
                if _st_skip(c.st_water, S, L):
                    continue
                got = T.alone(S, o, L, limits)
                exp = E.most([(S, o, L, "here")], cap=CAP)
                exp = INF if exp >= CAP else exp
                if got != exp:
                    probs.append(f"{S} {o} at {L:g} cm: the table keeps {_say(got)} alone, the engine {_say(exp)}")
                    break
    return probs


def _say(n) -> str:
    return "no limit" if n == INF else f"{n:g}"


# -- the joint proof (the engine's baskets) ---------------------------------------------------------------

def _engine(P):
    from pipeline.deliver.answers.engine import Engine
    if not P.cands:
        return None
    return Engine(P, P.cands[0].key)


def joint_problems(c: Cell, T: Table, key_seed: str) -> List[str]:
    """`card_check.check_caps`' method over the decoded table: its limits accept EXACTLY the baskets the engine
    accepts (here), every single fish, random baskets, each limit and each engine rule band at its number and one past
    it; a fish under Release or Closed keeps none."""
    if c.P is None or not A_JOINT_PROOF:
        return []
    from pipeline.deliver.answers.engine import CAP
    E = _engine(c.P)
    if E is None:
        return []
    probs: List[str] = []
    limits = T.limits()
    keep = sorted({r.fish for r in T.keep_rows})
    if not keep:
        return probs
    Ls = sorted(set(E.lengths()) | set(mids(T.cuts())))
    cands = [(S, o, L, "here") for S in keep for o in common.ORIGINS for L in Ls if not _st_skip(c.st_water, S, L)]
    alone = []
    for f in cands:
        a, b = T.accepts([f], limits), E.accepts([f])
        if a != b:
            probs.append(f"one {f[1]} {f[0]} of {f[2]:g} cm: the table {'keeps' if a else 'refuses'} it, the engine "
                         f"{'keeps' if b else 'refuses'} it")
        if a and b:
            alone.append(f)
    if probs or not alone:
        return probs[:5]
    rng = random.Random(f"{SEED}:{key_seed}")

    def run(seq):
        a, b = T.accepts(seq, limits), E.accepts(seq)
        if a != b:
            seq = _smallest(seq, lambda x: T.accepts(x, limits) == a and E.accepts(x) == b)
            probs.append(f"the basket {[(f[0], f[1], round(f[2], 1)) for f in seq]}: the table "
                         f"{'accepts' if a else 'refuses'} it, the engine {'accepts' if b else 'refuses'} it")
    for _ in range(BASKETS):
        run([rng.choice(alone) for _j in range(rng.randint(2, 6))])
    for name, pred, n in limits:
        if n == INF or n > CAP:
            continue
        held = [f for f in alone if pred(f, T.row_for(f[0], f[1], f[2]))]
        if not held:
            continue
        for k in (int(n), int(n) + 1):
            for _rep in range(2):
                run([rng.choice(held) for _j in range(k)])
    for r in E.rules:
        slots = [f for f in alone if E.checks(r, f)]
        hit = {}
        for f in slots:
            h = E._band(r, f[2])
            if h is not None and h[1] not in (None, 0) and r.k != "gate":
                hit.setdefault((h[0], int(math.floor(h[1]))), []).append(f)
        for (_bi, t), fs in hit.items():
            if t > CAP:
                continue
            for k in (t, t + 1):
                run([rng.choice(fs) for _j in range(k)])
    return probs[:5]


def _smallest(seq, still: Callable) -> list:
    """The shortest sub-basket (fish dropped one at a time) on which the table and the engine still disagree the same
    way: the problem names the limit, not a crowd."""
    seq = list(seq)
    i = 0
    while i < len(seq):
        x = seq[:i] + seq[i + 1:]
        if x and still(x):
            seq = x
        else:
            i += 1
    return seq


def one_day_problems(T: Table, P, st_water: bool) -> List[str]:
    """A one-day line ("On Jul 31 only: 60 cm (24 in) or longer.") against that day's engine: the lengths it keeps are
    exactly the ones the engine lets the fish be kept at that day."""
    E = _engine(P)
    probs: List[str] = []
    if E is None:
        return probs
    for r in T.keep_rows:
        for x in (W.parse_sheet_line(h["t"]) for h in r.r["sheet"]["here"]):
            if x[0] != "one_day":
                continue
            sizes = [y[1] for y in x[2] if y[0] == "size"]
            if not sizes:
                continue
            for o in r.origins():
                for L in sorted(set(E.lengths()) | set(mids({v for a, b in sizes[0] for v in (a, b)}))):
                    if _st_skip(st_water, r.fish, L):
                        continue
                    said = any(a < L < b for a, b in sizes[0])
                    if said != bool(E.most([(r.fish, o, L, "here")], cap=1)):
                        probs.append(f"row {r.key}: its one-day line says {'yes' if said else 'no'} at {L:g} cm; the "
                                     "day's engine the other")
                        break
    return probs


# -- the top line and the fold -----------------------------------------------------------------------------

def top_problems(c: Cell, T: Table) -> List[str]:
    probs: List[str] = []
    kinds = [b["kind"] for b in T.boxes]
    top, fold = T.top, T.fold
    form = top[0]
    if "keep" not in kinds:
        want = "release" if set(kinds) == {"release"} else "closed" if set(kinds) == {"closed"} else "none"
        if form != want or fold[0] != want:
            probs.append(f"the top line reads {form}, the fold {fold[0]}, the boxes {kinds}")
        return probs
    keep = [b for b in T.boxes if b["kind"] == "keep"]
    if form == "apart":
        named, others = top[1], top[2]
        if len(keep) != len(named) + (others is not None):
            probs.append(f"the top line names {len(named) + (others is not None)} boxes, the table has {len(keep)}")
            return probs
        E = _engine(c.P) if c.P is not None else None

        def most(b):
            fs = sorted({r.fish for r in b["rows"]})
            return E.most(E.fish(fs), cap=CAP) if E is not None else b["n"]
        from pipeline.deliver.answers.engine import CAP
        left = list(keep)
        for n, fish in named:
            b = next((x for x in left if {r.fish for r in x["rows"]} == set(fish)), None)
            if b is None:
                probs.append(f"the top line says {n} {fish}; no box holds exactly those fish")
                continue
            left.remove(b)
            if most(b) != n:
                probs.append(f"the top line says {n} {fish}; the engine keeps {most(b)} of them here")
        if others is not None:
            if len(left) != 1:
                probs.append(f"the top line says {others} other trout and char; {len(left)} boxes are left")
            elif most(left[0]) != others:
                probs.append(f"the top line says {others} other trout and char; the engine keeps {most(left[0])}")
        nums = tuple(n for n, _f in named) + ((others,) if others is not None else ())
        if E is not None:
            fish = sorted({r.fish for r in T.keep_rows})
            got = E.most(E.fish(fish), cap=int(sum(nums)) + 1)
            if got != sum(nums):
                probs.append(f"the boxes counted apart come to {sum(nums)}; the engine keeps {got} of {fish} here")
        if fold[0] != "apart" or fold[1] != nums:
            probs.append(f"the fold {T.t['fold']['value']!r} is not the top line's numbers {nums}")
    elif form == "up_to":
        _f, n, alsos, more, plus = top
        if len(keep) != 1:
            probs.append(f"an 'Up to' top line over {len(keep)} keep boxes")
        if fold[0] != "up_to" or fold[1] != (n, *(a[0] for a in alsos)):
            probs.append(f"the fold {T.t['fold']['value']!r} is not the top line's numbers")
        if c.P is not None:
            E = _engine(c.P)
            if E is not None:
                fish = sorted({r.fish for r in T.keep_rows})
                # the lead's number is the most of the fish no "or", "and no more than" or "plus" names; each of those
                # the most of its own fish; "plus" adds up: the lead's number and each "plus" are the most of all
                named = {S for _k, fs, *_x in list(alsos) + list(more) + list(plus) for S in fs}
                rest = [S for S in fish if S not in named]
                if rest:
                    got = E.most(E.fish(rest), cap=int(n) + 1)
                    if got != n:
                        probs.append(f"the top line says up to {n}; the engine keeps {got} of {rest} here")
                if plus:
                    want = n + sum(k for k, _f in plus)
                    got = E.most(E.fish(fish), cap=int(want) + 1)
                    if got != want:
                        probs.append(f"the top line comes to {want}; the engine keeps {got} of {fish} here")
                for k, fs in [(a[0], a[1]) for a in alsos] + list(more) + list(plus):
                    got = E.most(E.fish(list(fs)), cap=int(k) + 1)
                    if got != k:
                        probs.append(f"the top line says {k} of {fs}; the engine keeps {got} here")
    else:
        probs.append(f"the top line reads {form} over a keep box")
    tone = {"up_to": "keep", "apart": "keep", "release": "release", "closed": "closed", "none": "none"}[fold[0]]
    if T.t["fold"]["tone"] != tone:
        probs.append(f"the fold's tone {T.t['fold']['tone']!r} is not {tone!r}")
    return probs


# -- possession (F0; card_check.possession_problems re-aimed) ----------------------------------------------

def reading_model(frame: dict, rule_key: Callable) -> dict:
    """The decided answers as `card.page_possession` reads them: per fish and origin, roles by rule key."""
    def res(d):
        if d is None:
            return None
        return {"roles": {rule_key(k): {"role": role, "by": by} for k, role, by in d.get("roles") or ()}}
    return {"R": {S: {"H": res(v.get("hatchery")), "W": res(v.get("wild"))}
                  for S, v in (frame.get("fish") or {}).items()}}


def possession_problems(c: Cell, T: Table) -> List[str]:
    """Each kept trout or char row: its possession line (on the row, its box's strip, or the possession band with its
    quiet line) against the possession rules that speak for it, measured against the page's lead (F0's one choice):
    `card_check.possession_problems` with `lead`."""
    if c.P is None or not A_POSSESSION_PROOF:
        return []
    from pipeline.deliver.answers import card as CD
    from pipeline.deliver.answers.card_check import possession_problems as PP
    from pipeline.deliver.answers.display.rule import POSSESSION_HOME
    lead = CD.page_possession(c.P, reading_model(c.frame, c.ids.__getitem__))
    probs: List[str] = []
    for r in T.keep_rows:
        if r.fish not in CD.POSSESSION_FISH:
            if r.poss is not None:
                probs.append(f"row {r.key}: a possession line on a fish the book's table has none for")
            continue
        duties = [f"{r.poss_text} ({POSSESSION_HOME})"] if r.poss_text else []
        x = {"species": [r.fish], "n": 1, "duties": duties}
        if r.poss is not None and r.poss[0] == "same":
            # (T810 L, Bowron Lake Park) "just 1, the same as the day": the box's number, and what the rules speaking for
            # this fish come to — a count of that number, or k days' limits of the row's day number
            probs += _same_as_the_day(c, T, r, x, lead, PP, POSSESSION_HOME)
            continue
        got = PP(c.P, x, f"row {r.key}", lead=lead)
        # (the row's own number, or the box's where G2 Q1 turned the row's own band into "counts toward the N")
        own = [y[1] for _t, y in r.lines if y[0] == "only"] + ([r.band[1]] if r.band and r.band[0] in ("own", "upto", "size")
                                                                 else [])
        if got and not duties and own:
            # T810 G2: a water's possession count equal to the fish's own day number is not said again (Tagish's lake
            # trout, "only 2"): the rules must say exactly that count, with no clause
            alt = {**x, "duties": [f"in your possession: no more than {W._n(min(own))} {common.lc_names([r.fish])} "
                                   f"({POSSESSION_HOME})"]}
            if not PP(c.P, alt, f"row {r.key}", lead=lead):
                got = []
        probs += got
    return probs


def _same_as_the_day(c: Cell, T: Table, r: Row, x: dict, lead, PP, home: str) -> List[str]:
    n = r.poss[1]
    where = f"row {r.key}"
    if n != T.boxes[r.b]["n"]:
        return [f"{where}: possession 'just {W._n(n)}, the same as the day' in a box of {T.boxes[r.b]['n']}"]
    day = r.band[1] if r.band and r.band[0] in ("own", "upto", "size") else None
    alts = [f"in your possession: no more than {W._n(n)} {common.lc_names([r.fish])}"]
    if day:
        k = n / day
        base = "1 day’s limit" if k == 1 else f"{W._n(k)} days’ limits"
        alts += [base] + ([f"{base}, not {W._n(float(lead.f['per_daily']))}"] if lead is not None else [])
        alts = alts[:1] + [f"in your possession: {a}" for a in alts[1:]]
    first = None
    for a in alts:
        got = PP(c.P, {**x, "duties": [f"{a} ({home})"]}, where, lead=lead)
        if not got:
            return []
        first = first if first is not None else got
    return [f"{where}: 'just {W._n(n)}, the same as the day', but {first}"]


# -- T793 and T805 re-aimed --------------------------------------------------------------------------------

def origin_problems(c: Cell, T: Table) -> List[str]:
    """T793 (`tree.origin_unsaid`) re-aimed at the table: a keep row of the rows frame with an origin condition (one
    origin sent back, or held to a limit of its own) whose fish the table keeps says an origin for them — the
    "Hatchery only" label, a twin row's origin word, a wild quota in the strip, a row's wild line, or `together`.
    T805 (`tree.across_rows`) re-aimed: a one-origin limit another keep row also holds is said ACROSS them, once —
    `together`, or one strip, line or row cap holding every fish it names."""
    from pipeline.deliver.answers.tree import across_rows
    probs: List[str] = []
    rows = c.frame.get("rows") or []
    by_fish = {}
    for r in T.keep_rows:
        by_fish.setdefault(r.fish, []).append(r)

    def says_origin(S: str, o: str) -> bool:
        for r in by_fish.get(S, ()):
            if r.label or r.origin:
                return True
            if any(x[0] in ("wild", "size_wild") or (x[0] == "also" and x[1][0] == "across") for _t, x in r.lines):
                return True
            if any(p[0] == "wild" for p in T.boxes[r.b]["strip"]):
                return True
            if any(W.band_wild_n(x["text"]) is not None for x in r.box["bands"]):
                return True
        return T.together is not None and T.together[0] == "origin"
    for i, row in enumerate(rows):
        conds = [x for x in row.get("conds") or [] if x.get("c") in ("origin", "origin2")]
        if row.get("kind") != "keep" or not conds:
            continue
        fish = {S for x in conds for S in (x.get("who") or row.get("members") or [])} & set(by_fish)
        for S in sorted(fish):
            if not any(says_origin(S, x["o"]) for x in conds):
                probs.append(f"rows-frame row {i}: {S} has an origin condition ({sorted({x['o'] for x in conds})}); "
                             "the table says no origin for it")
    for i, row in enumerate(rows):
        if row.get("kind") != "keep":
            continue
        mine = [S for S in row.get("members") or () if S in by_fish]
        if not mine:
            continue
        for a in across_rows(rows, i, mine):
            others = [S for S in a["others"] if S in by_fish]
            if not others:
                continue
            every = set(a["mine"]) | set(others)
            said = T.together is not None and T.together[0] == "origin" and T.together[1] == a["n"] and \
                T.together[2] == a["o"]
            for r in T.keep_rows:
                for _t, x in r.lines:
                    if x[0] == "also" and x[1][0] == "across" and x[1][1] == a["n"] and \
                            every <= {r.fish, *x[1][3]}:
                        said = True
            boxes = {r.b for S in every for r in by_fish[S]}
            if len(boxes) == 1:
                b1 = T.boxes[boxes.pop()]
                if a["o"] == "wild" and (any(p[0] == "wild" and p[1] == a["n"] for p in b1["strip"]) or any(
                        W.band_wild_n(x["text"]) == a["n"] and {y.fish for y in b1["rows"][x["from"]:x["to"] + 1]}
                        >= every for x in b1["raw"]["bands"])):
                    said = True
            if not said:
                probs.append(f"the {a['o']} limit of {a['n']} across rows ({sorted(every)}) is not said across them")
    return probs


# -- the sheets --------------------------------------------------------------------------------------------

def sheet_problems(c: Cell, T: Table) -> List[str]:
    probs: List[str] = []
    together = T.together_text            # the line under the table, or the lakes' count under the top line (G2)
    docs = _documents(c.licence)
    for r in T.rows:
        sh = r.r["sheet"]
        where = f"row {r.key}'s sheet"
        head = W.parse_sheet_head(sh["head"])
        if head[0] != r.kind:
            probs.append(f"{where}: head {sh['head']!r} on a {r.kind} row")
        elif r.kind == "keep":
            band = next(x["text"] for x in r.box["bands"] if x["from"] <= r.i <= x["to"])
            total = W.band_total(band) if W.parse_band(band)[0] in ("shared", "own") else None
            if W.parse_band(band)[0] == "size":
                ok = head[2] == W.parse_band(band)[1] and head[1] == band
            elif total is not None:
                ok = head[2] == total and head[1] == W.band_part(band)
            else:
                ok = head[2] == T.boxes[r.b]["n"] and (head[1] or W.band_upto(T.boxes[r.b]["n"])) == band
            if not ok:
                probs.append(f"{where}: head {sh['head']!r}; the row's band {band!r} in a box of {T.boxes[r.b]['n']}")
        row_texts = {t for t, _x in r.lines} | {t for t, _x in r.quiet}
        strip_texts = set((r.box["strip"] or "").split("; ")) - {""}
        said_poss = said_stop = False
        for ln in sh["here"]:
            x = W.parse_sheet_line(ln["t"])
            k = x[0]
            if ln["bold"] != (k == "stop"):
                probs.append(f"{where}: {ln['t']!r} bold {ln['bold']}")
            if k == "size":
                rng = r.ranges if r.ranges is not None else ((0, INF),)
                if tuple(x[1]) != tuple(rng):
                    probs.append(f"{where}: {ln['t']!r}; the row keeps {rng}")
            elif k == "size_wild":
                if r.wild_ranges is None or tuple(x[1]) != tuple(r.wild_ranges):
                    probs.append(f"{where}: {ln['t']!r}; the row's wild fish keep {r.wild_ranges}")
            elif k == "sub":
                if r.fish != "ST" or r.ranges != ((x[1], INF),):
                    probs.append(f"{where}: {ln['t']!r} on row {r.key}")
            elif k == "hatchery":
                if not r.label:
                    probs.append(f"{where}: 'Hatchery only' on a row without the label")
            elif k == "box":
                others = {S for S, _o in x[2]}
                want = {y.fish for y in T.boxes[r.b]["rows"] if y.fish != r.fish}
                if x[1] != T.boxes[r.b]["n"] or others != want:
                    probs.append(f"{where}: {ln['t']!r}; the box holds {T.boxes[r.b]['n']} of {sorted(want)}")
            elif k == "own":
                if not r.band or r.band[0] != "own" or r.band[1] != x[1]:
                    probs.append(f"{where}: {ln['t']!r}; the row's band {r.band}")
            elif k == "shared":
                band = next(y["text"] for y in r.box["bands"] if y["from"] <= r.i <= y["to"])
                held = {(y.fish, y.origin) for y in T.boxes[r.b]["rows"][r.band[3]:r.band[4] + 1]} - {(r.fish, r.origin)}
                nest = next((y for y in r.box.get("inner") or () if y["kind"] == "shared" and y["text"] == x[1]
                             and y["from"] <= r.i <= y["to"]), None)
                if nest is not None:
                    band = nest["text"]
                    held = {(y.fish, y.origin) for y in T.boxes[r.b]["rows"][nest["from"]:nest["to"] + 1]} - \
                        {(r.fish, r.origin)}
                if x[1] != W.band_part(band) or {(S, o) for S, o in x[2]} != held:
                    probs.append(f"{where}: {ln['t']!r}; the row's band {band!r} over {sorted(held, key=str)}")
            elif k == "size_band":
                band = next(y["text"] for y in r.box["bands"] if y["from"] <= r.i <= y["to"])
                held = {(y.fish, y.origin) for y in T.boxes[r.b]["rows"][r.band[3]:r.band[4] + 1]} - {(r.fish, r.origin)}
                nest = next((y for y in r.box.get("inner") or () if y["kind"] == "size" and y["text"] == x[1]
                             and y["from"] <= r.i <= y["to"]), None)
                if nest is not None:
                    held = {(y.fish, y.origin) for y in T.boxes[r.b]["rows"][nest["from"]:nest["to"] + 1]} - \
                        {(r.fish, r.origin)}
                elif r.band[0] != "size" or band.split(W.BAND_SEP, 1)[1] != x[1]:
                    held = None
                if held is None or {(S, o) for S, o in x[2]} != held:
                    probs.append(f"{where}: {ln['t']!r}; the row's band {band!r} over {sorted(held, key=str)}")
            elif k == "uses":
                if not any(y[0] == "uses" and y[2] == x[1] for _t, y in r.lines):
                    probs.append(f"{where}: {ln['t']!r}; the row says no 'uses the' over {x[1]}")
            elif k == "possession":
                said_poss = True
                if r.poss is None or r.poss[0] == "count" and x[1][:2] != r.poss[:2] or \
                        r.poss[0] in ("days", "same") and x[1] != r.poss:
                    probs.append(f"{where}: {ln['t']!r}; the row says {r.poss_text!r}")
            elif k == "stop":
                said_stop = True
                got = [y for _t, y in r.lines if y[0] == "stop"]
                if not got or (got[0][1], got[0][2]) != (x[1], x[3]) or x[2] != (r.fish,):
                    probs.append(f"{where}: {ln['t']!r}; the row's stop duty {got}")
            elif k == "licence":
                probs += _licence_problems(c, r, x[1], docs, where)
            elif k == "one_day":
                pass                                   # the dates' proof reads it (`date_problems`)
            elif k == "line":
                kind, t = x[1], x[2]
                ok = (kind == "row" and t in row_texts) or (kind == "quiet" and t in row_texts) or \
                    (kind == "strip" and t in strip_texts) or (kind == "together" and t == together) or \
                    (t in row_texts | strip_texts) or (together is not None and t == together)
                if not ok:
                    probs.append(f"{where}: {ln['t']!r} is not a line its row, box or table says")
        if r.poss is not None and r.kind == "keep" and not said_poss:
            probs.append(f"{where}: says no possession; the row says {r.poss_text!r}")
        if any(y[0] == "stop" for _t, y in r.lines) and not said_stop:
            probs.append(f"{where}: says no stop duty; the row says one")
        if len(sh["src"]) > 4 or not set(sh["src"]) <= set(T.t["src"]):
            probs.append(f"{where}: its sources {sh['src']}")
    return probs


def _documents(licence: Optional[dict]) -> List[dict]:
    from pipeline.deliver.answers import licence as L
    from pipeline.deliver.answers import stamps as ST
    if licence is None:
        return []
    profiles = licence.get("profiles") or []
    pix = L.profile_index(ST.DEFAULT_PROFILE)
    if pix >= len(profiles):
        return []
    return ST.documents_of(profiles[pix])


def _licence_problems(c: Cell, r: Row, parts, docs, where) -> List[str]:
    """The licence line: each stamp a document the post-pass licence answer lists for the fish (by its name); each
    yearly limit and record duty one of the fish's decided lines states."""
    probs: List[str] = []
    fa = (c.frame.get("fish") or {}).get(r.fish) or {}
    o = r.origin or lead_of(fa)
    dec = fa.get(o) or {}
    lines = dec.get("lines") or ()
    names = {c.doc_names.get(d["doc"], d["doc"]) for d in docs if r.fish in d["sp"]}
    for p in parts:
        if p[0] == "stamp":
            if p[1] not in names:
                probs.append(f"{where}: the stamp {p[1]!r}, which the licence answer does not list for {r.fish}")
        elif p[0] == "annual":
            if not any(x["t"] == "annual" and c.rule(x["r"]).get("take") == p[1] for x in lines):
                probs.append(f"{where}: '{p[1]} a year', which no yearly limit of the fish's answer says")
        elif p[0] == "record":
            if not any(x["t"] == "record" or (x["t"] == "duty" and c.rule(x["r"]).get("record_retention"))
                       for x in lines):
                probs.append(f"{where}: a record duty the fish's answer does not carry")
    return probs


# -- the kept-elsewhere lines ------------------------------------------------------------------------------

def _elsewhere_holds(E, q, a, kinds: Sequence[str], other: str, away: str):
    """Whether "Kept n <place>? Keep N more here." holds for a box of `kinds`: keeping n of SOME one fish of the box
    there (the card's worked line kept its first kind, `card_check._check_elsewhere_line`; the words name none) leaves
    exactly N here. True; None (no such basket can be kept today: nothing to prove); else the numbers the engine gives."""
    n, place = q[1], q[2]
    places = (other, away) if place == "elsewhere" else (place,)
    claim, seen = a[1], []
    for p in places:
        for S in kinds:
            for o in common.ORIGINS:
                got: list = []
                for f in E.fish([S], (p,), (o,)):
                    while len(got) < n and E.can_keep(got, f):
                        got.append(f)
                if len(got) != n:
                    continue
                m = E.most(E.fish(list(kinds)), got, cap=int(claim) + 1)
                if m == claim:
                    return True
                seen.append(m)
    return seen or None


#: T810 L (the review's findings 1 and 2): every covered keep row's kept-elsewhere sentence is on the table, with its
#: number. False = the check is off (the mutation pin)
A_ELSEWHERE_SAID_PROOF = True


def elsewhere_said_problems(c: Cell, T: Table) -> List[str]:
    """The kept-elsewhere SENTENCES (T810 L): the cutover dropped `elsewhere` from the rows the table covers, so the
    table must say it. Held to the shipped rows frame (never to the words the builder read): each covered keep row
    (`kind` keep: its `scope` says whose count it is) has its sentence on the table — "Your own limit, counted across
    …" for a region's, an area's or B.C.'s count, "<whose> rule: N a day here …" with the row's own daily number for a
    water's — and every sentence on the table stands for a covered keep row. A table that lost one fails."""
    if not A_ELSEWHERE_SAID_PROOF:
        return []
    rows = c.today.get("rows") or []
    keep = [rows[i] for i in c.cell["covers"] if i < len(rows) and rows[i].get("kind") == "keep"]
    said = [p for _t, p in T.elsewhere_says]
    probs: List[str] = []
    # (owner 2026-11-17 23:29, "Yes, name the fish") the list's subjects: one per covered keep row's fish (its one fish,
    # else the pool); more than one -> every line labelled with one of them, each subject's lines together; one -> none
    subjects = list(dict.fromkeys(W.elsewhere_subject(r.get("members") or r.get("all_members") or ()) for r in keep))
    labels = [W.split_subject(t)[0] for t in c.cell["table"]["elsewhere"]]
    if len(subjects) > 1:
        bad = [t for t, lb in zip(c.cell["table"]["elsewhere"], labels) if lb not in subjects]
        if bad:
            probs.append(f"the kept-elsewhere list speaks of {subjects}; a line without its subject: {bad[0]!r}")
        runs = [lb for i, lb in enumerate(labels) if i == 0 or labels[i - 1] != lb]
        if len(runs) != len(set(runs)):
            probs.append(f"the kept-elsewhere list's subjects are not kept together: {runs}")
    elif any(lb is not None for lb in labels):
        probs.append(f"a kept-elsewhere list of one subject {subjects} carries a label")
    for r in keep:
        sc = r.get("scope") or {}
        if sc.get("of") == "water" and not sc.get("share"):
            if not any(p[0] == "rule" and p[2] == r.get("daily") for p in said):
                probs.append(f"the covered keep row of {r.get('members')} (its own {r.get('daily')} a day) has no "
                             "kept-elsewhere sentence on the table")
        elif not any(p[0] == "own" for p in said):
            probs.append(f"the covered keep row of {r.get('members')} ({sc.get('of')}'s count) has no kept-elsewhere "
                         "sentence on the table")
    dailies = {r.get("daily") for r in keep if (r.get("scope") or {}).get("of") == "water"}
    for text, p in T.elsewhere_says:
        if p[0] == "rule" and p[2] not in dailies:
            probs.append(f"{text!r}: no covered keep row keeps {p[2]} a day by its water's own rule")
        if p[0] == "own" and not any((r.get("scope") or {}).get("of") != "water" or (r.get("scope") or {}).get("share")
                                     for r in keep):
            probs.append(f"{text!r}: no covered keep row is counted by a region's, an area's or B.C.'s limit")
    return probs


def elsewhere_problems(c: Cell, T: Table) -> List[str]:
    """Each kept-elsewhere line read back; its "Keep N more here" proved by the engine after that many kept at that
    place, for the keep box whose line it is; a size question answered with a ceiling over the same size."""
    if c.P is None or not T.elsewhere:
        return []
    E = _engine(c.P)
    if E is None:
        return []
    probs: List[str] = []
    other = "stream" if c.P.kind == "stream" else "lake"
    away = "lake" if c.P.kind == "stream" else "stream"
    for text, (q, a) in T.elsewhere:
        if q[0] == "kept":
            if a[0] not in ("more", "more_but", "more_tally"):
                probs.append(f"{text!r}: a count question with a size answer")
                continue
            seen: List[float] = []
            ok = False
            for b in T.boxes:
                if b["kind"] != "keep":
                    continue
                got = _elsewhere_holds(E, q, a, sorted({r.fish for r in b["rows"]}), other, away)
                if got is None or got is True:
                    ok = True
                    break
                seen += got
            if not ok:
                probs.append(f"{text!r}: after {q[1]} kept {q[2]}, the engine keeps {sorted(set(seen))} more here")
        else:
            _q, fish, cm = q
            if a[0] != "over":
                probs.append(f"{text!r}: a size question with a count answer")
            elif a[2] != cm:
                probs.append(f"{text!r}: two sizes")
    return probs


# -- the year bar and the rule refs -------------------------------------------------------------------------

def year_problems(c: Cell, T: Table, frames, starts, moments) -> List[str]:
    from pipeline.deliver.answers.year import stamp_years
    states = set(asked(c.frame).values())
    # T810 G2 (owner 15:41): the bar only when every fish is released, never on a closed table (Fraser in October)
    want = c.display.get("status") != "closed" and len(states) == 1 and states <= {"release"}
    y = T.t["year"]
    if (y is not None) != want:
        return [f"the year bar is {'shown' if y else 'not shown'}; the fish are {sorted(states)}"]
    if y is None:
        return []
    fish = sorted({r.fish for r in T.rows})
    got = stamp_years([{"fish": f.get("fish") or {}, "rows": [{"members": fish}]} for f in frames], list(starts),
                      moments)[0]["rows"][0]["year"]
    if [list(x) for x in y["runs"]] != [list(x) for x in got]:
        return [f"the year bar's runs {y['runs']} are not the page's year rule over {fish}"]
    if y["title"] != W.YEAR_TITLE:
        return [f"the year bar's title {y['title']!r}"]
    return []


def src_problems(c: Cell, T: Table) -> List[str]:
    probs: List[str] = []
    n = len(c.ids) if c.ids is not None else None
    for ref in T.t["src"]:
        if n is not None and not 0 <= ref < n:
            probs.append(f"the table cites rule {ref}, which does not exist")
    for r in T.rows:
        fa = (c.today.get("fish") or {}).get(r.fish) or {}
        o = r.origin or lead_of(fa)
        dec = fa.get(o)
        if dec is not None and dec["win"] not in r.r["src"]:
            probs.append(f"row {r.key}: its deciding rule {dec['win']} is not in its sources {r.r['src']}")
    return probs


# ============================================================================================
# The dates (spec 3.3), against the following segments
# ============================================================================================

def answer(frame: dict, display: dict, key: str, rule: Callable, poss: Optional[float]) -> Optional[tuple]:
    """A row's answer on one segment: ("closed", scope) | ("release",) | ("keep", ((origin, law), …), possession);
    None: no rule speaks for it."""
    S, _, o = key.partition("/")
    fa = (frame.get("fish") or {}).get(S)
    if not fa:
        return None
    if o:
        dec = fa.get(o)
        if dec is None:
            return None
        laws = ((o, law(dec)),)
    else:
        ld = lead_of(fa)
        if ld is None:
            return None
        dec = fa[ld]
        laws = tuple((x, law(fa.get(x))) for x in common.ORIGINS)
    if display.get("status") == "closed":
        return ("closed", "water")
    st = _STATE[dec["status"]]
    if st == "closed":
        return ("closed", _scope(rule(dec["win"]).get("species") or ()))
    if st == "release":
        return ("release",)
    return ("keep", laws, poss)


def _scope(species) -> object:
    from pipeline.regs.parsing import catalogue as C
    raw = list(species or ())
    got = set(common.expand(raw))
    if any(x in getattr(C, "OPEN_SUBJECTS", ()) for x in raw) or TC <= got:
        return "trout_char"
    return tuple(sorted(got & TC))


def _counts(laws, cuts=None) -> tuple:
    cuts = set(cuts or ()) | law_cuts(*[lw for _o, lw in laws])
    return tuple((o, tuple(count_at(lw, L) for L in mids(cuts))) for o, lw in laws)


def same(a, b) -> bool:
    if a is None or b is None:
        return a is b
    if a[0] != b[0]:
        return False
    if a[0] != "keep":
        return True
    cuts = law_cuts(*[lw for _o, lw in a[1]], *[lw for _o, lw in b[1]])
    return _counts(a[1], cuts) == _counts(b[1], cuts) and a[2] == b[2]


def _keep_range(lw) -> Optional[Tuple[float, float]]:
    ks = [(x, y) for x, y in zip(sorted({0.0, *law_cuts(lw)}), sorted({0.0, *law_cuts(lw)})[1:] + [INF])
          if count_at(lw, x + 0.25 if y == INF else (x + y) / 2) > 0]
    return (ks[0][0], ks[-1][1]) if ks else None


def change(a, b) -> Tuple[str, frozenset]:
    """(kind, what tightens) of the change from answer `a` to `b` (spec 3.3), written apart from E's."""
    if same(a, b):
        return "same", frozenset()
    if b is None:
        raise _Fail("no rule speaks for a row after a day it is answered")
    if a[0] == "closed":
        return "reopens", frozenset()
    if b[0] == "closed":
        return "tighter", frozenset({("closed", b[1])})
    if a[0] == "release":
        return ("opens", frozenset()) if b[0] == "keep" else ("same", frozenset())
    if b[0] == "release":
        return "tighter", frozenset({("release",)})
    ad, bd = dict(a[1]), dict(b[1])
    cuts = law_cuts(*ad.values(), *bd.values())
    looser = all(count_at(bd.get(o), L) >= count_at(la, L) for o, la in ad.items() if la is not None
                 for L in mids(cuts)) and not (a[2] is not None and b[2] is not None and b[2] < a[2]) \
        and not (a[2] is None and b[2] is not None)
    if looser:
        return "looser", frozenset()
    parts = set()
    if "hatchery" in ad and "wild" in ad and keeps(ad["wild"]) and not keeps(bd.get("wild")) and keeps(bd.get("hatchery")):
        parts.add(("hatchery_only",))
    lead_b = next(o for o, lw in b[1] if lw is not None and keeps(lw)) if len(b[1]) == 1 else \
        ("hatchery" if keeps(bd.get("hatchery")) else "wild")
    lb = bd[lead_b]
    la = ad.get(lead_b) if keeps(ad.get(lead_b)) else next(lw for _o, lw in a[1] if keeps(lw))
    ra, rb = _keep_range(la), _keep_range(lb)
    if rb[0] > ra[0]:
        parts.add(("floor", rb[0]))
    if rb[1] < ra[1]:
        parts.add(("ceil", rb[1]))
    pts = sorted({0.0, *cuts})
    pieces = list(zip(pts, pts[1:] + [INF]))
    inside = [(x, y) for x, y in pieces if x >= rb[0] and y <= rb[1]]
    drops = [(x, y, count_at(lb, (x + y) / 2 if y != INF else x + 0.25)) for x, y in inside
             if count_at(lb, (x + y) / 2 if y != INF else x + 0.25) < count_at(la, (x + y) / 2 if y != INF else x + 0.25)]
    if drops:
        ns = {n for _x, _y, n in drops}
        if len(ns) == 1:
            n = ns.pop()
            parts.add(("count", n) if len(drops) == len(inside) else ("band", n, drops[0][0], drops[-1][1]))
    if a[2] != b[2] and b[2] is not None and (a[2] is None or b[2] < a[2]):
        parts.add(("possession", b[2]))
    return "tighter", frozenset(parts)


def _norm_part(p: tuple) -> tuple:
    if p[0] == "band":
        return ("band", p[1], float(p[2]), float(p[3]))
    if p[0] in ("floor", "ceil"):
        return (p[0], float(p[1]))
    if p[0] == "closed" and isinstance(p[1], tuple):
        return ("closed", tuple(sorted(p[1])))
    return p


def _day_before(day: int) -> int:
    d = day - 1 if day > 1 else K.DAYS
    return d - 1 if d == K.LEAP else d


def _effective(days: List[int], t: int) -> Optional[int]:
    if t != K.LEAP:
        return t
    i = days.index(t)
    nxt = days[i + 1] if i + 1 < len(days) else K.DAYS + 1
    return None if nxt == K.LEAP + 1 else K.LEAP + 1


def next_change(starts, moments, answers: Callable[[int], object], j: int) -> Optional[tuple]:
    """(kind, first day, last day, what tightens) of segment `j`'s next change, walking the later start days
    (wrapping), each compared with the segments that start then and share a weekday (and the side of an hours
    window) with `j`; None: none all year."""
    at = [K.Moment.from_json(m) for m in moments] if moments is not None else [K.ALWAYS] * len(starts)
    days = sorted(set(starts))
    a = answers(j)
    if a is None:
        return None
    me = at[j]
    p = days.index(starts[j])
    for q in range(p + 1, p + len(days)):
        t = days[q % len(days)]
        e = _effective(days, t)
        if e is None:
            continue
        seen = [i for i, s in enumerate(starts) if s == t and at[i].weekdays & me.weekdays
                and (at[i].inside is None or at[i].inside == bool(me.inside))]
        kinds = {change(a, answers(i)) for i in seen}
        moved = kinds - {("same", frozenset())}
        if not moved:
            continue
        if len(moved) == 1 and len(kinds) == 1:
            kind, then = moved.pop()
        elif all(k in ("looser", "opens", "reopens") for k, _ in moved):
            kind, then = "looser", frozenset()
        else:
            raise _Fail(f"a change on day {e} that differs by weekday")
        return kind, e, _day_before(e), then
    return None


def _texts_by_row(T: Table) -> Dict[str, Tuple[str, str]]:
    """Each row key -> (where, the date text that holds it) from the date set: the most specific level that says one."""
    d = T.cell["dates"] or {}
    out: Dict[str, Tuple[str, str]] = {}
    for r in T.rows:
        got = []
        year_long = any(x[0] == "closed_year" for _t, x in r.lines)
        for lvl in ("card", "top"):
            if d.get(lvl) and not (lvl == "top" and year_long):
                got.append((lvl, d[lvl]))
        if str(r.b) in (d.get("box") or {}):
            got.append(("box", d["box"][str(r.b)]))
        for k, v in (d.get("band") or {}).items():
            b, _, i = k.partition(":")
            x = T.boxes[int(b)]["raw"]["bands"][int(i)]
            if int(b) == r.b and x["from"] <= r.i <= x["to"]:
                got.append(("band", v))
        if f"{r.b}:{r.i}" in (d.get("row") or {}):
            got.append(("row", d["row"][f"{r.b}:{r.i}"]))
        levels = [g[0] for g in got]
        if len(set(levels)) != len(levels) or {"card", "top"} <= set(levels):
            raise _Fail(f"row {r.key} has dates at {levels}")
        if got:
            # (owner 2026-11-18 00:52, "said once") the most specific level holds the row: its own, its band's, its box's,
            # then the card line; each row's date is then proved against the following segments (`date_problems`)
            out[r.key] = got[-1]
    return out


def date_problems(key_cells: Sequence[Optional[dict]], frames, displays, starts, moments, rule: Callable,
                  possession: Sequence[Dict[str, Optional[float]]]) -> List[str]:
    """Every date text of every table of one key, against the following segments."""
    if not A_DATE_PROOF:
        return []
    probs: List[str] = []
    n = len(starts)

    def answers_of(key):
        return lambda i: answer(frames[i], displays[i], key, rule, (possession[i] or {}).get(key.partition("/")[0]))
    for j in range(n):
        cell = key_cells[j]
        if cell is None:
            continue
        T = Table(cell)
        try:
            texts = _texts_by_row(T)
        except _Fail as e:
            probs.append(f"segment {j}: {e}")
            continue
        sheet = (cell["dates"] or {}).get("sheet") or {}
        for r in T.rows:
            ans = answers_of(r.key)
            from_j = j
            one = [x for x in (W.parse_sheet_line(h["t"]) for h in r.r["sheet"]["here"]) if x[0] == "one_day"]
            if one:
                # spec 3.3 rule 6: a one-day segment's row shows the day before's stricter answer
                if moments is not None or j == 0 or j + 1 >= n or starts[j + 1] - starts[j] != 1 or \
                        one[0][1] != starts[j]:
                    probs.append(f"segment {j}: row {r.key}'s one-day line on a segment that is not one day")
                    continue
                prev, now, nxt = ans(j - 1), ans(j), ans(j + 1)
                k, _p = change(prev, now)
                if k != "looser":
                    probs.append(f"segment {j}: row {r.key}'s one-day line, but the day only loosens? ({k})")
                if same(nxt, prev):
                    from_j = j + 1
            try:
                want = next_change(starts, moments, ans, from_j)
            except _Fail as e:
                probs.append(f"segment {j} (day {starts[j]}): row {r.key}: {e}")
                continue
            got = texts.get(r.key)
            where = f"segment {j} (day {starts[j]}) row {r.key}"
            if any(x[0] == "closed_year" for _t, x in r.lines):
                # T810 G2 Q8: "closed all year" — closed today, and no later segment of the year changes it
                if r.kind != "closed" or want is not None:
                    probs.append(f"{where}: says 'closed all year' ({r.kind}; next change {want})")
                continue
            if want is None:
                if got is not None:
                    probs.append(f"{where}: dated {got[1]!r} ({got[0]}); its answer holds all year")
                if r.key in sheet:
                    probs.append(f"{where}: a sheet sentence {sheet[r.key]} for no change")
                continue
            kind, day, last, then = want
            if got is None:
                probs.append(f"{where}: its answer changes on day {day} ({kind}) and no date says so")
                continue
            p = W.parse_date_line(got[1])
            if p.form != kind:
                probs.append(f"{where}: {got[1]!r} reads {p.form}; the change on day {day} is {kind}")
            elif kind in ("tighter", "opens") and p.last != last:
                probs.append(f"{where}: {got[1]!r} names day {p.last}; today's answer lasts to day {last}")
            elif kind in ("looser", "reopens") and p.day != day:
                probs.append(f"{where}: {got[1]!r} names day {p.day}; the change is on day {day}")
            elif kind == "tighter" and {_norm_part(x) for x in p.then} != {_norm_part(x) for x in then}:
                probs.append(f"{where}: {got[1]!r} says {sorted(p.then, key=str)}; what tightens is "
                             f"{sorted(then, key=str)}")
            if kind == "reopens" and r.kind != "closed":
                probs.append(f"{where}: a reopening date on a {r.kind} row")
            ss = sheet.get(r.key)
            if not ss or len(ss) != 1:
                probs.append(f"{where}: the sheet's coming change {ss}")
                continue
            q = W.parse_sheet_sentence(ss[0])
            if q.form != kind or q.day != day:
                probs.append(f"{where}: the sheet says {ss[0]!r}; the change is {kind} on day {day}")
            elif kind == "tighter":
                qt = {_norm_part(x) for x in q.then}
                if qt != {_norm_part(x) for x in then} and not (len(qt) == 1 and next(iter(qt)) == ("closed", None)
                                                                 and all(x[0] == "closed" for x in then)):
                    probs.append(f"{where}: the sheet says {ss[0]!r}; what tightens is {sorted(then, key=str)}")
        for k in sheet:
            if k not in {r.key for r in T.rows}:
                probs.append(f"segment {j}: a sheet date for row {k}, which the table does not hold")
    return probs


def possession_counts(T: Optional[Table], frame: dict, rule: Callable) -> Dict[str, Optional[float]]:
    """Each kept fish's own possession as a number, from the table's (proved) possession line: a water's count, or k
    days' limits times the fish's day number."""
    out: Dict[str, Optional[float]] = {}
    if T is None:
        return out
    for r in T.keep_rows:
        if r.poss is None:
            continue
        if r.poss[0] in ("count", "same"):
            out[r.fish] = r.poss[1]
        else:
            fa = (frame.get("fish") or {}).get(r.fish) or {}
            dec = fa.get(lead_of(fa)) or {}
            out[r.fish] = r.poss[1] * dec["daily"] if dec.get("daily") else None
    return out


# ============================================================================================
# One key, every key
# ============================================================================================

def check_key(cells, frames, displays, licences, starts, moments, rule: Callable, parts=None, *,
              st_water=False, doc_names=None, ids=None, seed: str = "") -> Tuple[int, List[str]]:
    """Every cell of one answer key -> (tables checked, problems). `parts(j)`: segment j's reader Part (None: no
    engine proofs)."""
    probs: List[str] = []
    checked = 0
    tables: List[Optional[Table]] = []
    for j, cell in enumerate(cells):
        where = f"segment {j} (day {starts[j]})"
        probs += [f"{where}: {p}" for p in null_problems(frames[j], displays[j], cell)]
        if cell is None:
            tables.append(None)
            continue
        try:
            from pipeline.deliver.answers.model.trout import TroutFrame
            TroutFrame.model_validate_json(json.dumps(cell, ensure_ascii=False))
            T = Table(cell)
        except (W.WordsError, _Fail, ValueError) as e:
            probs.append(f"{where}: {type(e).__name__}: {e}")
            tables.append(None)
            continue
        tables.append(T)
        # spec 3.3 rule 6 (owner "80 cm on the row"): a one-day segment's rows that only loosen for the day show the day
        # before's stricter answer, and their sheets say the day: the table is the day before's law, and each one-day
        # line the day's own (`one_day_problems`); the dates' proof checks the day is one day
        one = j > 0 and any(x[0] == "one_day" for r in T.rows for x in
                            (W.parse_sheet_line(h["t"]) for h in r.r["sheet"]["here"]))
        at = j - 1 if one else j
        c = Cell(cell, frames[at], displays[j], licences[j], rule, parts(at) if parts else None, st_water,
                 doc_names, ids)
        c.today = frames[j]
        got: List[str] = []
        if list(cell["covers"]) != covers_of(frames[j]):
            got.append(f"covers {list(cell['covers'])}; the rows frame's trout rows are {covers_of(frames[j])}")
        try:
            for f in (law_problems, top_problems, possession_problems, origin_problems, sheet_problems,
                      elsewhere_problems, elsewhere_said_problems, src_problems):
                got += f(c, T)
            got += joint_problems(c, T, f"{seed}:{j}")
            if one and parts:
                got += one_day_problems(T, parts(j), st_water)
            got += year_problems(c, T, frames, starts, moments)
        except (W.WordsError, _Fail) as e:
            got.append(f"{type(e).__name__}: {e}")
        probs += [f"{where}: {p}" for p in got]
        checked += 1
    poss = [possession_counts(T, frames[j], rule) for j, T in enumerate(tables)]
    try:
        probs += date_problems(cells, frames, displays, starts, moments, rule, poss)
    except (W.WordsError, _Fail) as e:
        probs.append(f"dates: {type(e).__name__}: {e}")
    return checked, probs


# ============================================================================================
# The rows 34 cutover (T810 L): every rows frame, every key — not only the trout tables
# ============================================================================================

#: T810 L (the review's findings 4 and 5): the page's possession row and the cut rows are proved over the shipped frames.
#: False = each check is off (the mutation pins)
A_PAGE_POSSESSION_PROOF = True
A_CUT_PROOF = True


def page_possession_expected(frame: dict, ids: Sequence[str], rule: Callable) -> Optional[dict]:
    """The page's possession row the shipped rows frame must carry (F0, contract 2.11), derived again from what it ships:
    the possession roles its decided answers give (fish by fish, hatchery then wild, each answer's roles in stored
    order, as `card.possession_role_rules` walks them) over the bundle's rules — the lead `card.possession_lead` picks
    with its sentence (`display.rule.rule_facts` `plain`) and its value words (`display.rule.possession_value`), and the
    also-lines (every other multiplier of another number of days, then every count, less the trout- and char-only ones,
    `card.trout_only`). The same choice functions; the inputs are the shipped bytes and the bundle."""
    import types
    from pipeline.deliver.answers.card import possession_lead, trout_only
    from pipeline.deliver.answers.display.rule import possession_value, rule_facts
    per: Dict[int, object] = {}
    caps: Dict[int, object] = {}
    for fa in (frame.get("fish") or {}).values():
        for o in ("hatchery", "wild"):
            d = fa.get(o)
            for ix, role, _by in (d or {}).get("roles") or ():
                if role in ("possession", "possession_cap"):
                    (per if role == "possession" else caps).setdefault(
                        ix, types.SimpleNamespace(key=ids[ix], f=rule(ix), ix=ix))
    lead = possession_lead(list(per.values()))
    if lead is None and not caps:
        return None

    def says(r) -> str:
        w = rule_facts(r.f).get("plain")
        if not w:
            raise _Fail(f"the possession rule {r.key} has no sentence")
        return w
    also = [r for r in per.values() if r is not lead and float(r.f.get("per_daily") or 0) != float(lead.f["per_daily"])] \
        if lead is not None else []
    also = [{"says": says(r), "src": r.ix} for r in also + list(caps.values()) if not trout_only(r)]
    out_lead = None if lead is None else {"says": says(lead), "value": possession_value(lead.f), "src": lead.ix}
    if out_lead is None and not also:
        return None
    return {"lead": out_lead, "also": also}


def page_possession_problems(frame: dict, ids: Sequence[str], rule: Callable) -> List[str]:
    """F0's row as shipped against `page_possession_expected`: a wrong value word, a lost or extra also-line, a lead
    that is not the pick, a row missing where one is owed (or the reverse) — each fails."""
    if not A_PAGE_POSSESSION_PROOF:
        return []
    if "page_possession" not in frame:
        return ["the rows frame has no `page_possession` (rows 34, F0)"]
    try:
        want = page_possession_expected(frame, ids, rule)
    except _Fail as e:
        return [str(e)]
    got = frame["page_possession"]
    if got != want:
        return [f"page_possession {json.dumps(got, ensure_ascii=False)[:300]}; the frame's possession roles give "
                f"{json.dumps(want, ensure_ascii=False)[:300]}"]
    return []


def cut_problems(frame: dict, cell: Optional[dict]) -> List[str]:
    """The rows 34 cut (`answers.cut_over`, contract Q-D9) as shipped: a row the cell's table covers carries no `group`
    and no `elsewhere` (the table says them); every other row is as the card made it — a keep row keeps its kept-
    elsewhere line, on a NULL cell too (Q-D3: no table there, nothing covered)."""
    if not A_CUT_PROOF:
        return []
    covers = set(cell["covers"]) if cell is not None else set()
    probs: List[str] = []
    for i, r in enumerate(frame.get("rows") or ()):
        if i in covers:
            if r.get("group") is not None or r.get("elsewhere") is not None:
                probs.append(f"row {i} is covered by the table and still carries its "
                             f"{'group' if r.get('group') is not None else 'kept-elsewhere line'}")
        elif r.get("kind") == "keep" and r.get("scope") is not None and r.get("elsewhere") is None:
            probs.append(f"row {i} ({r.get('members')}) is a keep row no table covers, and it lost its kept-elsewhere "
                         "line")
    return probs


def check_model(model, bundle: str, data: dict, keys=None, log=print) -> dict:
    """THE PROOF over every key and segment of `model` (built or decoded: the same `answers.Model`) holding the trout
    section: {tables (checked), cells (non-NULL trout cells), keys, problems}."""
    import time
    from pipeline.deliver.answers import licence as L
    from pipeline.deliver.answers import rows as RW
    t0 = time.monotonic()
    if "trout" not in model.sections:
        return {"tables": 0, "cells": 0, "keys": 0, "problems": []}
    B = common.load(bundle)
    ids = data["rule_ids"]
    doc_names = L.document_names(B.path)
    rule_doc: Dict[int, dict] = {}

    def rule(ref: int) -> dict:
        got = rule_doc.get(ref)
        if got is None:
            e, _, r = ids[ref].partition("::")
            got = rule_doc[ref] = B.rules[(e, r)]
        return got
    only = None if keys is None else set(keys)
    probs: List[str] = []
    tables = cells = done = 0
    part_cache: Dict[tuple, object] = {}
    for k, key in enumerate(model.keys):
        if only is not None and k not in only:
            continue
        tc = model.sections["trout"][k]
        cells += sum(1 for x in tc if x is not None)
        # T810 L: every key's rows frames — the page's possession row (F0) and the cut — trout table or not
        for j, f in enumerate(model.sections["rows"][k]):
            probs += [f"key {k}: segment {j}: {p}" for p in page_possession_problems(f, ids, rule)
                      + cut_problems(f, tc[j])]
        if all(x is None for x in tc) and all(not asked(f) for f in model.sections["rows"][k]):
            continue
        rk, kd = common.rule_key(key), common.key_dict(key)
        starts, moments = model.segments[k], model.moments[k]

        def parts(j, k=k, rk=rk, kd=kd, starts=starts, moments=moments):
            lad = model.sections["ladder"][k][j]
            day = starts[j] if starts[j] != K.LEAP else K.LEAP + 1
            md = tuple(K.month_day(day))
            at = K.Moment.from_json(moments[j]) if moments is not None else None
            sig = (rk, kd["kind"], kd["steelhead"], id(lad), md, at)
            P = part_cache.get(sig)
            if P is None:
                if len(part_cache) > 4096:
                    part_cache.clear()
                P = part_cache[sig] = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"],
                                              kd["steelhead"], lad, md, {}, at)
            return P
        n, got = check_key(tc, model.sections["rows"][k], model.sections["display"][k], model.sections["licence"][k],
                           starts, moments, rule, parts, st_water=bool(rk.steelhead_water), doc_names=doc_names,
                           ids=ids, seed=str(k))
        tables += n
        done += 1
        probs += [f"key {k}: {p}" for p in got]
        if done % 250 == 0:
            log(f"trout check: {done} keys, {tables} tables, {len(probs)} problems, {time.monotonic() - t0:.0f} s")
    log(f"trout check: {tables} tables of {cells} non-NULL cells, {len(probs)} problems "
        f"({time.monotonic() - t0:.0f} s)")
    return {"tables": tables, "cells": cells, "keys": done, "problems": probs}
