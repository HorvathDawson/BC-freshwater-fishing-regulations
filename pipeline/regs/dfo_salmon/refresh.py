"""THE REFRESH PREVIEW (T366): what a DFO page refresh would change, said before anything is written.

    python -m pipeline.regs.dfo_salmon.live run --regions 2 --dry-run    # fetch + parse, write NOTHING, print this
    python -m pipeline.regs.dfo_salmon.live run --regions 2              # the same summary, then the refresh

T361's cause: the typed rows the owner marked against were a season old, a re-emit from the newer page moved them, and six
owner marks silently read "rule changed". The owner's rule (2026-11-03): ONLY A PLACE OR ITS BINDING is reviewed —

  places   new / reworded (drift) / dropped / revived / section moved — `entries.reconcile`'s outcomes. A new or reworded
           place is a place question: it is held unplaced and goes to the DFO review queue (`live queue`).
  rows     every row change on a known place, old -> new verbatim with its FN numbers and HOW IT WAS READ (species,
           dates, limit, gear) — for the DFO DIGEST, a quick skim; a mark never goes stale on it (`marks._place_only`).
  marks    every owner mark the refresh would move ("rule changed" / "binding moved"), and every mark on a place the
           preview lists as dropped or moved — computed with the new rows, through the one staleness decision.
  urgent   a row the table structure could not be read into rules (new wording, an unread number) or a row whose place
           matches no curated DFO place — the URGENT notice path (T321), never the digest.

ONE VINTAGE (item 2): `vintage` lists every region whose typed rows are not from the raw page in the cache (the stamp
`fetch.page_stamp` the typed file carries, else its "Date modified"), and every raw page not fetched for `STALE_DAYS` in
season — the review app's banner and `live queue` print it.

Reads only; nothing here writes a file.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

#: an in-season fishery notice number, as printed in a cell
FN = re.compile(r"\bFN\s*\d{3,4}\b", re.I)
#: (item 2) a raw page not fetched for this many days is told on the banner — only IN SEASON (owner-tunable)
STALE_DAYS = 7
SEASON_MONTHS = frozenset(range(4, 13))          # Apr 1 – Dec 31: when DFO prints in-season notices
REFRESH = "python -m pipeline.regs.dfo_salmon.live run --regions {slug} --dry-run   (then without --dry-run)"


# ── the rows ─────────────────────────────────────────────────────────────────────────────────────────────────────────

def feed_map(typed: Optional[Mapping]) -> Dict[str, dict]:
    """fingerprint -> {rows, rules} of one region's typed feed (`review.dfo.typed`'s shape)."""
    return {l["fingerprint"]: {"rows": list(l.get("rows") or []), "rules": list(l.get("rules") or [])}
            for l in (typed or {}).get("locators") or []}


def fns(rows: Iterable[str]) -> List[str]:
    return sorted({re.sub(r"\s+", "", m.group(0)).upper() for r in rows for m in FN.finditer(r)})


def _md(d: Mapping) -> str:
    import calendar
    a = f"{calendar.month_abbr[d.get('from_month') or 1]} {d.get('from_day') or 1}"
    b = f"{calendar.month_abbr[d.get('to_month') or 12]} {d.get('to_day') or 31}"
    return f"{a}–{b}"


def how_read(rule: Mapping) -> str:
    """One rule as the feed READ it: type, fish, dates, limit, origin, gear — the digest's "how it was interpreted"."""
    out = [str(rule.get("type") or "?")]
    sp = rule.get("species") or []
    if sp:
        out.append("/".join(sp))
    elif rule.get("when_targeting"):
        out.append("targeting " + "/".join(rule["when_targeting"]))
    w = rule.get("when") or {}
    if w.get("dates"):
        out.append(", ".join(_md(d) for d in w["dates"]))
    for k in ("take", "possession", "annual"):
        if rule.get(k) is not None:
            out.append(f"{k} {rule[k]}")
    if rule.get("origin"):
        out.append(str(rule["origin"]))
    for g in rule.get("gear") or []:
        bits = [f"{k} {'/'.join(v) if isinstance(v, list) else v}" for k, v in g.items() if k != "slot"]
        out.append(f"gear {g.get('slot')}: {', '.join(bits)}" if bits else f"gear {g.get('slot')}")
    if rule.get("lengths"):
        out.append(f"lengths {json.dumps(rule['lengths'], separators=(',', ':'))}")
    return " · ".join(out)


def _rules_of_row(fp: str, i: int, rules: Sequence[Mapping]) -> List[Mapping]:
    """The typed rules read out of row `i` of the locator `fp` (`feed.build`: rule ids `<fp8>.r<i>…`)."""
    pat = re.compile(rf"^{re.escape(fp[:8])}\.r{i}(?!\d)")
    return [r for r in rules if pat.match(str(r.get("rule_id") or ""))]


def row_changes(ef, old: Mapping[str, dict], new: Mapping[str, dict]) -> List[dict]:
    """Every curated place whose rows differ between two typed feeds (fingerprint maps): {location_id, place, old,
    new, fns, read}. `old`/`new`: the rows only on one side, verbatim; `read`: how each new row was read."""
    out: List[dict] = []
    for loc in ef.locations:
        fps = list(loc.fingerprints or [])
        was = [r for fp in fps for r in (old.get(fp) or {}).get("rows", [])]
        now = [r for fp in fps for r in (new.get(fp) or {}).get("rows", [])]
        if was == now or not (was or now) or not any(fp in new for fp in fps):
            continue                                     # unchanged, or not on the new page (a dropped place)
        gone = [r for r in was if r not in now]
        added = [r for r in now if r not in was]
        if not gone and not added:
            continue                                     # only the order moved
        read = []
        for fp in fps:
            got = new.get(fp) or {}
            for i, row in enumerate(got.get("rows") or []):
                if row in added:
                    read.append({"row": row, "read": [how_read(r) for r in _rules_of_row(fp, i, got.get("rules") or [])]
                                 or ["(no rule read from this row)"]})
        st = loc.source_text or {}
        out.append({"location_id": loc.location_id,
                    "place": f"{st.get('waters') or loc.water}: {st.get('specific_area') or ''}".rstrip(": "),
                    "old": gone, "new": added, "fns": fns(added + gone), "read": read})
    return out


# ── the places ───────────────────────────────────────────────────────────────────────────────────────────────────────

PLACE_KINDS = {"new": "new", "drift": "reworded", "dormant": "dropped", "revived": "revived",
               "section_moved": "section moved"}


def place_changes(rep) -> Dict[str, List[dict]]:
    """`entries.reconcile`'s report -> {new, reworded, dropped, revived, section moved: [{fingerprint, location_id,
    water, scope, detail, likely}]} (only kinds that occur)."""
    out: Dict[str, List[dict]] = {}
    for o in rep.outcomes:
        kind = PLACE_KINDS.get(o.status)
        if kind is None:
            continue
        out.setdefault(kind, []).append({
            "fingerprint": o.fingerprint, "location_id": o.location_id, "water": o.water, "scope": o.specific_area,
            "detail": o.detail, "likely": (o.candidates[0]["location_id"] if o.candidates else None)})
    return out


# ── the marks ────────────────────────────────────────────────────────────────────────────────────────────────────────

def marks_moved(slug: str, region_old: Mapping[str, dict], region_new: Mapping[str, dict], *,
                places: Mapping[str, List[dict]], typed_dir: Optional[Path] = None,
                entries_dir: Optional[Path] = None, marks_path: Optional[Path] = None) -> List[dict]:
    """Every owner DFO mark in force on the region that the refresh MOVES (its stale reasons with the new rows differ
    from now — "rule changed" / "binding moved"), and every mark on a place the preview drops or moves. Read only."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import dfo as D
    from pipeline.regs.review import marks as M
    path = Path(marks_path or CURATED.regulations.review.marks)
    locs = D.locations(entries_dir)
    everything = dict(D.typed(typed_dir))
    after = {k: v for k, v in everything.items() if k not in region_old}
    after.update(region_new)
    watch = {p["location_id"]: kind for kind in ("dropped", "section moved")
             for p in places.get(kind, []) if p.get("location_id")}
    out: List[dict] = []
    for m in M.latest(M.load(path)).values():
        got = locs.get(m.target) if m.scope == "dfo" else None
        if got is None or got[0] != slug:
            continue
        _s, loc, water = got
        before = M.stale_reasons(m, M.Prints.model_validate(D.prints(loc, water, feed=everything)))
        now = M.stale_reasons(m, M.Prints.model_validate(D.prints(loc, water, feed=after)))
        if now != before or m.target in watch:
            out.append({"target": m.target, "verdict": m.verdict, "date": m.date,
                        "reasons": [r.value for r in now if r not in before],
                        **({"place": watch[m.target]} if m.target in watch else {})})
    return out


# ── the urgent rows ──────────────────────────────────────────────────────────────────────────────────────────────────

def unread_rows(ef, typed: Mapping) -> List[dict]:
    """Rows the table structure could not be read into rules (T321 `words`: `new` wording, an `unread` number), each
    {fingerprint, location_id, k, sp, d, t, new?}."""
    index = ef.by_fingerprint()
    out = []
    for loc in typed.get("locators") or []:
        for w in loc.get("words") or ():
            if w.get("k") in ("new", "unread"):
                hit = index.get(loc["fingerprint"])
                out.append({"fingerprint": loc["fingerprint"], "location_id": hit.location_id if hit else None,
                            **{k: w[k] for k in ("k", "sp", "d", "t", "new") if k in w}})
    return out


# ── the summary ──────────────────────────────────────────────────────────────────────────────────────────────────────

def preview(slug: str, ef, rep, typed_new: Mapping, typed_old: Optional[Mapping], unplaced: Sequence[Mapping], *,
            typed_dir: Optional[Path] = None, entries_dir: Optional[Path] = None,
            marks_path: Optional[Path] = None, with_marks: bool = True) -> dict:
    """One region's refresh preview (module doc)."""
    old, new = feed_map(typed_old), feed_map(typed_new)
    places = place_changes(rep)
    rows = row_changes(ef, old, new) if typed_old is not None else []
    s = {"region": slug,
         "page": {"was": {**((typed_old or {}).get("page") or {}),
                          "date_modified": (typed_old or {}).get("date_modified")} if typed_old is not None else None,
                  "now": {**(typed_new.get("page") or {}), "date_modified": typed_new.get("date_modified")}},
         "places": places, "rows": rows,
         "unplaced": [{"fingerprint": u["fingerprint"], "kind": u["kind"], "water": u["water"], "scope": u["scope"],
                       "rows": list(u.get("rows") or []), "fns": fns(u.get("rows") or [])} for u in unplaced],
         "unread": unread_rows(ef, typed_new)}
    if with_marks and typed_old is not None:
        s["marks"] = marks_moved(slug, old, new, places=places, typed_dir=typed_dir, entries_dir=entries_dir,
                                 marks_path=marks_path)
    return s


def rows_text(s: Mapping) -> str:
    """The digest's lines for a region's row changes: place, old -> new verbatim (FN), how each new row was read."""
    L: List[str] = []
    for c in s.get("rows") or []:
        L.append(f"* {c['location_id']} — {c['place']}" + (f"  [{', '.join(c['fns'])}]" if c["fns"] else ""))
        L += [f"    - was: {r}" for r in c["old"]]
        for r in c["read"]:
            L.append(f"    + now: {r['row']}")
            L += [f"        read: {x}" for x in r["read"]]
    return "\n".join(L)


def text(s: Mapping) -> str:
    """The printed summary of one region's preview."""
    pw, pn = (s.get("page") or {}).get("was") or {}, (s.get("page") or {}).get("now") or {}
    L = [f"── Region {s['region']}: page {pw.get('date_modified') or '?'}"
         f"{' (' + pw['sha256'][:12] + ')' if pw.get('sha256') else ''} -> {pn.get('date_modified') or '?'}"
         f"{' (' + pn['sha256'][:12] + ')' if pn.get('sha256') else ''}"]
    places = s.get("places") or {}
    if any(places.values()):
        L.append("PLACES (owner review):")
        for kind, ps in places.items():
            for p in ps:
                like = f", likely {p['likely']}" if p.get("likely") else ""
                L.append(f"  {kind:<14} {p.get('location_id') or p['fingerprint']}  {p['water']}: {p['scope']!r}{like}")
    else:
        L.append("PLACES: none new, reworded, dropped or moved")
    if s.get("unplaced") or s.get("unread"):
        L.append("URGENT:")
        for u in s.get("unplaced") or []:
            L.append(f"  matches no curated place ({u['kind']}): {u['water']}: {u['scope']!r} — {len(u['rows'])} row(s)"
                     f"{' [' + ', '.join(u['fns']) + ']' if u['fns'] else ''} -> review queue (live queue)")
            L += [f"      {r}" for r in u["rows"]]
        for u in s.get("unread") or []:
            L.append(f"  not read from the table ({u['k']}) on {u['location_id'] or u['fingerprint']}: "
                     f"{u['sp'] or '-'} | {u['d'] or '-'} | {u['t']}")
    rows = s.get("rows") or []
    L.append(f"ROWS CHANGED (digest): {len(rows)} place(s)" if rows else "ROWS CHANGED: none")
    if rows:
        L.append(rows_text(s))
    if "marks" in s:
        ms = s["marks"]
        L.append(f"MARKS that would move: {len(ms)}" if ms else "MARKS that would move: none")
        for m in ms:
            why = ", ".join(m["reasons"]) or ""
            L.append(f"  {m['target']} ({m['verdict']}, {m['date']}) {why}"
                     f"{' — its place is ' + m['place'] if m.get('place') else ''}")
    return "\n".join(L)


# ── one vintage (item 2) ─────────────────────────────────────────────────────────────────────────────────────────────

def vintage(typed_dir: Optional[Path] = None, cache_dir: Optional[Path] = None, *,
            today: Optional[date] = None, stale_days: int = STALE_DAYS) -> List[dict]:
    """Every region whose TYPED rows are not from the raw page in the cache (`typed_older`), and every raw page not
    fetched for `stale_days` in season (`raw_old`): [{region, kind, text}]. Empty = one vintage."""
    from pipeline.common.curated import GENERATED
    from pipeline.regs.dfo_salmon import fetch as F
    td = Path(typed_dir or Path(GENERATED.regs.dfo_salmon) / "typed")
    cache = Path(cache_dir or Path(__file__).resolve().parents[3] / F.DEFAULT_CACHE)
    today = today or datetime.now(timezone.utc).date()
    manifest = F._load_manifest(cache) if (cache / "manifest.json").exists() else {}
    out: List[dict] = []
    for p in sorted(td.glob("region-*.json")):
        slug = p.stem.split("region-", 1)[1]
        raw = cache / "raw" / f"region{slug}-eng.html"
        if not raw.exists():
            continue
        d = json.loads(p.read_text(encoding="utf-8"))
        rs = F.page_stamp(raw.read_text(encoding="utf-8", errors="replace"))
        st = d.get("page") or {}
        if (st.get("sha256") and st["sha256"] != rs["sha256"]) or \
                (not st.get("sha256") and d.get("date_modified") != rs["date_modified"]):
            out.append({"region": slug, "kind": "typed_older",
                        "text": f"Region {slug}: the typed rows are from the page of {d.get('date_modified') or '?'}"
                                f"{'' if st else ' (unstamped)'}, the saved page is {rs['date_modified'] or '?'} — "
                                f"the queue shows last vintage's rows. Refresh: {REFRESH.format(slug=slug)}"})
        got = (manifest.get(slug) or {}).get("fetched_at")
        fetched = (datetime.fromisoformat(got.replace("Z", "+00:00")).date() if got
                   else datetime.fromtimestamp(raw.stat().st_mtime, timezone.utc).date())
        age = (today - fetched).days
        if today.month in SEASON_MONTHS and age > stale_days:
            out.append({"region": slug, "kind": "raw_old",
                        "text": f"Region {slug}: the saved DFO page was fetched {age} days ago ({fetched}), in season — "
                                f"refresh: {REFRESH.format(slug=slug)}"})
    return out
