"""Rebuild `app/design/verification-artifact.html` from what the page actually renders.

THE DOCUMENT'S CLAIM IS THAT THE OUTPUT IS RIGHT, so every sheet in it has to be the output —
not a description of it, and not a copy made once. It was built by hand, which is why it went
stale the moment the corpus moved: it was still showing thirteen rivers against a corpus of
1,032 entries long after the corpus had become 1,480, and nothing about the page said so.

So it is generated, the same way `regs-v3.html` is. The difference is where the sheets come
from: the page decides what to draw in its own JavaScript, so the only honest way to capture
that is to RUN it. Playwright opens the real page, walks to a water and a stretch, and lifts
the rendered `.screen` — the same DOM a reader sees, markup and all.

Everything beside each sheet comes from the shipped bundle, so the halves of the comparison have
different sources: the sheet is what the page drew, the verbatim is what the book said, and the
binding is what the matcher decided. They agree or they do not.

THE PAGE IS NINETEEN INDEPENDENT COLUMNS, not one app you navigate. Reading `.screen` without
scoping returns the first column's — the Fraser's — for every water asked for, and that is exactly
what this tool did for its first several runs: nineteen sheets, all of them the Fraser, each filed
under a different river's name, with correct numbers beside them the whole time. Every selector
here is scoped to the column whose own heading matches, and a sheet whose heading is not the water
it was asked for is REFUSED rather than captioned.

    python pipeline/tools/build_verification_artifact.py
    python pipeline/tools/build_verification_artifact.py --waters "Fraser River,Shannon Lake"
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PAGE = REPO_ROOT / "app" / "design" / "verification-artifact.html"
SOURCE_PAGE = REPO_ROOT / "app" / "design" / "regs-v3.html"
BUNDLE = REPO_ROOT / "data" / "generated" / "bundle" / "bundle.sqlite"


def _squash(s: str) -> str:
    return " ".join((s or "").split())


def _entries_for(db: sqlite3.Connection, sets: list[int], kind: str) -> list[dict]:
    """The entries whose rules are IN these rulesets, split into a water's OWN rows and the
    regional/provincial ones.

    Scoped by RULESET, not by item. Keying on the item was wrong twice over. It was broader than
    this column's own caption, which promises the rows reaching *that stretch* — on the Fraser it
    returned every row on 251 sections of river. And on a lake the book writes as several waters
    it returned NOTHING: the page draws Kootenay Lake under the FWA polygon's id, while its rows
    bind to the three curated arms, so an item-keyed query saw only the provincial text and
    reported a lake with no rules of its own. A ruleset is what the page actually drew.

    `kind` is "own" or "zone". A zone entry's id starts with `z`; everything else was written for a
    named water. That is the distinction the page draws in its "set by" column, so the artifact has
    to draw the same one or the two disagree about what a reader is looking at.
    """
    if not sets:
        return []
    q = ",".join("?" * len(sets))
    rows = db.execute(
        "SELECT DISTINCT e.entry_id, e.name, e.verbatim FROM ruleset rs"
        f" JOIN entry e ON e.entry_id = rs.entry_id WHERE rs.set_id IN ({q})"
        " ORDER BY e.entry_id", sets).fetchall()
    out = []
    for eid, name, verbatim in rows:
        is_zone = eid.startswith("z")
        if (kind == "zone") != is_zone:
            continue
        out.append({"id": eid, "name": name, "verbatim": _squash(verbatim or "")})
    return out


def _custody(db: sqlite3.Connection, sets: list[int]) -> dict:
    """Every rule's `verbatim` against the passage its entry came from, in four buckets.

    THE ONE CHECK THAT CANNOT BE FUDGED: a rule quotes a span of the passage it was read from,
    so if the quote is not in the passage the parser wrote a sentence the book does not contain.

    It reports HOW it found each quote rather than pass/fail, because two tolerances are needed
    and both should be visible to someone deciding whether to trust this:

      case      "It is unlawful to fish with nets…" becomes a rule reading "Fish with nets…",
                so the predicate is re-capitalised as a sentence. 8 rules corpus-wide.
      markers   the passage writes `**No Fishing **upstream` and the rule `**No Fishing**
                upstream` — the bold markers move across the space. 73 rules corpus-wide.

    Neither changes a word. Anything that survives both is a quote the passage does not contain,
    and corpus-wide there are none — which is the claim worth printing, and it is worth much
    more printed beside the tolerances it needed than as a bare zero.
    """
    import re as _re

    def plain(x: str) -> str:
        return _squash(_re.sub(r"\*+", "", x or "")).lower()

    n = {"exact": 0, "case": 0, "markers": 0, "absent": 0}
    if not sets:
        return {**n, "ok": 0, "miss": 0}
    q = ",".join("?" * len(sets))
    for verb, src in db.execute(
            "SELECT DISTINCT r.verbatim, e.verbatim FROM rule r"
            " JOIN entry e ON e.entry_id = r.entry_id"
            " JOIN ruleset rs ON rs.entry_id = r.entry_id AND rs.rule_id = r.rule_id"
            f" WHERE rs.set_id IN ({q})", sets):
        a, b = _squash(verb), _squash(src)
        if not a:
            continue
        if a in b:
            n["exact"] += 1
        elif a.lower() in b.lower():
            n["case"] += 1
        elif plain(verb) and plain(verb) in plain(src):
            n["markers"] += 1
        else:
            n["absent"] += 1
    n["ok"] = n["exact"] + n["case"] + n["markers"]      # what the page already reads
    n["miss"] = n["absent"]
    return n



def _match_trace() -> tuple[dict[str, dict], dict]:
    """How every synopsis row reached its water, taken from the MATCHER ITSELF.

    The sheet and the verbatim already sit side by side in this document, and both are true of a
    water that was chosen SOMEWHERE ELSE. Nothing in either column says the book's "SHANNON LAKE
    (netted off portion on the south end of the lake)" is the netted corner rather than the lake
    — and when that binding was wrong, both columns went on agreeing with each other perfectly.

    So the binding is reported as its own fact, and it is not re-derived here: `match_rows` is the
    function the batch exporter calls, run over the same rows, the same registry and the same
    overrides. What it returns is what the corpus was built with.

    `via` is the authority, and the distinction that matters for review is:

      name      the row's name resolved to exactly one registry item on its own. Nothing was
                curated; nothing needs re-checking when the registry moves.
      override  a curator named the ids. Every one of these is a decision a human made about
                which water the book meant, and they are the rows worth reading first.

    Returns ({entry_id: trace}, corpus-wide counts).
    """
    from collections import Counter

    from pipeline.atlas.registry import default_registry_path, load_registry
    from pipeline.common.curated import CURATED
    from pipeline.regs.matching.matcher import load_overrides, match_rows
    from pipeline.regs.parsing.batch_exporter import _row_entry_id
    from pipeline.regs.parsing.rows import load_synopsis_rows

    rows = load_synopsis_rows()
    registry = load_registry(str(default_registry_path()))
    overrides = load_overrides(CURATED.regulations.overrides)
    results = match_rows(rows, registry, overrides)

    out: dict[str, dict] = {}
    for m in results:
        ids = [i for i in (m.item_id, *m.also) if i]
        out[_row_entry_id(rows[m.index], m)] = {
            "water": m.water,                       # the heading AS PRINTED, parentheticals and all
            "via": m.via or m.status,
            "status": m.status,
            "items": ids,
            "reason": _squash(m.reason)[:400],
            "mus": sorted(_reg_mus(rows[m.index])),
        }
    counts = {
        "rows": len(rows),
        "via": dict(Counter(m.via or m.status for m in results)),
        "status": dict(Counter(m.status for m in results)),
        "overrides": len(overrides),
        "registry": len(registry),
    }
    return out, counts


def _reg_mus(row: dict) -> set[str]:
    from pipeline.regs.matching.matcher import parse_reg_mus
    return parse_reg_mus(row)


def _item_names(db: sqlite3.Connection) -> dict[str, dict]:
    """item_id -> its registry name, kind, and how many sections carry it.

    The section count is the part worth printing beside a binding. An override that resolves to a
    real item and then reaches ZERO sections is indistinguishable, in the two columns above, from
    one that was never written — the rules simply do not appear. And one that reaches thousands
    has bound a name to the wrong water at a scale no reader would ever notice from a single sheet.
    """
    out = {}
    for ord_, item_id, name, kind in db.execute("SELECT ord, item_id, name, kind FROM item"):
        n = db.execute("SELECT COUNT(*) FROM item_section WHERE ord = ?", (ord_,)).fetchone()[0]
        out[item_id] = {"name": name, "kind": kind, "sections": n}
    return out




_EXCEPT = re.compile(r"does not include|excluding|except", re.I)


def _open_carve_outs(db: sqlite3.Connection) -> list[dict]:
    """Rows whose rules reach every tributary, where the book names an exception NOBODY APPLIED.

    A synopsis row marked [Includes Tributaries] puts its rules on the whole catchment above the
    water — tens of thousands of sections from one line of print. Several of those lines then name
    what they do NOT cover: "Does not include the Kootenay River upstream from Kootenay Lake to the
    U.S. border near Creston".

    The parser records that sentence faithfully, as a rule of its own, and stops there — carving it
    out is a curator's job, because "upstream from Kootenay Lake to the U.S. border" is a reach
    somebody has to point at. The field for it (`rule.tributary_excludes`) is built, resolved by
    the same machinery as any other extent, and tested. It is simply still empty on these rows.

    Until it is filled, each of these rules covers water the book says it does not — which no
    single sheet can reveal, because the sheet where it shows is the EXCEPTED water's, and there it
    looks like an ordinary rule. Only the pair does. So it is counted here, and measured: how far
    the rule reaches, and how much of that sits on the very water named as excepted.
    """
    import json as _json

    from pipeline.common.curated import CURATED

    out = []
    corpora = (CURATED.regulations.entries.catalogue, CURATED.regulations.entries.dfo_salmon)
    for path in sorted(q for d in corpora for q in Path(d).glob("region-*.json")):
        for e in _json.loads(path.read_text(encoding="utf-8")).get("entries", []):
            rules = e.get("rules") or []
            expands = (bool(e.get("includes_tributaries"))
                       or bool((e.get("tributaries") or {}).get("included"))
                       or any(r.get("tributaries_only") or r.get("includes_tributaries")
                              for r in rules))
            if not expands:
                continue
            if any(r.get("tributary_excludes") for r in rules) or \
                    (e.get("tributaries") or {}).get("excludes"):
                continue                                  # a curator has already carved it
            said = [r for r in rules if _EXCEPT.search(r.get("verbatim") or "")]
            if not said:
                continue
            n = db.execute(
                "SELECT COUNT(DISTINCT sr.sid) FROM ruleset rs"
                " JOIN section_ruleset sr ON sr.set_id = rs.set_id"
                " WHERE rs.entry_id = ?", (e["entry_id"],)).fetchone()[0]
            out.append({
                "entry": e["entry_id"],
                "name": e.get("name") or "",
                "sections": n,
                "said": [_squash(r.get("verbatim") or "")[:190] for r in said],
            })
    out.sort(key=lambda r: -r["sections"])
    return out



def _pick_stretch(db: sqlite3.Connection, runs: list[dict]) -> int:
    """Which stretch to show — the one where a mistake would be visible.

    "The last one" was the rule, on the reasoning that a river's head carries its own rules
    where its mouth carries regional defaults. It is wrong often enough to matter: the last
    stretch of the Bella Coola is a ZERO-KILOMETRE remnant with no rules at all, and the
    document was holding it up as that river's evidence.

    So it is chosen by what is actually on it: the stretch reached by the most of the water's
    OWN synopsis rows, since those are the ones a binding can get wrong. Regional and
    provincial text reaches every stretch equally and cannot separate them. Ties go to the
    longest, then to the furthest upstream.
    """
    if not runs:
        return 0
    best, best_key = 0, None
    for i, r in enumerate(runs):
        sid = r.get("set")
        own = 0
        if sid is not None:
            own = db.execute(
                "SELECT COUNT(DISTINCT entry_id) FROM ruleset"
                " WHERE set_id = ? AND entry_id NOT LIKE 'z%'", (sid,)).fetchone()[0]
        size = r.get("area") if r.get("area") is not None else (r.get("km") or 0.0)
        key = (own, size or 0.0, i)
        if best_key is None or key > best_key:
            best, best_key = i, key
    return best


def _bind(entries: list[dict], trace: dict, items: dict, w: dict) -> list[dict]:
    """Attach each entry's binding — the step neither column above can show.

    The sheet and the verbatim sit side by side, and both stay true of a water that was chosen
    somewhere else entirely. Nothing in either column says the book's "SHANNON LAKE (netted off
    portion on the south end of the lake)" is the netted corner rather than the whole lake — and
    while that binding was wrong, the two columns went on agreeing with each other perfectly.

    `direct` says whether the binding names the very item the page is keyed on. It is FALSE for a
    good reason on every lake the book writes as several waters: the page draws Kootenay Lake under
    FWA's single polygon, while its rows bind to the three curated arms cut out of it. So a false
    here means "the rules arrived through a part, not the whole", which is exactly the arrangement
    worth looking at — not a failure.
    """
    page_item = w.get("item", "")
    out = []
    for e in entries:
        t = trace.get(e["id"])
        if t is None:
            # A zone entry (provincial or regional text) is not a synopsis water row and has no
            # match to report. Saying so beats an empty cell that reads like a failure.
            out.append({**e, "bind": None})
            continue
        out.append({**e, "bind": {
            "via": t["via"], "status": t["status"], "water": t["water"],
            "reason": t["reason"], "mus": t["mus"],
            "direct": page_item in t["items"],
            "items": [{"id": i, **items.get(i, {"name": "", "kind": "", "sections": 0})}
                      for i in t["items"]],
        }})
    return out


def capture(waters: list[str] | None, port: int) -> tuple[list[dict], dict]:
    """Open the real page and lift the rendered sheet for each water."""
    from playwright.sync_api import sync_playwright

    trace, counts = _match_trace()

    data = json.loads(re.search(
        r'<script id="d" type="application/json">(.*?)</script>',
        SOURCE_PAGE.read_text(encoding="utf-8"), re.S).group(1))
    names = [n for n in data if not n.startswith("_")]
    if waters:
        names = [n for n in names if n in waters]

    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    items = _item_names(db)
    bad: list[str] = []
    carve = _open_carve_outs(db)
    out: list[dict] = []
    with sync_playwright() as pw:
        # The system Chrome, not Playwright's own download. `playwright install` pulls ~150 MB
        # onto a machine that already has a browser, and this tool is run by hand on a dev box
        # rather than in CI.
        try:
            browser = pw.chromium.launch(channel="chrome")
        except Exception:
            browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width": 430, "height": 1400})
        page.goto(f"http://127.0.0.1:{port}/regs-v3.html", wait_until="load")
        for name in names:
            w = data[name]
            runs = w.get("runs") or []
            pick = _pick_stretch(db, runs)
            got = page.evaluate(
                """([name, i]) => {
                  /* THE PAGE IS NOT ONE APP YOU NAVIGATE — it is nineteen independent phone
                     columns side by side, each with its own `.screen`. So an unscoped
                     `document.querySelector('.screen')` returns the FIRST column's, which is
                     the Fraser's, and it does so for every water asked for. That is exactly
                     what happened: this document shipped nineteen sheets and every one of them
                     was the Fraser, captioned with somebody else's name. Everything here is
                     scoped to the column whose own heading matches. */
                  const col = [...document.querySelectorAll('.col')].find(
                      c => (c.querySelector('.wname') || {}).textContent?.trim() === name);
                  if (!col) return null;
                  /* Back to this column's stretch list — it may already be showing a sheet from
                     an earlier water in the loop, and `back` is a no-op once it is at the top. */
                  for (let k = 0; k < 3; k++) {
                    if (col.querySelector('button.r.st')) break;
                    const b = col.querySelector('button.back');
                    if (!b) break;
                    b.click();
                  }
                  const st = [...col.querySelectorAll('button.r.st')];
                  /* The rung's OWN name, read off the control before it is clicked.
                     Reconstructing it here would be a second implementation of the page's
                     naming — the thing this document exists not to do. */
                  let rung = "", size = "";
                  if (st.length) {
                    const b = st[Math.min(i, st.length - 1)] || st[0];
                    const sub = b.querySelector('.subj'), mt = b.querySelector('.meta');
                    rung = (sub ? sub.textContent : b.textContent).trim().replace(/\\s+/g, " ");
                    size = mt ? mt.textContent.trim().replace(/\\s+/g, " ") : "";
                    b.click();
                  }
                  const s = col.querySelector('.screen');
                  if (!s) return null;
                  /* THE MAP IS NOT WHAT IS BEING VERIFIED, and it is most of the bytes: one
                     Fraser sheet came to 546 KB, nearly all of it path data for a river drawn
                     at full precision. The claim here is about the RULES beside it. */
                  const c = s.cloneNode(true);
                  c.querySelectorAll('svg, .mapbox, canvas').forEach(n => n.remove());
                  /* The heading the SHEET ITSELF carries, returned so the caller can REFUSE a
                     sheet that is not the water it asked for rather than captioning it. `.wsub`
                     is the app's own description of the stretch — taken verbatim rather than
                     rebuilt, for the same reason as the rung name. */
                  const txt = q => ((s.querySelector(q) || {}).textContent || "")
                      .trim().replace(/\\s+/g, " ");
                  return {html: c.innerHTML, rung, size,
                          head: txt('.wname'), sub: txt('.wsub')};
                }""", [name, pick])
            if not got or not got.get("html"):
                print(f"  !! {name}: the page did not render a sheet")
                bad.append(name)
                continue
            sheet = got["html"]
            # THE SHEET MUST NAME THE WATER IT WAS ASKED FOR. Without this the capture fails
            # silently and beautifully: a real sheet, correctly rendered, filed under another
            # river's name — and every number beside it goes on agreeing, because the numbers
            # come from the bundle and never look at the picture. It shipped that way once.
            if name.lower() not in (got.get("head") or "").lower():
                print(f"  !! {name}: the sheet says {got.get('head')!r} — refusing it")
                bad.append(name)
                continue
            run = runs[pick] if runs else {}
            # The columns are scoped to the stretch the sheet shows; custody is a claim about the
            # whole water, so it takes every stretch's ruleset.
            here_sets = [run["set"]] if run.get("set") is not None else []
            all_sets = [r["set"] for r in runs if r.get("set") is not None]
            out.append({
                "name": name,
                "sheet": sheet,
                "km": w.get("total") or 0.0,
                "runs": len(runs),
                "nrules": len(w.get("rules") or []),
                "unplaced": len(w.get("unplaced") or []),
                "kind": w.get("kind") or "stream",
                "stretch": {"n": pick + 1, "of": len(runs),
                            "from": run.get("from"), "to": run.get("to"),
                            "rung": got.get("rung") or run.get("label") or "",
                            "size": got.get("size") or "",
                            "sub": got.get("sub") or "",
                            "mus": list(run.get("mus") or []),
                            "oob": bool(run.get("oob")),
                            "lake": run.get("bkind") == "part"},
                "own": _bind(_entries_for(db, here_sets, "own"), trace, items, w),
                "zone": _bind(_entries_for(db, here_sets, "zone"), trace, items, w),
                "custody": _custody(db, all_sets),
                "item": w.get("item", ""),
            })
            print(f"  {name}: {len(sheet):,} bytes, stretch {pick+1}/{len(runs)}, "
                  f"{out[-1]['custody']['ok']} quotes found, "
                  f"{out[-1]['custody']['absent']} absent")
        browser.close()
    if bad:
        raise SystemExit(f"refused {len(bad)} sheet(s) that did not name their water: "
                         + ", ".join(bad) + "\n"
                         "The capture is wrong, and a partial document is worse than none — it "
                         "would look complete.")
    counts["carve_outs"] = carve
    return out, counts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--waters", help="comma-separated subset; default every water on the page")
    ap.add_argument("--port", type=int, default=8899,
                    help="a static server already serving app/design (default 8899)")
    a = ap.parse_args()

    waters = [s.strip() for s in a.waters.split(",")] if a.waters else None
    got, counts = capture(waters, a.port)
    if not got:
        print("nothing captured — is the page being served?")
        return 1

    html = PAGE.read_text(encoding="utf-8")
    pack = json.loads(re.search(r'<script id="pack" type="application/json">(.*?)</script>',
                                html, re.S).group(1))
    pack["waters"] = got                       # the CSS block is authored, not generated
    pack["matching"] = counts
    blob = json.dumps(pack, ensure_ascii=False, separators=(",", ":"))
    html = re.sub(r'(<script id="pack" type="application/json">).*?(</script>)',
                  lambda m: m.group(1) + blob + m.group(2), html, count=1, flags=re.S)
    PAGE.write_text(html, encoding="utf-8")
    print(f"\nwrote {len(got)} water(s), {len(blob):,} bytes into "
          f"{PAGE.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
