"""Rebuild `app/design/verification-artifact.html` from what the page actually renders.

THE DOCUMENT'S CLAIM IS THAT THE OUTPUT IS RIGHT, so every sheet in it has to be the output —
not a description of it, and not a copy made once. It was built by hand, which is why it went
stale the moment the corpus moved: it was still showing thirteen rivers against a corpus of
1,032 entries long after the corpus had become 1,480, and nothing about the page said so.

So it is generated, the same way `regs-v3.html` is. The difference is where the sheets come
from: the page decides what to draw in its own JavaScript, so the only honest way to capture
that is to RUN it. Playwright opens the real page, walks to a water and a stretch, and lifts
the rendered `.screen` — the same DOM a reader sees, markup and all.

Everything beside each sheet comes from the shipped bundle, so the two halves of the comparison
have different sources: the sheet is what the page drew, the verbatim is what the book said.
They agree or they do not.

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


def _entries_for(db: sqlite3.Connection, item_id: str, kind: str) -> list[dict]:
    """The entries reaching this water, split into its OWN rows and the regional/provincial ones.

    `kind` is "own" or "zone". A zone entry's id starts with `z`; everything else was written
    for a named water. That is the distinction the page draws in its "set by" column, so the
    artifact has to draw the same one or the two disagree about what a reader is looking at.
    """
    rows = db.execute(
        "SELECT DISTINCT e.entry_id, e.name, e.verbatim FROM entry e"
        " JOIN rule r ON r.entry_id = e.entry_id"
        " JOIN ruleset rs ON rs.entry_id = e.entry_id AND rs.rule_id = r.rule_id"
        " JOIN section_ruleset sr ON sr.set_id = rs.set_id"
        " JOIN item_section isec ON isec.sid = sr.sid"
        " JOIN item i ON i.ord = isec.ord"
        " WHERE i.item_id = ? ORDER BY e.entry_id", (item_id,)).fetchall()
    out = []
    for eid, name, verbatim in rows:
        is_zone = eid.startswith("z")
        if (kind == "zone") != is_zone:
            continue
        out.append({"id": eid, "name": name, "verbatim": _squash(verbatim or "")})
    return out


def _custody(db: sqlite3.Connection, item_id: str) -> dict:
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
    for verb, src in db.execute(
            "SELECT DISTINCT r.verbatim, e.verbatim FROM rule r"
            " JOIN entry e ON e.entry_id = r.entry_id"
            " JOIN ruleset rs ON rs.entry_id = r.entry_id AND rs.rule_id = r.rule_id"
            " JOIN section_ruleset sr ON sr.set_id = rs.set_id"
            " JOIN item_section isec ON isec.sid = sr.sid"
            " JOIN item i ON i.ord = isec.ord WHERE i.item_id = ?", (item_id,)):
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


def capture(waters: list[str] | None, port: int) -> list[dict]:
    """Open the real page and lift the rendered sheet for each water."""
    from playwright.sync_api import sync_playwright

    data = json.loads(re.search(
        r'<script id="d" type="application/json">(.*?)</script>',
        SOURCE_PAGE.read_text(encoding="utf-8"), re.S).group(1))
    names = [n for n in data if not n.startswith("_")]
    if waters:
        names = [n for n in names if n in waters]

    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
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
            # The LAST stretch that is not out of bounds: the head of a river carries the
            # water-specific rules, where the mouth is usually regional defaults only.
            runs = w.get("runs") or []
            pick = max(0, len(runs) - 1)
            got = page.evaluate(
                """([name, i]) => {
                  document.querySelectorAll('button.back').forEach(b => b.click());
                  document.querySelectorAll('button.back').forEach(b => b.click());
                  const el = [...document.querySelectorAll('.wname')]
                      .find(e => e.textContent.trim() === name);
                  if (!el) return null;
                  (el.closest('button') || el.closest('.col') || el).click();
                  const st = [...document.querySelectorAll('button.r.st')];
                  if (st.length) (st[Math.min(i, st.length - 1)] || st[0]).click();
                  const s = document.querySelector('.screen');
                  if (!s) return null;
                  /* THE MAP IS NOT WHAT IS BEING VERIFIED, and it is most of the bytes: one
                     Fraser sheet came to 546 KB, nearly all of it path data for a river drawn
                     at full precision. The claim here is about the RULES beside it. */
                  const c = s.cloneNode(true);
                  c.querySelectorAll('svg, .mapbox, canvas').forEach(n => n.remove());
                  return c.innerHTML;
                }""", [name, pick])
            if not got:
                print(f"  !! {name}: the page did not render a sheet")
                continue
            run = runs[pick] if runs else {}
            out.append({
                "name": name,
                "sheet": got,
                "km": w.get("total") or 0.0,
                "runs": len(runs),
                "nrules": len(w.get("rules") or []),
                "unplaced": len(w.get("unplaced") or []),
                "kind": w.get("kind") or "stream",
                "stretch": {"n": pick + 1, "of": len(runs),
                            "from": run.get("from"), "to": run.get("to"),
                            "lo": run.get("label") or "", "hi": "",
                            "within": ", ".join(run.get("mus") or [])},
                "own": _entries_for(db, w.get("item", ""), "own"),
                "zone": _entries_for(db, w.get("item", ""), "zone"),
                "custody": _custody(db, w.get("item", "")),
            })
            print(f"  {name}: {len(got):,} bytes, stretch {pick+1}/{len(runs)}, "
                  f"{out[-1]['custody']['ok']} quotes found, "
                  f"{out[-1]['custody']['absent']} absent")
        browser.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--waters", help="comma-separated subset; default every water on the page")
    ap.add_argument("--port", type=int, default=8899,
                    help="a static server already serving app/design (default 8899)")
    a = ap.parse_args()

    waters = [s.strip() for s in a.waters.split(",")] if a.waters else None
    got = capture(waters, a.port)
    if not got:
        print("nothing captured — is the page being served?")
        return 1

    html = PAGE.read_text(encoding="utf-8")
    pack = json.loads(re.search(r'<script id="pack" type="application/json">(.*?)</script>',
                                html, re.S).group(1))
    pack["waters"] = got                       # the CSS block is authored, not generated
    blob = json.dumps(pack, ensure_ascii=False, separators=(",", ":"))
    html = re.sub(r'(<script id="pack" type="application/json">).*?(</script>)',
                  lambda m: m.group(1) + blob + m.group(2), html, count=1, flags=re.S)
    PAGE.write_text(html, encoding="utf-8")
    print(f"\nwrote {len(got)} water(s), {len(blob):,} bytes into "
          f"{PAGE.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
