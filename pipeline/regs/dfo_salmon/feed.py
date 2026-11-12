"""feed — the scraped rules, structured, keyed by the locator they sit on.

**Rules are a feed. Locators are curated. They meet at read time and never in one file.**

That is the same shape the gauges use: `data/curated/gauges/matches.json` holds the match from
station to node and is written once by a human, while the readings arrive on a schedule and are
joined to it at build time. A reading is never written into the curated file, and neither is a
quota. The reason is measured rather than stylistic — Region 6 held 77/77 waters over 2.3 years
while its rules churned +110/-97, so a run that rewrote curated data every time would be rewriting
a file whose durable half never moved.

    curated   data/curated/dfo_salmon/places/   locators: item, cut-points, tributaries
    feed      data/generated/regs/dfo_salmon/typed/          rules: species, dates, limits, gear
    join      EntryFile.by_fingerprint()                     fingerprint -> location_id -> geometry

This module writes only the feed, and takes **no curated input**: it is a pure function of the
page, so the scheduled run needs nothing a curator has touched and cannot corrupt anything one
has. `--resolve` reports how the feed would join, which is diagnostics, not data.

THE JOIN KEY IS THE FINGERPRINT — the scrape's own key, the way a reading carries its station id.
It is deliberately not `location_id`, which exists only on the curated side: deriving the feed's
key from curated state would make the feed unbuildable without it, and would silently re-key every
rule the day a curator merged two locators. Wording drift is absorbed on the curated side, where
`fingerprints` is a list precisely because DFO flipped the Kispiox sign count and flipped it back.

CLI
---
    .venv/bin/python -m pipeline.regs.dfo_salmon.feed              # write the feed
    .venv/bin/python -m pipeline.regs.dfo_salmon.feed --resolve    # how it joins to the locators
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.common.curated import GENERATED
from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES, normalize_slug
from pipeline.regs.dfo_salmon.typed import FORMAT as TYPED_FORMAT, row_text, to_rules
from pipeline.regs.dfo_salmon.wording import row_words

logger = logging.getLogger(__name__)

RULES_DIR = Path(GENERATED.regs.dfo_salmon) / "rules"
FEED_DIR = Path(GENERATED.regs.dfo_salmon) / "typed"


#: (T679 D6, owner 2026-11-12) the `check` a place reads when its rows did not type (`unread_rule`); the worker's
#: `ROW_HELD_VERDICT` says the same of a place it holds with no last answers
ROW_UNREAD = "DFO's row for this place could not be read — check DFO"


def unread_rule(fp: str, recs: List[dict], why: str) -> dict:
    """(T679 D6) THE ONE RULE OF A LOCATOR WHOSE ROWS DID NOT TYPE: an advisory naming no species (it reads for every
    salmon) carrying `check` (`ROW_UNREAD`), so every species reads "check DFO" on every day at the place
    (`deliver.salmon.verdict_on`) — never a dropped row, and never a wider place's answer standing in for it. Its words
    are the rows' limits; its season is unread, so `when.unparsed` holds the date cells as printed (the model's rule
    for a `check`); `review_reason` keeps why it did not type."""
    from pipeline.regs.parsing.catalogue import CatalogueRule, RuleType
    from pipeline.regs.parsing.catalogue_parts.dates import When
    limits = [str(r.get("limits_gear") or r.get("species") or "").strip() for r in recs]
    dates = [str(r.get("dates") or "").strip() for r in recs]
    rule = CatalogueRule(rule_id=f"{fp[:8]}.r0", type=RuleType.advisory,
                         verbatim=" / ".join(x for x in limits if x) or "row",
                         when=When(unparsed=[d for d in dates if d] or ["(no dates printed)"]),
                         check=ROW_UNREAD, review_reason=f"{ROW_UNREAD}: {why}")
    return rule.model_dump(mode="json", exclude_defaults=True, by_alias=True)


def type_rows(fp: str, recs: List[dict], robust: bool = True) -> Tuple[List[dict], Optional[str]]:
    """One locator's scraped rows -> (its typed rules, why they did not type or None).

    (T679 D6, owner 2026-11-12: "Python reads DFO rows tolerantly too") ROBUST, as the feeds worker (`feed.ts` `build`):
    a row the catalogue refuses (or any error typing it) no longer raises out of the page and stops the release. The
    locator carries `error` ("<Type>: <first 200 characters>") and the one `unread_rule`, so the place reads "check" and
    the rest of the region stands; `build` logs it and `deliver.salmon` names it. `robust=False` raises, as before: the
    Mac's live run (`live.type_page`) keeps holding the region, the parity spec of the worker's `pythonParity` mode."""
    rows = [row_text(r) for r in recs]
    try:
        rules = [r for i, rec in enumerate(recs) for r in to_rules(rec, fp[:8], i)]
        return [r.model_dump(mode="json", exclude_defaults=True, by_alias=True) for r in rules], None
    except Exception as e:                              # noqa: BLE001 — said: `error` on the locator, the place reads check
        if not robust:
            raise
        why = f"{type(e).__name__}: {str(e)[:200]}"
        return [unread_rule(fp, recs, why)], why


def build(scraped: dict, robust: bool = True) -> dict:
    """One region's scraped rows -> the typed feed, grouped by the locator fingerprint. `robust` (default): a locator
    whose rows do not type reads "check" (`type_rows`) instead of stopping the page."""
    by_fp: Dict[str, List[dict]] = defaultdict(list)
    for rec in scraped.get("rules", []):
        by_fp[rec["fingerprint"]].append(rec)

    locators = []
    for fp, recs in by_fp.items():
        rules, error = type_rows(fp, recs, robust)
        if error:
            logger.warning("DFO region %s: the rows of %s did not type (%s): the place reads 'check DFO'",
                           scraped.get("region"), fp, error)
        locators.append({
            "fingerprint": fp,
            # The rows this locator published, verbatim. A rule's `verbatim` is a span of one of
            # these, so whoever joins the feed can check the quote without the page.
            "rows": [row_text(r) for r in recs],
            "rules": rules,
            **({"error": error} if error else {}),
            # T321: the rows the card shows in DFO's words — new wording (unparsed), an unread number, or a note
            **({"words": words} if (words := [w for w in map(row_words, recs) if w]) else {}),
        })

    # T349: the page's default sentences ("There is no fishing for salmon in Region 3 except …"), each the one rule of
    # its page-default location (`cascade.statement_locators`, keyed `page:<slug>:<scope>`)
    from pipeline.regs.dfo_salmon.cascade import statement_locators
    locators.extend(statement_locators(str(scraped.get("region") or ""), scraped.get("preamble") or []))

    return {
        "region": scraped.get("region"),
        "date_modified": scraped.get("date_modified"),
        # (T671) how these rows were typed (`typed.FORMAT`): the release refuses rows typed by older code
        "typed_format": TYPED_FORMAT,
        # T366 ONE VINTAGE: the page these rows came from ({sha256, date_modified}) and its header notes, so a reader
        # can tell typed rows older than the raw page, and the release ships the header of the rows' own page
        **({"page": scraped["page"]} if scraped.get("page") else {}),
        # (T358) the header format the notes are in: a reader takes notes only in today's (`header.FORMAT`). (T482) The
        # SCRAPED file's own format, never today's stamped on notes parsed before it: a pre-T358 rules file was stamped
        # /2 here, so the release took its `cm` notes as current and no size or daylight note folded (R1, R2, R6)
        **({"notes": scraped["notes"], **({"header_format": scraped["header_format"]} if scraped.get("header_format") else {})}
           if "notes" in scraped else {}),
        "preamble": scraped.get("preamble") or [],
        "locators": locators,
    }


def emit(slug: str, *, rules_dir: Path = RULES_DIR, out_dir: Path = FEED_DIR) -> tuple[int, int]:
    src = rules_dir / f"region-{slug}.json"
    if not src.exists():
        return 0, 0
    feed = build(json.loads(src.read_text(encoding="utf-8")))
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"region-{slug}.json").write_text(
        json.dumps(feed, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return len(feed["locators"]), sum(len(l["rules"]) for l in feed["locators"])


def resolve(slug: str, *, feed_dir: Path = FEED_DIR) -> dict:
    """How this region's feed joins to the curated locators. Reporting only — writes nothing.

    A fingerprint the entry file does not know is the page publishing a reach nobody has bound,
    which PLAN.md holds for a curator rather than publishing around. It is reported here so the
    scheduled run can say so without the feed depending on curated state to be written.
    """
    from pipeline.regs.dfo_salmon import entries as E

    p = Path(feed_dir) / f"region-{slug}.json"
    if not p.exists():
        return {"region": slug, "bound": 0, "unbound": [], "no_extent": []}

    ef = E.load(slug)
    index = ef.by_fingerprint()
    bound, unbound, no_extent = 0, [], []
    for loc in json.loads(p.read_text(encoding="utf-8"))["locators"]:
        hit = index.get(loc["fingerprint"])
        if hit is None:
            unbound.append(loc)
            continue
        bound += 1
        if not hit.binding.extents:
            no_extent.append(hit.location_id)
    return {"region": slug, "bound": bound, "unbound": unbound, "no_extent": no_extent}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regions", nargs="+", choices=ALL_SLUGS)
    ap.add_argument("--rules-dir", type=Path, default=RULES_DIR)
    ap.add_argument("--out", type=Path, default=FEED_DIR)
    ap.add_argument("--resolve", action="store_true",
                    help="report how the feed joins to the curated locators")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    todo = args.regions or [s for s in ALL_SLUGS if not PAGES[s].is_stub]
    tl = tr = 0
    for slug in todo:
        nl, nr = emit(normalize_slug(slug), rules_dir=args.rules_dir, out_dir=args.out)
        tl += nl; tr += nr
        print(f"region {slug:<3} {PAGES[slug].name:<38} locators={nl:<4} rules={nr}")
    print(f"\n{tl} locators, {tr} rules -> {args.out}")

    if args.resolve:
        print("\nHow it joins:")
        tb = tn = 0
        for slug in todo:
            rep = resolve(normalize_slug(slug), feed_dir=args.out)
            tb += rep["bound"]; tn += len(rep["no_extent"])
            for loc in rep["unbound"]:
                print(f"  region {slug}: UNBOUND fingerprint {loc['fingerprint']} — "
                      f"{len(loc['rules'])} rule(s): {loc['rows'][0][:70]}")
        print(f"  {tb} locators bind to a curated record; {tn} of those have no extent yet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
