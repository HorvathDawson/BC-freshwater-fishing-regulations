# Waterbody-grouped split curation — model + handoff

**Status: active workflow (2026-08-04).** This supersedes the row-at-a-time review of
`14-locators-to-curate.json` for the *completeness* pass. It does **not** discard any prior
curation — it is a grouping + reconciliation VIEW over the same rows.

## Why we changed

`14-locators-to-curate.json` is a flat list of 651 locator rows. A single waterbody's regulation
usually contains **several** splits, and they land in **separate** rows. Reviewing rows in
isolation made it easy to:

- **miss a split** the reg text contains but the parser never turned into a row (e.g. DEAN RIVER's
  Crag-Creek→canyon reaches), and
- **mistake a real split for a duplicate** (e.g. `great-central-4ff0ab`, actually its own Stamp
  River reach).

The fix: **group every reg entry that has splits, and solve all of its splits together, checked
against the reg text**, so nothing is half-finished before we emit `splits.json`.

## The model

- **Unit = one reg entry** = `(waterbody name_verbatim, MU set, reg text)`. One synopsis listing.
  Each locator row now carries a stable **`entry_id`** (sha1 of that triple) so rows group durably.
- **Ground truth of "what splits exist" = the parsed synopsis.** `output/pipeline/parsing/
  synopsis_parsed.json` holds one entry per listing, each with a `rules` list; every rule with a
  non-empty `location_text` is a **boundary** (a split). The 651 locator rows were derived from
  these rules.
- **Link key:** `normalize(locator.full_regulation) == normalize(synopsis.regs_verbatim)` where
  `normalize` strips markdown `*` and collapses whitespace. This joins **343/343** locator regs
  (all 651 rows). Within an entry, a boundary links to a row by normalized equality/containment of
  `locator_text`.

## The tool — `oneoff/waterbody_splits.py`

Regenerable view; never hand-edit its outputs. The curated data in `14-locators-to-curate.json`
stays the source of truth.

```bash
.venv/bin/python -m stream_sections.oneoff.waterbody_splits              # write JSON+MD, print summary
.venv/bin/python -m stream_sections.oneoff.waterbody_splits show "DEAN RIVER"    # one waterbody's card (JSON)
.venv/bin/python -m stream_sections.oneoff.waterbody_splits missing      # every reg boundary with NO row
.venv/bin/python -m stream_sections.oneoff.waterbody_splits incomplete   # entries not fully resolved
```

Outputs: **`docs/waterbody-splits.json`** (machine) and **`docs/waterbody-splits.md`** (human,
worst-first). Per entry it links **each reg boundary → the curated row(s) that resolve it**, with a
flag:

| flag | meaning | action |
|---|---|---|
| `OK` | boundary linked to ≥1 row | curate that row if still `todo` |
| `MISSING` | reg boundary with **no** row | a split we never captured — add/curate a row (or confirm it's covered by another boundary's row / a name-variant) |
| `DUP` | >1 row, same boundary | keep one, mark the rest duplicate → reference (see runbook) |
| orphan | a row matching no boundary | usually a `name`/`except` row (expected); investigate `rule`/`entry` src |

Per-entry **completeness**: `NO_SPLITS` · `COMPLETE` (every boundary resolved: curated/manual/
not_applicable/deferred, no todo) · `INCOMPLETE` (todo/unresolved) · `MISSING_SPLITS` (≥1 MISSING).

## Current state (2026-08-04, first run)

`372 entries → MISSING_SPLITS 13 · INCOMPLETE 183 · COMPLETE 79 · NO_SPLITS 97`; **22 MISSING
boundaries**. The `MISSING`/orphan matcher is a first-pass (substring) — **verify each MISSING by
hand**: some are true gaps, some are matcher misses (a row exists with slightly different wording),
some are covered by an `[Includes Tributaries]` A→B whose row is the reach itself.

## Workflow (per entry, worst-first)

1. `waterbody_splits incomplete` → pick a `MISSING_SPLITS` / high-boundary `INCOMPLETE` entry.
2. `waterbody_splits show "<NAME>"` → read the reg text + every boundary and its linked row/status.
3. For each boundary: `OK`+`todo` → curate the row (resolve coord/target per `docs/17`); `MISSING`
   → confirm it's a real split and add/curate a row, else annotate why it's covered; `DUP` → dedup
   to one canonical + reference.
4. When all boundaries are resolved the entry flips to `COMPLETE`; its splits are ready to author
   into `splits.json`.
5. Commit; re-run the tool to refresh the cards.

## Preservation guarantee

Every `curated`/`manual`/`not_applicable`/`deferred` decision already made is untouched — the tool
only reads them. `entry_id` is an additive field. `14-locators-to-curate.json` remains the editable
source of truth; `waterbody-splits.*` are regenerated from it. See `docs/17-manual-review-runbook.md`
for per-row writing rules and `docs/15` for lake-internal (deferred) splits.
