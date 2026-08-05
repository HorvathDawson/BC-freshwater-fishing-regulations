# Waterbody-grouped split curation (source-first) — model + handoff

**Status: active workflow (2026-08-05).** Completeness pass over the split curation in
`14-locators-to-curate.json`. It does **not** discard any prior curation — it regenerates the split
structure from the original regs source, **backfills** the curation we've already done, and **warns**
about anything that no longer maps.

## Why

`14-locators-to-curate.json` is a flat list of 651 locator rows. A waterbody's regulation usually
contains **several** splits, landing in **separate** rows, so reviewing rows in isolation made it easy
to **miss a split** the reg text contains — either a boundary with no row (e.g. DEAN RIVER's canyon
reaches) or a whole reg never started. The fix: rebuild **from source**, waterbody-by-waterbody,
checked against the reg text, so nothing is half-finished before we author `splits.json`.

## Sources & join

- **SPINE — `output/pipeline/extraction/synopsis_raw_data.json`.** The raw synopsis rows (the
  original regs): `water`, `mu`, `region`, `raw_regs`, `symbols`, `page`, `image`. 1395 entries.
  This is the authoritative "what regs exist / what waterbodies have splits" list.
- **PARSE — `output/pipeline/parsing/synopsis_parsed.json`.** Per-reg `rules`; a rule with a
  non-empty `location_text` is a **boundary** = a split. Joined to the spine by `normalize(raw_regs)
  == normalize(regs_verbatim)`.
- **CURATION — `14-locators-to-curate.json`.** Our work; **backfilled** into the structure, never
  mutated by the tool (except the optional `entry_id` stamp).

**Locators per entry** come from four places (matching the original `src` field):
`water` (a boundary encoded in the water NAME, e.g. `ALEXANDER CREEK (downstream of the Hwy 3 bridge)`)
· `rule` (a rule's `location_text`) · `except` (a rule's `exception`) · `entry` (`entry_location_text`).
Plus a non-boundary `name` membership for whole-water / tributary-set rows.

**Curation ↔ source join = WATER NAME + MU, not reg text.** Reg text is unreliable (near-identical
entries differ by a comma; curated `full_regulation` sometimes differs entirely). The join matches a
curated row to a source entry when `mus` overlap and the curated `name_verbatim` (truncated) is a
prefix of / contained in the source `water`, after dropping **alias** parentheticals (`(McNaughton)`,
`("Blackwater")`) while KEEPING **boundary** ones (`(downstream of falls)`).

## The tool — `oneoff/waterbody_splits.py`

Regenerable; never hand-edit its outputs. Curation stays the source of truth.

```bash
.venv/bin/python -m stream_sections.oneoff.waterbody_splits              # write JSON+MD, print summary
.venv/bin/python -m stream_sections.oneoff.waterbody_splits show "DEAN RIVER"    # one waterbody's card
.venv/bin/python -m stream_sections.oneoff.waterbody_splits missing      # reg boundaries with NO row
.venv/bin/python -m stream_sections.oneoff.waterbody_splits incomplete   # entries not fully resolved
.venv/bin/python -m stream_sections.oneoff.waterbody_splits drift        # curated rows not found in source
.venv/bin/python -m stream_sections.oneoff.waterbody_splits stamp        # write entry_id onto rows (row<->card link)
```

Outputs **`docs/waterbody-splits.json`** (`{cards, drift}`) and **`docs/waterbody-splits.md`**
(human, worst-first). Each card links **every reg boundary → the curated row(s) that resolve it**,
with the split info block (src, restriction_type, dates, includes_tributaries, page/image) an author
needs. Each curated row carries a stamped **`entry_id`** pointing back to its card.

Per-locator flag: `MISSING` (boundary with no curated row) · else the backfilled row status.
Per-entry **completeness**: `COMPLETE` (every boundary resolved) · `INCOMPLETE` (todo remains) ·
`MISSING_SPLITS` (≥1 boundary with no row) · `NO_CURATION` (split-bearing reg with zero curated rows).
Global **DRIFT**: curated rows matching no source locator (real anomalies / manual additions / naming).

The name+MU matcher is prefix/containment based — treat `MISSING`/`DRIFT` as **review candidates**,
not gospel: a `MISSING` may be a matcher-miss or a non-spatial `except`; a `DRIFT` may be a trib-set
row with no standalone source entry.

## Current state (2026-08-05)

The output JSON now carries **every field of each curated row** embedded under its locator (via
`l["rows"]`), so `waterbody-splits.json` is a **superset of `14-locators-to-curate.json` grouped by
reg entry** — the intended successor file. A card is emitted for **every entry that carries
curation**, not only split-bearing ones; whole-water / tributary-set entries (no boundary) get
completeness **`NO_SPLIT`**.

`383 cards → COMPLETE 99 · INCOMPLETE 210 · MISSING_SPLITS 3 · NO_SPLIT 71 · NO_CURATION 0`.
Rows embedded: **639/651** (7 Dean endpoint rows + 5 drift not embedded — see below).

**The 8 MISSING boundaries are all matcher/model artifacts, NOT unauthored gaps** (verified
2026-08-05):
- **DEAN RIVER (6)** — every canyon reach IS authored, as 7 `curated` *endpoint* rows (Crag Creek
  + Iltasyuko confluences, Anahim Lake boundary, 3 sign points, tidal boundary). They carry
  **empty `locator_text`**, and the backfill matches reach-boundaries by `locator_text`, so it
  can't link an endpoint row to a "from A to B" reach. Dean is effectively COMPLETE; the fix is a
  **reach↔endpoint linker** (or populating each endpoint row's `locator_text`).
- **NATION ARM (1)** — the `line` row `nation-arm-williston-lake-d2e637` **exists** (`todo`); the
  name+MU join missed it. Real remaining work = curate that one line row (not a missing split).
- **SHUSWAP (1)** — the "community pier … exempt from the bait ban" `except` is a **non-spatial**
  person-based exemption, not a geographic split → should be `not_applicable`.

**5 DRIFT**: 3 are trib-set rows whose source water carries an *alias* paren (WEST ROAD/Blackwater,
KINBASKET/McNaughton, WAHLEACH/Jones) — all `not_applicable`, harmless. 2 are `todo` rows that
matched no source entry (`arrow-park-mosquito-creek`, `lost-lake-near-taweel-lake`) — worth a look.

## Workflow (per entry, worst-first)

1. `waterbody_splits incomplete` → pick a `NO_CURATION` / `MISSING_SPLITS` / high-boundary entry.
2. `waterbody_splits show "<NAME>"` → read the reg text + every boundary + its linked row/status.
3. Resolve each boundary: `todo` → curate the row (coord/target per `docs/17`); `MISSING` → confirm
   it's a real split and add/curate a row, else annotate why it's covered; then re-run.
4. When every boundary is resolved the entry flips to `COMPLETE`; its splits are ready for `splits.json`.
5. Commit; re-run `waterbody_splits` (and `stamp` if row membership changed) to refresh.

## Preservation

Every prior `curated`/`manual`/`not_applicable`/`deferred` decision is untouched — the tool only
reads them and backfills. `entry_id` is additive. See `docs/17-manual-review-runbook.md` for per-row
writing rules and `docs/15` for lake-internal (deferred) splits.
