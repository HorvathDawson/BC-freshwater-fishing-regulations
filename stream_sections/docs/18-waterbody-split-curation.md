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
.venv/bin/python -m stream_sections.oneoff.waterbody_splits regs-md      # synopsis-style table, live locators bolded inline (docs/waterbody-splits-regs.md)
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

## Output shape (de-duped, readable)

`waterbody-splits.json` is a **superset of `14-locators-to-curate.json` grouped by reg entry** — the
intended successor file. Entry-level fields (`water`, `mu`, `region`, `page`, `reg_text`,
`entry_id`) live **once on the card**; each locator embeds only the **curation-specific** part of
its rows via `trim_row` (id, status, anchor_kind, resolver_hint, coord, target, label, notes — plus
`name_verbatim`/`mus` only when they diverge from the card). Empty/derivable fields are dropped
(`row_ids`/`statuses`/`anchor_kinds`, the `OK` flag, empty `dates`/`restriction_type`). A card is
emitted for **every entry that carries curation**, not only split-bearing ones; whole-water /
tributary-set entries (no boundary) get completeness **`NO_SPLIT`**. Result: ~940 KB (was ~1.7 MB),
**650/651 rows embedded**, 0 orphans.

**Endpoint linker.** Some boundaries are curated as *endpoint* rows with **empty `locator_text`**
(identity in `label`), e.g. Dean River's canyon reaches → Crag Creek / Iltasyuko confluences, sign
points, tidal boundary. `label_matches` (`canon`: km→m, above→upstream, below→downstream, drop
filler) links such a row to a "from A to B" reach when its label tokens all appear in the reach —
so a reach is resolved by the endpoint rows that bound it.

## Current state (2026-08-05)

`387 cards → COMPLETE 100 · INCOMPLETE 210 · MISSING_SPLITS 2 · NO_SPLIT 75 · NO_CURATION 0`.

**The MISSING boundaries are all matcher/model artifacts, NOT unauthored gaps** (verified 2026-08-05):
- **DEAN RIVER — RESOLVED.** The endpoint linker now maps all 10 reaches to their curated endpoint
  rows; the entry is `COMPLETE`.
- **NATION ARM (1)** — a **parse duplicate**: the one line-rule was parsed into two `location_text`
  variants (with/without "(east)"); the curated `line` row `nation-arm-williston-lake-d2e637`
  (`todo`) matches one, the dup variant flags MISSING. Real work = curate that one row.
- **SHUSWAP (1)** — the "community pier … exempt from the bait ban" `except` is a **non-spatial**
  person-based exemption, not a geographic split → mark `not_applicable`.

**1 DRIFT**: `lost-lake-near-taweel-lake` (now `not_applicable`) — whole-lake quota whose source
`entry_location_text` didn't join; harmless. (The former trib-set/alias and `arrow-park` drifts now
link via a `name_key` whole-water match; `arrow-park` + `lost-lake` were marked `not_applicable`.)

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

## Cutover plan — the grouped file BECOMES `14` (end state)

**Direction (decided 2026-08-05):** `14-locators-to-curate.json` is a flat list that is hard to
review; the grouped, de-duped structure in `waterbody-splits.json` is the better shape. The plan is
to **finish cleaning up curation, then make the grouped file the source-of-truth and rename it into
`14`'s role** (this doc, `18`, becomes the canonical spec; the old `14-locators-to-curate.md` folds
in).

**Why it can't happen yet — the "writer" blocker.** Today the flow is one-way:
`14` (hand-edited) → `waterbody_splits.py` reads it → `waterbody-splits.json` (generated, read-only).
The grouped file is a *projection*; every editing tool writes to `14`. To retire `14` the grouped
file must gain a **writer** (edit-in-place tooling that persists status/coord/target changes back
into it), so it is both source AND writable. Then `14` is redundant.

**Sequence:**
1. **Writer for the grouped file** — persist a per-locator edit (status, coord, target, note) back
   into `waterbody-splits.json`, so it stops being regenerated-from-`14` and becomes source-of-truth.
2. **Rewire `curation_status.py`** onto the grouped file (progress / work-queue / annotate / label /
   apply), plus the **`regs-md` view** (**prototyped** as `waterbody_splits regs-md` →
   `docs/waterbody-splits-regs.md`): ALL reg entries as a synopsis-style markdown table with each
   entry's **live locator phrases bolded inline** in the reg text and `not_applicable` locators left
   **un-highlighted**, so live splits vs n/a are visible at a glance.
3. **Deleted (2026-08-05)** — the dead one-shots `migrate_curation_schema.py`, `classify_lakes.py`,
   `mark_auto_lakes.py` (already applied; effects baked into rows + git history) and
   `build_review_html.py` (offline HTML labeller). Recoverable from git if ever needed.
4. **Rename/replace** — grouped file takes `14`'s role; re-point mentions in `00-README`, `15`,
   `16`, `17`, `oneoff/README.md`; fold `14-locators-to-curate.md` into this doc.

**Keep** — `waterbody_splits.py` (becomes the reader/writer core) and the independent oneoffs
`resolve_lake_offsets.py`, `review_comments.py`, `complex_regs_report.py`,
`name_variants_compile.py` (0 refs to `14`).
