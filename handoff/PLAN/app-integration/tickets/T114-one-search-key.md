# T114 — One search key: the SQL filter and the ranking disagree on what matches ("barriere")

Status: DONE — ebc435e5, 6a9384d3, review e7c4d3ca (app + pipeline code; the shipped bundle/release need a deliver to carry the keys — see As built).

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding app 1
(CONFIRMED on `bundle.sqlite`). The pipeline half (one name normaliser) is T61 2.8; this ticket is the app split.

## Decision being unified
"Does this query match this water's name or alias?"

## Where it is decided today
- SQL `LIKE` filters: `app/packages/data/src/release/queries.ts:34-49` and `app/packages/data/src/bundle/queries.ts:73-99`.
- Ranking: `app/packages/core/src/search.ts:33` `normalise` (strips accents and apostrophes).

How they differ: `item.name LIKE '%barriere%'` returns 0 rows and `'%barrière%'` returns 6, so ranking never sees them;
only 1 alias catches "barriere". "childrens" never finds "Children's Fishing Pond".

## Work
- Core `searchKey(text)` (the current `normalise`), and stored `name_key` / `alias_key` columns built in the pipeline by
  the same rule (share the rule with T61 2.8's normaliser; one fixture table both languages test against).
- Both queries filter on the key columns with `searchKey(query)`.
- Pin: an app grep test that no `LIKE` on raw `name`/`alias` remains in the query modules.

## Acceptance
"barriere" finds the 6 Barrière waters; "childrens" finds Children's Fishing Pond (tests on the fixture store).
Parity test: the Python key and `searchKey` agree on the fixture table.

Rebuild: deliver (new columns) + app. Depends: T61 2.8 for the shared rule (or land the key first and let T61 reuse it).
Size: S. Owner: no.

## As built (2026-10-23, lane-T114)

- **One function.** `@app/core` `searchKey` (`app/packages/core/src/searchKey.ts`, no imports) is the old
  `search.ts` `normalise`, renamed everywhere (`rankWaters`/`matchTier`/`duplicateNames`, `@app/ui` `search.ts`);
  there is no `normalise` left. Python twin: `pipeline/common/search_key.py` `search_key`, character classes spelled
  as JavaScript's (JS `\s`/`trim()`, marks U+0300–U+036F), NOT `tiles/names.normalise` (that one spaces punctuation:
  "children s").
- **One case table, both languages:** `pipeline/common/search_key_cases.json` (17 cases: Barrière, Children's/’,
  Sts'a'í:les, NBSP, BOM, tab, `x_y 100%`, …) — `pipeline/tests/test_search_key.py` and
  `app/tools/search-key-parity.test.ts` (tools/, because core may not import node:fs).
- **Stored keys.** `schema.sql`: `item.name_key`, `alias.alias_key` (last columns, nullable so seeding tests need not
  write them; the bundler writes every one, `bundle/build.py::_items`, named-column inserts). Release `core` copies
  them (`contract.py` item/alias columns; `pack.generated.ts` re-emitted). Format stays `core/1`.
- **Both queries** (`bundle/queries.ts` and `release/queries.ts` SEARCH) filter AND tier on `i.name_key` /
  `a.alias_key`; both readers bind `?1` with `nameNeedle(query)` = `likeEscape(searchKey(query))`, `null` (answer
  nothing) when the key is under 2 chars ("''" would be `LIKE '%%'`).
- **Dev fixture:** `tools/build-fixture.mjs` imports `searchKey.ts` directly (Node type stripping; CI node 22 ≥ 22.18)
  to key `packages/data/dev/bundle.sqlite`; regenerated, deterministic (same sha twice).
- **Tests / pins** (`packages/data/src/bundle/drivers/searchKey.test.ts`, both readers over an in-memory store — the
  bundle schema, and the release contract's core/near columns): "barriere" and "BARRIÈRE" → exactly the 6 Barrière
  waters (not Barrie Creek); "childrens" / "children’s" / "Children's Fishing Pond" → the pond; alias "dore creek" →
  matchedAs "Doré Creek"; "''" → []. Pin: no `i.name|a.alias|item.name|alias.alias LIKE` in either query module, and
  each SEARCH contains the two key filters. Mutations run: SQL back to `i.name LIKE` → 3 acceptance + 2 pin tests fail;
  `nameNeedle` without `searchKey` → BARRIÈRE/childrens fail on both readers + the bind pin. Python:
  `test_the_key_is_written_onto_the_bundle` runs `_items` on the real schema (writing raw `name` fails it).
- **On the real data** (a scratch copy of `data/generated/bundle/bundle.sqlite`, keys added by `search_key`): raw
  `LIKE '%barriere%'` 0 waters, key 6 (Barrière River, East Barrière River/Lake, North Barrière Lake, South Barrière
  Lake, Upper South Barrière Lake); "childrens" raw 0, key 1 (Children's Fishing Pond).

**Rebuild needed (not run here):** `python -m pipeline.deliver` (bundle → release) so the shipped bundle and release
`core` carry `name_key`/`alias_key`, then the app. An app with this change over an OLDER bundle/release fails every
water search ("no such column: i.name_key") — ship them together. After the deliver: `release-perf.test.ts`
(RELEASE_DIR/BUNDLE) and `test_release.py` against the new release.

Left / follow-ups:
- Place search (`SEARCH_PLACES`, `place.name LIKE`) and pointer search (`SEARCH_POINTERS`, `see_pointer.name LIKE`)
  still compare raw strings — "barriere" will not find the town of Barrière. Same fix (a `name_key` on `place` and
  `see_pointer`) is a follow-up ticket.
- ~~`item_name_nc` / `alias_nc`~~ dropped — see "As built: core size gate" below.
- T61 2.8: fold `tiles/names.normalise` and the DFO-suggestion keys into one pipeline normaliser and reuse
  `pipeline.common.search_key` for search.
- `core/menu.ts` glossary `fold` (NFKD, keeps apostrophes) is a separate glossary-term filter, not water search; left.

Owner questions:
- Old-bundle tolerance: built with NONE (no raw-name fallback — that would be the second rule this ticket removes);
  the deliver and the app must ship together. Default kept unless you want a `withArea`-style fallback.

## Adversarial review (2026-10-23)

Fixed (e7c4d3ca):
- **A third copy of the match decision:** `packages/data/src/fixture.ts` `makeFixtureSource().searchNames` matched by
  `toLowerCase().includes` with no length guard. It now keys query, name and alias with `searchKey` and answers nothing
  under two key characters, like both readers.
- **No cross-source pin:** the conformance suite (`conformance/src/suite.ts`, run on the fixture and the fixture
  release) gains "matches a name by its search key, not its spelling" (the water's own name capitalised, with "É" and a
  curly apostrophe: "V’ÉDDER RIVER") and "a query whose search key is under two characters answers nothing" ("R’",
  "''"). Mutations: fixture back to `toLowerCase().includes` → the spelling case fails (fixture); fixture guard
  removed → the "R’" case fails; release SEARCH back to `i.name LIKE` + `likeEscape(query)` → the release spelling
  case fails with 3 release-reader acceptance tests and 3 pin tests.
- **Wrong wording:** `searchKey.test.ts` said dropping the length guard makes "''" answer every water. It does not:
  `rankWaters` drops every row for an empty key, so "''" answers nothing with or without the guard; it is "é'" (key
  "e") that pins it (mutation run: guard off → only the "é'" assertion fails, both readers).
- **Node floor:** `tools/build-fixture.mjs` imports `searchKey.ts` (unflagged type stripping, Node ≥ 22.18) but
  `engines.node` said `>=22`; now `>=22.18`.

Checked and sound:
- Parity beyond the table: every code point U+0000–U+10FFFF (surrogates aside) wrapped as "x?y" through both copies —
  75 differ, all characters newer than Python's Unicode 15.1 (node's ICU lower-cases them, Python does not); none is
  in a BC name. The Python `_WS` class equals JS `\s` exactly (25 code points, checked by enumeration).
- Mutations re-run: tier `CASE` back to `i.name LIKE` (bundle) → the 2 pin tests fail; apostrophe delete dropped from
  `searchKey` → 7 fail (both readers' "childrens", 4 table cases, core's ladder test); release alias filter back to
  `a.alias LIKE` → the release alias test and 2 pins fail.
- Other copies: no other `LIKE` on a water name/alias in the app; `menu.ts` `fold` (glossary) and the pipeline's
  `tiles/names.normalise` (alias de-dupe in `_items`) are other decisions, already in Left / follow-ups.
- Release build: `copy_file` takes the core columns from the contract, so `name_key`/`alias_key` flow from the bundle;
  `conformance/src/release.test.ts` builds a fixture release from the dev bundle and passes.
- Strict check (`REGS_STORE_REQUIRED=1 pnpm -C app check`): platform, fixture (deterministic), pack, typecheck pass;
  vitest 7247 pass, 5 fail — all T02/T03/T07/T08/T09 store-vintage checks (`golden/taps.json` here predates the T128
  re-cut on redesign/stream-sections); none touches search, and they clear once the lane is on the current dev branch.

Left / follow-ups (review):
- The 75 newer-Unicode letters: if a key ever needs them, pin Python's `unicodedata` version or lower-case in one
  language only. Nothing to do for BC names.


## As built: core size gate (2026-10-23)

The first deliver with the keys failed the release: `SIZE GATE: core: 1,377,692 B gzip > 1,300,000`. Fixed without
moving the gate, and without a second matching rule:

- **Dead indexes dropped.** `_core_flags` no longer builds `item_name_nc` / `alias_nc` (water SEARCH is a contains-LIKE
  on the keys; nothing read them). `see_pointer_nc` / `place_name_nc` stay (their searches are still on raw names).
- **Keys stored thin.** `pipeline.common.search_key.stored_key`: NULL where the text is ASCII and its key is its lower
  case (most BC names), else `search_key`. The bundler writes `stored_key`; both SEARCH forms read
  `COALESCE(i.name_key, lower(i.name))` / `COALESCE(a.alias_key, lower(a.alias))` (`@app/data` bundle/queries.ts
  `NAME_KEY` / `ALIAS_KEY`, the release SEARCH imports them). ASCII-only thinning makes `lower()` exact on any SQLite
  build (ICU or not). The key function is still one function; the dev fixture's full keys read the same.
- **Pins.** `test_release.py::test_every_cores_effective_key_is_the_search_key`: over every item and alias row of a
  built core, `COALESCE(key, lower(text)) == search_key(text)` (and most rows store none). `test_search_key.py`:
  `stored_key` is NULL only where ASCII lower is the key; the bundler test writes NULL for "Mabel Creek"/"Mable Creek".
  `searchKey.test.ts` seeds thin keys (spelled out, no copy of the rule) and finds "mabel"/"MABEL LAKE"/alias "mable"
  through the fallback on both readers; the pin wants the COALESCE form. Mutation: `NAME_KEY = "i.name_key"` → 3 fail.
- **Measured** (scratch on r-6960fe8201e3's core, gzip -9): no keys 1,283,617; full keys 1,374,991; full keys − NOCASE
  indexes 1,175,578; thin keys − indexes 1,087,851. Shipped: **r-e88ea8112b1a core 1,091,913 B gz** (gate 1,300,000,
  unchanged); release total 13,648,526 B gz (gate 15,000,000).
