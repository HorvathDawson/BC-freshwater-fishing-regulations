# T114 — One search key: the SQL filter and the ranking disagree on what matches ("barriere")

Status: DONE — 72e0b223, 197b76d0 (app + pipeline code; the shipped bundle/release need a deliver to carry the keys — see As built).

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
- `release/build.py::_core_flags` still builds `item_name_nc` / `alias_nc` (NOCASE on the raw strings); SEARCH no
  longer reads them (a contains-LIKE never used them). Drop them in the next release change (core size).
- T61 2.8: fold `tiles/names.normalise` and the DFO-suggestion keys into one pipeline normaliser and reuse
  `pipeline.common.search_key` for search.
- `core/menu.ts` glossary `fold` (NFKD, keeps apostrophes) is a separate glossary-term filter, not water search; left.

Owner questions:
- Old-bundle tolerance: built with NONE (no raw-name fallback — that would be the second rule this ticket removes);
  the deliver and the app must ship together. Default kept unless you want a `withArea`-style fallback.
