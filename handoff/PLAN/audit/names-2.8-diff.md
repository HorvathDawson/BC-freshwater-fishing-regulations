# T61 2.8: one name key, what it moves

`pipeline/common/names.py` `key()` is now the only answer to "are these two spellings the same name?"
(`compact_key` is the same key with the spaces removed; `drop_notes` is the matcher's parenthetical strip).
Every number below comes from side builds measured on 2026-10-24. Nothing was promoted. The full table is in
`names-2.8-diff.json`.

## The key

The key folds accents (NFKD, combining marks deleted) and case (casefold). It deletes apostrophes (' ’ ‘ ʼ `).
Every other run of characters that are not letters or digits becomes one space, and the result is trimmed.
It does NOT fold St./Saint, Mt./Mount or Cr./Creek, and it does not drop the kind word. Measured over 20,632
registry names: no name spells "Saint", and no "Mt"/"Mount" pair exists, so that fold would join nothing.

| normaliser (before) | "St. Mary's River" | apostrophe | accents | punctuation | case |
|---|---|---|---|---|---|
| tiles/names.normalise (bundle alias dedupe, gauge/stocking/bathymetry match, lake_facts) | `st mary s river` | space | folded | space | lower |
| regs/matching/matcher._norm / _norm_keep | `st marys river` | deleted | folded | space (`_norm` also drops "(...)") | lower |
| atlas/registry/build._norm_name, registry/flowing._norm | `st. mary's river` | kept | kept | kept | casefold |
| atlas/reach/steelhead._norm | `st. mary's river` | kept | kept | kept | lower |
| dfo_salmon/locations.normalize (was also the DFO name-index lookup key) | `st mary s river` | space | **not** folded (deleted) | space; expands cr./rd./r./& | lower |
| dfo_salmon/splitwork._norm | `st mary s river` | space | not folded | space | lower |
| reach/build + graph/tributaries cut-label joiner | `st. mary's river` | kept | kept | kept | lower |
| deliver/bundle/spans own-name compare | `st. mary's river` | kept | kept | kept | lower |
| added_streams _canon/_canon_name/canon/nm | `stmarysriver` | deleted | not folded | deleted, spaces too | lower |
| added_streams/fwa_match._norm | `st. mary's river` | kept | kept | kept | lower |
| **names.key (now all of the above)** | `st marys river` | deleted | folded | space | casefold |
| app search key (T114, unchanged) | `st. marys river` | deleted | folded | **kept** | lower |

Before this change, the four spellings ("St. Mary's", "ST. MARY'S", "St Marys", "St. Mary’s") gave 2 or 3
different keys in most normalisers. They now give one.

## Kept as different questions (named, not merged)

- `search_key` is what the app's search compares, and it must match the TypeScript byte for byte. It is FINER than
  `key`: equal search keys always give equal keys (pinned). The reverse is not true, which leaves a pre-existing
  gap for T114. 14 registry variants differ from a kept spelling only by punctuation ("SANDRIFT NO.2" against
  "SANDRIFT NO. 2"). Neither the old nor the new alias dedupe stores them, so a query typed with exactly that
  punctuation misses them.
- `tiles/names.display` is the text shown on the map. `area_catalog.area_id` and `registry/build._slug` are ids
  that curated files store. `dfo_salmon/entries._slugify` (water_id) and `locations.normalize`/`fingerprint` are
  locator-text fingerprints whose hashes are stored. Changing any of these would change ids or hashes, so they are
  not name keys.
- These are text or CLI filters, not keys: `gauges/consume/tidal._norm` (HYDAT remark text),
  `regs/review/coverage._folded` (verbatim squash), `dfo_salmon/untangle|parse._norm` (whitespace cleanup) and
  `dossier`/`whatchanged` `--name` substring filters.
- Left on purpose: `atlas/graph/prune._has_own_name` (casefold) belongs to the braid lane (T67), and changing it
  would change the graph. `atlas/build.py --gnis` is a debugging lookup.

## Effect, base vs new (the same code with the old bodies patched back in)

| stage | result |
|---|---|
| atlas (`atlas/names` vs `atlas/names_base`) | graph equal (1,957,196 nodes, 1,557,783 edges, each equal); **registry.json, section_handles.txt, geometries and every sidecar are byte-identical** |
| reach (`scratch/names28/reach_new` vs `reach_base`) | digest a7224300598f3a50 on both; `binding_diff`: **no binding changed**; every jsonl byte-identical |
| bundle (live atlas + live reaches, only the deliver code differs) | 64 tables; only `alias` differs: **3 rows dropped**; gauge tables and cut labels are identical |

Changed rows, classified:

| row | before | after | class |
|---|---|---|---|
| alias `wbk:329060781` "Bobbs" (Bobbs Lake) | `bobbs` ≠ `bobb s` (its sibling alias BOBB'S) | both `bobbs`; BOBB'S stays | explained |
| alias `wbk:329370967` "JOE'S LAKE" (Joes Lake) | `joe s lake` ≠ `joes lake` | `joes lake` = name | explained |
| alias `wbk:329537267` "TSEKET LAKE" (Ts'Eket Lake) | `tseket lake` ≠ `ts eket lake` | `tseket lake` = name | explained |

Totals: **3 explained, 0 unexplained.** 0 rule bindings, 0 gauge bindings and 0 registry items changed. Search
loses nothing: each dropped alias has the same app search key as a row that is kept.

These are generators that were not run (AGENTS 38). The change takes effect at their next regeneration:
- Gauge match: 4 station keys change (08LB023, 08NE119, 08NJ121, 08NN009, all apostrophes). The 3 curated ones
  are name+radius matches whose FWA name keys the same way.
- Stocking and bathymetry matchers.
- The added-streams generator: all 672 municipal names key identically.
- `lake_facts`: nothing feeds it yet.

DFO match proposals are identical for every water. The worklist now finds Squamish River's curated split.

The live atlas (`atlas/full`, 2026-10-10) predates two weeks of atlas code, so a comparison of the side builds
against live would show only that older drift. The base-vs-new comparison removes it.
