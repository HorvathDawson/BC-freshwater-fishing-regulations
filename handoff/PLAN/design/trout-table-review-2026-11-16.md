# Trout table plan: fresh review (2026-11-16)

An independent Opus reviewer (wrote none of the plan) attacked the spec, the contract and the app check against the
code at 8f0fb905f, at the owner's request of 16:50 ("we want to make sure the plan is robust so that we change the
data all the way in python and the front end and it all just works withotu fighting it"). Read-only; every finding
was checked at path:line. The plan is fixed from this list before any build code. Owner: D = data side (main chat),
A = app side (app chat).

## Blockers

**B1 (D, ticket L; optional A, M0). A phone can install or open a release it cannot read, and then every regs page
fails.** `readsRelease` (`app/packages/data/src/manifest.ts:79-92`) and the held set (`boot.ts:353`) compare file
formats only; section versions are checked only when the regs store opens (`store.ts:116-123`), and that refusal is
final (`WaterScreen.tsx:123`, no retry for `RegsStoreError`). A rows-34 release is still `regs-region/1`
(`contract.py:142`). (a) The S-built app opens its held rows-33 set (formats match) and refuses every regs page; the
new set is used only at the next launch; offline = no regs. (b) The contract's `app_reads` sections fix only the
publish stamp, and old 0.7.x rows carry no sections. FIX: at the switch bump `regs-region/1` → `regs-region/2` in
`contract.py` FILES; the held-set path (T403) then installs over the old set before the map and `app_gate` derives
`minAppVersion` from the format; the app gets it through `pack.generated.ts`. Optional general fix (A, M0):
`readsRelease` also checks `manifest.regulations.versions` against `KNOWN_VERSIONS` + optional sections. Drop adding
`sections` to `app_reads` (or backfill it through `--backfill`).

**B2 (D, ticket I, land now). The test that should stop L or S landing alone does not exist.** The only writer→reader
version pin covers `TIDAL_SECTIONS` (`pipeline/tests/test_answers_tidal.py:121,128-132`); rows and ladder are not
pinned; `store.test.ts:17` hand-types `rows: 33`; `versions.ts:4` cites a test file that does not exist. Spec 7.4's
"a format-pair test fails if either lands alone" is false. FIX: one test holding every `A.SECTIONS` version, plus the
optional `trout`, equal to `app_reader_versions()`; extend `app_reader_versions` to parse the app's optional sections.

## Should-fix

1. **Fight items with no ticket or test.**
   - #2 page possession lead picked twice and differently (`card.py:410-422` vs `standing.ts:97-104`). FIX: ticket F0
     (D) ships the lead inside the cutover's version bump; S (A) reads it; tests: pytest that `possession_duty` and the
     shipped lead use the same rule; vitest that `standing.ts` holds no pick logic.
   - #3 page row repeats trout possession lines: add to F0 and S.
   - #4 `caveats.ts:225-243` timed (b) "must skip covered fish": no owner, and skipping loses a trout answer that
     changes inside an hours window (the table is drawn from `inside=false`). FIX: A0 counts these; F writes the
     hours-window words into the table; S skips covered fish with a vectors test that no fish is spoken twice. The
     `strip.ts` "Closed from …" pairing with the card line gets an owner in G.
   - #1 `splitRows`, `looseRows`, `otherFishOf` (`others.ts:24-53`) keep `TROUTISH`. FIX in S: other fish = rows not in
     `covers`; delete `TROUTISH` and `TROUT_LINE_FISH`; test over every fixture cell that each rows-frame row is drawn
     exactly once; test for "lone other fish stays".
   - #6 `STEEL_LINE`, `quietOf` (`tail.ts:83-91`): add to S's deletions.
2. **Checks that would quietly stop checking (#9).** `across_unsaid` skips rows with no group (`tree.py:141-143`);
   `card_check.check_tree` and `possession_problems` too (`card.py:406`). Spec L says "delete", the contract says
   "re-aim": contradiction. J's decoder misses joint limits (box total, shared bands, `together`, possession below
   the day's number), proved today by `card_check.check_caps` baskets through `engine.Engine`
   (`card_check.py:630-660`). FIX (J): reuse Engine's basket proof over the decoded table; an unparseable sentence
   fails the build; J's stamp reports tables checked = non-NULL trout cells as a floor; J checks "possession line ⇔
   differs from the page's lead". FIX (L): "re-aim at J, with a mutation test per check".
3. **Ticket A too early.** Split: A0 now (prototype estimate; counts of rows mixing trout and non-trout (Q-D4),
   angler-only rules closing trout alone (Q-D7), hours-window trout answers). A1 after I and J: contract 2.9's method
   on the cutover form, every `WEB_BUDGET` moment, the `water-shared` dictionaries; A1 gates L.
4. **Week lines and licence line built in the wrong pass.** `say_weekdays` (`answers.py:556-571`),
   `stamp_licence_documents` (`:576-582`), `drop_closed_steelhead_stamp` (`:600-604`) run after the rows derive. FIX
   (contract 2.2, I): compose week lines, licence/stamp line and steelhead stamp in `compose_key` from post-pass values.
5. **Unowned pieces.** The `trout_vectors` emitter, `trout-hand.json`, the `drafts` section, `regs_fixture --item`,
   the staging `side` scenario (Q-D11), the `app_reads` change. `--check` proves vectors = fixture, not builder; each
   fixture re-cut moves `taps.json`, 9 VM fixtures and `sync.mjs` (`regs_fixture.py:1-27`). FIX: ticket V-py after I
   owning one refresh script (side release → fixture → vectors → golden sync) + the side-deploy scenario.
6. **Hand-kept types and lists.** Contract 2.3 leaves "wire types emitted, or described in the codec text" open. FIX
   now: emit `TroutFrameWire`, `TRowWire` and `CellIds` (`inputs.ts:24`) from `contract._REGION_TABLES`. I must also
   name or derive the other hand lists of table names: `store/build.py:37-43` (refuses unknown section `:282`),
   `store/common.py:58-62`, `release/shards.py:22-26`, the slices.
7. **Owner decisions not carried consistently.** Reopening date on a closed FISH: spec change 2 says "water or fish is
   closed today"; contract 2.5 / spec 3.3 rule 4 allow it only in `DateSet.top` (Bish Creek closed steelhead could
   never say "reopens"): fix E, G, J. Year bar: T810 item 9 = only an all-release table; contract `YearBar` and spec
   3.1 also allow "every fish closed": align. "All rows get arrow": today's fold rows are salmon and trout only
   (`KeepTab.tsx:43-48`, `FoldRow` `KeepBlocks.tsx:250`): O's text = the arrow replaces "Details" on salmon and trout
   folds; Other fish list unchanged (confirm with the app chat what its Q1 showed). Q-D7 open but spec 3.1, F, O carry
   notes unconditionally: mark conditional. Spec 4.1-4.7 stale against the contract (`Box.n`, `label` enum, `w`,
   `notes`, fold "5 a day"): replace with a pointer; the contract wins.
8. **One big cutover merge.** Land additive parts behind the flag first (J, F0, the caveats skip, the `covers` split
   behind the dev switch, the version test B2, the emitters). The cutover = flag on, rows 34 + format bump, re-cut
   fixture and goldens, deletions. K (`answers_diff`) must learn rows 34 or every dropped trout `group` reads as a
   change.

## Minor
- `composed.a11y` "from Python's reference functions" makes a second composer: define the join order as the payload's
  order; pin only the TS output.
- Name clash: `OPTIONAL_SECTIONS` beside `contract.OPTIONAL` (optional files) and a hand-typed app `OPTIONAL_SECTIONS`:
  rename one.
- Guarantee 2 points J's parse at 2.9 (bytes); `YearBar.runs` carries day counts (an exception to guarantee 2): state it.
- Bump the `trout` version on any shape change during the build-behind (APKs read side releases from M0).
- Ticket T: a rule for the crawler page's `{day}` slot.
- G is ordered after F; the words module should exist while F is written.
- Two fold grammars on one screen: Python `fold` ("Up to 5") beside TS `shortValue` ("5 a day", `others.ts:63`): show
  both in G's catalogue.

## Verified true
`store.ts:116-123` refuses an absent listed section (manifest repeats it); `reads.ts:297` fixed columns;
`versions.ts` rows 33, no optional list; `FRAMES` `frames.py:20`; `emit.py` `emitted_ts` writes `T | null`;
`json_schema(by_alias=True)`; `app_gate` formats only; `app_reads.json` regs-* = `regs-region/1`; the T793/T805
checks at `answers.py:584-598` walk `group.tree`; `card_of` at `rows/assemble.py:118-120`; `page_possession` and
`possessionRules` differ; `TROUTISH` adds BT, CT_ws, CT_c; `TROUT_LINE_FISH` = RB, CT, GB; `groupGlance` value null;
timed (b) per fish, skips `week`; `keepOf` loop `keep.ts:123-131`; regs fixture = taps ∪ leap ∪ `--item`; `cell`
columns `contract.py:18`; first_tap gate at HEAD 4,200,000 (the salmon raise has not landed); `minAppVersion` gate
(T251, `boot.ts`), stamped at `publish.py:84`.

## Owner decisions raised
1. Ship the trout section in normal releases before the switch (rides by A1's measured amount; J runs on every real
   release; real staging previews; the cutover shrinks to dropping trout groups + the bump). Recommended yes if A1
   shows about 100 kB or less.
2. Year bar when every trout/char fish is closed but the water is open: follow his item 9 (all-release table only).
3. A reopening date on a closed fish's row (Bish Creek steelhead): his 15:36 words already say yes ("drop reopen
   unless it is currently closed then reopen date matters").
4. Q-D7 angler-only notes: keep them in the page's caveat block (recommended).
5. "All rows get arrow": salmon and trout folds only; the Other fish list untouched (recommended).
