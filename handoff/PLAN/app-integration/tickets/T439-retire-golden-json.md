# T439 — Retire golden.json and the tests that only compare against it

Status: DONE — cc8b6b63d (owner 2026-11-06: "the golden as before is bad and we should retire it and the tests with it").

## Why
`app/fixtures/golden/golden.json` (with `pins.json`, `tokens.json`, `inventory.json`) was extracted ONCE from the
regulations prototype (`app/design/regs-v11.html`, v38), deleted in T87. `sync.mjs` calls it FROZEN, but it was
hand-edited for wording rulings up to 2026-10-28 and carries 19 `until:` / 14 "never" skips (audit 2026-11-05, tests
finding 11). It is neither the prototype's truth nor the pipeline's, and re-cutting it from today's answers would
only prove "today equals today". What stays as the oracle: the release-backed `taps.json` / `hydrate.json` (re-cut by
`sync.mjs` per release), the pipeline's card gate (`pipeline.deliver card_check`) and the Python answers tests.

## Work
1. List every reader of golden.json / pins / tokens / inventory (~56 files under `app/conformance/src/regs`, plus
   `fixtures.ts`, `golden.ts`, `goldenSlices*`, `oracles.ts`). For each test: golden-only (delete) or it also checks
   something real (keep; move that check onto taps.json / hydrate.json or a unit fixture).
2. Anything a deleted test checked that nothing else does: listed in this ticket, and moved or recorded "no" with why.
3. Delete the frozen files and their sync/notes; `sync.mjs` keeps only the release-backed re-cut.
4. `pnpm -C app check` (or the conformance + knip steps) green; knip finds no orphan.

Size: M. Owner: no.

## Done (cc8b6b63d)

`golden.json`, `pins.json`, `tokens.json`, `inventory.json` are deleted. `app/fixtures/golden/` now holds `taps.json`, `hydrate.json`
and the new `states.json`: the 134 tap QUESTIONS (id, water, part, md, the prototype's part label `sub`, each state's sheet keys),
extracted once from golden.json. No answers and no prototype text. `taps.py` and `hydrate.mjs` read it (re-run on the committed
states: `taps.json` states identical, `hydrate.json` byte-identical). `verify_map.py` (golden-only, already failing on 28 kinds) is
deleted; `sync.mjs` is taps.py then hydrate.mjs. Verified: `tsc -b`, vitest logic (conformance, tools, core) and render (ui-native),
`knip`, `check-tickets.mjs` all green.

| File | Result |
|---|---|
| conformance `bait`, `banner`, `caveats`, `gear`, `grouphead`, `grouplines`, `header`, `keep`, `licence`, `methods`, `others`, `plainrow`, `sheets`, `standing`, `strip`, `tail`, `tree`, `pins` `.parity.test.ts` (18) | deleted: golden-only (app VM text vs golden text) |
| `goldenSlices.test`, `line.golden.test`, `exclusions.test`, `subcapTotal.test`, `t100MapPage.test` | deleted: golden-only (slice freshness, word table vs golden, exclusion honesty, load-time golden rewrites) |
| helpers `exclusions`, `keepGroups`, `keepPage`, `gearPage`, `eq`, `goldenSlices`, `subcapTotal`, `t100MapPage`; core `testing/eq` + test | deleted: orphans |
| `golden.ts`, `oracles.ts` | kept, golden-free: cases come from taps.json; the golden load-time rulings, `pinsOracle`, `EXCLUDE` removed |
| `harness.parity` | kept; "golden and taps from one sync" moved to states.json vs taps.json (121 tapped + 11 gone + 2 tidal = 134) |
| `inputs.release` 7b (mergedParts on every state) | moved: pinned literally to the four closed-group states (chw0 2/8, x15p9 2/2, x16p7 2/2, x17p5 2/2) |
| `book.parity` | kept: spelling + PDF page of every book run in vedder_1010's 59 frozen sheets; sheet list from the frozen page, golden glossary-pages equality dropped |
| `edge.parity` | kept: tidal, hours window, weekday row, unnamed, outside now assert the VM directly (banner big/paras, caveat title/item, closed/closedMore); lines-equal-golden and the 12 moment states' whole Keep tab dropped |
| `keep.props.parity` | kept (properties over every state; only the case source changed) |
| `card`, `settle`, `describe`, `tap`, `reads`, `store`, `blob`, `regs.*`, `ruleSources`, `golden.test` and the other release/fixture suites | kept untouched (hydrate/taps/release oracles) |
| `fixtures.ts` / `fixtures.test` | kept: frozen-page writer takes sheet keys from the committed file (else the page's own `sheetKeysOf`); `golden_lines.json` writer removed |
| ui-native `RegsHeader.test` | moved: drawn over each frozen page's own header VM |
| `PinBar`, `pinStore`, `RegsWide` tests | moved: `fixtures/pin-geometry.json` (inputs of 3 states, no expected bars); bar compared to words typed in the test / the store's own lines |
| `gear.render`, `methods-licence.render`, `SheetHost` tests | golden mounts and golden-line mutation pins deleted; literal tests kept |
| `HowToTell.test` | moved: the hatch sheet's lines typed in the test |
| `golden-headers/pins/sheets`, `golden_lines.json` | deleted |
| tools `emit-fish.test` (golden sp: tips, pictured codes), `regs-tokens.test` (palette vs prototype tokens) | those checks deleted; type scale and spacing moved to literals; weight/radius/face checks kept |
| tools `regs-census`, `regs-walk`, `regs-web/states` (+test) | prototype census print / golden sheet titles dropped; states and sheet keys from `states.json` |

Checked by a deleted test and by nothing else now ("no"): (1) the text of every presenter (Keep, Gear, Methods, Licence, header, sheets,
pins) on all 134 states against the prototype's: no, the prototype is deleted and the core unit tests pin each presenter over hand
fixtures; answers stay held by the pipeline card gate and the Python answers tests. (2) Prototype pin bars replayed over every
state: no (core `pins.test.ts` plus three states' geometry in the RN tests). (3) Palette equal to the prototype's tokens: no (the
map's `tokens.json` is the palette; `check-colours`). (4) Tab names / word table equal to the prototype's: no (`words.test.ts`).
(5) Fish tips and captions equal to the prototype's output: no (FISH_TIPS still checked against the source table). (6) Every vedder_1010 sheet's lines and the
Gear/Methods/Licence tabs line-for-line in the RN render: no (literal assertions remain). Not re-run: `sync.mjs` against a new
release (the machine's newest release is newer than the committed `taps.json`, a skew that predates this ticket).
