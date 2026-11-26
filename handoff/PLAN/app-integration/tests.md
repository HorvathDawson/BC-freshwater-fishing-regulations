# tests.md: how the Regulations page is proved

Four layers, the release-backed parity harness, the browser walk, and the reader gate. The fixtures they read are
`app/fixtures/README.md`; re-cutting them is `node app/tools/regs-recut/sync.mjs` (the release does it on its
candidate, AGENTS W46). The retired prototype's `check.mjs` checks were mapped onto these layers; the browser walk
keeps their names.

## 1. The layers and the parity harness

### 1.1 The layers

| layer | where | runner | may assert | may not |
|---|---|---|---|---|
| **A. core unit** | `app/packages/core/src/regs/*.test.ts` | vitest `logic` (node) | a presenter's output over hand-built inputs (`core/src/regs/testing/`): whole expected strings, grammars, invariants (no bare 0, each sentence once) | read the store (core imports nothing but its own; `node:fs` is banned) |
| **B. parity** | `app/conformance/src/regs/*.parity.test.ts`, `*.release.test.ts`, `*.fixture.test.ts` | `logic` | for every oracle state: `buildRegsPage(loadRegsInputs(store, tap))` against the oracles, block by block | judge layout, colour or pixels |
| **C. RN render** | `app/packages/ui-native/src/regs/*.test.tsx` | vitest `render` (jsdom + react-native-web) | structure, order, roles, presses (`onOpen(key)`, `onDay`), `allowFontScaling`, hit slop, palette identity, a mount per tab from the frozen VM fixtures | pixel geometry (jsdom has no layout) |
| **D. browser** | `app/tools/regs-walk.mjs`, `regs-census.mjs`, `regs-text-scale.test.ts` | Chrome over cdp against Expo web, at the phone size (§4) | heights, overlaps, line counts, the colour census on the rendered app | run in `pnpm check`: they need Chrome and a dev server; they are release gates |

Porting rule: text against data is A (plus B for every state); order, presence and presses are C; pixels or computed
colour are D, or a token lint when the property is static in RN (`app/tools/regs-tokens.test.ts`).

### 1.2 The parity harness

`app/conformance/src/regs/golden.ts` (the store, the per-state inputs cache, `cases()`, `requestOf`), `oracles.ts` (the
oracle files) and `fixtures.ts` (`syncFixtures()`, the frozen VM fixtures render tests mount).

- **The oracles** (`app/fixtures/golden/`): `states.json` (the tap questions: water, part, date, each state's sheet
  keys), `taps.json` (the Python reference decoder's answers to them: item, part, key, segment, cell, the 366 display
  frames) and `page-oracle.json` (page v36's own code run over the same release). All are cut by `sync.mjs`, never by
  hand.
- **The store** is `parityStore()`, chosen by `REGS_PARITY=province|fixture|auto` (default auto): the live release when
  its water file carries the answers the oracles were cut from (`answers_sha256`), else the committed fixture release
  (`app/packages/data/dev/regs-release/`, the CI path). Neither paired is a SKIP with its reason, never a wall of false
  failures; `REGS_STORE_REQUIRED=1` turns the skip into a failure.
- **A state's request** (`requestOf`): the tap's item, part, date and weekday as `taps.json` gives them (never
  recomputed from another year); `today` is Oct 10 2026 for every state. The tap the app makes must reproduce
  `taps.json`'s key, segment and cell first (`harness.parity.test.ts`), so a parity failure is never a tap failure in
  disguise.
- **Comparison**: `plain()` and the one-space join (`contracts.md` §1, §5). Each state is its own `it(id)`, so one wrong
  state never hides the others.
- **Frozen VM fixtures** (`ui-native/src/regs/fixtures/*.json`) are written by `UPDATE_REGS_FIXTURES=1` and held fresh.
- **Run**: `pnpm -C app test -- regs`; one file: `pnpm -C app vitest run conformance/src/regs/<name>`.

## 2. The browser walk

`node app/tools/regs-walk.mjs [--url …] [--states …] [--theme light|dark] [--clock proto|real]` walks every state of
`WALK_STATES` (`app/tools/regs-web/states.mjs`) on Expo web, light and dark, presses every tab, opens and closes every
sheet of the state, and prints `PASS|FAIL <name> <state>`; exit 1 on any FAIL. The checks it runs, by the prototype's
names:

| check | means |
|---|---|
| `header-compact` | header at most 120 px; the date and the action on the name row |
| `header-label-two-lines` | the full place label if it fits two lines, else the short one; the action at least 44 × 44 |
| `header-one-shape` | the header does not change on scroll |
| `group-head-compact` | a group head's title, number and unit on one line |
| `pictures-whole` | fish pictures keep their aspect and never overlap |
| `bait-ways-compact` | bait and ways rows compact (reported, not gating) |
| `r20f-headers` | a tier's text edge is the head's |
| `r20g-possession-fold` | the possession sheet under 300 px |
| `trout-table` | the trout table opens from its fold; a row's picture opens a sheet |
| `tabs`, `sheets open`, `no console errors` | every tab presses, every sheet opens and closes, no console error anywhere |

`regs-census.mjs` is the colour census on the rendered app (light, dark, CVD), with the testID convention's selectors
(`contracts.md` §2.7); `regs-text-scale.test.ts` lays the page out at 200 % text.

## 3. What every change keeps green

| gate | command | the rule |
|---|---|---|
| boundaries | `node tools/check-boundaries.mjs` | core imports nothing; parity tests live in conformance; asset `require()` only in `apps/mobile` |
| platform | `node tools/check-platform.mjs` | no `document.`, `window.localStorage`, `Alert.alert` or `accessibilityState` in shared packages; a `.native` / `.web` pair is complete (AGENTS 34, 34a) |
| style | `pnpm style` | a token change is `pnpm style:build` plus the regenerated `packages/map/style/*` |
| colours | `node tools/check-colours.mjs` | no colour literal outside `packages/map/style/{tokens,themes/*}.json` |
| deps | `node tools/check-deps.mjs` | every dependency has a `deps.md` line |
| theme, cvd | `packages/ui-native/src/theme.test.ts`, `tools/cvd.test.ts` | a new palette key gets a contrast assertion; status colours stay apart under every deficiency; `sub` on `card` at least 6.5:1 |
| all | `pnpm -C app check` | every step of `scripts.check` (AGENTS 31) |

## 4. The phone the tools shoot, and the reader gate

The browser tools shoot a 390 × 844 CSS px phone at DPR 2 (`states.mjs` `PHONE`), fonts loaded.

**The comprehension reader gate** (manual): a fresh reader (a new agent with no history) gets PNG screenshots only and
a fixed question set, and gives answers, a confidence of 1-5 and "what confused you". It passes when it scores at least
the prototype's reader did on the same set, with no answer differing from the prototype reader's and no confidence drop
of 2 or more; a miss is a bug against what draws the confusing thing. The owner launches it (it is an agent run).
