# T41 — web parity: side-by-side shots, the walk of every tab and sheet, the reader gate

Status: PARTIAL — 279ec371, a2159a32, 5dcce89d, d9e87835, 459811a8. Left: O3 (timed caveat box), O5 (sheet height) → T82 (the reader gate passed 2026-10-23 at 390 px light + dark: reader/2026-10-23-app-rerun.md); the native walk → T50. Close as DONE when T82 closes.

Phase 4 · Depends on T27, T29, T35, T37, T38, T39, T40 · Blocks release · Decisions D1, D2, D5 (`--ref 04765ab5`), D7 ·
Size 1 day

## Goal
Three browser tools that prove the Expo-web app matches the prototype where unit/render tests cannot (pixels, layout,
scroll), and run the comprehension reader gate (tests.md §4):
1. `compare-regs.mjs` — the same state in both, 390×844, one PNG with the prototype left and the app right (+ a diff band);
2. `regs-walk.mjs` — walks every tab and opens every sheet of a state list in the app, asserting the px checks that
   tests.md §2 routes to layer D, and fails on any console error;
3. `--reader` mode — shoots the reader set with the prototype's PNG names.
No new app dependency: Chrome via CDP, like `app/design/regs-v11/tools/cdp.mjs` (Playwright is NOT added; `check-deps`).

## Files
- create `app/tools/regs-web/cdp.mjs` — a copy of `app/design/regs-v11/tools/cdp.mjs` `open(url, {width, height, offline, init})`
  (tools/ may not import from design/ by convention; copy is 80 lines)
- create `app/tools/compare-regs.mjs`, `app/tools/regs-walk.mjs`, `app/tools/regs-web/states.mjs` (state table)
- create `app/design/regs-v11/port/reader/{prompt-gate39.md, reader-prefix.md, states.json}` — copied from the session
  scratchpad `…/scratchpad/r20i/` (paths in the prompt rewritten to `$OUT/TALL`, `$OUT/FRAMES`); committed so the gate
  survives the session
- modify `app/apps/mobile` web entry: a **deep link** for tests — `?regs=<item>&part=<i>&date=YYYY-MM-DD&tab=<tab>&sheet=<key>&age=&res=&theme=&scroll=`
  parsed once in `App.tsx` (web only, `App.web.tsx` or `Platform.OS === "web"` branch) into Shell initial state; ignored on native
- create `app/tools/regs-web/states.test.ts` (logic): every golden state id maps to a deep link and back

## Signatures
```js
// app/tools/regs-web/states.mjs
export const PHONE = { width: 390, height: 844, dpr: 2 };
/** golden id → both URLs. proto: file://…/staged/regs-v11.html#shot=1&w=…&d=…&p=…&sec=…&sheet=<golden key>; app: http://localhost:8081/?regs=… */
export function urlsOf(id, { tab = "keep", sheet = null, who = null, theme = "light", scroll = 0, ref = "04765ab5" } = {}) → { proto: string, app: string };
export const WALK_STATES;   // the union of check.mjs's state sets: CARDS2 ∪ R17 ∪ R20S ∪ TRS ∪ SBS ∪ STATES_I (≈ 24 ids) + x31p_1010 (tidal) + x15p2_1010 (hours)

// compare-regs.mjs   node app/tools/compare-regs.mjs --state vedder_1010 [--tab gear] [--sheet pos:pos] [--theme dark] [--ref 04765ab5] [--font-scale 2] [--out DIR] [--reader OUT]
//   stages the prototype at --ref with app/design/regs-v11/port/stage.mjs (never the working tree), opens both at 390×844 DPR 2
//   (the prototype inside its #shot=1 frame: the .app box is 390×844 in a 440×900 window — crop to it), waits for fonts,
//   writes OUT/<id>-<tab>[-<sheet>]-<theme>.png = [proto | app | per-pixel diff (red where |Δ| > 24)] and prints the diff %.
// regs-walk.mjs      node app/tools/regs-walk.mjs [--states vedder_1010,…] [--theme light|dark] → exit 1 on any FAIL line
```

## Ports
- `app/design/regs-v11/tools/shoot.mjs`, `frames.mjs` (@04765ab5: 390×844 frames with 90 px overlap, stepped back so no
  line sits under the pin stack), `tall.mjs` (whole-tab shots), `cdp.mjs` — the shooting method and the state names.
- `check.mjs` px checks routed to D (tests.md §2): `header-compact` (header ≤ 120 px; date + action on the name row),
  `header-one-shape` (header identical at scroll 0 and 900), `header-label-two-lines` (all headers one height; action ≥ 44),
  `group-head-compact` (Vedder first block ≤ 150 px), `r20d-compact-head`, `r20d-pinned-stack` (stack ≤ 90 px, lines not cut),
  `r20f-pictures-first` (picture ≥ 80 px wide, caption ≤ 2.6 lines), `pictures-whole` (no overlapping images),
  `bait-ways-compact` (Methods fits one screen under the header), `r20g-possession-fold` (sheet < 300 px), `r20b-howtell-inline`,
  `r20d-flat`/`r20f-headers` (one text edge: every tier/kind text left = the head's ± 1 px).

## Reads
Rendered DOM of Expo web (react-native-web: `data-testid` from `testID`, roles), the staged prototype's DOM.

## Tests
`regs-walk.mjs` checks, each printed `PASS|FAIL <name> <state> — why` (the names are the check.mjs names so a reader can
match them): for every state in `WALK_STATES`, light and dark:
1. `header-compact` — `[data-testid="regs-header"]` height ≤ 120; the date pill and the action share the name's row (top within 4 px).
2. `header-one-shape` — header height/text identical at scrollTop 0 and 900.
3. `header-label-two-lines` — the label box ≤ 2 line-heights + 2; the action box ≥ 44×44.
4. `r20d-pinned-stack` — scroll each group's first kind row to 60 % of the viewport: pin bar top = header bottom ± 1.5,
   bar ≤ 90 px, every pin line ≤ 2.6 line-heights; the bar's text equals `pinChain` for that y (T28 — from the page's own
   anchors, read through a `window.__regsDebug` hook exposed only in dev).
5. `pictures-whole` — no two `[data-testid^="pic:"] img` rects overlap > 1 px; each img's rendered aspect = manifest ± 3 %.
6. `bait-ways-compact` — Methods section height ≤ viewport − header (vedder_1010, klwa_1010, cow1_1010, vedder_515).
7. `r20g-possession-fold` — the `pos:pos` sheet body < 300 px tall (vedder_1010).
8. `sheets open` — for every golden sheet key of the state (`appKeyOf`), open it via deep link: the sheet heading equals the
   golden title, no console error, Done closes it.
9. `tabs` — pressing each tab scrolls its section head to ≤ 3 px under the header (r20e-four-tabs' landing rule) unless at the end.
10. `no console errors` across the whole walk.
`compare-regs.mjs` has no pass/fail except the reader gate; its diff % is printed for the PR.

**A — `states.test.ts`**: `urlsOf("vedder_1010")` → proto hash `w=vedder&d=1010&p=0&sec=keep`, app query
`regs=gnis:<item>&part=0&date=2026-10-10&tab=keep` (item from taps.json); every one of the 134 golden ids maps uniquely.

## Acceptance
```sh
pnpm -C app/apps/mobile web &          # Expo web on :8081, store from tools/serve-tiles.mjs /regs.sqlite (T01)
node app/tools/regs-walk.mjs           # all PASS, light and dark
node app/tools/compare-regs.mjs --state vedder_1010 --out /tmp/cmp && open /tmp/cmp
node app/tools/compare-regs.mjs --reader /tmp/reader      # TALL + FRAMES PNGs named as prompt-gate39.md expects
# then the human runs the fresh reader on /tmp/reader with port/reader/prompt-gate39.md (tests.md §4); pass = 39/39, no answer differing from the prototype reader
pnpm -C app vitest run tools/regs-web
```

## Notes
- The prototype runs offline with fallback fonts in check.mjs; compare-regs loads fonts in BOTH (not offline) — the
  side-by-side is about the design, not the fallback metrics. The golden pin anchors were measured with fallback fonts:
  never compare app px to golden `pins.anchors`, only to a fresh prototype measurement in the same run.
- D2 (a): the app has the FaceBar row above the regs header — the app column is taller by that row; compare-regs aligns the
  two columns at the regs header's top, not at the screen top.
- The deep link is a test affordance on web only; it never ships a route on native (D1 keeps navigation in Shell).
- Reader runs cost agent time; they are run by the human (like parser runs) — this tool only produces the PNGs.
