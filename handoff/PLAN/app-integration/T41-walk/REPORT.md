# T41 — the web parity walk of the Regulations screens

2026-10-22 · worktree `wf_ab98736d-d44-1` · app on Expo web (this worktree, its own port) against the live release
**r-f734cbb93724**, served read-only by `app/tools/serve-tiles.mjs` on its own port · prototype **regs v38 @ bdda3586**
(the port goldens' ref `7fb6809f`), staged from git, never the working tree · 390 × 844 at DPR 2 · the app's clock on the
prototype's "today" (2026-10-10, `PROTO_TODAY`) so both headers say *Today* on the same day.

Everything merged since the tickets were written is in the walk: T23/T29 header + pin bar, T30–T37 tabs + sheets, T38
adipose, T42 edge states (x31p tidal, x15p2 timed), T43 book links + credits, T44 WaterFrame, T45/T46 (phone width here;
their own shots cover wide), T39/T40 gates, T49 calendar (the date pill).

## How to run it again

```sh
TILE_PORT=<tiles> RELEASE=r-f734cbb93724 node app/tools/serve-tiles.mjs &
(cd app/apps/mobile && EXPO_PUBLIC_DATA_BASE=http://localhost:<tiles> npx expo start --web --port <app>) &
node app/tools/regs-walk.mjs --url http://localhost:<app>                 # 736 PASS, 0 FAIL — light + dark
node app/tools/compare-regs.mjs --all --url http://localhost:<app> --scale 0.4 --jpeg 0.72 --out handoff/PLAN/app-integration/T41-walk/shots
node app/tools/compare-regs.mjs --reader /tmp/reader --url http://localhost:<app>   # then the human runs the reader (tests.md §4)
```

The app opens straight on a state through the **web start link** (a test affordance; native has none, D1):
`?regs=<item>&part=<i>&date=YYYY-MM-DD&tab=<tab>&sheet=<key>&age=&res=&theme=&scroll=` (`@app/ui` `parseStartLink`,
read once by `apps/mobile/src/startLink.web.ts`). In a development web build the page exposes `window.__regsDebug`
(`openSheet(key)`, `pinLines()`) so the walk opens all ~63 sheets of a state without a reload.

## The walk (`regs-walk.mjs`) — 23 states × light + dark: **736 PASS · 0 FAIL**

| check (check.mjs name) | pass | fail | what it measures on the rendered app |
|---|---|---|---|
| header-compact | 46 | 0 | regs header ≤ 120 px (119.4); name, date pill and Map on one row (tops within 4 px) |
| header-label-two-lines | 46 | 0 | place label ≤ 2 line-heights + 2; the action ≥ 44 × 44 |
| header-one-shape | 46 | 0 | header height and words identical at scroll 0 and 900 |
| group-head-compact | 44 | 0 | first group head ≤ 150 px (Vedder 76.6) |
| r20f-headers (r20d-flat) | 46 | 0 | every block of a group starts on the head's text edge (± 1 px) |
| r20d-pinned-stack | 174 | 0 | each kind row at 60 % and just past each head: bar under the header (± 1.5), ≤ 90 px, lines ≤ 2.6 LH, the bar's words = the store's `pinChain`, the group's own title first once its head is gone, the row not under the bar |
| pictures-whole | 46 | 0 | no two fish pictures overlap; each picture's aspect = the manifest's (± 3 %), drawn `contain` |
| bait-ways-compact | 8 | 0 | Methods fits the viewport under the header (vedder 553 · klwa 621 · cow1 512 · vedder_515 512 ≤ 631) |
| r20g-possession-fold | 2 | 0 | the possession sheet, opened BY THE START LINK: body < 300 px (was 325 → **255** after F1) |
| tabs | 184 | 0 | each tab pressed: its section lands ≤ 3 px under the header and is selected (page end: the scroll's tab) |
| sheets open | 46 | 0 | **2 904 sheet opens**: every golden sheet key of the state, by its app key — heading = the golden title, Done closes it |
| no console errors | 48 | 0 | none in the whole walk, either theme |

States: vedder_1010 · klwa_1010 · cow1_1010 · chw1_1010 · wilkes_1010 · vedder_515 · x12p_1010 · x3p_1010 · iron_720 ·
x34p_1010 · x35p_1010 · bc0_1010 · bc0_501 · ape_1010 · ape_501 · x15p3_1010 · bc1_501 · hamill_615 · klwa_1012 ·
glenn_615 · chw2_1010 (check.mjs's CARDS2 ∪ R17 ∪ R20S ∪ TRS ∪ SBS ∪ STATES_I) + x31p_1010 (tidal) + x15p2_1010 (timed).

The first run failed 2 `r20g-possession-fold` (325 px) — a real bug, fixed (F1). Its other failures were the walk's own
(a group title read with its scope badge; Methods measured with the report line under it) and were fixed in the tool.

## The side-by-sides (`compare-regs.mjs --all`) — 109 shots

Each shot is `[prototype | app | per-pixel diff]`, both columns cut at the regs header's top. The diff % is printed for
the PR, never a gate: the faces are the same, but a line break a pixel apart moves everything under it, so 7–24 % is the
range of a matching page. **Read the shots, not the %.**

Tally over the 109 shots: **match 95 · deliberate 2 · fixed 4 · open 8**. A shot is *match* when what differs is only
the deliberate, everywhere differences G1–G7 below (a Gear / Methods / Licence shot also carries the rhythm note R1).

### Deliberate everywhere (owner decisions, cited) — not counted per shot

- **G1 the water frame above the regs header** — ‹ back and the Regulations · Conditions · Map strip sit in the water
  frame's row (T44 `WaterFrame`; D2 answered 2026-10-20; OWNER RULE 2026-10-21 one water screen; back everywhere — the
  one back is the frame's; the prototype's ‹ in the regs header is not drawn).
- **G2 no bottom tab bar in water mode** — D1 answered 2026-10-20 (the bar is hidden while latched to a water).
- **G3 the sheet's "DONE"** — the app's one `Sheet` (map and regs sheets alike; T37/T40: an 11 px tracked word grown to
  44 × 44); the prototype's "Done" is sentence case. Also an owner question (Q4): the regs sheets could differ.
- **G4 one-part waters show their part label** ("Whole water", "Mouth → Source") — OWNER 2026-10-21; the 22 golden
  states stay keyed in `exclusions.ts NO_MAP_LABEL_IDS`.
- **G5 the Map action where the prototype had Parts or nothing** — D1: the app frames every part from the tiles and the
  parts are stepped on the water's own map (P15); the prototype's "no map of this water" note is never ported.
- **G6 status colours** — unchanged: they wait for the owner's T47 pick.
- **G7 the date pill opens the T49 calendar** — "Which day?" is the map's own date sheet (T49), not the prototype's.

### Fixed (test-first, in this branch)

- **F1 the possession sheet** (`pos:pos`): the term's words and the examples were drawn at body size with square
  markers, the "doubles" sentence run into the first example; the prototype draws them small in sub (`.glex`,
  `ul.posx`) with the doubles words bold in ink. Core `possessionSheet` marks the term para `small` and the list `small`
  with each item's `strong` words held apart (`ListItem`); `List` draws the small form. Body 325 → 255 px
  (`r20g-possession-fold` < 300 now passes). Golden lines unchanged (the parity harness passes whole).
- **F2 the hatch sheet** (`hatch`): the hatchery_wild term was drawn as a second sheet's body (bold title, body text);
  the prototype's `.htw` draws it as a small heading over small sub words. Core `hatchSheet` emits a `head` + small
  paras. Golden lines unchanged.
- **F3 a row's sources sheet** (`src:r…`): each deciding rule's book words ran on after its title at body size,
  unquoted; the prototype's `.srow` puts them on their own line, small, sub, quoted (`q.sq`). `SourcesView` `Row` draws
  that. (The golden-mount test's `bare()` drops curly quotes on both sides: the prototype's `<q>` quotes are CSS.)

### Open (owner calls, or follow-ups wider than this walk)

- **R1 vertical rhythm of Gear / Methods / Licence** — the app's sections run ~15–45 % taller than the prototype's
  (Vedder: gear 731 vs 630, methods 553 vs 414 + the Reference fold the prototype draws as its own section, licence 926
  vs 777 px): a 16 px rule-and-margin above each section head, notes at 17.7 px line height vs ~16.8, list rows 27 vs
  25.6 px, and the "Report a problem with this page" line under each section. Words and order match. Kootenay LWA
  Methods is 621 of the 631 px it may take (bait-ways-compact passes by 9 px). A T30–T32 spacing pass.
- **O1 no tile behind the adipose drawings / fish pictures** — the T38 owner question (D4 / r20h `picture-surface`).
- **O3 the timed caveat** (x15p2 "At certain times") has no grey box around its hours — the T42 owner question "caveat look".
- **O4 sheet typography** in licsrc / srcs / other / sp sheets: section kickers drawn upper-case (the prototype's are
  sentence case: "To fish here", "These decide it (12)", "What to look for"), the licence documents as plain body lines
  (the prototype's `.doc` row with a rule), rule titles at body size (prototype 12.5 bold). Words and order match (the
  golden-mount test holds every line); those sheets run ~20 % taller. A T36/T37 typography pass.
- **O5 sheets open taller** on a phone than the prototype's (the app's `Sheet` leaves less of the page above).

## The reader gate (tests.md §4) — produced, not run

`node app/tools/compare-regs.mjs --reader OUT` shoots the reader set from the app with the prototype's PNG names, from
`app/design/regs-v11/port/reader/states.json` (29 entries: 20 TALL, 2 TALL/NEW, the Vedder FRAMES series + 6 row frames).
`prompt-gate39.md` now names `$OUT/TALL`, `$OUT/FRAMES` and `$OUT/TALL/NEW`; the questions are unedited. The reader is
an agent run: **the human launches it** (pass = 39/39, no answer differing from the prototype reader's) and files the
result in `handoff/PLAN/app-integration/reader/`. T39's `regs-census.mjs` now reads the same states.json (each water once).

## Owner questions

- **Q1 (R1)** Tighten Gear / Methods / Licence to the prototype's rhythm (rule + 16 px above each head, note line height,
  list row height), or keep the app's looser spacing? Kootenay LWA Methods has 9 px to spare.
- **Q2 (O4)** Port the prototype's sentence-case sheet kickers and `.doc` rows (licsrc, srcs, other, sp), or keep the
  app's upper-case kicker everywhere?
- **Q3 (R1)** The "Report a problem with this page" line closes every tab section (~50 px each): keep it per section, or
  once at the page's foot?
- **Q4 (G3)** Regs sheets' "DONE" (the app's shared Sheet, upper-case) vs the prototype's "Done": keep one Sheet style?
- Still open from earlier tickets and seen here: T38/D4 no tile behind drawings (O1), T42 caveat look (O3), T47 status
  colours (G6).

## Not done here

- `--font-scale 2` in compare-regs: react-native-web drops the font-scale props, so a browser cannot show the 200 % app;
  T40's `regs-text-scale.test.ts` (the DOM laid out by headless Chrome at 2×) is where that is measured.
- Native: this is the Expo web walk (D28 wants native first; the native engine is not running yet).
- The reader run itself (human-launched).

## Every shot

| state | tab / sheet | theme | diff % | verdict | shot |
|---|---|---|---|---|---|
| vedder_1010 | keep | light | 13.63 | **match**  | [shot](shots/vedder_1010-keep-light.jpg) |
| vedder_1010 | gear | light | 11.27 | **match** R1 rhythm | [shot](shots/vedder_1010-gear-light.jpg) |
| vedder_1010 | methods | light | 11.45 | **match** R1 rhythm | [shot](shots/vedder_1010-methods-light.jpg) |
| vedder_1010 | licence | light | 16.89 | **match** R1 rhythm | [shot](shots/vedder_1010-licence-light.jpg) |
| vedder_1010 | keep · sheet `pos:vedder_1010\|pos` | light | 18.96 | **fixed** F1: term words + examples small, the doubles words bold (body 325 → 255 px) | [shot](shots/vedder_1010-keep-pos_pos-light.jpg) |
| vedder_1010 | keep · sheet `hatch` | light | 23.11 | **fixed** F2: the term under a small heading, its words small (sheet ~180 px shorter); O1 no tile behind the drawings | [shot](shots/vedder_1010-keep-hatch-light.jpg) |
| vedder_1010 | keep · sheet `gl:single_barbless_hook` | light | 16.17 | **match**  | [shot](shots/vedder_1010-keep-gl_single_barbless_hook-light.jpg) |
| vedder_1010 | keep · sheet `licsrc:lic\|vedder_1010\|adult` | light | 22.61 | **open** O4 sheet typography: section kickers upper-case (prototype sentence case), the licence documents as plain body lines | [shot](shots/vedder_1010-keep-licsrc_adult-light.jpg) |
| vedder_1010 | keep · sheet `srcs:bait\|vedder_1010` | light | 10.07 | **open** O4 sheet typography | [shot](shots/vedder_1010-keep-srcs_bait-light.jpg) |
| vedder_1010 | keep · sheet `srcs:gl\|vedder_1010\|hook` | light | 17.45 | **open** O4 sheet typography | [shot](shots/vedder_1010-keep-srcs_gl_hook-light.jpg) |
| vedder_1010 | keep · sheet `other:vedder_1010\|o0` | light | 15.34 | **open** O4 sheet typography | [shot](shots/vedder_1010-keep-other_o0-light.jpg) |
| vedder_1010 | keep · sheet `rule:zp:conduct::conduct.r1` | light | 14.51 | **match**  | [shot](shots/vedder_1010-keep-rule_zp_conduct_conduct_r1-light.jpg) |
| vedder_1010 | keep · sheet `sp:BCB` | light | 23.07 | **open** O4 "What to look for" upper-case kicker; T46 picture held smaller (deliberate) | [shot](shots/vedder_1010-keep-sp_BCB-light.jpg) |
| vedder_1010 | keep · sheet `src:vedder_1010\|r0` | light | 21.29 | **fixed** F3: each rule's book words quoted on their own line, small, sub | [shot](shots/vedder_1010-keep-src_r0-light.jpg) |
| klwa_1010 | keep | light | 11.79 | **match**  | [shot](shots/klwa_1010-keep-light.jpg) |
| klwa_1010 | gear | light | 8.13 | **match** R1 rhythm | [shot](shots/klwa_1010-gear-light.jpg) |
| klwa_1010 | methods | light | 11.3 | **open** R1 rhythm; bait-ways-compact see walk | [shot](shots/klwa_1010-methods-light.jpg) |
| klwa_1010 | licence | light | 15.9 | **match** R1 rhythm | [shot](shots/klwa_1010-licence-light.jpg) |
| klwa_1010 | keep · sheet `other:klwa_1010\|o0` | light | 17.88 | **open** O4 sheet typography | [shot](shots/klwa_1010-keep-other_o0-light.jpg) |
| cow1_1010 | keep | light | 11.48 | **match**  | [shot](shots/cow1_1010-keep-light.jpg) |
| cow1_1010 | gear | light | 14.34 | **match** R1 rhythm | [shot](shots/cow1_1010-gear-light.jpg) |
| cow1_1010 | methods | light | 12.42 | **match** R1 rhythm | [shot](shots/cow1_1010-methods-light.jpg) |
| cow1_1010 | licence | light | 16.52 | **match** R1 rhythm | [shot](shots/cow1_1010-licence-light.jpg) |
| chw1_1010 | keep | light | 13.8 | **match**  | [shot](shots/chw1_1010-keep-light.jpg) |
| chw1_1010 | gear | light | 10.99 | **match** R1 rhythm | [shot](shots/chw1_1010-gear-light.jpg) |
| chw1_1010 | methods | light | 11.88 | **match** R1 rhythm | [shot](shots/chw1_1010-methods-light.jpg) |
| chw1_1010 | licence | light | 17.32 | **match** R1 rhythm | [shot](shots/chw1_1010-licence-light.jpg) |
| wilkes_1010 | keep | light | 11.88 | **match**  | [shot](shots/wilkes_1010-keep-light.jpg) |
| wilkes_1010 | gear | light | 8.99 | **match** R1 rhythm | [shot](shots/wilkes_1010-gear-light.jpg) |
| wilkes_1010 | methods | light | 9.26 | **match** R1 rhythm | [shot](shots/wilkes_1010-methods-light.jpg) |
| wilkes_1010 | licence | light | 14.19 | **match** R1 rhythm | [shot](shots/wilkes_1010-licence-light.jpg) |
| vedder_515 | keep | light | 16.07 | **match**  | [shot](shots/vedder_515-keep-light.jpg) |
| vedder_515 | gear | light | 11.54 | **match** R1 rhythm | [shot](shots/vedder_515-gear-light.jpg) |
| vedder_515 | methods | light | 10.78 | **match** R1 rhythm | [shot](shots/vedder_515-methods-light.jpg) |
| vedder_515 | licence | light | 15.01 | **match** R1 rhythm | [shot](shots/vedder_515-licence-light.jpg) |
| x12p_1010 | keep | light | 11.83 | **match**  | [shot](shots/x12p_1010-keep-light.jpg) |
| x12p_1010 | gear | light | 9.13 | **match** R1 rhythm | [shot](shots/x12p_1010-gear-light.jpg) |
| x12p_1010 | methods | light | 13.34 | **match** R1 rhythm | [shot](shots/x12p_1010-methods-light.jpg) |
| x12p_1010 | licence | light | 18.85 | **match** R1 rhythm | [shot](shots/x12p_1010-licence-light.jpg) |
| x3p_1010 | keep | light | 10.37 | **match**  | [shot](shots/x3p_1010-keep-light.jpg) |
| x3p_1010 | gear | light | 8.85 | **match** R1 rhythm | [shot](shots/x3p_1010-gear-light.jpg) |
| x3p_1010 | methods | light | 13.26 | **match** R1 rhythm | [shot](shots/x3p_1010-methods-light.jpg) |
| x3p_1010 | licence | light | 12.94 | **match** R1 rhythm | [shot](shots/x3p_1010-licence-light.jpg) |
| iron_720 | keep | light | 10.83 | **match**  | [shot](shots/iron_720-keep-light.jpg) |
| iron_720 | gear | light | 11.4 | **match** R1 rhythm | [shot](shots/iron_720-gear-light.jpg) |
| iron_720 | methods | light | 11.26 | **match** R1 rhythm | [shot](shots/iron_720-methods-light.jpg) |
| iron_720 | licence | light | 14.04 | **match** R1 rhythm | [shot](shots/iron_720-licence-light.jpg) |
| x34p_1010 | keep | light | 12.6 | **match**  | [shot](shots/x34p_1010-keep-light.jpg) |
| x34p_1010 | gear | light | 9.04 | **match** R1 rhythm | [shot](shots/x34p_1010-gear-light.jpg) |
| x34p_1010 | methods | light | 11.21 | **match** R1 rhythm | [shot](shots/x34p_1010-methods-light.jpg) |
| x34p_1010 | licence | light | 14.48 | **match** R1 rhythm | [shot](shots/x34p_1010-licence-light.jpg) |
| x35p_1010 | keep | light | 11.83 | **match**  | [shot](shots/x35p_1010-keep-light.jpg) |
| x35p_1010 | gear | light | 9.12 | **match** R1 rhythm | [shot](shots/x35p_1010-gear-light.jpg) |
| x35p_1010 | methods | light | 13.34 | **match** R1 rhythm | [shot](shots/x35p_1010-methods-light.jpg) |
| x35p_1010 | licence | light | 18.85 | **match** R1 rhythm | [shot](shots/x35p_1010-licence-light.jpg) |
| bc0_1010 | keep | light | 11.96 | **match**  | [shot](shots/bc0_1010-keep-light.jpg) |
| bc0_1010 | gear | light | 9.56 | **match** R1 rhythm | [shot](shots/bc0_1010-gear-light.jpg) |
| bc0_1010 | methods | light | 11.55 | **match** R1 rhythm | [shot](shots/bc0_1010-methods-light.jpg) |
| bc0_1010 | licence | light | 15.32 | **match** R1 rhythm | [shot](shots/bc0_1010-licence-light.jpg) |
| bc0_501 | keep | light | 13.76 | **match**  | [shot](shots/bc0_501-keep-light.jpg) |
| bc0_501 | gear | light | 10.26 | **match** R1 rhythm | [shot](shots/bc0_501-gear-light.jpg) |
| bc0_501 | methods | light | 11.92 | **match** R1 rhythm | [shot](shots/bc0_501-methods-light.jpg) |
| bc0_501 | licence | light | 16.24 | **match** R1 rhythm | [shot](shots/bc0_501-licence-light.jpg) |
| ape_1010 | keep | light | 10.73 | **match**  | [shot](shots/ape_1010-keep-light.jpg) |
| ape_1010 | gear | light | 8.36 | **match** R1 rhythm | [shot](shots/ape_1010-gear-light.jpg) |
| ape_1010 | methods | light | 11.02 | **match** R1 rhythm | [shot](shots/ape_1010-methods-light.jpg) |
| ape_1010 | licence | light | 16.81 | **match** R1 rhythm | [shot](shots/ape_1010-licence-light.jpg) |
| ape_501 | keep | light | 10.68 | **match**  | [shot](shots/ape_501-keep-light.jpg) |
| ape_501 | gear | light | 8.18 | **match** R1 rhythm | [shot](shots/ape_501-gear-light.jpg) |
| ape_501 | methods | light | 10.94 | **match** R1 rhythm | [shot](shots/ape_501-methods-light.jpg) |
| ape_501 | licence | light | 16.74 | **match** R1 rhythm | [shot](shots/ape_501-licence-light.jpg) |
| x15p3_1010 | keep | light | 13.42 | **match** G7 Map, not Parts (D1) | [shot](shots/x15p3_1010-keep-light.jpg) |
| x15p3_1010 | gear | light | 10.78 | **match** R1 rhythm | [shot](shots/x15p3_1010-gear-light.jpg) |
| x15p3_1010 | methods | light | 11.77 | **match** R1 rhythm | [shot](shots/x15p3_1010-methods-light.jpg) |
| x15p3_1010 | licence | light | 16.52 | **match** R1 rhythm | [shot](shots/x15p3_1010-licence-light.jpg) |
| bc1_501 | keep | light | 13.77 | **match**  | [shot](shots/bc1_501-keep-light.jpg) |
| bc1_501 | gear | light | 10.27 | **match** R1 rhythm | [shot](shots/bc1_501-gear-light.jpg) |
| bc1_501 | methods | light | 12.21 | **match** R1 rhythm | [shot](shots/bc1_501-methods-light.jpg) |
| bc1_501 | licence | light | 16.18 | **match** R1 rhythm | [shot](shots/bc1_501-licence-light.jpg) |
| bc1_501 | keep · sheet `parts` | light | 11.39 | **deliberate** D1: the prototype's "no map of this water" note is not ported; the ✓ on the part read | [shot](shots/bc1_501-keep-parts-light.jpg) |
| hamill_615 | keep | light | 12.56 | **match**  | [shot](shots/hamill_615-keep-light.jpg) |
| hamill_615 | gear | light | 7.82 | **match** R1 rhythm | [shot](shots/hamill_615-gear-light.jpg) |
| hamill_615 | methods | light | 9.26 | **match** R1 rhythm | [shot](shots/hamill_615-methods-light.jpg) |
| hamill_615 | licence | light | 13.27 | **match** R1 rhythm | [shot](shots/hamill_615-licence-light.jpg) |
| klwa_1012 | keep | light | 11.78 | **match**  | [shot](shots/klwa_1012-keep-light.jpg) |
| klwa_1012 | gear | light | 8.09 | **match** R1 rhythm | [shot](shots/klwa_1012-gear-light.jpg) |
| klwa_1012 | methods | light | 10.72 | **match** R1 rhythm | [shot](shots/klwa_1012-methods-light.jpg) |
| klwa_1012 | licence | light | 15.86 | **match** R1 rhythm | [shot](shots/klwa_1012-licence-light.jpg) |
| glenn_615 | keep | light | 12.51 | **match**  | [shot](shots/glenn_615-keep-light.jpg) |
| glenn_615 | gear | light | 7.05 | **match** R1 rhythm | [shot](shots/glenn_615-gear-light.jpg) |
| glenn_615 | methods | light | 9.88 | **match** R1 rhythm | [shot](shots/glenn_615-methods-light.jpg) |
| glenn_615 | licence | light | 13.19 | **match** R1 rhythm | [shot](shots/glenn_615-licence-light.jpg) |
| chw2_1010 | keep | light | 14.11 | **match**  | [shot](shots/chw2_1010-keep-light.jpg) |
| chw2_1010 | gear | light | 9.92 | **match** R1 rhythm | [shot](shots/chw2_1010-gear-light.jpg) |
| chw2_1010 | methods | light | 12.13 | **match** R1 rhythm | [shot](shots/chw2_1010-methods-light.jpg) |
| chw2_1010 | licence | light | 15.63 | **match** R1 rhythm | [shot](shots/chw2_1010-licence-light.jpg) |
| x31p_1010 | keep | light | 11.91 | **match**  | [shot](shots/x31p_1010-keep-light.jpg) |
| x31p_1010 | gear | light | 15.41 | **match** tidal words (D20 b) — the v38 prototype draws them too | [shot](shots/x31p_1010-gear-light.jpg) |
| x31p_1010 | methods | light | 15.29 | **match** tidal words (D20 b), as v38 | [shot](shots/x31p_1010-methods-light.jpg) |
| x31p_1010 | licence | light | 15.29 | **deliberate** D33 (b): the stored tidal licence answer, not the prototype's empty section | [shot](shots/x31p_1010-licence-light.jpg) |
| x15p2_1010 | keep | light | 14.18 | **open** O3 the timed caveat draws no grey box (T42 owner question "caveat look"); G7 Map, not Parts | [shot](shots/x15p2_1010-keep-light.jpg) |
| x15p2_1010 | gear | light | 7.99 | **match** R1 rhythm | [shot](shots/x15p2_1010-gear-light.jpg) |
| x15p2_1010 | methods | light | 11.33 | **match** R1 rhythm | [shot](shots/x15p2_1010-methods-light.jpg) |
| x15p2_1010 | licence | light | 14.46 | **match** R1 rhythm | [shot](shots/x15p2_1010-licence-light.jpg) |
| vedder_1010 | keep | dark | 13.65 | **match**  | [shot](shots/vedder_1010-keep-dark.jpg) |
| vedder_1010 | gear | dark | 10.91 | **match** R1 rhythm | [shot](shots/vedder_1010-gear-dark.jpg) |
| vedder_1010 | methods | dark | 10.03 | **match** R1 rhythm | [shot](shots/vedder_1010-methods-dark.jpg) |
| vedder_1010 | licence | dark | 16.11 | **match** R1 rhythm | [shot](shots/vedder_1010-licence-dark.jpg) |
| vedder_1010 | keep · sheet `pos:vedder_1010\|pos` | dark | 12.8 | **fixed** F1: term words + examples small, the doubles words bold (body 325 → 255 px) | [shot](shots/vedder_1010-keep-pos_pos-dark.jpg) |
