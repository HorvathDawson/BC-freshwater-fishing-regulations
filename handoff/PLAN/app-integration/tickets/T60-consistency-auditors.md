# T60 — Consistency auditors (audit N16)

Status: DONE — 34606600 (fixes), 7e70a909 (the six gates), 0e23effb (style.sha256 re-stamp), c6ba9cd6 (back-layers snapshot), + this ticket commit. Lane: worktree agent-ac8a6de77769cfaac,
2026-10-23. Leftover copy → proposed ticket "T60-copy" (Outcome F6); owner question Q-T60a (Outcome F9).
Owner 2026-10-22: Fable-model agents audit the app for consistency.

Checks to build, each pinned with a mutation test: route equality (one route per place), one surface per action (Map
was offered twice), data-vs-UI (every number/word on screen traces to the bundle or core words), one icon set, one
colouring pathway, back everywhere. Run the auditors on the Fable model; each finding becomes a fix or a ticket.

## Outcome (2026-10-23, Fable lane)

**Commits** (worktree branch, for the merger into `redesign/stream-sections`): `34606600` fix(app): the findings fixed;
`7e70a909` check(app): the six gates + `app/tools/consistency.ts` (shared walker / comment stripper); this commit: the
ticket. `app/package.json` is unchanged: every gate is a vitest file under `app/tools/` (logic project) or
`app/apps/mobile/src/` (render project), so `pnpm test` — which `pnpm check` runs — runs them; the colour-pathway gate
reads `scripts.check` from package.json and fails if `colours` or `test` is dropped from it.

**The checks, each with its mutation proof** (every planted defect made its gate RED; the plant was then restored —
`scratchpad/mutations.sh`, all seven ✓):

| # | check | where | method | mutation proof |
|---|---|---|---|---|
| 1 | one route per place | `tools/one-route-per-place.test.ts` | reducer: every exported way into a water (`openTap` from either map, `waterOpener.tap/flow/item/searched`, `openWater`, `exitAction`, `crossAction`, `partAction`, `showRegsActions`) lands ONE frame per (water, part, section, point), differing in `view` alone; static: each screen component is mounted by exactly one element in a `.tsx` (allowances name why: SpotScreen as the draft form, TabBar under browse and under a spot, DaySheet from the map and the regs pill, MapScreen a canvas) | a planted second `<MoreScreen` mount → RED; in-test pin: an entry that forgets the part / the tapped section is a different place |
| 2 | one surface per action | `apps/mobile/src/surfaces.test.tsx` | the real Shell (map mocked as in back.test) at 21 places — four tabs, a water's three views + day sheet, spot viewed / editing / delete asked, four capture steps, search, Layers sheet, the four More pages: no two controls of one role share an accessible name; `ALLOWED` names the one deliberate pair (Rule book links: one per rule, several rules cite one page) | in-test pin over the collector (`duplicates`); live: found F5 (fixed) |
| 3 | data vs UI | `tools/data-vs-ui.test.ts` | scans every component for JSX text and copy-like string literals (two words, a capitalised word, a glyph) and for typed FIGURES (a number with a unit or sign); (1) no component types a figure — `KNOWN_FIGURES` is `{}`; (2) the hand-written copy equals a frozen `KNOWN_COPY` list that may only shrink | a planted sentence → RED; a planted `"+1 h"` → RED; in-test pin: label / sentence / glyph / figure found, style word / key / id / `case "…":` not |
| 4 | one icon set | `tools/one-icon-set.test.ts` (extended) | + typed triangles / carets (▼ ▶ ▾ ▸ ▲ …) refused in components (a glyph inside a core word — ▲/▼ `WATER_MAP_WORDS`, ▾ `DATE_CARET`, ↗ `LINK_OUT` — is data to a component); + MUTATION PIN for every glyph scan (the file had none) | a planted `"▼"` → RED |
| 5 | one colouring pathway | `tools/one-colour-pathway.test.ts` (+ existing `check-colours.mjs`) | token readers (`colour(`, `translucent(`, `waterStatusColour(`, `tempBandColour(`, `resolveTheme(`, `STYLE_META`) only in `theme.ts`; no derived colour (`${palette.x}33`, `rgba(${…`); `pnpm check` wiring pinned | a planted `colour("light", …)` in a component → RED; a planted `"#123456"` → `check-colours.mjs` RED |
| 6 | back everywhere | `apps/mobile/src/surfaces.test.tsx` (same run) + existing `back-layers.test.ts` / `back.test.tsx` | at every pushed place (17 of the 21) a visible way back is on screen: a ‹ ("Back", "Back to <tab>"), × ("Close…"), Done, Cancel, Keep | FaceBar mutated to draw neither ‹ nor × → every water place RED |

**Findings → what was done**

- F1 FIXED (34606600): `regs/KeepBlocks.tsx`, `regs/PlainRow.tsx`, `regs/sheet/Fold.tsx` drew their fold mark as a typed
  ▼ ▶ / ▾ ▸ beside a set that has `expand` / `collapse` (N1 says the fold mark is the set's). Now `<Icon>` / `<IconText>`
  like every other fold (GaugeGroups, DonorPanel, ReferenceFold, WaterMapView). No golden pins the glyph; the regs
  suites (KeepTab, SheetBody, Edges, HowToTell, a11y, rhythm) pass.
- F2 FIXED: `menu/InfoPages.tsx`, `menu/MoreScreen.tsx` typed ↗ for an external link; now `WORDS.LINK_OUT` (the regs
  page's own glyph).
- F3 FIXED: `TabBar.tsx` typed the four tab names; now `BROWSE_TABS` in `core/tabs.ts`, built from the words each place
  already had (`WATER_VIEW_LABEL.map/.conditions`, `SPOTS_MENU.title`, `MENU_WORDS.title`) — one word per place.
- F4 FIXED (the typed FIGURES, and the legend words beside them): Shell's Conditions ramp title + Low/Normal/High →
  `FLOW_LEGEND` (core/conditions.ts); the Map tab's "Closed today" / "Closed on <day>" → `closedLegend` (core/day.ts);
  the chart key's "reading / middle half / 10th–90th / median" → `CONDITIONS_WORDS.key`; the spot weather window's
  "−1 h / visit / +1 h" → `SPOT_WINDOW` (core/spotWords.ts). No component types a number any more.
- F5 FIXED: `SpotScreen.tsx` offered the way out of an edit / a draft twice — the top "Cancel editing" / ‹ Back link and
  a bottom ghost "Cancel" / "Back" button beside Save. The bottom one is gone (the top ‹ is where every screen's back
  is); the Save row keeps Save alone. SpotScreen / Spots.shell / capture.render / back tests pass.
- F6 PROPOSED TICKET "T60-copy — move component copy to core words" (size M): ~105 hand-written strings still typed in
  components, frozen file by file in `KNOWN_COPY` (tools/data-vs-ui.test.ts): `apps/mobile/src/App.tsx` (the map's
  attribution lines — 8 strings duplicating the publishers core's `sourcesVM` names — and the dev hint), `SpotScreen`
  (36), `SpotCapture` (13), `SearchSheet` (8), `GaugeTrace` (7), `Hydrograph` (6 sentences), `Chrome` (3 legend a11y
  fragments), `WaterScreen` / `WaterViews` ("Loading this water" ×2, "No such water in this bundle.", "no data release is
  open"), `Sheet` ("Done", "Close" — the one close word, T41 Q4), `Credits` ("Sources"), `FishSpinner` ("Loading"),
  `SpotsScreen` ("NO PHOTO"). Scope: a words table per screen in core (SPOT_WORDS grows from `SPOT_WINDOW`,
  SEARCH_WORDS, CHART_WORDS …), each string deleted from `KNOWN_COPY` as it moves; the gate makes the list only shrink.
  No wording changes.
- F7 NOTED (allowed, named): a regs page draws several "Rule book page N, opens the PDF" links with the same name when
  several rules cite one printed page — one Source link per rule (T43) is data, not a second way to one action.
- F8 NOTED (allowed, named in `SCREENS`): `DaySheet` is mounted by the Shell (map calendar) and by WaterScreen (regs
  pill) — one sheet asked from two screens, only one open; `TabBar` under the browse body and under an open spot;
  `MapScreen` is a canvas drawn by the browse maps, the water Map view and the capture's point step; `SpotScreen` is
  reused as the capture's details form (`mode="draft"`).
- F9 OWNER QUESTION Q-T60a (default: leave): two glyphs are still TYPED, but in core words, so the gates allow them —
  the date pill's caret `WORDS.DATE_CARET` (" ▾", the prototype's "Today ▾", pinned by six header/pill tests) and the
  ▲ / ▼ in `WATER_MAP_WORDS` button labels ("▲ Upstream"). Should either become the set's chevron (`expand` /
  `next`)? Default: no — they are prototype wording the owner approved; recorded here, not changed.
- F12 FIXED (0e23effb): the branch's own `pnpm style` gate was RED — `46cab8cd` edited `style.meta.json` + `tokens.json`
  by hand without re-running `pnpm style:build`, so the committed `style.sha256` was stale (`git diff --exit-code` fails
  for everyone). Re-stamped (style.json / meta unchanged by the rebuild; only the sha moves). The main checkout's working
  tree already carried the same new sha, uncommitted.
- F13 (c6ba9cd6): `tools/back-layers.test.ts`'s "surfaces found today" snapshot listed "SpotScreen: takes onBack" only
  because the removed bottom button's `? p.onBack : leaveEdit` ternary matched its `onX:` scan; the line is dropped
  (SpotScreen still registers with `useBackLayer`; back.test drives it). Run 2 of the strict check caught it.
- F10 Back everywhere: no gap — all 17 pushed places show a way back (and back.test's system back agrees).
- F11 Route equality: no gap — every entry builder lands one place; no screen is mounted twice without a reason.
- Not touched: `pipeline/`, `tickets/README.md` (the merger updates the T60 row and files T60-copy / Q-T60a), stocking /
  bathymetry / salmon code. `graphify update .` not run: the worktree has no `graphify-out/` (main checkout only).

**Strict check** (`scripts/heavy.sh t60-check 30 -- env REGS_STORE_REQUIRED=1 pnpm -C app check`, unfiltered, in the
worktree with the main checkout's `.venv` linked in): run 3, 2026-10-23 23:1x — every gate ✓ (tickets, boundaries,
platform, style, colours, spinner, gauge-policy, calendar, fish, answers, pack, deps, typecheck), vitest
`Test Files 366 passed | 4 skipped (370) · Tests 8179 passed | 22 skipped (8201)`, `check rc=0`. Run 1 was red on the
stale style sha (F12), run 2 on the back-layers snapshot (F13); both fixed and committed.
