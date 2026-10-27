# T214 — app polish: quota wording, desktop navigation, part highlight, steady stretch buttons

Status: PARTIAL (items 1, 2, 4 built; item 3 waits on the T211 lane's map paint)

Source: owner 2026-10-27.

1. Quota sub-cap wording names its total: "Of these, only 1 may be over 50 cm (20 in) — 2 if one is a hatchery
   steelhead" → "Of your 2 trout and char, only 1 may be over 50 cm (20 in) — 2 if one is a hatchery steelhead" (the
   group and its number from the total it belongs to; every such line, all regions). Golden parity: a reasoned wording
   amendment, not a blind re-cut.
2. Desktop: no swiping between Regulations / Conditions / Map (swipe only on touch/mobile); Esc acts as Back.
3. Regs tab map-section navigation: make the highlighted part/stretch much more obvious (selected vs others), every
   water colour incl. amber + cvd.
4. Upstream/downstream navigation still flickers: the "Flows into X" line appears/disappears and moves the buttons —
   reserve its space (or place it where it cannot shift the controls); a test asserts the buttons keep their position.

## Outcome (2026-10-27)
- **1 (built)**: core `tree.ts` `subcapLead`/`tierWords`: a size tier whose store words start "Of these, " reads "Of your N <group>, " (N = the
  number of the tier it sits in, the group = the row title through `lc`); the cap's own numbers stay big. The pipeline still prints "Of these"
  (not touched). Golden parity is a reasoned amendment: `oracles.ts subcapRuled` rewrites page v36's 117 "Of these, " strings at load from the
  golden's own facts (group title; parent's first big number, a folded share's `leadN`, the head's `big` at the top), `SUBCAP_REWRITES` = 117
  pinned in `exclusions.test.ts`; the Kitimat store walk reads the lead the same way. No re-cut. VM fixtures re-cut (the lead only).
  Tests: `tree.test.ts` (lead, bold, no parent / no number / no title, proper noun), `exclusions.test.ts`, `tree.parity.test.ts`.
- **2 (built)**: `useViewSwipe(route, go, touch)` is off when `touch` is false; the Shell passes `isTouchDevice()` (`touchDevice.web.ts`:
  primary pointer coarse; native always true). `useEscapeBack` (`escapeBack.web.ts`, mounted in `Shell`): Escape is the one Back on the
  desktop web, not while a layer/search/dialog is open, not typed in a field, not when already handled. Tests: `gestures.test.tsx`,
  `escapeBack.web.test.tsx`; the swipe tests (`WaterMode.test.tsx`, `tools/water-frame-swipe.test.ts`) stub a phone.
- **3 (NOT built)**: the part/stretch highlight is the map paint (`packages/map/src/paint.ts`, T203's halo) which the T211 lane is changing;
  do it after that merges (stronger selected-vs-others cue; amber + all cvd models via `tools/water-status-cvd.test.ts`).
- **4 (built)**: T203 moved the chip LAST, but the bar sits at the foot of the screen, so a line that appears grows the bar upward and lifts the
  steps and "Show regulations" anyway. Now the VM carries `exitHeld` (the water's mouth flows into another water, at any part) and the bar keeps a
  44 px slot (`water-map-exit-slot`) at every part, the chip filling it (one line, ellipsis); the part name keeps two lines (46 px) so a name that
  wraps in one part does not move the steps either. Test: `WaterMapView.test.tsx` (same children, order, slot and name heights at the mouth and one
  step up, and back). Layout is asserted structurally (jsdom lays nothing out); a phone check is the owner's.
