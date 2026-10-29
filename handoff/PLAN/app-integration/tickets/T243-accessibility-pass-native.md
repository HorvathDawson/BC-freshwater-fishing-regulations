# T243 — accessibility pass (native)

Status: DONE — 98bb69021, cbaf6e30a, e0e70fed5, d12bd8799, d34ea54e3. Leftovers (the screens T257 was rewriting, the owner's phone walk) → T258.

Source: owner 2026-10-28 — approved proposal P9 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
TalkBack + VoiceOver walk of map → water → regs → conditions → salmon → menu: every control labelled, roles set, focus
order sane, sheets announce; dynamic type up to 200 % without clipping (TYPE tokens scale); contrast ≥ 4.5:1 for text in
light / dark / colour-blind (a token test); touch targets ≥ 44 pt (MIN_HIT audit); reduce-motion respected.
## Acceptance
A written walk with findings fixed or ticketed; a contrast test over the three palettes; a11y render tests extended.
Depends: none. Size: M. Owner: no (an owner phone walk is welcome).

## Outcome (2026-10-29 overnight)
- CONTRAST (98bb69021): a token test over light / dark / colour-blind — every TEXT tone (ink, sub, faint, accent, live,
  place, closed, restricted, open) holds 4.5:1 on every ground (card, wash, page, tint), onAccent on accent too
  (`theme.test.ts`). Failing before: `faint` 4.05:1 on page/tint (light, CVD) and 4.26 on tint (dark); `live` (the gauge
  figures) 3.7–4.3:1 everywhere in light/CVD. Token values moved: faint #6E757B→#646B71 (light, CVD), #7C858C→#848D94
  (dark); live #04879B→#027486 (light, CVD). The style's ΔE guards still pass.
- LABELS, ROLES, TOUCH TARGETS (cbaf6e30a, e0e70fed5): `shellPressAudit` (regs/testing/a11yAudit.ts) — the regs view's
  rule over the rest of the app: every press (button, link, tab, radio, switch, checkbox) has a name and a `hitProps`
  box reaching 44 × 44; no labelled container hides words; no wordless view carries a label without a role.
  `a11y.shell.test.tsx` walks the Map / Conditions / Spots / More tabs, a spot, the spot capture, all six menu pages,
  search with results, the Layers sheet (both maps) and a water's three views, on a phone and a desktop (36 states), with
  planted violations pinning each rule. Found and fixed: ~45 presses with no declared box (tab bar, rail, top bar incl.
  the bell and sync — unmoved — options, layer switches, menu rows, search rows, spots, Button), standalone links at 18–28
  px (licences, About, a spot's text links: now 44 px rows), the sheet scrim (a role-less "Close"), the flow legend's
  ticks and bar (labels never said on the web: now images).
- WEB, AXE + KEYBOARD (`tools/a11y-walk.mjs`, d12bd8799): axe-core 4.10.2 (fetched at run time, nothing installed) WCAG
  2.1 A/AA over map, conditions, spots, more, offline, help, sources, layers, search, a regs page × phone/desktop ×
  light/dark — CLEAN after the fixes (before: aria-prohibited-attr ×6 on Conditions, ×1 on Layers; scrollable-region-
  focusable on Offline maps: a menu page's scroll is now focusable). A 30-stop Tab walk per desktop scene: every stop
  shows a focus mark. Shots + `a11y-walk.json`: `T243-shots/`.
- REDUCED MOTION: sheets, search, the regs page and the spinner already honoured it; the MAP CAMERA did not (framing a
  water, search's return, the water copy's zoom probe, a spot's centring glided 300–450 ms) — now `map/src/motion.ts`
  `cameraMs` jumps under reduce motion (web: the media query; native: AccessibilityInfo). Test: `motion.test.ts`.
- TEXT SIZE: the regs page's caps stand (T40). New: the search field (a fixed 44 px row) follows the OS size to 2×; the
  bottom tab labels to 1.3× (Claude call: a quarter of a phone cannot hold "CONDITIONS" larger; the platforms' own tab
  bars hold theirs; the name is still said in full).
## Calls made (owner asleep)
- Tab-bar labels capped at 1.3× text size (above). Default kept until the owner's phone walk says otherwise.
- The scrim is said as a "Close" button (a screen-reader user's second way out besides Done / Escape / back).
- react-native-web ignores `hitSlop`, so on the WEB a few icon presses stay at their drawn size (rail 40 px, back links
  32 px) — above WCAG 2.5.8's 24 px floor; on phones the slop makes 44. Listed in T258, not changed.
## Not verified
- TalkBack / VoiceOver on a device (owner walk welcome, T258); 200 % text on a device (jsdom has no layout).
