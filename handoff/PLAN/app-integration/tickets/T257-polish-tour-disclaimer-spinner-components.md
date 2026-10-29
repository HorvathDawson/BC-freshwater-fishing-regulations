# T257 — polish pass: coach-mark tour, "Before you fish" sheet, one delayed spinner, shared components

Status: DONE — 2dfe54f3f, a9dbc83de, 8f196181b, 6cc0268c1, 75b2a70b5 (web walked desktop + phone, light + dark; native not walked on a device)

Source: owner 2026-10-29 polish pass (four items + add-ons: delayed spinner, generic components, pixel-diff proof).

## Work
1. First-run tour (T248) as coach marks: spotlight on the real element (date bar, Standard·Salmon, Conditions tab, a
   water, sync, bell), callout anchored to it, progress dots, Next/Back/Skip, reduced motion, phone + desktop, resumable
   from More → Help.
2. "Before you fish" (T236): calm sheet, clear title, three short points, "I understand", link to Sources & licences.
3. Every loading state is a FishSpinner through one delayed-show wrapper (shows after ~300 ms, stays ≥ 400 ms); guard.
4. Consistency sweep: extract generic components from the best existing instances (look unchanged), migrate call sites,
   guards against ad-hoc re-implementations; before/after shots + pixel diff per pair.
## Acceptance
Render tests + guards; tsc; shots in `T257-shots/` (desktop + phone, light + dark) with a diff table.
Depends: T236, T248. Size: M. Owner: no.

## Built (2026-10-29)
- TOUR (coach marks): `ui-native/src/CoachMarks.tsx` — a registry (`CoachContext`, `useCoachTarget(key)`, `useCoachRefs()`)
  the Shell provides; the controls register their own host view (CornerStrip `coach` prop: the date bar `days`, the
  Standard/Salmon strip `modes`; MapScreen `map`; TabBar `tab:<key>`; TopBar `sync`, `bell`). `CoachTour` measures them
  (`measureInWindow`, minus its own layer's origin), dims round a spotlight (4 scrim pieces + a 2 px accent ring), and
  anchors a callout (card, 1.5 ink edge, offset slab, caret, "n of N", title in the sheet face, Back / Next, Skip, dots)
  under or over the control, inside the 16 px gutters. A step whose control is not on screen is dropped (no Salmon
  offered; the bell stays where it is beside refresh and is spotlit with it when up). Cross-fade + 8 px rise; reduced
  motion: fade only. A back layer (back / Escape skip). Steps: core `WALKTHROUGH_WORDS` (days, modes, map, Conditions tab,
  Runs tab, refresh + bell), `WALKTHROUGH_VERSION` walkthrough-2. Held until the Map tab is in front; More → Help's
  "Show the walkthrough again" goes to the Map tab first.
- BEFORE YOU FISH: `FirstRunNotice` — the same Sheet (dialog on desktop), a quiet lead line, three numbered points
  (`LEGAL_WORDS.firstRun.points`: not the official regulations / the official sources decide / regulations change in
  season), "I understand", and a "Sources and licences" link (opens More → Sources over it).
- ONE SPINNER, ONE TIMING: `LoadingState` + `useDelayedShow` (shown after 300 ms, held ≥ 400 ms once shown; `immediate`
  for a surface that already waited). Every FishSpinner goes through it (11 sites + the boot screen); new spinners where
  a wait was words only or a hand-made loop: the sync button's checking state (was a turning arc), busy CornerStrip
  chips (Salmon loading: was the word "Loading…"), the water map's "Loading the parts", a pending page part on a slow
  line, adding a photo, and every `Button busy` (Refresh ×2, Ask for the flow, Send now, Retry).
- SHARED PIECES (extracted unchanged from the copies at the call sites): `Button` kinds `small` (3 sites) and `link`
  (3 sites + the first-run link), `busy` (5), `icon`, `a11yLabel`; `ErrorState` (9 sites: water/regs page ×2 with
  Try again, Spots ×2, spot, capture, photos, report ×2); `round(side)` for every circle (11 sites).
- Guards: `tools/one-spinner.test.ts` (no ActivityIndicator, FishSpinner only via LoadingState, no turning loop, no
  waiting word as a label), `tools/one-button.test.ts` (no literal radius, no typed-out pill / error line / retry link,
  a ratchet on accent-filled controls). Both mutation-checked. `tools/back-layers.test.ts` pins the tour.
- Deliberate look changes (one-offs): SpotCapture's Next is the Button (14 px padding + slab, was 15 / none); the
  report checkbox is square (was radius 4); the hydrograph legend's fill swatch is square (was radius 2); busy controls
  keep their word and show the fish (were "…", "Refreshing…", "Sending…", "Retrying…", "Loading…").
- Left for T260: Chip/Badge, SegmentedControl, EmptyState, Banner, SectionHeader, caption token, fontWeight literals,
  icon ladder, IconButton; RunsControls/RunsSheet radii (run-timing lane in flight).
- Proof: `T257-shots/` before/after (isolated builds) + `DIFF.md`: every migrated screen 0.00 % (the regs pair shows the
  migrated ErrorState + Try again); only the two redesigns differ. Tests: tsc -b clean; ui-native/ui/core/tools suites
  green on the committed tree except failures that predate T257 (back-layers OfflinePage — T238; SpotDropSheet scrim).
