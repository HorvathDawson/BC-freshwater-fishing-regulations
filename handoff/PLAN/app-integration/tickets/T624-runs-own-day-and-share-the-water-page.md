# T624 — Runs keeps its own day; a shared water link is the water's own page

Status: IN REVIEW (a938841a0)

Source: owner 2026-11-10, answering T620's two open choices (T608B findings G10 and G17).

## Work
1. **Runs is its own thing** (owner: "I think for now Runs is its own thing complete"). Undo T620's G10 change
   (f508d2e96 seeded Runs from the map's picked day). Runs keeps its own day, never moved by the map's day and never
   moving it, and names the day it shows the way Conditions does, from the one B.C. clock. Web and phone the same,
   from shared code. Update `app/tools/one-answer.test.ts` so it pins this (Runs' day is independent and labelled).
2. **Share the water's own page** (owner: "yes make preview right"). When the water's slug is known, the share action
   sends `/water/<slug>` (whose preview is the water's own card) instead of `/?regs=…` (the generic home card); keep
   the day/part in the link only as far as the water page already reads them. Fix the comment in
   `app/packages/core/src/share.ts` that says a link opens "in a browser or the app": today it opens the website.
   Test: the shared address for a water with a slug, and the fallback when there is none (shown, not silent).

## Acceptance
- Changing the map's day does not move Runs; Runs says which day it shows. A link shared from a water previews as that
  water.

Depends: T620. Size: S. Owner: looks at the shots.
