# T899 — In-season notices on a water open by default

Status: IN PROGRESS — left (Claude): owner sees pictures, review, merge into app/integrate-082

Owner 2026-11-19 ~01:10 (app chat), shot of a Fraser water's "2 in-season changes · see ›" fold with its two notices,
verbatim: "in season notices should default open".

Build: the in-season fold on a water's regulations starts open (each notice shown) on every open of the page; the
reader can still fold it. Phone and web. A render test pins it.

## What was built (lane `app/t898-t899`, 2026-11-19 ~01:25 MST)
- `InSeasonBanners` (`app/packages/ui-native/src/regs/InSeasonBanner.tsx`, one component for phone and web) starts open:
  every open of the page shows each notice under the "N in-season changes · see ›" line; a press on the line folds them, a
  second opens them again. The T700 bell focus still opens them (now already open).
- Render tests (`app/packages/ui-native/src/InSeason.test.tsx`): open on mount with the notice whole; folded by a press,
  open again on the next mount; the count line, fold and unfold.
- Picture: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/t898-shots/t899-inseason-fraser-390-light.png` (Fraser River, CPR Bridge at Mission → Landstrom Bar, Nov 19, 390 light,
  the pipeline's last in-season feed: "2 in-season changes · see ›" and both notices open).
