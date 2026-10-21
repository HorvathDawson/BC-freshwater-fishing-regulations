# Gear / Methods / Licence tabs on Expo web (T34, T35)

2026-10-21. Vedder River, part 0 ("Mouth → Vedder Crossing Bridge"), today (Oct 21), 390 × 844, light theme.

How they were taken: Expo web on port 39431, reading the owner's data server at :39217 (release r-1bb06fa28daf,
read-only). A one-line `initialRoute` in `apps/mobile/src/App.tsx` opened the water; it was not committed. Shots by
headless Chrome over CDP (`app/design/regs-v11/tools/cdp.mjs`).

- `../T34-shots/vedder-gear-{1,2}.png`: the Gear tab (lines, the Hooks change, source lines, Bait).
- `vedder-methods-{1,2}.png`, `vedder-methods-3-reference-open.png`: Ways to fish, then the Reference fold, closed and open.
- `vedder-licence-{1,2}.png`: 16 or older, B.C. resident. `vedder-licence-u16-other-province.png`: after picking
  Under 16 and then Canadian, other province. The page rebuilds for that angler.

Compared with the prototype shots (`app/design/regs-v11/screens/ur4/tall/t-vedder-oct10-{gear,methods,licence}.png`),
the layout, words and order match. Known differences:
- The "Who is fishing?" selector is `Choice variant="segmented"`. Its type is `TYPE.regsTab` (700 12.48/15), not the
  census's 14.98 line height.
- The ✓/✕ names use `regsBodySemi` (lh 20.25) and the notes use `meta` (lh 17.72). The prototype has 19.5 and 16.22.
  This follows the tokens.md rule: pick one table entry and keep it.
- Native was not shot (no emulator in this lane).
