# T935 — iPhone: the year bars' closed days (and the "Closed" key swatch) draw no colour

Status: OPEN.

Source: owner 2026-11-20 ~00:11 (main chat), photo of his iPhone `T935-shots/owner-2026-11-20-fraser-iphone.jpg` (Fraser
River, CPR Bridge at Mission → Landstrom Bar, Nov 20). Verbatim: "On apple DFO calendar when closed has no color". The
SALMON bar shows only its open run (Oct–Nov 30); Jan–Oct and Dec are blank, and the key's "✕ Closed" has no swatch.
Android draws the red hatch (T934 screenshot, Cheakamus).

Suspected cause (to confirm in the iOS simulator): the closed hatch is a react-native-svg `<Pattern>` with
`patternTransform="rotate(135)"` (`Patterns` in `app/packages/ui-native/src/regs/YearStrip.tsx`), filled by
`url(#…-closed)`; open is a plain colour and draws. The game-fish bar and the trout year bar use the same pattern, so their
closed days are likely blank on iOS too.

## Work
1. Reproduce on the iOS simulator (a water with closed days on both bars, e.g. Fraser at Mission, Cheakamus).
2. Draw the hatch without a transformed Pattern (explicit diagonal stripes clipped to the run, or an untransformed
   pattern), the same look on Android, iOS and the web; every bar and key swatch that uses it.
3. Shots: iOS sim + Android emulator, same water, light/dark.
## Acceptance
Closed days show the red hatch on iOS on every year bar and key; Android and web unchanged in look; pictures to the owner.
Depends: none. Size: S. Owner: pictures. Batched with T922–T925, T934.
