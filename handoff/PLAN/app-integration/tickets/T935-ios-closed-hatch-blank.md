# T935 — iPhone: the year bars' closed days (and the "Closed" key swatch) draw no colour

Status: DONE 2026-11-20 (33aa04409, shots 8bcbb359a 7008a6c73; independent review clean, web checked, smoke green).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T935 | iPhone: the year bars' closed days and the Closed key swatch draw no colour (owner) | DONE | 33aa04409 8bcbb359a 7008a6c73 | owner 2026-11-20 "On apple DFO calendar when closed has no color" |

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

## Built (lane/t935, 0d69e34a2 + shots cb0258128)
Cause confirmed on the iPhone 17 simulator: react-native-svg 15.16 on iOS applies a `<Pattern>`'s `patternTransform` to the
tile's own drawing (the JS Pattern passes it as the node `matrix`), so the rotated closed hatch (and the rotate(45) "flies"
pattern) drew nothing. Fix: no SVG patterns; `regs/stripes.ts` draws each run's stripes as shapes cut to the run, same
geometry; YearStrip (game-fish + salmon bars, both keys) and TroutYearBar use it. `stripes.test.ts`; regs vitest 104/104.
## Owner 2026-11-20 00:39 (main chat): pictures approved
Shown iPhone before/after (Fraser at Mission, Chilliwack) and Android after. Verbatim: "Yes they do". Lands after the
independent review and a web check.
