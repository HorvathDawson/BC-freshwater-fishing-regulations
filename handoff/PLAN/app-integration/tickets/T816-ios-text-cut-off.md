# T816 — iOS: text cut off (the hour on "When were you there?", the weather icons on the spot card)

Status: DONE (f7b41c208; landed 2026-11-17 in land/app-1117, proven on the simulator; owner re-tests on the 0.7.12 build).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T816 | iOS: text cut off (the hour on "When were you there?", the weather icons) | DONE | f7b41c208 | owner iPhone build 36, 2026-11-16 |

Source: owner 2026-11-16 ~21:22, testing iPhone TestFlight build 36 (app chat; pictures in the chat). Verbatim: "text is cut off weird". His pictures: on "When were you there?" (saving a spot, Lynn Creek) the hour
"18:00" shows with its top cut off; on the spot card's WEATHER line the two weather icons before "wind 9 km/h" are cut
to fragments.

## Look at
Inferred, not checked: T741 (in build 36) moved to per-weight font files; a font whose line box is shorter than its
glyphs clips on iOS (lineHeight smaller than the font's ascent + descent, or a fixed height). The weather icons may be a
glyph font or an icon set drawn in a text box of a fixed height. Reproduce on the iOS simulator, fix by the font metrics
or the box, not by per-screen nudges; Android and web unchanged.

## 2026-11-16 21:45 — cause and fix (branch `app-fix/ios-1116`, app chat lane)
Not the font files (T741 changed only the import paths; the same eight .ttf files) and not icons: the "two weather icons"
are the big temperature ("8°C") cut to fragments. Three Text styles spread `TYPE.figureBig` (19 px in a 23 px line box)
and raised only the size: the hour at 30 px (`SpotCapture.tsx` WhenScreen) and the spot record's flow figure and
temperature at 32 px (`spot/Record.tsx`), each still in the 23 px box. JetBrains Mono's line is 1.32 em (hhea 1020 / −300),
so 39.6 px at 30. iOS lays a line shorter than that with the descent on the box's bottom and draws nothing above its top:
16.6 px off the top at 30 px, through the top third of the digits; Android and the web draw the overflow, so only the
iPhone showed it.
Fix in the type scale (`packages/ui-native/src/type.ts`): `FACE_METRICS` (each face's ascent / descent / cap height,
pinned to the font files), `lineBoxFloor` (cap height + descent: the box that keeps capitals and figures whole on iOS)
and `typeAt(key, size)` (an entry at another size, its line box scaled with it: figureBig at 30 = 36.32, at 32 = 38.74);
the three sites use `typeAt`. Gate: `type.lineBox.test.ts` reads the .ttf files, checks every TYPE entry and every Text
style in `packages/ui-native/src` + `apps/mobile/src` built on a TYPE entry (spread, array, `typeAt`); it named exactly
the three sites before the fix, and its near-misses must fail. Android / web: the same three line boxes grow to fit
(the hour row is held by its 44 px step buttons, so the hour now sits centred between them; the record's figure row grows ~16 px); nothing else moves.
