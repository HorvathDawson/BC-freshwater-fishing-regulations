# T884 — a fish sheet's big "20 a day" is cut off at the top on an iPhone

Status: IN PROGRESS (app chat lane app/t884-t886-clip-hatchery-art). Pictures to the owner before it lands.

Owner 2026-11-18 ~21:30, on his shot of Cultus Lake → Black crappie (the big "20 a day" highlighted), verbatim: "Also on
Apple the part highlighted is cut off weird because of row sizes".

## Look for
- The fish sheet's big number line (ui-native `regs/`), and every other big number line: the trout table's "Keep 2", the
  keep tab's "5 a day", "Up to 15".
- Likely a line box (`lineHeight`) shorter than the big face's letters, which iOS cuts off the top (T816), or a fixed row
  height.

## Done when
- Large numerals never clip on iOS or Android, at 1× and 200 % text.
- A render test pins it (the line's height at or above what the numeral's face needs, or no fixed height).
- Before / after on the iOS simulator: Cultus Lake → Black crappie.

## Done (lane app/t884-t886-clip-hatchery-art, 1d19ef12b)
- Cause: a big number is a nested Text with no lineHeight (LineText), so it sits in the OUTER line's box. Black
  crappie's 26 px "20" was in `regsBodySemi`'s 20.25 px box; the Keep tab's tier lines ("5 a day", "Up to 15") in
  `tier` 21.25 / `tierSmall` 18.75. iOS draws nothing above a box shorter than the glyphs (T816); Android and the web
  draw the overflow. The digits need 0.947 em (their tops reach 0.677 em in Bricolage 800, plus the 0.27 descent).
- Fix: `LineText.lineBox` — a line holding a big number is at least the number's size tall (line-height 1, the
  prototype's `.kn`). One shared component: web and native alike. The trout table's "Keep 2" is a whole Text in
  `TYPE.screen` / `lead` (27 / 20.4 px boxes), already tall enough; the render test checks it too.
- Test: `regs/bigNumber.render.test.tsx` — every number in the display face on every frozen page's Keep tab and every
  sheet, and each base style, sits in a box ≥ size × 0.947; the near-miss (26 in 20.25) fails. Without the fix 17 fail.
- iOS simulator, Cultus Lake → Black crappie, dark, text at default / 1.79× / 2.14× (test-bed scenario `t884`): before
  (the 0.8.1 simulator build, 447e7dff) shows the "20" cut at all three; after (a simulator build of 7a13d8671) shows it
  whole at all three. Pictures: scratchpad t884/T884-{before,after}-ios-{large,accessibility-medium,accessibility-large}.png.
