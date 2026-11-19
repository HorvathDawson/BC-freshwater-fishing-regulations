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
