# T427 — The Conditions legend's watershed forecast: leaner, fewer lines

Status: DONE 2026-11-05 (owner looks at the shots) — was: OPEN — owner 2026-11-05 (screenshot, Conditions map bottom bar): "make this the watershed forecast a bit leaner.
basically it uses more lines then what we need we can combine some of them together etc just to make it a bit less thick."

## Today (7 lines + a bordered card)
1. "Flow vs. past years, this date" · "31 reaches coloured"
2. the histogram over 3. the Low → High ramp, 4. the "Normal" bracket, 5. "Low · Normal · High"
6. the card: "Chilliwack River watershed" / 7. "Flow vs. past years, by day" (says the header's words again)
8. the day strip (Now, Fri 6, …) / 9. "Now: Much below normal"
Code: `ui-native/src/BasinDays.tsx` (the card), `shell/MapLegend.tsx` (the bar), words in `core/src/basinDays.ts` +
`core/src/conditions.ts`.

## Proposal (cut to about 4 rows; nothing the reader needs is lost)
- **Drop the repeated subtitle** "Flow vs. past years, by day": the bar's header already says what the colour means.
- **One title row for the card:** "Chilliwack River watershed" left, the chosen day's verdict right ("Now · much below
  normal") — removes the separate verdict line; the verdict stays in the day cell's accessible name.
- **Labels on the ramp row:** "Low" and "High" at the ramp's ends (inside or flush beside it), "Normal" on its bracket;
  no separate label line.
- **The card loses its own border + padding** inside the bar (the bar is already a card; T367: one card per thing):
  a rule line between the provincial key and the watershed rows instead.
- Day cells: keep the strip one tap per day, the same height; selected cell as today.
- Check the histogram still reads (it may sit closer on the ramp).
Owner reviews the shot; follow `map-ui-layout-rule` (square corners, cards not loose text, no big gaps).

## Acceptance
Same information in about 4 rows at 390 wide (before/after shots, light + dark, 390 × 844 and 1440 × 900); tests for the
merged title/verdict row and the a11y names; nothing else on the Conditions bar moves.

Links: T417 (watershed conditions follow the watershed on screen), T367 (layout rules). Size: S. Owner: looks at the shots.

## Outcome (2026-11-05)
- Strip card (bottom bar): one title row = watershed left, the day shown right ("Now · normal"; core `BASIN_DAYS_WORDS.verdict`,
  `BasinDaysVM.verdict`, null when Change lights no day); the subtitle and the separate verdict line are gone; no border/padding,
  a `line` rule from the key above. Rows layout (a water's view) is unchanged (keeps its subtitle, still a card).
- Ramp (`Chrome.tsx` `LegendRamp`, also the Change legend): Low / High flank the bar on its row; "Normal" sits inside its bracket.
  The label line is gone (24 px to a 16 px bracket row). Histogram still stands on the bar.
- Day cells and their a11y names unchanged. Tests: BasinDays.test (merged row, no subtitle, no card border, no verdict on Change),
  LegendRamp.test (new text order), core basinDays.test (verdict). Shots (after only, 390 x 844, light + dark):
  .playwright-mcp/T427-after-light-390.png, T427-after-dark-390.png. 1440 x 900 not taken.
