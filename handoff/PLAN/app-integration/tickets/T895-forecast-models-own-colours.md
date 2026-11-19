# T895 — Each flow forecast model draws in its own colour, so a toggled model can be told apart

Status: IN PROGRESS — left (Claude): owner sees pictures, then merge into app/integrate-082

Owner 2026-11-19 ~00:40 (app chat), verbatim: "also can we make the different models forecast have different colors so
when they are toggled on or off you can tell which are which."

Today the Conditions flow chart draws the forecast models (CLEVER · 10 d, COFFEE · 5 d, ELF · 30 d) in one style, with
dotted chips in the legend; turning one on or off does not show which line was which. Build: one token colour per model
(light, dark and the colour-blind palette; told apart by more than hue alone, e.g. dash pattern), the chart line and its
legend chip in the same colour, on the phone and the web. Pictures at 390 px light and dark, before it lands.

Same turn, verbatim: "we want all the in flight tickets in 0.8.2 and small bugs found".

## What was built (2026-11-19 ~00:58, lane app/t895-forecast-colours)

- **Before:** every model drew in the chart's one trace colour (`color.ui.live`); T377 handed out dashes by the ORDER
  drawn, so ELF's pattern changed when COFFEE was switched off, and the legend chips were all the same teal.
- **Tokens** (`app/packages/map/style/tokens.json` + `themes/*.json`, `pnpm style:build`): `color.ui.forecast.clever`,
  `.coffee`, `.elf` — light `#D9631A` / `#B04FC0` / `#5E5A00`, dark `#D75A09` / `#FF6FA8` / `#EEE070`, cvd `#DD6600` /
  `#AA66EE` / `#5C4A00`. `Palette.forecast` in `ui-native/src/theme.ts`; the models are `FORECAST_MODELS`
  (`CHANGE_POLICY.order`, @app/core `change.ts`).
- **Dash per MODEL** (`FORECAST_DASH`, @app/ui `chart.ts`): CLEVER long dash `7,4`, COFFEE dots `0.1,4`, ELF dash-dot
  `9,3.5,0.1,3.5`, round caps; a model with no token (one the publisher adds later) draws in the trace colour with
  `OTHER_MODEL_DASH`. Colour is never the only cue.
- **Hydrograph** (one component, phone and web): the chosen model's ribbon, trace and end dot, each other model's trace,
  and each legend switch (border + swatch, now 24 px with round caps) take the model's own colour and dash. Off, a switch
  is grey in its own dash. The measured line, the envelope, the median and the earlier years are unchanged.
- **Tests:** `app/tools/forecast-models-cvd.test.ts` (each model its own token and dash in light/dark/cvd; 3:1 on the
  card; ΔE2000 apart from each other and from live/sub/ink/accent, with a dichromacy floor, 17 in cvd);
  `ui-native/src/Hydrograph.models.test.tsx` (in all three palettes, switching COFFEE off removes its line and greys its
  chip while CLEVER and ELF keep their exact colour and dash).
- **Pictures** (Chilliwack River at Vedder Crossing 08MH001, the local feed file of 2026-10-28 + the LATEST release's
  climatology, the real Hydrograph in headless Chrome, 390 px, @2x copies too): scratchpad `t895-shots/`
  `t895-vedder-390-{light,dark}-{all-models,coffee-off}.png`.
- Not changed: the Change chart under the Change horizon still draws each model's own thin line in grey.
