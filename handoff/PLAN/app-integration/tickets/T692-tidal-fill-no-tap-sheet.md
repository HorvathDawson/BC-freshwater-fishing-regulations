# T692 — The tidal fill opens nothing when tapped; a tidal page is the tidal card alone

Status: DONE — c4f101908 (left (Claude): a look at the next emulator APK).

Source: owner 2026-11-12, by dialog. Fill: "Keep fill, no tap sheet". The 16 wholly tidal waters: "Keep in search, tidal card only",
with the stray "SALMON (DFO) Closed" line gone. RULINGS P52.

## Built
- The `tidal_water` fill stays drawn and labelled "Tidal water". A tap on it does nothing: removed on web and native,
  not hidden: `TIDAL_LAYERS`, `anyTidal`, `MapRenderer.pressTidal` (both adapters), the `onPressTidal` prop and its
  hover-cursor flag, `BrowseHost`/`Shell`/`MapScreen` wiring, the `TidalSheet` component and its lazy part, core
  `tidalSheet` / `TidalSheetVM` and the sheet title "Tidal water — DFO rules". The legend row no longer says "Tap it to read more".
  `TIDAL_SHEET_WORDS.body` stays: it is the tidal banner's and the T688 map bar's paragraph.
- Why the page said "SALMON (DFO) Closed": the water screen asked DFO's salmon for every page, and a tidal page has no
  section DFO lists, so the salmon card answered "closed" (the card for a water DFO does not list) and drew its bar and
  row over the tidal card. The cause is in the app, not in pipeline data (`WaterScreen` now asks no salmon while the
  page's status is tidal; no salmon bar, card, glance row or pending face). Nothing for the pipeline lane.
- Stays: the T688 Map view of a wholly tidal water (bar "Tidal water", the card's first paragraph, "Show regulations for
  this water"); search lists the 16.

## Tests (failed before)
- `app/packages/map/src/tidal-fill-inert.test.ts`: the fill is still in the style; no tidal layer list, no `pressTidal` on either renderer; no `tidalSheet`; the legend does not promise a tap.
- `app/packages/ui-native/src/WaterScreen.tidal.test.tsx`: a tidal page is the banner alone, no salmon asked.
- Removed: the web/native tidal parity case and `TidalSheet.test.tsx`; `back-layers.test.ts` lost the sheet's row.

## Left
- A tidal page that is a PART of a listed water (the Fraser's tidal reach) takes the same rule (status tidal): Claude's reading.
- The stale "tap target" wording in `app/packages/map/style/layers.source.json` ($comment of the tidal layer) is
  left for the style lane (it sits beside the themes work).

Depends: T688. Size: S.
