# T728 — A Flow / Level pick in the Conditions card does not change the Conditions map's layer

Status: IN PROGRESS (before closed testing)

Source: owner 2026-11-14, verbatim: "when you chang elvel/condition in the card it should not change cindition map layers too"

## Problem
On a water's Conditions view, the gauge card's Flow / Level toggle also set the Conditions map's Show (Flow / Level /
Temperature). Back on the map, it was painting whatever the card was last switched to.

## Cause
`shell/WaterModeHost.tsx` handed the card the map's own state: `parameter={flowParam}` and
`onParameter={setFlowParam}` (`useMapColouring`: `setLayerChoice(chooseShow(...))`, the map's Layers choice). The card
had no state of its own; its toggle wrote the map's. The coupling was deliberate (pinned by the
`ConditionsLayers.shell.test.tsx` case "Flow in the chart is Flow on the map").

## Fix
- `WaterConditions` (`WaterViews.tsx`) owns the card's quantity: one `useState`, read by the card (`ConditionsPanel`
  `parameter` / `onParameter`) and the watershed days alike. The map's Flow or Level comes in as `opening`: where the
  card starts, read only until the reader picks in the card.
- `WaterModeHost` no longer passes `setFlowParam` anywhere; the map's Show changes only from the map's Layers sheet.
- "Flow leads, Level auxiliary" untouched: the lead gauge is still chosen in Flow (`leadOver`), whatever the card shows.

## Not changed (question for the owner)
The other way round still holds: the map on Level opens the card on Level (the card starts on the map's layer). A
card pick is kept while the water's Conditions view is up (a part step keeps it) and starts again from the map's
layer when another water is opened.

## Acceptance
- `ConditionsLayers.shell.test.tsx` "THE CARD'S PICK IS THE CARD'S": map on Level, the card opens on Level; Flow in the
  card charts Flow; back on the map, Show is still Level and the map still asks level. Fails before the fix.
- Left (Claude): the emulator look on the next APK (the emulator was another lane's on 2026-11-14).
