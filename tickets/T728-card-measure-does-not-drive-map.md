# T728 — A Flow / Level pick in the Conditions card does not change the Conditions map's layer

Status: IN PROGRESS (in build 32 as first built; one behaviour changed after the owner's 2026-11-16 answer "we should fix anything that doesnt seem desirable": 81a28e7c2 on branch `app-fix/small-looks-1116`, merged onto `redesign/stream-sections` 2026-11-16 (265fae08a; benches 572a6957a), in every app build since; left (Claude): the emulator look)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T728 | A Flow / Level pick in the Conditions card no longer changes the Conditions map's layer | IN PROGRESS (in build 32 as first built; the 2026-11-16 change 81a28e7c2 merged onto redesign/stream-sections 2026-11-16 via `app-fix/small-looks-1116`; left (Claude): the emulator look) | 6dd66973d, 81a28e7c2 | owner 2026-11-14 ("when you chang elvel/condition in the card it should not change cindition map layers too"); code + test done; left (Claude): an emulator look on the next APK |

Source: owner 2026-11-14, verbatim: "when you chang elvel/condition in the card it should not change cindition map layers too"

## What
On a water's Conditions view, the gauge card's Flow / Level toggle also set the Conditions map's Show (Flow / Level /
Temperature): `shell/WaterModeHost.tsx` handed the card the map's own state (`parameter={flowParam}`,
`onParameter={setFlowParam}`).

Done:
- 6dd66973d: `WaterConditions` (`WaterViews.tsx`) owns the card's quantity (one `useState`, read by `ConditionsPanel`
  and the watershed days); `WaterModeHost` no longer passes `setFlowParam`; the map's Show changes only from the map's
  Layers sheet. "Flow leads, Level auxiliary" untouched (`leadOver`).
- 81a28e7c2 (owner 2026-11-16, on the small behaviours left as built: "we should fix anything that doesnt seem
  desirable"): a water's Conditions card always opens on Flow (his 2026-11-10 ruling: Flow leads, Level auxiliary); a
  pick made in the card lasts while that water is open; a water with no flow reading says so and offers Level.
- Emulator look 2026-11-20 (L4, `small-looks`): Coquitlam River -> Conditions: the card opens on Flow; Level picked;
  back on the map nothing about the map's layer changed (shots
  `docs/app-integration/L4-android-shots-2026-11-20/T728/`). The Conditions-layer map itself (its Show choice)
  was not opened. Found: the estimate card ABOVE the gauge card also changes with the pick (headline Normal -> Below
  normal): T971.

## Acceptance
- `ConditionsLayers.shell.test.tsx` "THE CARD'S PICK IS THE CARD'S": map on Level, the card opens on Level; Flow in the
  card charts Flow; back on the map, Show is still Level and the map still asks level. Fails before the fix.
- Left (Claude): the emulator look on the next APK (the emulator was another lane's on 2026-11-14).
