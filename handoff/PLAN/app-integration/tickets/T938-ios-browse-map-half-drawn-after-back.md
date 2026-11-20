# T938 — iPhone: after search → water → Back, the browse map draws only its top part

Status: OPEN.

Source: found 2026-11-20 by the main chat's T929/T928 lane on the iOS simulator (iPhone 17e, 390 pt), while trying to
reproduce T928. Reproduces every time, on the 0.8.3 build (1f989bb48) and on lane/t929-t928.
Shot: lane/t929-t928 `handoff/PLAN/app-integration/tickets/T928-shots/ios-browse-map-part-drawn-after-search-water-back.png`.

## What happens
Search a water, open its page, press Back: the browse map draws only its top ~60%; below it is flat grey. It stays so
through pans and a tab change. No backgrounding needed.

## Likely cause (lane's reading, unproven)
The browse map is resized while hidden under the water page (`opacity: 0`) and its drawing surface does not follow the new
frame. May or may not be the tester's T928 case (theirs was shifted sideways, the day bar too).

## Work
Prove the cause on the simulator; make the map's surface follow its frame when it is shown again (or never resize it while
hidden). A test where one can pin it; before/after simulator shots to the owner. Check Android (emulator) for the same walk.

## Acceptance
Search → water → Back draws the whole browse map on iPhone (and Android stays right), light and dark.
