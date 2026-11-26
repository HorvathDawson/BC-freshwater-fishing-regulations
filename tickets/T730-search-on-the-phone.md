# T730 — Search on the phone: swipe down to close, small recents, the map stays, a fresh search each time

Status: IN PROGRESS (in build 32 as first built; one behaviour changed after the owner's 2026-11-16 answer "we should fix anything that doesnt seem desirable": 5f8ca1bb6 on branch `app-fix/small-looks-1116`, merged onto `redesign/stream-sections` 2026-11-16 (265fae08a; benches 572a6957a; Android back test fe89a5d47); left (Claude): the emulator look)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T730 | Search on the phone: swipe down to close, small recents, the map stays where search moved it, a fresh search each time | IN PROGRESS (in build 32 as first built; the 2026-11-16 change 5f8ca1bb6 merged onto redesign/stream-sections 2026-11-16 via `app-fix/small-looks-1116`; left (Claude): the emulator look) | b74512aee, 5f8ca1bb6 | owner 2026-11-14 ("you can't swipe down to close it also recent searchs take up a huge amount"; "it should have stayed at the location it moved to"; "it didn't reset search … (detail: ticket file) |

Source: owner 2026-11-14, from using the app on a phone, verbatim:
1. "When you open search on app you can't swipe down to close it also recent searchs take up a huge amount."
2. "If you are in search and type something like thompson Then click on thompson river it moves to show it but then if you click back it goes back to original location when it feels like it should have stayed at the location it moved to Incase the user was just trying to get there. Then after clicking back the search closes but if you highlight search again it goes back to where you were highlighting it didn't reset search state"

## What (the verbatim words win)
a. Search closes by a swipe down (from the grabber, or from the list when it is at its top). b. Recent searches take far
less room. c. A result tapped moves the map to it; Back (or any close) leaves the map THERE. d. After that Back, search
is closed AND reset: focusing the field again opens an empty search (the recents).

Done (b74512aee; render tests `app/packages/ui-native/src/Search.close.shell.test.tsx`, `Search.shell.test.tsx`):
a. `SearchSheet.tsx` takes `useSheetDrag` (RNGH pan on the grabber, and on the list armed only at its top,
`simultaneousWith`); the wide panel has none. b. PROPOSAL for the owner: recents = ONE compact block "Recent searches ·
Clear", at most 4 as small square tags (`core/src/recentSearches.ts`; alternatives: 3 compact 36 px rows, or the tags on
one scrolling line). c. `restore` is gone (`useShellSearch.ts`, `BrowseHost.tsx`): every way out keeps the map. d. Every
close resets search (`setSearchMemory(NO_SEARCH)`); on a wide screen too Escape / ✕ / a click beside it forget the query
— say so to the owner; if desktop should keep its query, that is a one-line layout exception.
Changed after the owner's 2026-11-16 12:08 answer "we should fix anything that doesnt seem desirable" (5f8ca1bb6, merged
2026-11-16): Back from a place picked in search closes search too (one Back returns to the map); the on-screen "All
results" row still steps back to the results.
Emulator look 2026-11-20 (L4, `small-looks`, build of 124d79efa; shots
`docs/app-integration/L4-android-shots-2026-11-20/T730/`): grabber swipe closes (light and dark); recents one
block, 208 px tall; Thompson River then Back stays framed (`after-back-map-stays-on-thompson.png`); next open empty.
Found: T972.

## Work (left, Claude)
Emulator on the next APK, the parts not seen 11-20: the drag from the list scrolled and not scrolled, the keyboard
down, the wide layout (the Gboard handwriting popup sat over the left of the sheet in every shot: an emulator artefact).
Not this ticket: `apps/mobile/src/back.test.tsx` had 2 red tests at T717's commit 6c4547b13 ("Found multiple elements
with the text of: tap the map").

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-14 ("you can't swipe down to close it also recent searchs take up a huge amount"; "it should have stayed at the location it moved to"; "it didn't reset search state"); code + render tests done; left (Claude): an emulator look on the next APK

## Owner

- [ ] 2026-11-20 — Look at the search sheet (`docs/app-integration/L4-android-shots-2026-11-20/T730/`): recents as one block of tags, swipe down closes.
