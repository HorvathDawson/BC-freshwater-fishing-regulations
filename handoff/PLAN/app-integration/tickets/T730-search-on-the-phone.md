# T730 — Search on the phone: swipe down to close, small recents, the map stays, a fresh search each time

Status: IN PROGRESS (in build 32 as first built; one behaviour changed after the owner's 2026-11-16 answer "we should fix anything that doesnt seem desirable": 5f8ca1bb6 on branch `app-fix/small-looks-1116`, merged onto `redesign/stream-sections` 2026-11-16 (265fae08a; benches 572a6957a; Android back test fe89a5d47); left (Claude): the emulator look)

Source: owner 2026-11-14, from using the app on a phone, verbatim:
1. "When you open search on app you can't swipe down to close it also recent searchs take up a huge amount."
2. "If you are in search and type something like thompson Then click on thompson river it moves to show it but then if you click back it goes back to original location when it feels like it should have stayed at the location it moved to Incase the user was just trying to get there. Then after clicking back the search closes but if you highlight search again it goes back to where you were highlighting it didn't reset search state"

## The four behaviours (the verbatim words win)
a. Search closes by a swipe down, as every other bottom sheet in the app does. The keyboard and the list's own scroll
   still work: a swipe down closes only from the grabber, or from the list when it is at its top.
b. Recent searches take far less room.
c. A result tapped (Thompson River) moves the map to it; Back (or any close) leaves the map THERE, not where it was
   before search. A water opened from a result already comes back framed on that water (T717 keeps the browse map
   framed under the page).
d. After that Back, search is closed AND reset: focusing the field again opens an empty search (the recents), not the
   old query with its highlighted row.

## Causes
a. `app/packages/ui-native/src/SearchSheet.tsx` (the phone sheet) had no drag at all: its grabber was a picture
   (`<View>` at the old line 142) and the list a plain `FlatList`; only the field's ‹ and the system back closed it.
b. `app/packages/core/src/recentSearches.ts:10` kept 6, and `search/rows.tsx` drew each as its own 48 px full-width row
   (`search/SheetRow.tsx` `recent`, old line 68) under a heading row and below the long hint: about 340 px of a sheet.
c. `app/packages/ui-native/src/shell/useShellSearch.ts` (old lines 41-75): T48's "search gives the camera back" kept the
   camera from when search opened and, on every close after a pick had moved the map, sent it back
   (`restore`, drawn as a `fit.camera` by `shell/BrowseHost.tsx` old line 216).
d. The same hook held the query and the pick (`searchMemory`) across a close: only a water opened from a result cleared
   it (`openSearched`), so ‹ / back / Escape kept "thompson" and the selected row for the next open.

## Fix
a. The phone sheet takes the app's sheet drag (`useSheetDrag`, the same rule as `Sheet`: past a third of its height or a
   downward flick closes, otherwise it springs back): an RNGH pan on the grabber, and one on the list armed only when the
   list is at its top (its scroll runs beside it, `simultaneousWith`). The close is the sheet's `onClose` (the same close
   as ‹). The wide panel has none (no bottom edge; its ✕, Escape and a click beside it stay).
b. PROPOSAL for the owner to look at (screens on the next APK): the recents are ONE compact block right under the start
   line and before the longer hint: "Recent searches · Clear" on one line, then the searches as small square tags (the
   one button's `small` kind: card ground, hairline, accent words, a magnifier), wrapping. At most 4 are kept (an older
   one is dropped, never hidden behind a "more"). Why: tags of short words fit 4 on one or two lines (about 80 px where
   6 rows took about 340), they are the app's existing small control (no new look), square, inside the sheet's card, and
   the most likely tap is the first thing under the field. Alternatives if the owner prefers: 3 compact 36 px rows, or
   the tags on one scrolling line.
c. Closing search sends the camera nowhere: `restore` is gone (hook and BrowseHost). Every way out of search keeps the
   map where it is (one load path): ‹, back, Escape, swipe, a tab, the wide panel's ✕ or a click beside it.
d. Every close resets search (`setSearchMemory(NO_SEARCH)` when `searching` turns false). A water opened by a tap on
   the map beside the half sheet does not close search (it stays under the water), so its Back still returns to the
   results as they were. One rule for every layout: on a wide screen too, Escape / the panel's ✕ / a click beside it
   forget the query (the T69 focused-but-closed field keeps the focus, but Enter there reopens an EMPTY search). Say so
   to the owner with the screens; if desktop should keep its query, that is a one-line layout exception.

## Acceptance
- `app/packages/ui-native/src/Search.close.shell.test.tsx` (each fails before the fix):
  a swipe down on the grabber closes search; on the list at its top it closes, the list scrolled down it does not; a
  slow short drag springs back; the recents are one block of at most 4 tags with Clear; a water row tapped then back
  leaves the map framed on it (no camera sent back); reopened, the field is empty and the recents show (‹, back,
  swipe and Escape alike).
- `Search.shell.test.tsx` updated where it pinned the old T48 behaviour (camera given back; query kept after back,
  Escape, ✕ and a click beside the wide panel).
- Not this ticket: `apps/mobile/src/back.test.tsx` has 2 red tests at T717's commit 6c4547b13 ("Found multiple elements
  with the text of: tap the map": the browse map kept under the page is a second map); they fail the same with T730's
  files reverted.
- Left (Claude), emulator on the next APK: the swipe from the grabber and from the list (keyboard up and down, list
  scrolled and not, full and half), the recents block in light / dark, Thompson River framed then back stays there,
  reopen is empty.

## Changed after the owner's answer, 2026-11-16 (app chat)
Owner (question dialog in the main chat, 2026-11-16 12:08, on the list of small behaviours left as built): "we should fix anything that doesnt seem desirable". This one was left as built with an alternative on offer; the app chat judged it undesirable.
Now, on branch `app-fix/small-looks-1116` (5f8ca1bb6, not merged): Back from a place picked in search closes search too (one Back returns to the map); the on-screen "All results" row still steps back to the results. Each fix has a test that failed first;
`check:fast` passes. Left: the emulator look, then the merge with its benches.

## Emulator look 2026-11-20 (L4, `small-looks`, build of 124d79efa)
Shots in `handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T730/`. A swipe down from the grabber closes the search sheet (mid-drag it follows the finger; closed after), light
and dark. The recents are one block, 208 px tall, with "Recent searches / CLEAR" and square tags (a second tag after Thompson River;
they wrap). Thompson River picked, then Back: the map stays framed on the river (`after-back-map-stays-on-thompson.png`), and the next
open is an empty field with the recents. Not seen: the drag from the list scrolled and not scrolled, the keyboard down, the wide
layout (the Gboard handwriting popup sat over the left of the sheet in every shot: an emulator artefact). Found: T972.
