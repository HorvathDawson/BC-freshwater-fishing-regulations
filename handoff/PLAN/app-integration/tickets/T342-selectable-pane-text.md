# T342 — Pane text copyable with the phone's own controls (remove the salmon-only Copy button)

Status: DONE in code (device check in the batch 5 emulator sitting) (e604eb5ae) 2026-11-04 (was: OPEN (owner 2026-10-31: "the copy we added isn't what we wanted at all. It shouldn't be a button in only the)
salmon regs. It should just make it so a user could use basic phone controls to copy the text that is on screen if
they wanted (on the panes, not the map)").

## Work
1. Remove the salmon rules' Copy button (`RegionRules.tsx`, `SALMON_WORDS.ruleCopy` / `ruleCopied` in core
   `salmon.ts`) and its tests. Keep the Updates-log "Copy log" action (a different purpose) unless the owner says.
2. Make reading text on every PANE selectable with the platform's own long-press → select → Copy (RN `<Text
   selectable>` on iOS/Android; web: no `user-select: none` on pane text): water page / regs card / salmon card /
   gauge + forecast panes / spot screen / menu info pages / glossary / licences. Prefer ONE shared text component
   (the app's `Text`/type tokens) that panes use, not a prop sprinkled per screen.
3. NOT selectable: the map, map labels/chips, buttons, tabs, and any text that is itself a tap target (a long-press
   there must keep its current meaning).
4. Check: selection doesn't break scrolling, swipe-back or the bottom sheet drag; TalkBack/VoiceOver unchanged.
   Test bed: a long-press on a regs line shows the system Copy menu (Android emulator + iOS sim).
Size: S–M.

## Done (2026-11-04)
One mechanism: ui-native `Text.tsx` (selectable inside a pane's `<Selectable>`: WaterFrame, Sheet, MenuPage, SpotScreen;
no opinion outside a pane) and `Pressable.tsx` (never under a tap target); the water's Map view opts out. All 111
components take both from there (`tools/selectable-text.test.ts`). The salmon Copy button, its words and `copyText.ts`
are gone; the Updates-log "Copy log" stays. Left: the emulator / simulator long-press → system Copy check.
