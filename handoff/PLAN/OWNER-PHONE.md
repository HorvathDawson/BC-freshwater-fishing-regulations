# Phone checks for 0.5.0 (T77)

(owner 2026-11-04: no separate phone walk — Play internal and Closed testing cover these; the list stays as what to
watch for in testers' reports.)

What only your phone can judge. Install the 0.5.0 build from Play internal testing (or a dev build: phone in developer
mode, USB on, `pnpm -C app/apps/mobile android`). Say "fine" or what felt wrong for each line; each failure becomes a
ticket.

## New in 0.5.0
1. **Map fling (T312):** pan and fling the map at a few zooms. Does it glide far enough, and stop naturally?
2. **Copy text (T342):** long-press a line on a water's Regulations; the phone's own Copy should appear. A long-press on
   a button, a tab or the map should NOT select text.
3. **Whitefish (T344):** open a page that lists whitefish, tap it, swipe between Lake and Mountain whitefish. The swipe
   should not fight the sheet's drag or the edge-back gesture.
4. **Closed water:** Chilliwack River's closed part. "Closed all year on N stretches" folded; no "Where you can't fish";
   no Licence section or tab.
5. **Change (T380), after the feeds worker is redeployed:** Conditions → Change on a river with a forecast. "From the
   Nth percentile today to about the Mth" should read right.
6. **Forecast days (T375):** Conditions' day chips run every day, Now to +10; the forecast stays on over a weekend.
7. **Salmon card (T372):** a water with a DFO place that isn't on the map (Kitimat River). The "differs in some spots"
   fold lists it with DFO's words.

## Standing checks (from OWNER-TODO "Phone testing")
- Back: edge swipe and the back button through sheets, views and tabs; sideways view swipe on a water.
- Colouring: no water grey before its colour while panning; the five hatches; "At my location".
- Nothing under the status bar or the gesture bar; a font-size change keeps the open water.
- TalkBack walk (map → water → regs → conditions → salmon → menu); the largest system text.
