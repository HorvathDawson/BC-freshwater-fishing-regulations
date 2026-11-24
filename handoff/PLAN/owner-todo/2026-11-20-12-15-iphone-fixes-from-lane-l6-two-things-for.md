order: 420

## 2026-11-20 12:15: iPhone fixes from lane L6, two things for you (T723, T760; NOT asked yet)

- **The Save bar over the iPhone keyboard (T723).** On a spot's page, while you type a title or notes, a purple
  "Save spot" bar now sits on top of the keyboard (iPhone only; Android and the web are unchanged). The page's own Save
  row stays under the keyboard, as before. It is a proposal: keep it, or change its look? Default until you answer:
  keep. Pictures: `handoff/PLAN/ios/shots/t723/after-l6-spot-notes-bar.jpg` (the bar over the notes),
  `handoff/PLAN/ios/shots/t723/before-l6-spot-save-row.jpg` (before: Save hidden under the keyboard). Also fixed with
  it, no choice needed: the Conditions "Alert me" value field now sits above the number pad
  (`handoff/PLAN/ios/shots/t723/after-l6-cond-alert-value.jpg`), and the left-edge swipe closes the map's credits card.
- **Optional phone check: a first install on mobile data with "Wait for Wi-Fi" (T760).** Only a real iPhone can show
  it (the simulator is always on Wi-Fi). Before the fix, an iPhone on mobile data that chose "Wait for Wi-Fi" would
  restart the download twice and then fail it as "stuck" after about 30 minutes. Now it should wait quietly and start
  by itself on Wi-Fi. Check when TestFlight exists: install, mobile data on, Wi-Fi off, pick "Wait for Wi-Fi", leave it
  40 minutes, then turn Wi-Fi on. Default until you check: ship as built (Android already behaves this way).
- Still open from before: the oldest iPhone the app supports (T725, item (1) under "Open from the iOS day"); the memory
  numbers on the simulator are in T725.
- No action, for your eyes (T938, T928): the iPhone map left standing two-thirds to the right after a swipe back (the
  tester's picture) is reproduced on the simulator and fixed. Before:
  `handoff/PLAN/ios/shots/t938/before-fix-light-3-after-edge-back.jpg`; after:
  `handoff/PLAN/ios/shots/t938/after-fix-light-3-after-edge-back.jpg`. Also fixed: in the spot photo viewer a swipe to
  the next photo no longer hides the buttons (T836).
