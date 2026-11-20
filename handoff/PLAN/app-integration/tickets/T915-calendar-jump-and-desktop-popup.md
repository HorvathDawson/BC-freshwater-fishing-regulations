# T915 — Day calendar: no size jump in January; a desktop-shaped popup on wide web

Status: DONE — shipped in 0.8.3 (merged 1aeb9f3f2; staging + production 2026-11-19).

Owner 2026-11-19 ~12:45 (main chat), verbatim: "calendar still jumpy in size from january to other dates because of
"Only days in 2026 can be chosen: the regulations here are one calendar year." make calendar popup look better on
desktop (good for mobile but desktop doesnt like mobile popup)".

## Work
1. **No jump.** The calendar (`DayCalendar.tsx`, words `DAY_PICKER` in core `dayPicker.ts`) changes height when the
   month shown has greyed days with a reason (`outsideYear` in January, `afterUntil` near the cut): the note line
   appears and disappears. The calendar keeps one size whatever month is shown (e.g. the note's space always held, or
   the reason moved where it doesn't change the height); six week rows always, if months with five rows also change
   it. Phones and web.
2. **Desktop popup.** On wide web (the desktop layout class, `tabsAsRail(useLayoutClass())`, >= 1280 px), the calendar
   opens as a desktop popover anchored to the day button (card, square corners, the app's tokens), not the phone's
   bottom sheet. Phones and narrow web keep the sheet as it is. Web/native split by file (AGENTS 34a); keyboard and
   screen-reader behaviour kept (Escape closes, focus returns to the button).
3. Pictures: 390 light/dark (January vs November, same size) and 1440 light/dark (popover) before/after.

## Built (lane app/t915-calendar, 2026-11-19 13:03)
1. **One size.** `@app/core` `monthVM` gives `reasonRoom`: the longest reason any month of the range says. `DayCalendar`
   always draws that room unseen and unread (`aria-hidden`, opacity 0), and this month's reason over it (out of the flow),
   so the note coming and going no longer changes the height; it wraps as the real words would at any width and text
   size. Six week rows were already fixed (T877). Measured (Expo web, 390): January and November both 454 px (before:
   454 / 412). Phones and web alike (shared component).
2. **Desktop popover.** `regs/DaySheet` is now a platform pair (AGENTS 34a): `.native.tsx` = the bottom sheet as before;
   `.web.tsx` = the same sheet below the desktop class, and at `tabsAsRail` (>= 1280) a 344 px card hung 6 px under the
   button that opened it (left edges aligned, kept 16 px inside the window, flips above when there is no room below):
   square, 1.5 px ink rule, `palette.lift`, title + Done, no scrim (the map stays in view). Callers name their button
   (`anchor`: `map-day-pick`, `regs-date`). Escape (the Modal's), a click outside, Done and the browser's back
   (`useBackLayer`) close it; focus returns to the button; on open the keyboard is on the chosen day (arrow keys at
   once). A missing button is reported (`reportFailure`) and the card is still drawn.
3. Tests: `regs/DaySheet.test.tsx` (new), `DayCalendar.test.tsx`, `regs/SheetWide.test.tsx`, `core dayPicker.test.ts`,
   `tools/back-layers.test.ts` inventories. Pictures: scratchpad `t915-shots/T915-contact-sheet.png`.
## Owner 2026-11-19 ~13:20 (main chat, after the before/after pictures)
Asked "Put it in 0.8.3?", verbatim: "Yes, put in 0.8.3 (Recommended)". Merge app/t915-calendar (39763ceed) into the
0.8.3 integration.
