# T915 — Day calendar: no size jump in January; a desktop-shaped popup on wide web

Status: IN PROGRESS (main chat; lane app/t915-calendar; 0.8.3 batch).

Owner 2026-11-19 ~13:00 (main chat), verbatim: "calendar still jumpy in size from january to other dates because of
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
