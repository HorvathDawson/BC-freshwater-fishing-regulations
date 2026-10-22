# T45 — a desktop layout for the Regulations view (web app)
Phase 4 · Depends on T44 (the shared frame), T23/T29 (header, pin bar), T34/T35, T37 (sheets) · Size 1–2 days

## Owner (2026-10-21)
"stuff like the regulation tab could have a desktop size mode too so that it looks better on the webapp. (obviously we
are mobile focused but we want to make a decent desktop experience too.)"

## Goal
Mobile stays the design: phone-width is unchanged and stays golden-parity with the prototype. On wide screens (web,
breakpoints from theme tokens: e.g. ≥ 900 px and ≥ 1280 px) the Regulations view uses the room instead of stretching one
phone column across the page:
- the reading column has a comfortable max line length (about 70 characters of body text), centred in the frame (T44);
- at ≥ 1280 px, a side column for what you reach for while reading: the tabs as a sticky section list, the pin bar's
  pinned lines and the day control, so the main column is just the page;
- sheets open as a side panel or a centred dialog sized to their content, not a phone-height bottom sheet stretched wide.
  Back and Escape close them (G1).

## Work
- One responsive hook (`useLayoutClass(): "phone" | "tablet" | "desktop"`, ui, from window size and tokens). Components
  take a layout class and never read the window themselves.
- Golden parity runs at phone width as today. Add a desktop render test that the page draws the same words in the same
  order (only the arrangement differs).
- Web shots at 1440x900 and 1024x768 for vedder_1010, klwa_1010 and bc1_501, each tab and one sheet, saved to
  `T45-shots/`. Show the owner before merging.

## Acceptance
- Phone width: zero diff against the current golden and render tests.
- Desktop: the body line length stays at or under 80 characters, no horizontal scroll, the pin bar still works, and the
  tab jump scrolls the main column.
- Sheets on desktop open beside or over the page, sized to their content.
