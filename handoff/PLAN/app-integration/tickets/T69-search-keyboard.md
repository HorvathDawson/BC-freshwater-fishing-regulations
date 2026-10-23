# T69 — Search on desktop: keyboard leftovers

Status: DONE — ef941f1f. Wide web: Escape / the panel's ✕ hand the focus back to the field (focused but closed, accent edge; typing, Enter, ↓ or a fresh focus reopens); map click, back and a tab change leave the focus where they put it; a phone still blurs (searchFocus.web/native). Enter no longer blurs the field on a wide screen. Top bar now precedes the sheet in the page (zIndex keeps it on top). Pinned by the 4 "(T69)" tests in Search.shell.test.tsx, 7 mutations killed. Not done: a mouse CLICK on an already-focused-but-closed field does not reopen (RN TextInput has no click prop; typing does) — follow-up if the owner wants it.
- Focus after closing search goes to the page body. Needs a "focused but closed" state (focusing the field re-opens).
- Tab order runs against what you see: the results panel is drawn before the top bar.
Acceptance: keyboard-only walk test (open, type, pick, close, Esc) with focus asserted at each step.
