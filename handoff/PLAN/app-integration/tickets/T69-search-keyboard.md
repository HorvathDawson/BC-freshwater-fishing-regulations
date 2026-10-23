# T69 — Search on desktop: keyboard leftovers

Status: DONE — ef941f1f, bd2aa353 (review). Wide web: Escape / the panel's ✕ hand the focus back to the field (focused but closed, accent edge; typing, Enter, ↓ or a fresh focus reopens); map click, back and a tab change leave the focus where they put it; a phone still blurs (searchFocus.web/native). Enter no longer blurs the field on a wide screen. Top bar now precedes the sheet in the page (zIndex keeps it on top). Pinned by the 4 "(T69)" tests in Search.shell.test.tsx, 7 mutations killed. Review bd2aa353: a mouse click on the focused-but-closed field now reopens too (web `onClick`, searchFocus.closedFieldClick); the wide-screen Enter-keeps-focus is web-only (an Android tablet >= 720 dp still drops its keyboard on Enter), pinned by TopBar.searchFocus.native.test.tsx. Nothing left.
- Focus after closing search goes to the page body. Needs a "focused but closed" state (focusing the field re-opens).
- Tab order runs against what you see: the results panel is drawn before the top bar.
Acceptance: keyboard-only walk test (open, type, pick, close, Esc) with focus asserted at each step.
