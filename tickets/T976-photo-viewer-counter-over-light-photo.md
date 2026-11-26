# T976 — The photo viewer's counter can't be read over a light photo

Status: PARTIAL — a dim pill behind the counter, the same 0.45 ground colour as the buttons' disc (`photo-counter-pill` / `photo-counter-backing`); render test in `PhotoViewer.motion.test.tsx`. Code 8c5dfce63. A proposal: the owner looks at light + dark shots over a light and a dark photo (not taken yet). left (Claude): the light + dark shots.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T976 | The photo viewer's counter ("2 of 3") can't be read over a light photo: white text, no backing (the buttons have a disc) | PARTIAL — a dim pill built + render-tested (8c5dfce63), a proposal; left: owner's look at light + dark shots | 8c5dfce63 | lane L6 2026-11-20, T836 iPhone simulator shots |

Source: lane L6, 2026-11-20, from the T836 iPhone simulator shots (build 6fdb3d8db). Shot:
`docs/ios/shots/t836/T836-ios-6-next-light.jpg`. The seeded photo (a fish drawing on white) fills the top of the
screen, and the counter ("2 of 3"), white text with no backing, almost disappears into it. The ‹ › ✕ buttons have a
dim disc behind them and stay readable.

## Work
Give the counter the same kind of backing the buttons have (a dim pill or a soft shade at the top), as the phone photo
viewers do. A proposal for the owner, with light and dark shots over a light photo and a dark one.

## Acceptance
"N of M" readable over a white photo and over a dark one, iPhone and Android; a render test pins the backing.

Depends: T836. Size: S. Owner: yes (the look, a proposal).


State 2026-11-22 (stopped for credits): review fixes done (6 items, see PR #43); left: nothing from the review, device looks/shots still owed; branch fix/T970-ui-smalls.
