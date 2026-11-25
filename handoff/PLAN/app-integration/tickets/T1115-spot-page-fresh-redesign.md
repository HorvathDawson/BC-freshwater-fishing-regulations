# T1115 — The saved-spot page: a full fresh redesign (it still feels clunky)

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1115 | The saved-spot page: a full fresh redesign (owner: still clunky); pictures first, with T1095 | OPEN — design in 0.10.1 (owner: "maybe in 0.9.2", the old name); build when the direction is picked | | owner 2026-11-25 |

Source: owner 2026-11-25 ~01:10 (planning chat), verbatim: "the spot page needs rework still it still feels very
clunky and I think we should have a full fresh look redesign maybe in 0.9.2". Related: T603, T674 (saved spots),
T714 (share a spot), T1095 (a tester asks for fish measurements on a spot: design the two together), T1093.

## Work (pictures first; nothing is built before the owner picks)
1. Ask the owner what feels clunky, with today's screens in front of him (the spot sheet, saving, editing, the
   alert form, photos, the list of spots, opening a spot from the map): a short multiple-choice batch, then his
   own words. Write his answers here.
2. Walk the page as a reader and list the friction with counts: taps to save a spot, to edit a note, to set an
   alert; what moves under the keyboard; what is hidden; what differs between Android, iPhone and web.
3. Two or three directions as pictures, light and dark, phone and web, from the app's tokens and components (never
   the old riffle prototype), each showing: the spot at a glance, edit in place, alerts, photos, and where a catch
   log (T1095) would sit so it is not bolted on later.
4. The owner picks; then a build ticket per part, each with its tests, and the emulator test bed (T603) updated.

## Acceptance
The owner has picked a direction from pictures; build tickets exist with frozen scope.

Severity: design work (the owner's judgement of the page). Release: design in 0.9.2 (owner: "maybe in 0.9.2"); the
build in 0.9.2 if small, else 0.10: decided when the direction is picked. Model: Sonnet for pictures and UI, Opus
if the spot store changes shape. Size: M–L. Owner: yes (what is clunky; the pick).

## Owner

- [ ] 2026-11-25 — The saved-spot page ("still feels very clunky"): say what feels clunky, then pick a design direction from pictures (0.10.1).
