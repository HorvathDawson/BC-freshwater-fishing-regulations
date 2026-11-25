# T1095 — Feature request: record fish measurements on a saved location, several per location, editable on a revisit

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1095 | Feature request: fish measurements on a saved location, several per location, editable on a revisit (report `0a9136aec3e19ab9`) | OPEN — 0.10, pictures first (owner 2026-11-24) | | planning chat 2026-11-24 |

Source: in-app report `0a9136aec3e19ab9` (staging worker, kind "feature"), 2026-11-24 19:32Z, verbatim: "Add a field
for fish measurements in locations. Make it so you can add multiple per location and edit even when you revisit."
Read by `pipeline reports`, planning chat 2026-11-24 22:20 MST. Owner 2026-11-24 22:35 (multiple choice): sent by "A tester"; when:
"0.10, pictures first". Related: saved spots (T603, T674), T714 (share a saved spot).

## Design first (no building until the owner picks)
1. What a "measurement" is: length only, or length + weight + species + date + kept/released + a photo. A saved spot
   holds a list of these (a small catch log), each row editable and deletable at any later visit.
2. Where it lives: on the phone only, like saved spots today (the app has no accounts); it travels with the spot in
   T714's share file only if the owner says so (a catch log is more private than a pin).
3. Tie to the rules, which is what this app is for: a typed length could be read against the water's size limits on
   that date ("under the 30 cm minimum here"). Useful, but it is a legal-sounding statement: pictures and exact
   words go to the owner before any of it is built.
4. Pictures of the spot sheet with the list, light and dark, for the owner.

## Acceptance
The owner has picked the fields and the look; then a build ticket with its own tests.

Severity: feature. Release: 0.10, pictures first (owner 2026-11-24). Model: Sonnet for
the pictures, Opus if the size-limit reading is built. Size: M. Owner: yes (scope and look).
