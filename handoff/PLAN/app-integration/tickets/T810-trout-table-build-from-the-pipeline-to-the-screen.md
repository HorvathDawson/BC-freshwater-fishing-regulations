# T810 — Trout and char block as the new table, built from the Python pipeline to the screen (umbrella; the spec is written, the build waits for the owner's go)

Status: OPEN, NOT STARTED beyond its first two steps (T805 = step 1, T809 = step 2). Nothing here holds any other fix
or a release (owner 2026-11-15).

Source: owner 2026-11-16 15:18: "have another opus agent spec out what would be involved in transitioning the trout
portion of the app to the new table format. want it implemented clean from pythion up to front end. etc etc. use opus
agent". The spec: `handoff/PLAN/design/trout-table-build-spec-2026-11-16.md` (tickets A to L for the main chat, M to S
for the app chat; this ticket stands for all of them until each is filed with its own number when it starts).

## The design to build
The trout mockup page, round 11 as corrected by round 12 (owner 15:16 and 15:36): the round 8 look with no outline;
today's rule in the bigger text, the coming change smaller underneath; rows between rounds 8 and 9; a coming closure
says what closes without the reopening date, and the reopening date shows only while it is closed today; a coming
change that only loosens a rule reads "rule changes <date>" with no details; Adams River shows the stricter 80 cm on
the row. Design record: `handoff/PLAN/design/trout-adversarial-review-2026-11-16.md` (four attacks; the fourth says
"yes, with conditions").

## Owner, 2026-11-16 15:42 (dialog on the spec's one-page summary)
- How to start: "Start steps 1 and 2" (T805 data gaps; T809 the changed-answers tool).
- Possession: "Keep the page line (Recommended)" (the trout table speaks only where a trout's possession differs).
- Size, if the switch does not fit the first-tap limit: "Raise the limit, shrink later (Recommended)".
- Other fish (kokanee, whitefish and the rest) in the same look: NOT ANSWERED ("what? we want to migrate the new
  trout style into the current app not the other way around. "): asked again.
- Steps 3 to 7 (the date rule, the table written in Python, the permanent proof, the app's table and sheet, the one
  switch) wait for his go.

## Standing conditions (from the spec)
Rules resolved in Python, the app only lays out; one load path; no silent fallbacks; built behind the current block
and switched in ONE release that drops the old card data (the first tap has almost no room for both); the old path
deleted after; every changed answer shown to him first (T809).

## Owner, 2026-11-16 15:47 (the other-fish question, asked again)
"we should just have the trout table under the trout entry like it currently is with the better text like 2 or 4 with
expand arrow etc. " Reading (main chat's): the page keeps its present shape. The Trout and char entry stays where it
is, folded to its short line with the better words ("Up to 2", "Up to 4") and the expand arrow; the new table opens
under it. The other fish entries are NOT redrawn: trout only. Consequences for the spec: its ticket Q (other fish on
the same table) is dropped; its ticket S cannot delete the old card code outright (it stays for the other fish; only
its trout path goes); the folded line of ticket O is the entry's own row, not a new block. Round 12's design answers
(15:36) are also his: a coming closure without the reopening date; the reopening date only while closed today;
"rule changes <date>" for a change that only loosens.

## Owner, 2026-11-16 15:48 (wild and hatchery rows)
"i dont like the new wild and hatchewry option you sent we should jsut say hatchery only unless specifically wild has
a seperate quota to wild like 1 wild and 2 hatchery or something.... otherwise it is hatrchery only or unspecified (the
same for both no mentiuon needed)". Rule for the table: same rule for both origins = no mention; keepable only when
hatchery = one row per fish saying "hatchery only", no wild row under Release; wild shown apart only when wild fish
have a quota of their own. The spec's row model (one row per fish and origin) changes accordingly.
