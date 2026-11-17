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

## The accepted design, as one list (owner, 2026-11-16, rounds 10 to 14; his words are in OWNER-TODO.md)
1. Look: the round 8 tinted keep box with white rows and white boxes for tighter limits; NO outline or frame line.
2. Text: the rule in force today is the bigger, bold text; the coming change is smaller and quieter on its own line
   underneath. Row spacing between rounds 8 and 9.
3. Beside a fish with no tighter limit: "up to the N" (the box's number).
4. Dates: a coming closure says what closes ("then closed to trout and char" / "to all fishing" / for one fish) and
   no reopening date; the reopening date shows only while it is closed today; a coming change that only loosens a
   rule reads "rule changes <date>" with no details; otherwise the next stricter change is dated. A box's top line
   carries a date only when every fish in it shares that date.
5. Adams River: the stricter 80 cm on the row, the one-day rule in the fish's sheet.
6. Wild and hatchery: same rule for both = no mention. Hatchery only = ONE row per fish with a filled "HATCHERY ONLY"
   label beside the name, and no separate wild row; when it applies to every fish of a keep box, the box's top line
   carries the same filled label once and the rows carry none. Wild is shown apart only where wild fish have a
   quota or size of their own (Kitimat, Lois, Khartoum, Ruby, Cultus). Lois / Khartoum steelhead: hatchery only.
7. Possession: the page keeps its own line; the table speaks only where a trout's possession differs.
8. Place: under the existing Trout and char entry (folded line with the better words and the expand arrow); other
   fish untouched.
9. Fish sheet: "Here, today", "How to tell it", "Sources"; swipe down to close; no sideways swipe; no year bar except
   an all-release table.
Still open in the design (from the fourth attack): a build rule for dated number / size / wild / possession changes
on the waters not hand-written in the mockup; Lois's "All together" line sitting outside both boxes.

## Owner, 2026-11-16 16:37: work on the transition
Typed (on the Nation Lakes possession question): "i feel like this possession stuff is just kind of broken but fixed in
the new trout format? so maybe we can just work on the transisiton to the new trout redisgn?" Read as his go to move on
to steps 3 to 7. First: the spec takes in his answers of 15:36, 15:42, 15:47, 15:48 (trout only, the folded entry with
the expand arrow, no "reopens" unless closed today, "rule changes <date>" for a loosening, hatchery only), and the app
chat's read-only check of the app half (tickets M to S against the real app code; its worker started ~16:30).

## Owner, 2026-11-16 16:50: robust, end to end, no fighting
"we want to make sure the plan is robust so that we change the data all the way in python and the front end and it all
just works withotu fighting it and makign sure everythign is streamlined etc". Plan rule: Python decides every word,
number and date; the app draws only (no rule logic in TS, payload types generated from `contract.py`); one trout path
after the switch. Gate before build code: a fresh Opus review of the whole spec (after the app half is merged) for
every place the two sides could fight; findings fixed in the spec first.

## Owner, about 16:55 (app chat's batch, labels picked verbatim)
Fold row: "All rows get arrow" (every folded row, other fish too, gets words plus the expand arrow). Pinned trout top
line: "Drop it (Recommended)". Lone other fish beside trout: "Stays as today (Recommended)". Year bar's "Trout: release
until …" line: "Drop it (Recommended)". The app chat's sizing file section 12 lists every place today's trout card
decides in TypeScript (49 in core, 10 hand-written payload types, 12 facts with two sources, 11 old/new paths): input
for the robustness review (owner 16:50).
Clarified about 17:15 (app chat re-asked): "Salmon + trout folds": the arrow replaces "Details" on the salmon and trout
folded entries; the Other fish list stays as it is.
