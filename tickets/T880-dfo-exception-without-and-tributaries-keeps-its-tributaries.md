# T880 — a DFO exception printed without "and tributaries" leaves out that stream only; its tributaries keep the row

Status: OPEN (main chat). After production (owner 21:0x).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T880 | A DFO exception printed without "and tributaries" leaves out that stream only (Bulkley: Two Mile Creek; the Suskwa on the chinook row) (owner) | OPEN | main chat | owner 2026-11-18 ~20:59 |

Owner 2026-11-18 ~20:59, a shot of DFO's Region 6 table
(/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/8d922a2e-image.png), Bulkley River, "all
tributaries of the Bulkley River other than Morice River and tributaries, Suskwa River and tributaries, and Two Mile
Creek" | All | Aug 1 to Dec 31 | No natural bait allowed; verbatim: "Make sure this rule is correct. Like the with
tributaries part for each exception except the one".

## Found
`6:bulkley-river:all-tributaries-of-the-bulkley-riv~2` binds gnis:2195 tributaries only, excepting gnis:14918 (Morice),
gnis:2637 (Suskwa) and gnis:30327 (Two Mile Creek) each WHOLE: a `tributary_excludes` carve-out removes the stream and
everything above it (atlas/graph/tributaries.py). Right for the Morice and the Suskwa ("and tributaries"); wrong for
Two Mile Creek: its tributaries (Robinson Lake and the streams into it) lose the bait rule. The reach already has the
other carve-out, `walk_past` (the stream left out, its tributaries walked and kept; used by the synopsis rows), but the
review app's extent fix says only "whole" exceptions (`proposals.excludes_of` / `excepted_items`: anything else is "a
hand edit").

Owner's dialog picks, 2026-11-18 ~21:02:
- "Yes, follow the words": wherever DFO names an exception WITHOUT "and tributaries", only that stream is left out. So
  the chinook row `6:bulkley-river:all-tributaries-of-the-bulkley-riv` ("all tributaries of the Bulkley River other than
  the Suskwa River.") keeps the Suskwa's own tributaries too.
- "After production (Recommended)".

All five DFO places with excepted tributaries (2026-11-18): Bella Coola (not including Atnarko) ×2 (Atnarko), the
Tatshenshini's "all other tributaries" (Kwatini, Stanley, Goat Creeks), Bulkley ×2. Read each against its words.

## Fix
1. The review app's extent fix says, per excepted tributary, "and its tributaries" or "the stream alone"
   (`excludes_of` / `excepted_items` carry `walk_past`; the form a toggle per row; the preview binds it).
2. Saved marks (Claude for the owner, his words above): Two Mile Creek walk_past on ~2; the Suskwa walk_past on the
   chinook row; the other three places read and kept or changed by their words.
3. Binding diff on the next data release names exactly these sections.
