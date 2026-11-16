# T809 — A tool that lists every answer that changed between two releases, in plain words, as one page for the owner

Status: OPEN (pipeline tool; started 2026-11-16 on the owner's word).

Source: the trout table build spec, `handoff/PLAN/design/trout-table-build-spec-2026-11-16.md`, section 5 ticket K and
section 7; owner 2026-11-16 15:42, asked how to start: "Start steps 1 and 2" (step 2 = this tool). Today the
changed-answers list he asks for before every shipment is put together by hand.

## What to build
- `pipeline/tools/audit/answers_diff.py`: compare two releases (or two local builds) per water and day range, for
  every block of the page (trout and other fish rows, salmon, closures, gear, licences), and write the differences in
  plain words, grouped by kind of change (closes / opens / limit lower / limit higher / size / wild or hatchery /
  possession / gear / wording only), with counts, water names and one example day each.
- Output: a text list for the main chat and one page for the owner (plain words, no ids in the visible text; ids in a
  quiet line). An identical pair gives an empty list.
- It reads built releases only (the bundle is the only source); no rule logic of its own beyond comparing answers.

## Tests
- A built pair with a known planted difference is reported exactly once, in the right group.
- An identical pair is empty.
- Mutation: a difference hidden in a day range that neither release starts a range on is still found.

## Pass
The tool run on the last two real releases gives a list the main chat can check against what was told to the owner
for that release; the T805 fixes are then shown to him with it before they ship.
