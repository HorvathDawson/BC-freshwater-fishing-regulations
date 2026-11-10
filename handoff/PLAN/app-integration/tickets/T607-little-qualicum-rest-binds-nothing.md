# T607 — Little Qualicum "all waters unless indicated below" binds nothing: a whole-water sibling empties the rest

Status: IN REVIEW — built 2026-11-10 (the new extent kind, our proposal, the check-again flag); waits on the owner's own re-save of the Little Qualicum place, then a reach rerun

Source: the audit of the older DFO marks (T562 item 1, 2026-11-10); coordinator follow-up the same day.

## Today
`1:little-qualicum-river:all-waters-unless-indicated-below` (owner mark, verified 2026-11-03) holds the extent "the
rest of the water" (`op: rest`). The mark landed and the file holds exactly that — and it binds 0 sections.

## Cause (read from the data and the code; no variant was run)
A DFO "rest" is the water minus EVERY other placed place on the same water
(`pipeline/regs/dfo_salmon/entries.py`, the `rests` block: owner 2026-11-02, "the remaining parts without any other
extent applying"). The Little Qualicum has four live places:
- "all waters unless indicated below" — rest
- "from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence" — between two cuts
- "from the hatchery fence to signs located approximately 35 meters downstream" — between two cuts
- "all open portions of the Little Qualicum River" — WHOLE water (owner mark, verified 2026-11-03, 22.8 km)
The fourth is a sibling like the others, and it is the whole river, so the rest is the river minus the whole river:
nothing. Not a missing cut, not a water id mismatch (all four are `1:little-qualicum-river` on gnis:10886), and the
rest computation does what it was told. Stamp River's rest binds because none of its five placed siblings is the whole
water.

## Scope
Two DFO places use `rest`; this is the only one with a whole-water sibling, and the only "landed but binds nothing"
place the audit found among the 83 older marks.

## The decision (owner)
"All open portions of the river" is not a carve-out; it reads as "wherever the river is open". Either:
1. THE RULE: a rest does not subtract a sibling whose extent is the whole water (a whole-water place is not a named
   area). In `entries.py` (the DFO salmon lane, not the review app), with a test; then the reach rerun.
2. THE DATA: "all open portions" takes the same extent as the rest (`rest`), by the owner's own save in the review
   app — no code. Then both bind the river minus the two named stretches.
Either way the two named stretches keep their own rules; which rule applies on the remainder when both general rows
bind it is the chain's existing "narrowest first" order.

## Not done
No mark and no curated file was changed. The fix is outside the review-app lane's paths (1) or the owner's save (2),
and both need the deterministic reach step rerun (not an atlas rebuild).

Depends: owner. Size: S. Owner: yes.

## Owner 2026-11-10: "this is wrong (this has to apply to all already matched items instead)" — BUILT
A THIRD EXTENT KIND beside "between cuts" and "the rest": **all the other placed places on this water (no stretch of
its own)** — `op: other_places`.
- THE BUILD (`entry_models.Op.OTHER_PLACES`, `atlas/reach/build._build_others`, `dfo_salmon/entries.to_reach_input`):
  it binds the UNION of what every other placed place of its water binds (the rest included, each with its own
  tributary walk), it is NEVER subtracted from a rest, and it is never a sibling of another place of its kind.
  Resolved at build time from the placements of the day, like a rest: a place placed later joins without re-marking.
  AN UNPLACED SIBLING is not in the union; a placed one that does not bind adds nothing (counted in the `union`
  diagnostic); with none bound the place is unresolved — never guessed as the whole water. The review app's preview
  names the places it covers right now and the ones not included until placed.
- ON THE LITTLE QUALICUM TODAY (sandbox preview, real files): the whole river is 8 sections; the two named stretches
  bind 1 + 2; with "all open portions" as this kind the rest keeps the other 5 and "all open portions" binds all 8
  (rest 5 + stretches 3). Tests: `pipeline/tests/test_other_places_extent.py` (the builder; the Little Qualicum's
  shape from the stored rows; the preview case, where the file still holds the old whole-river extent).
- A SECTION WHERE A SIBLING SAYS CLOSED AND THIS SAYS OPEN: the kind adds no section a sibling does not hold, and the
  salmon chain is narrowest first (`deliver.salmon.chain_order`: tier, then FEWER SECTIONS, then the id). With two or
  more placed siblings the union is strictly larger than each of them, so every sibling sorts before it and a
  sibling's closure answers first (read from the code; not run on a built feed). ONE EDGE, REPORTED NOT GUESSED: with
  exactly ONE placed sibling the two cover the same sections, the tie falls to the location id, and the open-portions
  place could sort first. Neither matching place is in that state (the Little Qualicum has three siblings; the
  Qualicum's is unplaced). Closing it means a tie rule in `chain_order` (the deliver lane): this kind sorts after
  the places it is made of. Not changed here.
- KNOWN DIFFERENCE, small: inside the union a rest's named siblings are resolved with their own tributary flag (or the
  water's); in the rest place's own binding a sibling with no flag inherits the REST place's. Identical whenever no
  flag is set on those places (the Little Qualicum's case).
- THE REVIEW APP: the kind is an extent op like the others (the op list comes from `marks.OP_WORDS`), saved through
  the normal mark + write-through (test: `test_review_write_through.py::test_a_place_saved_as_all_the_other_places…`
  — the places file holds `[{"op": "other_places"}]`). OUR PROPOSAL (`dfo_fix_proposals.json`, proposals only) on
  every DFO wording that reads "open portions / open waters / all open": 2 places —
  `1:little-qualicum-river:all-open-portions-of-the-little-qu` (saved as the WHOLE river: flagged) and
  `1:qualicum-river:all-open-portions-of-the-qualicum` (no extent saved). No other place matches.
- CHECK AGAIN (owner: "can you flag it so it doesn't look done anymore?"): `data/curated/review/recheck.json`
  (`pipeline/regs/review/recheck.py`) lists the Little Qualicum place with his reason. A listed place is not done in
  the checklist, reads "Check again: <reason>", sorts with the to-do places and is not hidden with the done ones; its
  mark STAYS IN FORCE for the build until he saves again (said on the place). No mark was edited or appended: the
  entry names the mark it was made on and goes inert by itself once any other mark is in force for the place (tests:
  listed → asking; newer mark → inert). Seen in the sandbox: the state line, the proposal, the preview naming the
  three places (8 sections), and after a sandbox re-save the entry inert.
- Outside the review lane's usual paths, each clean before the edit: `pipeline/regs/parsing/entry_models.py` (the op
  and its validator), `pipeline/atlas/reach/build.py` (the union, beside the rest), `pipeline/regs/dfo_salmon/
  entries.py`, and one entry in `pipeline/deliver/export/guide_words.py` (the op's words: that list is checked against
  the enum).

## What waits
1. The owner re-saves "all open portions of the Little Qualicum River" as the new kind (the review app: "Preview
   this proposal", then save). Until then his 2026-11-03 whole-river save binds, and the rest binds nothing.
2. Then the reach step and the bundle (deterministic; ~4 min, ~30 GB: through `scripts/heavy.sh`, a human run):
   `PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.reach.cli --build data/generated/atlas/full --out <scratch>/reaches/full --against data/generated/reaches/full`
   then `… -m pipeline.deliver.bundle --build data/generated/atlas/full --reaches <scratch>/reaches/full --out <scratch>/bundle.sqlite`.
   NOT run. Not checked: the app / worker reading a place of the new kind from the bundle (it carries sections like
   any place; no code there knows the op by name as far as the search found).
3. The one-sibling tie in `chain_order` (above), before a place of this kind is saved on a water with one sibling.
