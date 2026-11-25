order: 130

## 2026-11-15 afternoon — owner done labelling for now; clean-up asked (T743)
Owner, verbatim: "i thought skeena used to have a hells gate cut? also other ones liek this dont have the confluence
cut? (from Lakelse River mouth upstream to the Skeena River Overpass (New Hwy Bridge) at Ferry Island) again this doesnt
split on confluence.. (from the confluence with the Babine River downstream to the CNR Railway Bridge at Terrace. ) ...
all the audit stuff i will have to review later like shuswap stuff etc etc. i think i am done labelling for now. can you
audit everything a lot compile all results/issues into questions for me later. and make sure to fix all the remaining
items left. a lot have issues with bad cuts etc etc. also we could rebuild the cuts and make sure nothing causes issues?
also want to make sure we deduplicate the new cuts from proposal (if they rewference same thing they should be matched
etc) we just really want to clean it all up and then have questions ready for me later."
Main chat's plan: land the prepared fixes; run the full audit; clean the cuts (duplicates matched to one cut, confluence
ends on the confluence cut, bad positions fixed where the evidence is clear, the rest as questions); side rebuild and
checks; one page of questions for him. Nothing that changes what a wording means is decided for him.
- TATSHENSHINI checked 2026-11-15 (report `handoff/PLAN/curation-review/tatshenshini-border-2026-11-15.md`). (A) The BC
  border points exist in the data (the river leaves BC and comes back after 43 km in Yukon) but the app's picker cannot
  show them; app fix proposed. The Kwatini / Stanley / Goat place does not need one: those creeks are wholly in BC, so
  its whole-water extent is already right. (B) The Blanchard place cannot be edited because its extent carries a field
  the editor cannot show, and the app hides the edit button without saying why; plain "whole" gives the same 15
  sections. YOURS: on each of the two places, tick ✗ on rows 2 and 4, then "Confirm: the extent matches DFO's words"
  (works today), or tell the main chat to save those two verified marks for you. 15 other unmarked places cannot be
  edited in the app for the same kind of reason; listed in the report.
- ASKED 2026-11-15 as he left: "record everything i need to do for when i am back what i need to review with questions
  what was done etc etc. what needs testings etc etc". Kept in `handoff/PLAN/OWNER-RETURN-2026-11-15.md` (sent to him
  as a file when final) and as the first row of the To-Do page.
- DONE 2026-11-15 (your permission): Bella Coola "all tributaries" = tributaries only, Atnarko excepted (line 2730);
  Bella Coola whole "(not including Atnarko River)" = Atnarko excepted (2731, per your note "make sure this excludes
  atnarko"); the two Bulkley "all tributaries" places (2732, 2733) = tributaries only. YOURS: the two Bulkley places had
  no mark of yours, so they are saved as "unsure" from the proposal: confirm or change.
- BLOCKED 2026-11-15, NEEDS YOU: the side map rebuild did not start. The permission check refused its first step
  (`python -m pipeline.regs.review.proposals side … --out data/generated/side/dfo-cuts23`) twice for the rebuild job; the
  main chat does not run a command a job was refused. Run it yourself or tell the main chat to. Details in
  `handoff/PLAN/OWNER-RETURN-2026-11-15.md` section 6.
- ANSWERED 2026-11-15, on the blocked side rebuild step: "You can run it". Run by the main chat; rebuild continuing.
- BLOCKED 2026-11-15 17:20, NEEDS YOU: Play build 33 (0.7.11, code shrinker on; 6 of 6 phone checks, both CIs green,
  merged 35dce9bf3) refused to start: fast-fail fails on two rows of the proposals file lacking split-hunt evidence
  (my midday merge). Regenerating was denied to a job by the permission check; the main chat will not run it without
  your word. Say "regenerate the two proposal rows" (or run the suggester yourself); then the build runs.
- ANSWERED 2026-11-15 18:40: "Yes regenerate proposals if you need to." Done by the main chat: a fresh suggester run
  into a scratch folder; the two rows the test named (Stave "downstream of B.C. Hydro dam" second wording, Bella Coola
  "all tributaries") have no row in a fresh run because both places are placed now, so those two rows were removed from
  the proposals file (backup taken; no other row changed). The test passes; Play build 33 started.
- ASKED 2026-11-15 18:40 about T759: "What u mean ? should the app say "not known" instead of showing the wider open
  rule? I recommend yes." Explained in chat with an example; answer still open.
