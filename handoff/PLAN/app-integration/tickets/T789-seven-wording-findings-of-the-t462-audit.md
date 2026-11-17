# T789 — Seven wording findings of the T462 audit have no ticket (the owner's gate 1 for closed testing: each fixed or ticketed with his OK)

Status: IN PROGRESS (K6, K7 done; G9, D6 stopped as pipeline work; K8, G8, G10 wait for the owner) (app core wording; before closed testing; the app chat's area for the fixes).

Source: general audit 2026-11-16, decisions checker item 7 (`handoff/PLAN/audit-2026-11-16/decisions.md`); T462 is DONE, T495 gate 1.

- K6: the Skeena weekend caveat is cut at the colon (`app/packages/core/src/regs/caveats.ts` line 193).
- K7: "Open, except some parts" when nothing is closed (`core/src/regs/strip.ts` line 148).
- K8: the Shuswap pier exemption is dropped.
- G8: non-resident-alien weekend closures are not on the Licence tab.
- G9: fly-only waters say "One hook or fly". **FIXED ee9fb62ff** (lane/bugs-080, 2026-11-17): `gear.summary_parts` keeps the fly
  alone in both fly modes (p.80: an artificial fly is a dressed hook); every artificial-fly-only reading now says "one fly"
  (36 rules, 35 waters: Pennask, Lois, Big O.K., Campbell, Gordon, Nanaimo, Thompson below Kamloops Lake, West Road
  and its tributaries, ...), and the dated "changes" line of a water with such a period. Ships with the next data build.
- G10: the under-16 non-resident price line.
- D6: a Region 6 gear line is cut mid-word. **Traced, not landed** (lane/bugs-080, 2026-11-17): not the comma (the flag reads
  it); the quoted span `single,?\s+barbless\s+hook[\w\s,.]*` stops at the hyphen of "non-tidal". Fix:
  `(?:[\w\s,.]|-(?=\w))*` in BOTH twins, `pipeline/regs/dfo_salmon/typed.py` and `app/workers/feeds/src/dfo/typed.ts`
  `RE_HOOK_SPAN` (T556 parity), then `python -m pipeline.tools.emit.dfo_vectors` (the vectors go stale with the Python
  half alone) and the worker's `dfo.vectors.test.ts`; then `python -m pipeline.regs.dfo_salmon.feed` (no model) before
  the data build. Needs the app chat (app/workers). No re-parse.
Each changes words an angler reads. Fix or get the owner's OK to leave, one by one.

## Built (K6, K7, G9, D6)

- K6: already fixed by T800 (8b6bad68c, in this branch): the "at certain times" angler line now says the rule's own label, not `describeShort`'s cut at the first bracket; pinned by the T800 test in `caveats.test.ts`. No change here.
- K7: `strip.ts` counts an undrawn-part retention rule as "except some parts" only when it is a closure (`closedGate`); a release/limit gate leaves "Open". Test in `strip.test.ts` ("T789 K7"); mutation-checked (dropping the `closedGate` term fails it).
- G9: STOPPED, not app. The words "One hook or fly" come from the answers payload (`pipeline/deliver/answers/gear.py`) and the curated data (the audit says: curated data only); the app only draws them. Needs a Python/data change ("one fly" for artificial-fly-only waters, p.80); owner/pipeline lane.
- D6: STOPPED, not app. The Region 6 DFO gear line is cut in the pipeline's typing of the verbatim (`pipeline/regs/dfo_salmon/typed.py` ~175, the comma in "Single, barbless hook in tidal and non-tidal portions"); the audit's fix is "re-type the line", a pipeline change.
- K8, G8, G10: untouched, waiting for the owner.
