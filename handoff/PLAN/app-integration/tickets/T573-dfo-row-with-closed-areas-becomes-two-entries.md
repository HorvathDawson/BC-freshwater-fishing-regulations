# T573 — A DFO row that names closed areas inside its words becomes two entries (the Thompson)

Status: IN REVIEW — owner looks (the review side is built as a proposal; the build side below waits on him)

Owner 2026-11-10, on the Thompson (T572): "we need a way to represent this … it is like it should become 2 entries?"

## The shape
- ENTRY A, the open water: DFO's rules for the place, on the whole stretch its words name (Kamloops Lake outlet →
  the signs just below Gold Pan Provincial Park, 92.1 km).
- ENTRY B, the closed areas: one rule, "No fishing, year-round", on three stretches, each with DFO's own words:
  Deadman 3,616 m · Juniper 4,000 m · Ashcroft 2,342 m.
- NO NEW PRECEDENCE: the salmon chain is "narrowest first" (`deliver.salmon.chain_order`: tier, then the smaller
  extent). B is narrower than A, so B wins on its own stretches by the rule that already exists.
- Six cuts are not built: five new for B (Deadman upper signs, Juniper upper signs, Juniper lower signs, the
  Bonaparte's mouth, the Hwy 97C bridge) and A's Gold Pan signs, which the owner's own save proposed.

## What is built (2026-11-10), all of it a proposal
- `pipeline/regs/dfo_salmon/closures.py`: derives entry B from a stored row's exclusion list (`source_text.excludes`)
  — `python -m pipeline.regs.dfo_salmon.closures` writes `data/generated/review/dfo_closure_entries.json`. No fetch,
  no feed, no places file written; no existing place id changes (tests: `pipeline/tests/test_dfo_closures.py`, on
  the stored rows). Derived only where the words say no fishing / closed AND the list names stretches ("Deadman -
  from … to …"): the two Thompson wordings (the older one says only "except:" and takes it from the live wording's
  identical list).
- The review app: the Thompson place shows "This DFO row is two entries": A (the place itself, its proposal the
  whole stretch) and B (the rule, each area with its length, its two ends and DFO's words); the map draws B's
  stretches in red ("closed year-round (entry B, a proposal)"). `dfo.closure_view`; B's placement is in
  `dfo_fix_proposals.json` under `<place id>#closed`.
- The earlier four-open-stretches proposal (T572) is replaced by A = the whole stretch.

## NOT built — the build side, waiting on the owner
1. He accepts the six cuts (saving entry A's extent proposes Gold Pan; entry B's five need a place to be saved on).
2. Entry B becomes a location in `data/curated/dfo_salmon/places/region-3.json` (id `…#closed`, same water, its
   three pieces as its extent) — `dfo_salmon/entries.py` must seed it from `closures.derive` so a re-scrape keeps it.
3. Its rule ("No fishing, year-round": all salmon, take 0) must be in the rules the bundle and the live salmon feed
   read for that location (`dfo_salmon/typed.py` / `feed.py`; the feed is served by the worker — the other chat's
   code: no app/ or worker file was touched here, and whether one must change is not verified on a built bundle).
4. Then the atlas rebuild for the six cuts, the reach step and the bundle. Not started. The rebuild is the usual one
   run through `scripts/heavy.sh` (see the release runbook); it waits on step 1.
- Babine Lake (two wordings): "excluding … those waters within a 400 m radius of … Morrison Creek, Six Mile Creek" is a
  radius closure around tributary mouths, not named stretches: not derived; it needs its own decision.
- In the review app entry B cannot be SAVED yet (it is not a place); the owner reviews it on entry A's screen.
