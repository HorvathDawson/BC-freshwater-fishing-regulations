# T57 — Unnamed streams: real-data sweep that their regulations are right (incl. tributary rules)

Status: DONE — e3dfef2b (lane wf_63a2801d-ce3-1); review fixes 26f678d0 (`stem_ends_at_lake` now explains only a lake draining into the bounded joining water itself — before, any lake on any Nass r2 tributary would have been laundered; live: all 70 are Meziadin Lake → Meziadin River, result unchanged) and c4f71b79 (the T57 strata vitest no longer depends on the Region tests running first). Trib-rule miss count 0 unexplained (150 frontier pairs, all
explained below); MU-group lines clean; every unnamed section has a page. Nothing must rerun. Left: the two
not-seen classes and the 3 "lake at reach top" sections below (recorded, no default change).

## Result (2026-10-22, bundle reach_digest dc21329d925d042d, handles d768d6e135e62b99, release r-2f33edfe8452)

**Pages.** `pipeline/tests/test_unnamed_trib_sweep.py::test_every_unnamed_section_has_a_page` (needs_bundle):
all 1,893,082 unnamed sections have a store key, every key has cells, and the store names them
("Unnamed stream/lake/wetland · Region N"). Whole-map count without a page = 0.

**Sampled pages == base + bound overrides.** `app/conformance/src/regs/unnamed.sample.release.test.ts` (T53 review)
now samples, per region, 3 override-bound + 3 base-only + up to 3 NEAR AN MU LINE (MU stamp of 2+ units; 13,890
such unnamed sections in B.C.) + up to 3 IN A LAKE'S CATCHMENT (bound by a lake row's walk; 142,646 sections), and a
region-line section takes its place name ("Regions 5 and 6"). 11/11 pass on r-2f33edfe8452.

**The "and tributaries" miss count** (`python -m pipeline.tools.unnamed_trib_sweep`, result committed as
`pipeline/tests/fixtures/t57_trib_sweep.json`, pinned by the slow live test). Independent of the walk's own
traversal: one flow step at a time, an unnamed stream draining (through lakes) into a section that carries a
walking rule — by the walk, or in the rule's own reach — must carry it. 476 walking rules, 1,179,681 unnamed stream
sections. FRONTIER (lowest unnamed section without the rule): **150 pairs; 0 unexplained; 0 sections lost.** Every
one is a stop the walk makes on purpose:
- `barrier` 29 — the unnamed piece is an FWA 2300 barrier (canal/connector): Nicomekl r2/r3/r4/r6 (4 each),
  North Alouette r2 (2), Skeena r1 (2, into Lakelse Lake), Black Creek, Oyster, Puntledge, Lillooet, Serpentine
  r1/r4, Kootenay lower r4, Peace r2/r3 (1 each).
- `stem_ends_at_lake` 70 — Nass r2: RU-14, the Meziadin stem is walked to Meziadin Lake; the lake's 70 inflows are
  the Meziadin row's, not the Nass's.
- `within_area` 43 — West Road tributaries (Region 6 row, `within_area` region 6) r1/r2/r3, 14 each, all in Region 5
  (incl. one 10 m piece straddling the 5/6 line whose HOME is Region 5 — `in_region`); Pitt r1 (Garibaldi Park) 1.
- `through_lake` 7 — the inflow is the river running through the reach lake (Twin Lakes r1 2; Duck Lake r1–r5).
- `strahler` 1 — Nass r1: a bigger river cannot be a tributary.

Mutation pins: the stub key-rule test (drop the binding → exactly that miss, with its 2-section cost) and the live
test (drop one real trib binding from the first eligible unnamed section → the sweep finds exactly it, unexplained).

**Reported, not counted as misses** (`unnamed_lake_at_reach_top`): an UNNAMED lake at the top of a walking rule's reach
stops the walk like a named one ("X downstream of Y Lake" does not take Y Lake's inflows). 3 sections: Bull River r1
(2; the r1 reach ends at the unnamed lake on the Bull — r2/r3 run on above it, so this is the cut) and St. Mary River
r3 (1; FWA draws the St. Mary into an order-1 lake that drains by another blue line, so the lake is not seen as
mid-reach — 1 section, an atlas artifact). No change made; see owner question below.

**MU lines** (`mu_groups` in the result; 11 rules `within` an MU group: Haida Gwaii's 9 + bait ban, South Island's
summer stream closure): bound outside the group 0, group members of the rule's kind unbound 0, members without the
group's MU stamp 0. 6 unnamed sections carry a 1-6/1-10 stamp pair: 3 lie wholly inside 1-1..1-6 (bound), 3 wholly
outside (geometry: 0 m inside, they meet the line at a point — the `mus` stamp is "touches"); not bound, correctly.
No MU-group straddler exists among unnamed sections, so "mark, don't cut" has nothing to mark there; region straddlers
take their home region (as above).

**Not seen by this check** (said, not hidden): a `tributaries_only` rule (64) binds no reach, so a stream joining its
reach DIRECTLY is not checked (one joining a bound tributary is); a miss on a NAMED tributary shows only through the
unnamed streams above it.

Pre-existing gate failures seen while running targeted gates (not T57's files, not touched):
`test_no_silent_skips` (skipif in test_synopsis_period.py:57/62/71) and `test_dataflow_gates::test_gate_4_one_calendar_spec`
(app/packages/core/src/regs/edge.test.ts:100, app/packages/core/src/dayPicker.ts:44).

## Owner questions
- Q-T57a: a NAMELESS lake at the top of a reach (Bull r1, St. Mary r3: 3 sections) stops the tributary walk exactly
  as a named lake does. Should a nameless headwater pond be climbed through instead? **Default (built with): no
  change** — the walk cannot tell "downstream of the lake" cuts from ponds, Bull r1 is a real cut, and the St. Mary
  case is one section of an FWA artifact.

Owner 2026-10-22: "making sure unnamed streams have proper regs etc (including the regs they get from being a trib
of something with rules that apply)".

## Work
- For a sample of unnamed sections in EVERY region (stratified: base only, with a trib rule, near a region/MU line,
  in a lake's catchment), the Regulations page lines == the region base + every override the reach run binds to
  that section. Read both sides from the bundle/store; never from a copy.
- Count unnamed sections an "and tributaries" / "incl. tribs" rule of a named water SHOULD reach but does not (the
  reach trib walk): by rule, by region, with examples. Each miss is either fixed (reach) or explained.
- Check MU-scoped rules on unnamed waters that cross an MU line (straddling: mark, don't cut).

## Acceptance
A needs_bundle test pins: 0 unnamed sections without a page; sampled pages equal base + bound overrides; the trib-rule
miss count is recorded and each remaining miss has a reason in the ticket.
