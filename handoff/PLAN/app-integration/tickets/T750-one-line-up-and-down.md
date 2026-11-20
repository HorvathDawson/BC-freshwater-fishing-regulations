# T750 — Upstream and Downstream walk one line through every part; no branch picker

Status: IN REVIEW (app change e920d4d0d on `redesign/stream-sections` since 2026-11-14; in Play build 33 (0.7.11) and on the staging and production websites since; owner's order answer "upstream is fine" (f6bc9a028); left: the owner's look, owner: OWNER-TODO "the one-line Upstream / Downstream (T750)")

Source: owner 2026-11-14, staging website (dff2c7a22), Skeena River, Map tab, part 2 of 18 ("Skeena River Ecological
Reserve boundary (km 63.5) → Skeena River Ecological Reserve boundary near the Exchamsiks River confluence"). Pressing
Upstream opened "Which way upstream?" with two rows ("… near the Exchamsiks River confluence → Exchamsiks River
confluence" and "Skeena River"). Verbatim: "also this is bad navigating up the skeena it gives a branching option. we
should only have up or downastream that access all components. we just need to choose the most sensible next item. for
example here which ever has a lower downstream item maybe."

## Where the picker came from
`@app/core` `waterNav.stepsFrom` stepped by the release's `part_mark` graph (T460/T204): a mark with two upper parts, or
two marks saying they sit below one part, gave two candidates, and `@app/ui` `waterMapStep` opened a chooser for
more than one. On the Skeena, `part_mark` holds `2 → [3]` and `2 → [4]`: part_ix 3 (the main stem above the Reserve)
and part_ix 4 (the km-less side channel that leaves part 2 further up and rejoins at the Exchamsiks).
In release r-f749475d0962 (staging) 13 waters had such a fork at 14 parts: Skeena (2), Forsyth Creek, Mahood Creek,
Cottonwood River, Ash River, Wap Creek, Nimpkish River, Old Kootenay River Channel, Sustut River, West Road (Blackwater)
River, Klinaklini River, Kitsumkalum River, Wood River.

## The rule now (where it lives)
▲ = the next part in the water's group order, ▼ = the one before; that order is the release's `part_ix` (river order,
decided in Python at build time by `pipeline/deliver/bundle/topology.order_parts`: by km from the mouth, a km-less side
channel right after the main-stem part it runs beside, T710) — the same order "Show all N parts" lists. A split lake's
areas are walked in the group's exit order, as before. No new field and no release: the bundle already ships the total
order, the app only stopped second-guessing it with the marks.

Past the ends: ▼ at the lowest part steps into the water it goes on as (`water_continue`), ▲ at the top into the one it
goes on from — one item each, never a choice; "Flows into …" stays a link at the mouth. In r-f749475d0962 all 7 waters
with `water_continue` meet at the line's ends (Chilliwack/Vedder, Atnarko/Bella Coola, Page Creek/Matsqui Slough, Pass
Creek/Screw Creek); a junction anywhere else is now a release defect `waterGroup` throws (no silent fallback).

Checked against the owner's "lower downstream end first": at every fork above the order agrees (Skeena: the main stem's
lower end is mark 3, the side channel's mark 4 further up, so main stem then channel; Forsyth, Mahood, Sustut, Wood,
Kitsumkalum, Old Kootenay: the channel leaves the part below, so it comes first; West Road, Cottonwood: the braid beside
the stretch it leaves). Two waters differ, because `part_ix` ranks by a part's UPPER end: Wap Creek (part 2, the Mount
Griffin Reserve piece 27.8–28.1 km, sits inside part 3, 24.3–28.2 km) and Nimpkish River (the Ecological Reserve piece
sits inside "Davie River confluence → Source"). The current order puts the piece first; the strict rule would put the
enclosing part first. See owner question below.

## Built (app only)
- `packages/core/src/waterNav.ts`: `stepUp` / `stepDown` (one each, or null), `crossUp` / `crossDown` past the ends, the
  junction guard; `stepsUp`/`stepsDown`/`stepsFrom`/skip logic and the `chooser` words removed.
- `packages/ui/src/waterMap.ts`: the reducer's `choosing` state, `choose` / `closeChooser` actions, the view model's
  `chooser` and the step's `fork` removed (web and native share it).
- `packages/ui-native/src/water/WaterMapView.tsx`: the `Chooser` card removed.

## Acceptance
- Pure, on the committed regs fixture (cut from the real release): `waterMap.test.ts` "(T750)…" Skeena past the Reserve
  (main stem, side channel, Exchamsiks stretch, and back); ▲ from the lowest part n−1 times visits every part once, in
  `part_ix` order = the parts list, ▼ returns the same way, for the Skeena, Fraser, Columbia, Thompson, West Road
  (Blackwater) and Kootenay. `waterNav.test.ts`: the Zymoetz and Fraser forks walk the order; a junction inside the line
  throws.
- Shell: `StepInPlace.shell.test.tsx` walks the Skeena up past the fork and back through the real Shell: one part per
  press, no dialog ever added; `WaterMapView.test.tsx` the same on the view.
- Mutation-checked: skipping the side channel, or dropping the junction guard, fails 7 tests.
- Live release: `tools/every-part-reachable.test.ts` passes.
- Proof on a local web export (staging env, data-staging r-f749475d0962): the Skeena, Fraser, Columbia, Thompson and the
  forked West Road, Ash River, Wap Creek, Kitsumkalum, Cottonwood and Chilliwack→Vedder walked bottom to top and back, 0
  dialogs, every part once, the return the exact reverse; sequences, km and shots of the former fork points in
  `~/bcfish-session-backup-2026-11-12/step-order/` (`walks.md`).

## Owner question (default built)
Ranking inside one water by the part's UPPER end (today's `part_ix`) vs strictly by its DOWNSTREAM end differs only where
one part lies inside another's km range: Wap Creek and Nimpkish River. Default: keep `part_ix` (no release). If the owner
wants the strict rule, the pipeline change is: ship each part's downstream-end measure (`lo_km`, the lowest stem metre of
its main runs; a side channel's: the stem measure where it leaves) and rank `order_parts` by (`lo_km`, upper km, v) —
the topology rank assert and every golden follow; then a release.

Depends: none. Size: S. Owner: yes (the Wap/Nimpkish question; default stands).

## Owner answer 2026-11-14 (order rule)
Asked: keep the shipped order (a part ranked by its upper end) or switch to the strict downstream-end rule (differs only
on Wap Creek and Nimpkish River; needs a pipeline change and a release). Owner, verbatim: "upstream is fine". So the
shipped `part_ix` order stays; no pipeline change, no release for this.

- 2026-11-15 (CI red on e0785054a): the removed Chooser was still on tools/back-layers.test.ts's surface list (and named in Head's note); both taken off, nothing else of it left in the code. `pnpm check:fast` now runs the two inventories; fast-fail runs back-layers on any apps/mobile, ui, ui-native or map change.
