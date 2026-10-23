# T71 — One Map entry: drop the Regulations header's Map button (audit N2, owner call 3)

Status: OPEN.

Source: decisions.md (audit plan, call 3): "Keep only one Map entry (the view strip; drop the header's Map button)".
Still built: `core/regs/header.ts` emits `{kind:"map"}` when `canMap`; `page.ts` defaults `canMap` true;
`RegsHeader.tsx` draws it; RegsScreen/WaterScreen wire `onMap`. The decision text 485677bc lives only in worktree
wf_e2463c2d-be5-1. D2/D24 rows, IMPLEMENT-HANDOFF and APP-PREP §0 still say "beside Map".

## Work
- The header action becomes Parts only (or null); remove the `canMap` option; keep the 44 px slot so the name does not
  reflow.
- Update the golden header parity on purpose (re-key the exclusion if needed).
- Cherry-pick the 485677bc text; mark D2/D24 superseded; fix IMPLEMENT-HANDOFF and APP-PREP §0.
- A one-surface test: Map is offered once per water screen (`tools/one-menu` or a unit test), mutation-pinned.

## Acceptance
No Map control in the header; the view strip is the only Map entry; the test fails when a second entry is added;
light + dark shots; no live decisions row offers Map in the header.

Depends: none (T60 later audits the rule). Size: S. Owner: no (decided).
