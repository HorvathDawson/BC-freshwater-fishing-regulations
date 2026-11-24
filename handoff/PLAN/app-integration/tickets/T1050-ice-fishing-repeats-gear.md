# T1050 — Methods: "Ice fishing" repeats the Gear tab's line and hook counts

Status: OPEN — 0.9.1; owner: OWNER-TODO "T1050 ice fishing lines" (which words), then a pipeline change + the 0.9.1 data release.

Source: in-app report `4eedeb08d1964b44` (staging feeds worker, the owner, 2026-11-24 18:50 UTC), reason "other", on
Dick Lake `wbk:328989582`, part 0, "Today, Nov 24", Regulations view, web, app 0.9.0 (build 43, 127bf43), release
r-0aec88ed3edb. No screenshot attached. The words, verbatim:

> ice fishing includes same gear info which is just a duplicate of info?

Triage: lane L19, 2026-11-24. Picture (staging, 390 px wide, 1060 tall so both blocks show):
`handoff/PLAN/app-integration/T1050-shots/report-now.png`.

## What the screen shows

Gear tab: "Lines 1 line (2 while alone in a boat)", "Hooks One hook, lure or fly". Methods tab, under "✓ Ice fishing":
"Lines: 1.", "Hook, lure or fly per line: 1.", "Remove your ice hut before breakup.", "Warn others of your ice hole."
The first two bullets say again what the Gear tab already says.

## Cause

The pipeline's gear frame puts EVERY count clause scoped to ice fishing into the way's `while` list
(`pipeline/deliver/answers/gear.py:680-683`, `info = [circ_ref(e) for e in circ if k in e["circ"] …]`), whether or not it
differs from the count in force on the Gear tab; `app/packages/core/src/regs/methods.ts:66-71` (`wayChips`) words each
one ("Lines: 1.", via `METHODS.SLOT_LABEL`). The clauses are `zp:allowable_methods.r2b` (`while: [ice_fishing]`,
`lines_per_angler` max 1, `terminal_attachments_per_line` max 1); the Gear tab's are `zp:terminal_tackle.r1` / `r6`.

## What the book says

B.C. Synopsis p.8 (provincial): "Your basic fishing licence entitles you to: angle with one fishing line to which only
one hook, one artificial lure OR one artificial fly is attached … ice fish with one line and one lure, artificial fly or
other terminal attractor. It is your legal responsibility to warn other people of the existence of your ice hole and
remove your ice hut before ice breakup." The ice line adds nothing to the counts the Gear tab states (one line, one
attachment); its own content is the two duties. Whether to repeat it is a wording choice: the owner's.

## Options

- **A (recommended):** under a way, show a count only where it differs from the Gear tab's count for that slot (here
  both go; "Remove your ice hut before breakup." and "Warn others of your ice hole." stay). Decided in the gear frame
  (Python: a `while` clause whose bound equals the slot's base count is dropped), so the app only words it.
- **B:** keep them as one line: "Same gear as above: 1 line, 1 lure or fly."
- **C:** keep as is (the book prints the ice line on its own).

## Work (after the pick)

1. A: `gear.py` drops a way's `while` count equal to the slot's base answer; vectors / `answers_diff` list what moved
   (expect every water where ice fishing is allowed and no water rule changes the counts).
2. Tests that fail first (Dick Lake's ice-fishing bullets as whole strings), mutation-pinned; before/after shots light + dark.

Size: S. Needs: owner pick; pipeline change; the 0.9.1 data release (the gear frame ships in the release). Safe for 0.9.1.
Depends: none. Owner: yes (the words). Default: A is the build default once the owner picks; nothing changes until then.
