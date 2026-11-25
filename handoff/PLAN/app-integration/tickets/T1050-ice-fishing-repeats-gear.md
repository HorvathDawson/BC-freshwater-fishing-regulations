# T1050 — Methods: "Ice fishing" repeats the Gear tab's line and hook counts

Status: DONE — 2c072f33d, 295bbac15 (review: Kootenay's split count kept), 9e6e45694 + d0baa5fa0 (the mini release re-cut) (0.9.1; pipeline only, the app unchanged; reaches anglers with the 0.9.1 data release. Old apps read the new frames: a way's `while` was always optional)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1050 | Methods: "Ice fishing" repeats the Gear tab's line and hook counts (Dick Lake) | DONE — 0.9.1; needs the 0.9.1 data release | 2c072f33d, 295bbac15, 9e6e45694, d0baa5fa0 | staging report 4eedeb08d1964b44 (owner 2026-11-24); lane L19 |

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

## Owner answer (2026-11-24 ~12:33 MST, verbatim)

Question: the Methods tab's Ice fishing entry repeats the same line and hook counts the Gear tab already shows... What
should Ice fishing show?

Answer: **"Only what differs (Recommended)"** (the option's text: Ice fishing shows a line or hook count only where it is
different from the Gear tab's; on Dick Lake that leaves its own content, hut and ice-hole duties, and no repeated counts).

## Done (lane L20, 2026-11-24)

- `pipeline/deliver/answers/gear.py` `resolve`: the ice-fishing way's `while` leaves out a count clause whose count (max,
  min, unlimited: what `countTxt` words) equals the slot's main count, the Gear tab's (`repeats_main_count`,
  `SAYS_ONLY_WHAT_DIFFERS`, decision G7). The clause stays in the frame's `circumstantial` and `decides` (a pipeline
  citation only: see "Left" below). Only
  ice fishing: set lining's "Lines: 1." counts set lines, not the Gear tab's rods, and stays (1,186 + 931 waters
  unchanged). The members are not compared: the ice clause names `terminal_attractor` where the Gear tab's names
  `hook`, and the page words neither.
- Review of #130 (295bbac15): a count is a repeat only of ONE plain Gear-tab count. Where the Gear tab splits the main
  count into a shore count and a boat count (every circumstance beside it a boat: `_from_a_boat`), the ice count says
  which applies on the ice and is kept. In the 0.9.0 answers that is one frame, 437: **Kootenay Lake — Main Body**
  ("1 line (rod) from shore; no limit from a boat"); it keeps "Lines: 1." under Ice fishing and loses only "Hook, lure or
  fly per line: 1.". Dick Lake's "1 line (2 while alone in a boat)" is one plain count with a boat exception: its ice
  count still goes. The same commit pins the min / unlimited comparison and the spec guard (two mutants had survived).
- Tests: `pipeline/tests/test_answers_gear.py` `test_t1050_*` (Dick Lake's rules as the 0.9.0 bundle holds them; a
  differing count kept; a spec and a count with no main kept; set lining kept; Kootenay's shore / boat split kept; same
  max with another min, unlimited or a spec). Mutation-proved: an empty `SAYS_ONLY_WHAT_DIFFERS`, set lining added to it,
  no shore / boat test, the split not passed, comparing only `max`, or no spec guard: each fails them.
- Over the whole 0.9.0 answers (`r-0aec88ed3edb`; the answers step rebuilt from its own staging inputs reproduces the
  live answers file byte for byte, then rebuilt with the change): 590 of 1,116 gear frames change, every one ONLY by
  the ice-fishing way losing both clauses of `zp:allowable_methods.r2b` (1 line, 1 attachment); 7,549 named waters and
  702,682 unnamed lake / wetland sections. No frame keeps an ice count afterwards: none differs from the Gear tab in
  this bundle. No other section of the answers file (ladder, rows, licence, display, trout) and no other gear field moves.
  After the review fix (counted from the decoded frames, not a second rebuild): the same 590 frames change; 7,548 named
  waters lose both counts and Kootenay Lake — Main Body loses only the hook count.
- The mini release re-cuts (9e6e45694, d0baa5fa0): the mini corpus has no ice-fishing rule, so no answer in it changed;
  only the card check's `code_sha` stamp (in the water shards) moved with the gear code. 9e6e45694's subject ("its
  ice-fishing way lists no repeated count") is wrong on that point; history is left as it is.
- Pictures (Dick Lake, Nov 24, web 390 px, the 0.9.0 release before; a side release cut from the changed answers after):
  `T1050-shots/methods-before-light.png`, `methods-before-dark.png`, `methods-after-light.png`, `methods-after-dark.png`.

## Left (for the planning chat)

- The ice-fishing clauses (`zp:allowable_methods.r2b`) are reachable from no source line on the page: the Methods tab's
  ways source line cites only each way's `by` (`app/packages/core/src/regs/methods.ts` `waysVM`, line 96). That was so
  before this ticket for every way's `while` clause; the frame's `circumstantial` / `decides` hold it, the page does not.
