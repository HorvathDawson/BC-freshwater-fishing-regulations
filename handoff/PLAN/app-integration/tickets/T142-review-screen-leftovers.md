# T142 — Review screen leftovers: focus after a click, refusal wording, partial mark (Q96), draft-guard test

Status: DONE — 2e8234bb (blur, browser test), 1218d094 (refusal wording, shared with T135) (lane-T135). Q96's partial "text and rules verified" mark is NOT built (owner default: no);
documented below. Takes effect on merge: no rebuild.

Source: T134 "Left / follow-ups" and its adversarial review (T134-curation-review-screen.md).

## Work
1. **Focus after an answer click.** Clicking an answer button left the focus on it, so the next Enter re-clicked that
   button (Enter on a focused control is the control's) instead of recording the mark.
2. **Reviewer wording for refused marks.** A review-mode refusal passed the model's text through ("Value error, wrong
   needs a problem category and a note"), with a field path.
3. **Q96 — a partial "text and rules verified" mark** while an entry awaits its build (only T and R answerable).
   Owner default: **no**. Leave it unbuilt; document it.
4. **Browser test for the fix-mode draft guard** (T134 adversarial review fix 5 had none: the frontend has no test runner).

## Acceptance
- After clicking an answer, Enter records the mark (browser).
- Every refusal a reviewer can cause reads as what to do, with no "Value error" and no field path.
- `j` with an unsaved fix-mode edit asks first; cancel stays (edit kept), OK moves on (browser).
- Q96 stays as the owner's default, written down.

Depends: T134. Size: S. Owner: Q96 only.

## As built
- **Blur** (`curation-review/frontend/src/components/Checklist.tsx`): an answer button's click gives the focus back
  (`ev.currentTarget.blur()`), so the window's key handler takes the next Enter and records the mark. Keys-only use is
  unchanged (it never focused a button).
- **Refusals in the reviewer's words** — `pipeline/regs/review/marks.py`: `REFUSAL_WORDS` (the model's own phrase →
  the instruction), `refusal_words(msg, field)` and `unanswered_words(scope)`; `app.post_mark` sends every 422 through
  them with `path: ""` (a field path means nothing at the screen). A message no phrase matches passes through without
  pydantic's "Value error, " prefix, so a reworded model message never becomes a wrong instruction. Now:
  - R n with no problem / note → "A wrong mark needs a problem (pick one from the list under the checklist) and a note
    saying what is wrong."
  - a check unanswered → "Answer every check (T R P A) with y, n or ? before recording the mark, or defer it with a note."
  - no name → "Type your name first: every mark names its reviewer."; defer with no note → "To defer, write a note
    saying why."
  - the gate and awaiting-build refusals were already sentences; they pass through unchanged.
- **Q96 (not built, owner default no).** While an entry awaits its build only T and R can be answered, and
  `verdict_from_answers` gives a verdict only to a full set or to an n / ?, so "T y R y" on an unbuilt entry is refused
  ("Answer every check …"). A reviewer there can record wrong / unsure / deferred, never "T and R are fine". If the
  owner says yes: a `partial` verdict (scope entry / rule) holding T and R only, `Mark._consistent` accepting it only
  while `vintage.entry_state` is awaiting build, counted as "to do" on the progress bar, and going stale like any mark
  once the build lands (so P and A are asked then). Nothing else changes.
- **Browser test** — `pipeline/tests/test_curation_review_browser.py` (`-m slow`, `needs_atlas`, `needs_source`): the
  real backend (uvicorn, FIX MODE, temp copies of the catalogue / splits.json / marks store, an empty reach dir) and
  the real Vite dev server, each on a FREE port of its own (never 39217 / 39218 / 8787 / 5173), driven by Playwright's
  Chromium; both processes are stopped by their own handles.
  - `test_the_draft_guard_asks_before_j_leaves_an_edit`: type into `scope_note`, press `j` → the dialog's exact text;
    cancel → same entry, edit kept; `j` again, OK → the next entry.
  - `test_enter_after_clicking_an_answer_records_the_mark`: click "T y", "R ?" → the focus is on the page → Enter →
    the store holds exactly one mark, ADAM RIVER, unsure, {T y, R ?}.

## Checks
- `test_curation_review_browser.py` 2 passed (via `scripts/heavy.sh`, unfiltered); mutation pins run by hand 2026-10-23: the blur removed → `test_enter_after_clicking_an_answer_records_the_mark` FAILS;
  `go` without its `window.confirm` → `test_the_draft_guard_asks_before_j_leaves_an_edit` FAILS (both reverted, then 2 passed).
- `test_curation_review_queue.py::test_a_refused_mark_is_said_in_the_reviewer_s_words` (whole messages).
- Frontend `tsc -p tsconfig.json --noEmit` clean.

## Owner questions (built with the default; say if not)
1. **Q96 — a partial "text and rules verified" mark while an entry awaits its build?** *Default:* no (unbuilt; the
   design is above, about half a day).

## Left / follow-ups
- The browser test needs `curation-review/frontend/node_modules` (`npm install`); it fails naming that when absent.
