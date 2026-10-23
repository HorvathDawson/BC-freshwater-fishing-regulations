# T142 — Review screen leftovers (T134): focus after a click, error wording, partial mark while awaiting build

Status: OPEN

Source: T134 adversarial review 2026-10-23. Depends: T134 (done). Size: S. Rebuild: none.

## Work
1. Clicking an answer button leaves focus on it, so the next Enter re-clicks the button instead of recording the
   mark. Blur after a click (or let Enter record unless focus is a text field). Browser test.
2. A refused mark passes the model's text through ("Value error, wrong needs a problem category and a note"). Word it
   for the reviewer ("A wrong mark needs a problem and a note.").
3. Awaiting build only T and R can be answered, and T y + R y is refused ("answer every check"). Per Q96: default
   no partial verdict; if the owner says yes, add a `text_verified` verdict that never counts as verified for P/A.
4. The fix-mode draft guard (`onDirty`) has only a tsc check: add a browser test (j with a dirty draft asks first).

## Acceptance
Browser tests for 1, 2 and 4 on the tool's own ports (never 39217/39218).
