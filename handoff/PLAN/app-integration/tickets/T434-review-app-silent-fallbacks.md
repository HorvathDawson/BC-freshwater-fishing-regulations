# T434 — Review app: errors turned into empty answers

Status: OPEN — from the audit (T333–T335, 2026-11-05). Severity: **medium**. Verified by a second reader.

Source: `handoff/PLAN/audit-2026-11-05/` (findings: review-deploy 4, 5, 6, 7, 8; tests 15; `*.verified.md` has the evidence per finding).

## What happens
`reuse.py:1139-1149` `_applied_keys_for` catches everything and returns an empty set — T421's refusal becomes
"nothing applied" again one layer up (cached until the build changes). ~47 frontend `.catch(() => {})` / empty-list
fallbacks (`P1Quick.tsx:57` invents an empty verdict; `ExtentFixEditor.tsx:15`, `DfoReview.tsx:1013` hold copies of
server policy). `--reload` + the daemon write-through thread with no shutdown wait can drop an in-flight background
apply. Docstrings say "THE ONE WRITE" / "never writes the catalogue or splits.json" while `POST /api/inseason/matches`
and the T325 write-through do (its refusals not logged like marks'). 7 routes no frontend calls; API.md misses ~25
routes. The `splits` proposal domain (`apply.py:41`) is reachable only by old marks and untested.

## Fix
No catch in the cached function: callers show the error; every frontend fetch failure shown (one error banner
pattern) and server policy read from the API; write-through drains on shutdown; the docstrings and API.md made true;
dead routes removed (or kept with a reason); the `splits` domain removed or tested.

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: M. Owner: no (picks the order).
