# T434 — Review app: errors turned into empty answers

Status: DONE 2026-11-06 (chat 3, a89deff00 backend, 544d85a10 frontend) — from the audit (T333–T335, 2026-11-05). Severity: **medium**. Verified by a second reader.

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

## Result (2026-11-06)
- `_applied_keys_for` catches nothing; a missing input is a 503 whose `detail` names it (app-wide `FileNotFoundError`
  handler), and the frontend shows it. Test: `test_review_app_no_silent_empty.py`.
- Frontend (544d85a10): one `reportFailure` → a FailuresBanner in the NoticeStack; ~45 silent `.catch` fallbacks in 17 files
  now report; P1Quick no longer invents a verdict; `FALLBACK_OPS` / `OFFSET_FALLBACK` gone (read from
  `/api/extent-fix/ops`, `/api/split-anchors`). Test: `test_review_frontend_catches.py` (allowlist: 6 body-parse sites).
- Write-through drains on shutdown (`wait_idle(30)`); `--reload` only with `DEV=1` (both run.sh). No startup re-apply:
  the next mark re-applies its domain, and the marks are safe either way.
- Docstrings (app.py, writes.py) and API.md say two write routes + the write-through; in-season refusals are logged
  like marks'. API.md ends in a route table generated from `app.routes` (`curation-review/api_routes.py --write`),
  pinned by `test_review_api_routes.py`.
- Routes: `/api/review/lake-splits` deleted (no caller; the tickets CLI has `--lake-splits`). Kept, tagged PROBE: `GET
  /api/marks`, `/api/marks/splits`, `/api/review/tickets`, `/bundle`, `/answer/waters`, `/api/answer` — tests pin them.
- The `splits` review domain is removed (the store holds no split mark; T184 made them history).
