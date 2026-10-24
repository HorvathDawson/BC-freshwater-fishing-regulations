# T184 — The curation review as a shareable, phone-friendly artifact

Status: OPEN. Owner 2026-10-24: "make the curation review into an artifact instead so I can work on it remotely (a good
mobile version too) … I could send it to other people to help."

## Shape
- Export, from the SAME backend code (curation-review/backend), each target's review payload once per vintage:
  `/api/entries/{id}/review`, its book crop (page image + rectangle), map geometry, the angler's-page preview
  (`/api/answer`), vocab/problem lists. Book pages uploaded once as assets; crops are rectangles over them.
- Page: one target per screen on a phone (book crop, map, checks with big y / n / ? buttons + note), the laptop keys
  (t r p a, y n ?, Enter, o, m) kept; per-target timer like the local screen (`seconds`).
- Marks in the artifact's db with the viewer's id (`user` capability) → an importer writes them into the repo's mark
  history in the same shape `POST /api/marks` writes (so `gate pilot-report` and `commit_marks` work unchanged).
- Helpers: shared from the Share menu (claude.ai accounts; guests by email). Each mark records its reviewer; the page
  shows who has a target open/marked so two people do not double up.
- Review mode needs no live backend: it writes only marks, and everything it reads is exported per vintage. Fix mode
  (entry/split edits, /api/check, item search) stays the owner's local tool for acting on marks. After a rebuild the
  page is re-exported; marks keep their fingerprint (T97) and stay valid unless that target changed.

## Phases
1. The 30 pilot targets (handoff/PLAN/curation-review/PILOT.md, list regenerated for the current vintage) → the pilot
   runs on the artifact. 2. The whole queue, in batches per tier, re-exported per vintage.
Acceptance: the pilot report computed from imported artifact marks; a mark round-trip test (artifact doc → repo mark
→ pilot-report) pinned.

## Outcome (phase 1)

2026-10-24, release `r-0ed3c2b2a0a7` (atlas, reach, bundle and release agree; the same 30 targets as
r-bb4331929428, against which only the Z tier had moved — `pilot.json`, PILOT.md).

- **Export** `python -m pipeline.regs.review.export_artifact --pilot handoff/PLAN/curation-review/pilot.json
  --out curation-review/artifact/out` (through `scripts/heavy.sh`; ~4 min): imports the backend's own route
  functions (`app.get_entry`, `app.entry_review`, `app.entry_reaches`, `app.item_geojson`, `app.rule_resolved`,
  `app.item_tributary_geojson`, `app.get_mode`, `review.split_queue`, `review.located`, `answer.effective`).
  Per target: the review payload, rules with their binding, pre-checks, the book (page + crop rectangles in PDF
  points), the map (projected, simplified SVG paths: the water, one level of tributaries, each rule's bound
  sections — off-water ones thin — carve-outs, split points), the angler's answer on the part the screen's `o`
  link opens (`effective` for up to 3 fish on the export day), the marks in force, and each markable unit's
  `fp` (`fp_of`: sha256 of the prints `POST /api/marks` would stamp now). export.json 2.6 MB; 30 targets, 45
  markable units; 22 book pages as WebP (2.0 px/pt, q50) 2.8 MB, largest 204 KB.
- **Page** `curation-review/artifact/build_page.py` → `out/pilot.html` (2.6 MB; the prints are left out, the
  importer reads them from the export). Phone first: one target per screen, crop on top (tap: the page full
  screen, pinch / drag / wheel), coverage strip, map (tap or `m`: full screen; a rule picker), pre-checks, rules,
  the answer (`o`), then T R P A with big y / n / ? buttons, problem picker, note, Record / Defer; a zone entry
  rule by rule; a strip of the 30 for moving (and `j` `k`). Marks: db collection `marks`, one doc per
  (target, rule) {scope, target, rule_id, verdict, answers, problem, note, seconds, reviewer (user id), fp,
  release, pages, at}; the page refuses what the store would (wrong without problem + note, defer without a
  note, unanswered checks). Presence: collection `presence`, one doc per viewer {reviewer, target, at}, written
  when a target opens (no timer); "being reviewed by X" for 30 min. Names via `user.profiles`, only ids stored.
  Publish with `capabilities: {db: {}, user: {scopes: ["profile"]}}` and `files` = `out/publish_files.json`.
  `out/` is git-ignored (rebuilt per vintage).
- **Importer** `python -m pipeline.regs.review.import_artifact_marks --docs <dir|list.json> --export
  out/export.json [--reviewers ids.json] [--marks <store>] [--dry-run]`: prints and book sha from the export by
  `fp`, verdict re-derived from the answers, `Mark` validation, `marks.append`; idempotent. Pinned by
  `pipeline/tests/test_review_artifact.py` (round trip to `gate.pilot_report`, idempotence, 11 refusals, the
  prints-are-the-export's mutation pin). Real round trip on a copy of the store: 45 page-shaped docs → 45 marks
  → `gate pilot-report --marks <copy>` 30/30 marked; `gate stale --marks <copy>` 45 held, 0 stale (the
  imported prints are exactly what the screen stamps).
- Fixed on the way: `reuse.item_tributary_geojson` passed `_features` a fourth argument (TypeError) — the
  tributary layer of the local screen's map was broken too.

What the static page cannot do that the local screen does: the app's own page (A is checked against the
exported `effective` answer for up to 3 fish on one day, not the app; the owner's `local_url` link is kept);
another answer date; a zone rule's map (10^5–10^6 sections: the count only); `rule_resolved` beyond 20,000
sections; the extractor's row crop; anything after a rebuild until re-exported; fix mode (by design, T185).
