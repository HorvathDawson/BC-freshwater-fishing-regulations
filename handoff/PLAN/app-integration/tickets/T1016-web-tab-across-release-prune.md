# T1016 — Web: a tab kept open across two releases (or a site deploy) meets 404s on files production no longer serves

Status: PARTIAL — built d787d0dff, review fixes 36862bbbc (lane L6), automatic refresh b38201d83, 03f70b777, review #125 fixes 81b177cdd (lane L17, owner 2026-11-24); the reload + keepalive were proven with fakes only, not in a real browser; left (Claude): the feeds worker deploy with the 0.9.1 batch (its `expected` class for the two names), a production-export check of the chunk case, a real-browser check of the reload and the keepalive send, LOW 2 (offline / blocked storage is reported as the ReplacedRefreshNotMade defect: environment noise), LOW 3 (a too-early prune is now auto-refreshed under an `expected` report)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1016 | Web: a tab kept open across two releases (or a site deploy) meets 404s on pruned files / a gone chunk: say it, reload | PARTIAL — left (Claude): feeds worker deploy (0.10.0), export check of the chunk case, real-browser reload check, review LOW 2/3 | d787d0dff 36862bbbc b38201d83 03f70b777 81b177cdd | owner 11-24 "Always auto refresh": refreshes itself once per verdict, reports first; banner only as fallback |

Source: T1005 triage. Production keeps the live release and the previous one (`deploy/prod/upload.sh` prunes the rest
24 h on). The 404 kinds of T1005 (`3d5ded1b72ff865f`, `2c766955748940d9`, `f6d0af3f051de23a`, `bb863fcb6535d696`, …)
were all a crawler holding a cached r-fc137c4040fa; but a READER's tab opened before two releases and still open after
the prune reads the same pruned files (a region's regs / water slice, the in-season section lines, the tile folder's
mask and sprites), and one opened before a site deploy loads a chunk that is gone (`AsyncRequireError`, kind
`240b0d41d6fc2c88`). Each is reported as a defect and shown as a failed read.

## What
A 404 on an immutable file of a release the live pointer no longer names, or a gone chunk after a site deploy, is a
known state, not a defect: report it `expected` and refresh (web only; a phone reads its installed set).

Depends: none. Size: M. Owner: no.

## Done
- L6 d787d0dff: web-only triage (core `failure.ts` `setFailureTriage`, `apps/mobile` `replaced.web.ts`, `@app/ui`
  `replaced.ts` `replacedTriage`, `@app/data` `release/replaced.ts`, `siteProbe`): renamed reports `ReleaseReplaced` /
  `SiteReplaced`; worker `crashes.ts` EXPECTED += `/^(ReleaseReplaced|SiteReplaced): /` (needs a deploy: until then they
  arrive as `defect`); reproduced on the web over a fake origin (`../T1016-shots/prune-{light,dark}{,-after}.png`); the
  chunk case proved by tests only (the dev server loads chunks another way).
- Review #101 36862bbbc: 10 s bounds (`POINTER_MS`, `LOOK_MS`); no-answer chunk reported as `network`; a late 404 judged
  against the set it was read on (`openedSets`).
- L17 b38201d83, 03f70b777 (owner "Always auto refresh"): one automatic refresh per verdict per page life
  (`createReplacedState`, `autoReload`, `reloadOnce` marker `canifishthis.t1016.reloadedFrom` in `sessionStorage`);
  reports first (`failuresSent()`, crash reporter `sent()`, `keepalive: true`); the banner only as fallback, reported as
  `ReplacedRefreshNotMade` (a DEFECT, pinned in `tools/wire.test.ts`).
- Review #125 81b177cdd: every reloaded entry kept, at most `RELOADS_PER_SESSION` (3) reloads a session; `sent()` bounded
  by `SENT_WAIT_MS` (5 s).

## Work (left)
1. Feeds worker deploy with the batch (its `expected` class for the two names).
2. A production-export check of the chunk case.
3. A real-browser check of the reload and the keepalive send (proven with fakes only).
4. Review LOW 2: offline / blocked storage is reported as the `ReplacedRefreshNotMade` defect (environment noise).
5. Review LOW 3: a too-early prune is now auto-refreshed under an `expected` report (see (a) below).

## Known, left for the planning chat (not changed tonight; the pointer format stays)
- (a) A 404 on the PREVIOUS release (which production promises to keep 24 h) is classed `replaced`: a too-early prune by
  `deploy/prod/upload.sh` would be masked as expected. Telling previous from older needs a `previous` field on the pointer.
- (b) The web has no app-version gate (`appVersion: null` on the web), so the notice's button can apply in session a
  release that needs newer site code. This predates T1016: the Shell's safe-moment apply does the same.
- (c) The staged-release case can take two presses of the notice's action (review finding on #101; not reproduced by
  the lane).

## Owner's answers (2026-11-24, verbatim)
- Asked about the notice's words: **"Force refresh instead?"**
- Asked how (auto unless typing / always / keep the notice): **"Always auto refresh"**

- Not done (left, Status line): LOW 2, LOW 3; and the reload + keepalive are proven with fakes only.
