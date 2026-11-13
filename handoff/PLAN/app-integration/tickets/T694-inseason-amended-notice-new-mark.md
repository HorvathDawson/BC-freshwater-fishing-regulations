# T694 — A new or amended in-season notice carries a "New" / "Updated" mark until this device has seen it

Status: DONE — 10ad08512, 8205a5c8a (built with T706; left (Claude): nothing — owner look is the T706 post-release check).

Source: owner 2026-11-13, un-postponing it: "Yes add it in." In-season notices are BC Fish and Wildlife's, not DFO's.
Was POSTPONED 2026-11-12 (gap 2 of the in-season lane; T693 built gap 1).

## The gap
A notice matched to a water whose text is amended came in as a new id the reader was never told about: it was not in
the bell (matched) and nothing on its page said it changed since they last looked.

## Built (with T706, one change set)
- Every matched notice that is NEW or AMENDED since this device last saw it is unread in the bell's inbox (T706) and
  carries a mark on its water's Regulations page until seen: the collapsed line says "Updated · In-season change (…) ·
  see ›" (several: "3 in-season changes (1 new, 1 updated) · see ›", core `inSeasonSummary`) and the open card says
  "New" / "Updated" (core `inSeasonBanner(…, mark)`).
- **"Updated" ONLY when the feed links it**: the feed row's `status` is "changed" with `previous` set (the feeds worker's
  `buildFeed` links an amended row to the row of the same region + water that left the page in that reading). The app
  never guesses a link: a new id the feed does not link is "New", and the notice it may have replaced shows "Ended".
  The linked predecessor leaves the history (the updated one takes its place).
- SEEN = read, per device, stored locally (`canifishthis.inseasonInbox.v1` on web, `inseason-inbox.json` native —
  the established `keyStore` / `fileStore` pair). Seeing the mark on the page marks it read at once; the page keeps the
  mark for that visit (taken once per notice per water) so it does not vanish under the reader. Opening the row in the
  inbox marks it read and opens the page with the mark shown and the notices open.
- Code: `app/packages/core/src/inseasonInbox.ts` (`inboxMerge`, `inboxMark`), `core/src/inseason.ts` (marks words,
  summary), `@app/ui` `useInSeasonBanners` + `InSeasonInboxContext`, `ui-native/src/regs/InSeasonBanner.tsx`.
- Tests: `core/src/inseasonInbox.test.ts` ("UPDATED only when the feed links it …" — linked → updated, unlinked new
  id → new and the old one ends, a date amended in place → updated), `core/src/inseason.test.ts` (the marked line and
  summary, whole strings), `ui/src/inseason.render.test.tsx` ("the feed's link makes it Updated; a new id it does not
  link is New"), `ui-native/src/InSeason.test.tsx` (the mark on line and card).

## Measured
Staging feed 2026-11-13: 32 rows, all `unchanged`, no `previous` — the "Updated" path is pinned by tests and shown in
the T706 screenshots from a fixture, never yet seen live.

Depends: T693 (built), T706 (built with it). Size: S.
