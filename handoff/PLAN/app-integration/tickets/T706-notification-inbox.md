# T706 — Notification history: the bell opens an inbox of in-season notices

Status: DONE — 10ad08512, 8205a5c8a (left (Claude): nothing; the owner looks at it in the post-release checks).

Numbering: the two commits' subjects say "T700"; that number was taken meanwhile by the regs-header ticket
(T700-regs-header-names-its-own-part.md), so this ticket is T706. The commits are this ticket's.

Source: owner 2026-11-13: "maybe we should have a way to see history of notifications in case user wants to see them
again? Make it clean in an industry standard way", then "The inbox can auto be added just add it to the post release
checks for me to look at it." Ships with this release at finished quality (not a pick). In-season notices are BC Fish
and Wildlife's, not DFO's. Built with T694.

## Design, as built
- **The bell opens the inbox.** The bell's sheet keeps its app-update, data-update, sync and DFO sections as they were;
  its in-season section is now the inbox (`ui-native/src/InSeasonInbox.tsx`): unread notices under **New**, read ones
  under **Earlier**, newest first, and **Mark all as read** (shown while anything is unread). Each row is a card, square
  corners: its mark (New / Updated / Posted (read) / Ended), the date, the water and region, the change's first two
  lines, and "Not on the map yet" when it is not matched. Unread rows carry a dot and the attention bar.
- **Tapping a row** marks it read, then: a notice in effect matched to exactly one water held on this phone opens that
  water's Regulations page with the in-season notices open and its mark shown; otherwise (no water on this phone,
  several waters, or ended) its text opens in the inbox with "‹ All notifications" (the platform's back steps back to
  the list). A notice on many waters says "Shown on the Regulations page of each of the N waters it applies to."
- **The badge counts unread only** for the in-season part; DFO, sync failures and the release/app rows count as before.
- **Unread rules**: a notice met after the first read on this device is unread, matched or not. The FIRST read on a
  device starts quiet: matched notices are read; ones not on the map yet are unread unless T230's notice already showed
  them (`inSeasonSeen`). Read = a row opened, its mark seen on its water's page, or Mark all as read.
- **"Updated" (T694)**: only when the feed links it (`status` "changed" with `previous`); never guessed in the app.
- **Retention** (core named constants): kept until `INBOX_KEEP_DAYS_AFTER_END` = 30 days after the notice ended (its
  `to` passed, it left the feed's page — `removed` — or it is no longer in effect), and at most `INBOX_MAX_ITEMS` = 200
  notices (the newest). Read or not.
- **Per device, local**: `canifishthis.inseasonInbox.v1` (web) / `inseason-inbox.json` (native), format
  `inseason-inbox/1`. Nothing is sent anywhere.
- **The auto-opening "not on the map yet" sheet is folded in**: there is one sheet. It opens by itself once for an
  unread notice not on the map yet that it has not opened for (as T230's did, after the first-run notice and the
  walkthrough); closing it does not mark the rows read — they stay New, and the bell keeps counting them.
- **Failures fail loudly**: an index in a format this app does not read is reported (as before) and now also shows a
  line in the inbox ("The latest in-season notices could not be read by this version of the app. Shown: the last ones it
  read.", `httpInSeason().failure()`); a saved history that cannot be read is reported and starts again; one that
  cannot be saved is reported. No silent catch (`tools/failures.test.ts`: `ui/src/inseason.ts` FINE count 2 → 0).
- **Accessibility**: the bell is a button labelled with its count ("3 unread in-season notices"), each row a button whose
  label is its whole sentence ("Unread. Updated: Peace River, Region 7B, Nov 12. …"), group titles and the section are
  headers, the failure line an alert (`ErrorState`).
- Logic in core (`core/src/inseasonInbox.ts`) and the hook (`@app/ui` `useInSeasonInbox`); words in core
  (`INBOX_WORDS`); components presentational; web and native share the ui-native tree (the store is a file pair).

## Tests (run unfiltered, `rtk proxy pnpm vitest run …`)
- `core/src/inseasonInbox.test.ts` (8): unread count, first read quiet, read on open, Mark all read, Updated vs New,
  retention (ended + 30 days, cap 200), store text round trip + unreadable history throws, row targets, list words whole.
- `ui/src/inseason.render.test.tsx` (5 new): survives an app restart (store read back), opens once, read on open +
  focus, Mark all read, Updated/New, unreadable history reported, unknown feed format fails loudly (reported + line).
- `ui-native/src/InSeason.test.tsx` (8 new): rows, groups, labels, Mark all, detail + back, empty state, failure line,
  banner mark and focus.
- `pnpm -C app check` (2026-11-13): typecheck (app, worker, seo), promises, anchors, platform, boundaries and the rest passed; vitest 7,598 passed / 1 failed (`tools/one-day-table.test.ts`: the inbox's own ISO-day regex — fixed in 8205a5c8a, that file re-run alone: pass); `tickets` (T704) and `knip` (`tools/store-shots/run.mjs`) failed on another lane's uncommitted work.

## Screenshots (fixture, not live)
`~/bcfish-session-backup-2026-11-12/notifications/` (light + dark, 390×844, `captions.md`): staging's feed had no new or
amended notice and its matches.json was still `inseason-links/3`, so the in-season files came from fixtures built from
staging's rows; everything else was staging.

## Owner
Post-release check: open the bell on the phone, look at the inbox, tap a notice. Nothing to decide.

Depends: T694 (built with it), T673. Size: M.
