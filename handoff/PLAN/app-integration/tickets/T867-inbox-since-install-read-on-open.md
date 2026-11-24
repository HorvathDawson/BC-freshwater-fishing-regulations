# T867 — The notifications inbox: only what happened since install, read once seen, short, with data and app updates

Status: PARTIAL, left (Claude): T1030 (the release carries a changed-waters count) for the data row to say "N waters changed" (owner 2026-11-22 23:55 "Build all four"; code 1d488995b, in review: PR feat/T867-inbox).
Known LOW items left as they are (re-review of PR #58): (a) the bell opened by hand while the walkthrough is still due reads nothing until the tour ends; (b) if the apply curtain starts while the sheet is up, the rows read and show under Earlier in the same open. Gap: no test covers the apply-curtain exclusion on its own.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T867 | Notifications inbox: since install only, read once seen, short, with data and app updates (owner) | PARTIAL | app chat | left (Claude): T1030 (release changed-waters count) for "N waters changed" (code 1d488995b, in review) |

Owner 2026-11-18 ~19:50 (to the app chat), verbatim: "I don't think the inbox works properly for the app all in season
still show up under earlier always and data updates and stuff don't show up in history etc. also the history after
viewed I thought was supposed to then be read and not a giant list. All the in season don't need to show too. Just stuff
from when a person downloads maybe? Idk thoughts?"

Today (`core/src/inseasonInbox.ts`, T700/T694):
- a first read puts EVERY notice in effect into the history, matched ones already read ("quiet"), so "Earlier" is full
  from day one;
- read only on a row tap, its water's page mark, or "Mark all as read": opening the inbox reads nothing;
- kept: 30 days after a notice ends, up to 200 items;
- in-season notices only: data releases and app updates are not in the history.

Proposal (sent to the owner ~19:54):
1. Since install only: notices in effect at the first read stay out (they are on their waters' pages), except the ones
   not on the map yet (the inbox is their only home).
2. Opening the inbox reads what it shows: "New" holds what was unread at the open; the next open shows them under
   "Earlier"; the badge clears on open.
3. "Earlier" short: the last 30 days, at most ~20 rows.
4. Data updates and app updates as rows: "Regulations data updated <day> · N waters changed" (opens what changed),
   "Updated to <version>" (opens What's new).

## Owner answer 2026-11-22 23:55 (dialog)
"Build all four (Recommended)" — items 1–4 of the proposal above, in 0.9.0.

## Built (2026-11-23, lane L19)

Logic in `core/src/inseasonInbox.ts` (one function composes each row's words, tests pin whole strings), hook `useInSeasonInbox` in `@app/ui`, rows in `ui-native/InSeasonInbox.tsx`:
1. Since install: a notice quiet at the first read (or met begun, T983) and matched to a water is no history row; one not on the map still is.
2. Opening the sheet reads it: `shown(true)` snapshots the unread as this open's "New" (`inboxVM` `fresh`) and reads the history, so the badge clears; the next open lists them under "Earlier". "Mark all as read" is gone (nothing left for it to do).
3. "Earlier": read rows of the last 30 days, at most 20; "New" is never cut.
4. `events` in the history: "Regulations data updated <day>" (+ "N waters changed" when known) and "Updated to <version>", seen by `inboxObserve` from the release in use and the app version; the first sighting is recorded with no row. A tap opens the More tab (Data block) / What's new.
Migration: a history without `rev` reads once with matched rows read (unmatched keep their state); an upgrade shows no backlog as New.

Not done / needs a data release: the manifest carries no per-release changed-waters count, so the data row never says "N waters changed" yet (rule 32: absent figure renders nothing); `InboxTrack.release.changed` is the slot. "Opens what changed" opens the More tab's Data block (there is no what-changed page).
Pictures: `T867-shots/` (jsdom render of the sheet, light and dark; not emulator).

Review fixes (2026-11-23): a met notice stays quiet when its match is lost; the sheet reads only when in front (not under the first-run notice, the walkthrough about to open, or the apply curtain); a bad published day is reported, not stored; a notice listed while the sheet is up is read at close.
