# T867 — The notifications inbox: only what happened since install, read once seen, short, with data and app updates

Status: OPEN — looked at 2026-11-22 by lane L8 and left: it rewrites the inbox's core rules (`inseasonInbox.ts` first-read, read-on-open, 30 d/20 rows) and adds two new row kinds fed by release/app-update events, plus the page and its 480-line test; the owner's reply to the 11-18 proposal is not recorded here. Not a small ticket.

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
