# T916 — "This water in the rule book p.7" on Sweltzer River points at the wrong page

Status: IN PROGRESS (main chat; lane app/t916-water-book; 0.8.3 batch).

Owner 2026-11-19 ~12:55 (main chat, phone screenshot of Sweltzer River, Regulations, What you can keep, 0.8.2 on
staging), verbatim: "Has page 7 link at top (which is wrong) with real page 26 at bottom. Should remove top one...".
Screenshot: scratchpad (session 1db4ea05) t916-owner-sweltzer.png. The top line "This water in the rule book p.7 ↗"
(`waterBookOf`, core regs/waterbook.ts, T474) sits above the Salmon/Game fish cards; the page's own rules link p.26
("Closed to all fishing by the rule book (p.26)", "Rule book p.26 ↗").

## Work
Find which rank-0 rule on Sweltzer River carries printed page 7 in `facts.book` (`display.decided.book_pages`), and
why: wrong data (an entry's pages wrong, or a general/regional rule sitting at rank 0), or the line's own logic. Fix
it where it is wrong (Python data / the rank, or the app line), never by hiding the line for this one water. Count every
water in the live release where the line names a page that is not one of that water's own entries' pages, before and
after. Pictures 390 light/dark before/after.
