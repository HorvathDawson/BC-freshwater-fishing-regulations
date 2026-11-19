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

## Owner 2026-11-19 ~13:20 (main chat, after the before/after pictures)
- Asked "Put it in 0.8.3?", verbatim: "Ye sput to n 0?8.3 it was also the location that was awkward at the top". So yes,
  in 0.8.3, and the line's place at the top of the page (above the first status card) is awkward too: round 2 moves
  it down (pictures to the owner first).
- Asked whether waters named only inside a zone rule (Rubble Creek p.21, Horne Lake p.13, the p.4 youth waters) should
  get the line back, verbatim: "Keep it simple (Recommended)": the line is only for a water's own table row; zone-rule
  pages stay linked on the rules themselves.

## Owner 2026-11-19 ~13:45 (main chat, after the round 2 pictures of Elaho River)
Asked where the line goes; options were A, the page's foot; B, the end of What you can keep; or drop it. Verbatim:
"A: page foot (Recommended)". Merge app/t916-water-book (eaf71c64b) into the 0.8.3 integration; the release carries `row`.
