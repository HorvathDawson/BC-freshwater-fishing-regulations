# T668 — Audit: a DFO place that covers one river stretch in more than one region (Fraser, Regions 2 and 3)

Status: OPEN

Source: owner 2026-11-11: "look into how DFO places that match to multiple regions, same river section, like Fraser
2/3, is handled. To audit it."

## Question
DFO lists salmon places per region. Where one physical stretch is named under two regions (the Fraser under Region 2
and Region 3; likely also other boundary rivers), what does each step do with it, and what does the angler see?

## Work (read-only audit first; fixes become their own tickets)
1. List every DFO place whose matched sections overlap a place of ANOTHER region (same section handles, or the same
   blue line over an overlapping measure range). Count, and name them.
2. For each: follow it through the scrape, the place registry, the owner's matching in the review app, the feed build
   and the app. Say what is kept: both places, one, a merge; which region's rules win on the shared stretch; whether the
   rules differ between the two regions' listings today, and whether the app shows both, one, or a conflict.
3. Check the stretch split: sections auto-combine only when the live salmon rules are equal (T517); does a two-region
   stretch split or join correctly, and does the Salmon card say which region's notice it shows?
4. Check the review app: can the owner label the same stretch for two regions without one overwriting the other
   (marks keyed by place id), and does "rule changed" staleness (T361) treat them separately?
5. Report: a table per place, the defects found, and proposals (each a proposal for the owner; no value changed).

## Acceptance
- The owner can read, for every such place, what the app says today and why; each defect has a ticket.

Depends: T164, T549. Size: M. For the next chat.
