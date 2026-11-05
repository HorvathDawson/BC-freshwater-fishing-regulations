# T415 — Extraction regression: Heber River's text no longer comes out as the 09-24 run made it

Status: OPEN (found by T239's dry run, 2026-11-05).

A fresh extraction of the current book reproduces 1,512 of 1,515 entries. Chilliwack/Vedder and Coquihalla differ only
because curators hand-fixed their text (expected). Heber is a real regression: the extractor changed since the 09-24
run and now reads that row differently. Find the change in `pipeline/regs/extraction`, fix it, and pin it with a
test on that page, so the next book's diff is clean. Size: S. Owner: no. Before the next book.
