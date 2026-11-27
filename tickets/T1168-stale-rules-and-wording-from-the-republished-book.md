# T1168 — Bring the data up to the republished book: the Cowichan and Heber stale rules, the licence wording (FWID, WILD), the province's own errors noted

Status: OPEN — the licence wording in the first patch (owner 2026-11-25); the stale rules in a later ticket by his word; the gotcha entry now

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1168 | Bring the data up to the republished book: the Cowichan and Heber stale rules, the licence wording (FWID, WILD), the province's own errors noted | OPEN — the licence wording in the first patch (owner 2026-11-25); the stale rules in a later ticket by his word; the gotcha entry now | — | first patch: proposed FWID / WILD words to the owner in the pre-cut pack; a GOTCHAS entry for the two rows the new copy lost; the Cowichan and Heber rows are his review when he schedules them |

Source: T1157's read-only comparison (notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `T1157-changed-book.md`) and the owner's answers,
2026-11-25 night: "we can then fix stale rules in a future ticket."; on the licence wording "we should change this"
and, asked when, "First patch (Recommended)"; on the rows the province's new copy lost: "add to gotchas with info
about it when it was there and how it isnt there now. then later we will send the gotchas with everything".

## Work
1. Licence wording (first patch): since April 1, 2026 an angler may state their Fish and Wildlife ID (FWID)
   instead of showing the licence, and licences are bought through WILD (book PDF pp. 4, 8-10). The app still
   says to show the licence and photo ID (stricter, so nobody breaks the law). Proposed words to the owner; fees
   are unchanged.
2. GOTCHAS entry (now, docs): LITTLE QUALICUM RIVER (1-6) and LIZARD LAKE (1-3) are printed in the repo's copy of
   the 2025-2027 book (metadata 2025-11-18) and are missing from the whole gov.bc.ca copy edited 2026-08-25
   (checked on the rendered pp. 18-20), with no in-season notice or "Regulation Changes" line explaining it;
   PUNTLEDGE RIVER is printed twice there (bottom of p. 19 and top of p. 20). Our data keeps both rows. A re-parse
   of the new copy must not drop them or mint a second Puntledge entry. The gotchas are sent to the province
   later, together.
3. Stale rules (a future run, his review; parser runs are HUMAN-ONLY): COWICHAN RIVER (1-4) "No Fishing July
   4-July 14" was a 2025-only in-season closure and is stored as an every-year rule (`cowichan_river.r1`); HEBER
   RIVER (1-9): the closure above the lower canyon now ends Aug 31 (data: Sept 14) and the one below reads Jan
   1-Dec 31. Both tell an angler a water is closed when it is open; next in summer 2027.
4. Adopting the new copy as `data/source/fishing_synopsis.pdf` goes through `runbooks/annual-synopsis.md`, with
   the two lost rows kept by hand; the pins of T1166 move with it.

## 2026-11-26: items 3 and 4 are now T1179
Owner (P11): "adopt it eexcept we dont need a parser run you can run a subagent to produce the changes then youi can audit it and make sure it parsed it right. drop the dropped rows by adding a drop boolean to the entries witrh a note why. also can we add gotchas for them being dropped with info." The hand adoption (Cowichan, Heber, the dropped Little Qualicum River and Lizard Lake rows) is T1179; the GOTCHAS entry is written; this ticket keeps the licence wording (item 1). Item 2 changed from "we keep both rows" to "dropped with a `drop` flag".

## Acceptance
The licence words match the current book and he has seen them; the gotcha entry exists; the two stale rows are
corrected by his reviewed marks or each is a recorded decision. Severity: medium. Size: S (1, 2), M (3, 4).
Owner: the words; the rows.

## Owner

- [x] 2026-11-25 — The licence wording → "we should change this"; when → "First patch (Recommended)"; the stale rules → "we can then fix stale rules in a future ticket."; the rows the province lost → "add to gotchas with info about it when it was there and how it isnt there now. then later we will send the gotchas with everything" (2026-11-25)
- [ ] 2026-11-25 — Read the proposed FWID / WILD wording in the first patch's pre-cut pack. Later, when you schedule it: your parser run and review for the Cowichan and Heber rows.
- [x] 2026-11-26 — The republished book's stale rules: when → "After the re-cut" (2026-11-26 00:54)
- [x] 2026-11-26 — A build chat runs `python -m pipeline.tools.mini_release --write` (the fixture re-cut) once per branch, under the lock → "Yes" (2026-11-26; T1085, T1096, T1168)
