# T321 — DFO limits wording as a closed registry: unknown wording is held, shown in DFO's words, and alerts urgently

Status: OPEN (owner 2026-10-31: "It should be urgent yes. Because it could mean a quota changed but with a new wording.
Also display a specific waters unparsed items if they exist (wording change happened). Yes make this a ticket.")

## Why
T320's guard is "every NUMBER in a limits cell is read". A restriction in WORDS passes as fully read:
- "one hour before sunrise to one hour after sunset" (14 rows): the hours are dropped.
- "N coho per day in the aggregate" (6): read as a plain daily limit.
- "(see exception below)" (8): the pointer is dropped.
A notice that changes a quota in new wording could pass the same way.

Baseline: 75 saved DFO region web pages (12 test fixtures, 10 cached today, 53 archived 2017–2026) → 267 distinct
cells, 85 clause templates (numbers blanked). The list is in `T321-wording-baseline.txt`; the generator is
`/private/tmp/…/scratchpad/quota_variants.py`, to be moved into the repo as the registry builder.

## Work
1. **Registry** (`data/curated/dfo_salmon/wording.json`, one file both readers load: pipeline `typed`/`quota` and the
   feeds worker TS port): each clause template (numbers → N, FN → FN#, normalised spaces/case/dashes) → its meaning:
   a structure field (daily, hatchery, over, wild, monthly, max_cm, hours, gap, gear…), `note` (shown in DFO's
   words on the card), or `provenance` (FN numbers). Seed with the 85 baseline templates; each is reviewed, not
   bulk-accepted (everything-added-is-a-proposal).
2. **Close the gaps:** hours as words → structure (sunrise/sunset); "in the aggregate" and "see exception below" →
   notes on the card.
3. **Unknown clause = unparsed row:** any clause not in the registry → the row is flagged (`review_reason` "new
   wording: '…'"), its verdict `q.u`, the app shows DFO's words for that row, never a number.
4. **Water page shows its unparsed items:** a water whose DFO rows include an unparsed one shows a visible block on
   its card: "DFO changed the wording here — read their text" + the row's species, dates and DFO's words.
5. **Urgent alert (feeds worker):** a new unknown clause on a live run → URGENT owner email: the wording, the place,
   the region, the FN, the full cell, and what to do (add a registry entry + test line). Once per new template, not
   per run.
6. **Tests:** every saved page reads with zero unknown clauses; a mutation test (an unseen clause in a known cell →
   flagged + alert); TS-vs-Python parity on the registry; the three gap wordings.

Depends: T320. Size: M. Owner: review the seeded registry entries (proposals).
