# T781 — Skeena cascade review on the built release: what the app shows that is wrong or needs the owner's call

Status: OPEN (owner answered 2026-11-16, batches 5 and 6 in old owner to-do "Answers by batch"; each item is carried elsewhere: 1 Babine Lake → the 7 matching circles confirmed, the rest a later owner item, split and circles T812; 2 Gosnell / Thautil → T804 (one stated rule); 3 Gitnadoix → powerline on the mouth (T803, T833: 5debac3ef); 4 eulachon → T770 (DONE); 5 Kitimat east bank "Both banks closed (Recommended)", one-bank T754. Close when T803, T804 and T812 land)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T781 | Skeena cascade review on the built release: Babine Lake reads open inside its 13 unconfirmed closed areas, Gosnell Creek and the Thautil closed by the Morice row, Gitnadoix coho open below the powerline, the eulachon line missing | OPEN (owner answered 2026-11-16; items carried by T812, T804, T803/T833, T770 (done), T754) | — | `curation-review/skeena-cascade-review-2026-11-15.md` |

Source: `handoff/PLAN/curation-review/skeena-cascade-review-2026-11-15.md` (Part 2: 11 of 13 checks pass, one passes
with three exceptions, one fails).

## Items (each waits on the owner's answer; the recommended answer is in the review)
1. **Babine Lake, Aug 1 to 27 (errs OPEN).** The whole lake reads open for sockeye, including within 400 m of the 12
   creek mouths and east of the Gullwing Creek line: the 13 closed areas are carried, not confirmed, so they are only
   entries on the card. Recommended: keep the lake open and add an "open, closed in part" headline for those days
   (the mirror of "closed, open in part", RULINGS P47); and the owner confirms the 13 areas in the review app.
2. **Gosnell Creek and Thautil River (errs closed).** The rule book's Morice "No Fishing from Gosnell Creek to Lamprey
   Creek, Jan 1-Aug 31" binds 1,958 sections, all of Gosnell Creek, the Thautil and Lamprey Creek among them; DFO's
   same stretch binds 131, none on those creeks. Ruling T9 says a joining creek with a line of its own is not inside.
   Recommended: leave Gosnell and the Thautil out; Lamprey Creek (no line of its own) stays in.
3. **Gitnadoix below the powerline (errs OPEN).** Coho reads open Sep 1 to Oct 31; DFO opens only above the powerline.
   No cut exists. Recommended: add the powerline cut if the crossing is well above the mouth.
4. **Eulachon line** missing on the Lakelse all year and on the Kitsumkalum to Jun 15; Kitsumkalum Lake has no DFO bait
   line Jan 1 to Jun 15. Likely the same cause as T770 (gear lines do not stack). Recommended: always show it below
   Terrace.
5. Kept as printed unless the owner says otherwise: tributaries inside a salmon stretch (Kispiox, Morice, Lakelse);
   sockeye Sep 1 to 15 near the Kitsumkalum mouth; "a point above the Babine" at the gauge; the Kitimat east bank
   closed until T754.

## Notes for other tickets
- T771: the Skeena has 14 same-tier pairs that disagree, none decided by id today.
- A rest-of place with no sections of its own reads `outcome: unresolved, reason: no_sections` in the reach table
  (the bundle handles it); audits that count unresolved DFO rows count it (T775 / T776).

## Owner

- [x] 2026-11-16 — Babine Lake in August, 13 closed areas → "i think you can confirm them. it is just confluences and unless the names dont match they are easy to auto confirm" (2026-11-16 11:03).
- [x] 2026-11-16 — Gosnell Creek and the Thautil River inside the Morice closure → "yes they are out but please make sure this rule doesnt break other stuff. we need a trigger for why they are out? why does this tributary rule not apply? what is the condition just so it is a universal rule."; Gitnadoix, coho "upstream of powerline" → "the powerline is. the mouth essentially so maybe you could add the cut for powerline to the mouth then it is all good?" (2026-11-16 11:03).
