# T789 — Seven wording findings of the T462 audit have no ticket (the owner's gate 1 for closed testing: each fixed or ticketed with his OK)

Status: OPEN (app core wording; before closed testing; the app chat's area for the fixes).

Source: general audit 2026-11-16, decisions checker item 7 (`handoff/PLAN/audit-2026-11-16/decisions.md`); T462 is DONE, T495 gate 1.

- K6: the Skeena weekend caveat is cut at the colon (`app/packages/core/src/regs/caveats.ts` line 193).
- K7: "Open, except some parts" when nothing is closed (`core/src/regs/strip.ts` line 148).
- K8: the Shuswap pier exemption is dropped. **FIXED dbaa009b9** (lane/bugs-080b, 2026-11-17): the Salmon Arm Bay closure and the bait ban
  each say the book's bracket (p.32) on their own line (`labels.exempted_on_an_undrawn_part`); 2 of 3,404 lines moved.
- G8: non-resident-alien weekend closures are not on the Licence tab. **DATA LANDED 95989667f** (lane/bugs-080c,
  2026-11-17): `licence_frame.alien_weekend` {says, waters [{name, key: item_id}]} (`licence.alien_weekend`): the book's
  "Angling prohibited for non-guided non-resident aliens on Saturdays and Sundays" (pp.50-53) and the 10 waters with a
  weekend closure (Babine, Bulkley, Kispiox, Kitseguecla, Kitsumkalum, Kitwanga, Morice, Skeena, Suskwa, Zymoetz);
  every-day Lakelse and Telkwa not listed. Store `licence_alien`, frame's 4th index, additive (licence stays 7). Owed:
  the app's banner + reader (reads.ts, regs-fixture.test.ts:89 takes `u[0]` only), the owner's OK on the words.
- G9: fly-only waters say "One hook or fly". **FIXED ee9fb62ff** (lane/bugs-080, 2026-11-17): `gear.summary_parts` keeps the fly
  alone in both fly modes (p.80: an artificial fly is a dressed hook); every artificial-fly-only reading now says "one fly"
  (36 rules, 35 waters: Pennask, Lois, Big O.K., Campbell, Gordon, Nanaimo, Thompson below Kamloops Lake, West Road
  and its tributaries, ...), and the dated "changes" line of a water with such a period. Ships with the next data build.
- G10: the under-16 non-resident price line. **Traced, no data change** (lane/bugs-080c, 2026-11-17): the book prints
  no under-16 price (p.5 fee table has no such row; p.6 "you must buy a licence and any necessary supplementary
  licences and stamps"); every stored under-16 non-resident/alien document already carries its own price. The bug is
  the app's: `core/src/regs/licence.ts` "Or, for your own limit" joins the documents and shows `U.buy[0]`'s price.
- D6: a Region 6 gear line is cut mid-word. **Traced, not landed** (lane/bugs-080, 2026-11-17): not the comma (the flag reads
  it); the quoted span `single,?\s+barbless\s+hook[\w\s,.]*` stops at the hyphen of "non-tidal". Fix:
  `(?:[\w\s,.]|-(?=\w))*` in BOTH twins, `pipeline/regs/dfo_salmon/typed.py` and `app/workers/feeds/src/dfo/typed.ts`
  `RE_HOOK_SPAN` (T556 parity), then `python -m pipeline.tools.emit.dfo_vectors` (the vectors go stale with the Python
  half alone) and the worker's `dfo.vectors.test.ts`; then `python -m pipeline.regs.dfo_salmon.feed` (no model) before
  the data build. Needs the app chat (app/workers). No re-parse.
Each changes words an angler reads. Fix or get the owner's OK to leave, one by one.
