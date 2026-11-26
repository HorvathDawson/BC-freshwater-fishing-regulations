# T668 — Audit: a DFO place that covers one river stretch in more than one region (Fraser, Regions 2 and 3)

Status: PARTIAL — audit done; fixes 1–3 built in 20afd21e6; RULINGS Z4a (owner 2026-11-11: "straddles should always take the most strict rules from both regions") built in dfb4ea7c1 with the freshwater comparison primitives, the rest of unification is T669 on D1 + the owner's straddler answer to D2/D3 (2026-11-11: "some streams genuinely have 2 regions" — a straddler keeps both regions' places, strictest per fish); measured in T668-audit.md "Fixes 1–3 as built" (432 of 493 sections change, 197 straddlers keep both, 84 status flips toward closed); app half + fix 4 built in d199f0d4d (D1 and D4 confirmed by the owner 2026-11-11, "Yes and yes", with the Skeena addition: a row names DFO's lettered section as printed, "Region 6 · Skeena River · B(ii)"; see "App half" below); option 2 (the Python pair table, owner 2026-11-12) measured safe and built in c9488453c; the feeds worker reads dfo-pairs/1 and the pairs are uploaded (staging 2026-11-18, "upload done (dfo-pairs ok)"; production with r-6258ffded676); fix 5 hygiene settled by reading (lane L10, 2026-11-22: see "Fix 5" below, nothing to change); owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T668 | Audit: a DFO place covering one river stretch in more than one region (Fraser, Regions 2 and 3) | PARTIAL | 20afd21e6, dfb4ea7c1, d199f0d4d, c9488453c | owner 2026-11-11; audit in git history (T668-audit.md); fixes 1–3, Z4a, 4 built; pairs live (staging 2026-11-18); fix 5: nothing to change (L10 11-22); owner: see ## Owner |

Source: owner 2026-11-11: "look into how DFO places that match to multiple regions, same river section, like Fraser
2/3, is handled. To audit it."

## Question
DFO lists salmon places per region. Where one physical stretch is named under two regions (the Fraser under Region 2
and Region 3; likely also other boundary rivers), what does each step do with it, and what does the angler see?
Audit and measures: the T668 audit (git history: T668-audit.md).

## Done
- Fixes 1–3 (20afd21e6); RULINGS Z4a (dfb4ea7c1; chain `h` gives the card's hours line).
- App half + fix 4 (d199f0d4d): the "in part / not yet mapped" caveat lists only the page's chains' undrawn places (`u`);
  `withSalmonFeed` composes a chain's `s` groups as `live.compose` does, pinned by `core/src/salmonStraddle.golden.ts`
  (cut by `python -m pipeline.tests.test_t668_salmon_who --write`, `--check`); every chain carries `a` (the place that
  answers, `pipeline/deliver/salmon_who.py`) and `p`, so a species row says "Region 6 · A", "Region 6 · Skeena River · B(ii)",
  "Region 7", "Region 6 and Region 7 together". A release without `a`/`p` (r-303b4007a70a) names nothing.
- Option 2 (owner 2026-11-12, below), c9488453c, RULINGS Z4b: `pipeline/deliver/salmon_pairs.py` → `sync_inputs` `dfo-pairs`
  → `private/feeds/dfo/pairs.json` (`dfo-pairs/1`) → the worker's `feeds/dfo/index.json` `pairs` → the app's
  `salmonFeed.ts` `strictestIx`; a pair it does not hold keeps both regions' words — never looser. Measured over
  r-483b295de57c: 17 two-region chains, 197 sections, 2 distinct pairs, both resolve to both words: nothing moves today.
  Pairs uploaded (staging 2026-11-18, "upload done (dfo-pairs ok)"; production with r-6258ffded676). Worker pin:
  `app/workers/feeds/test/dfo.worker.test.ts` "(T668) the feed answers per place" (pinned in 55cfffde4).
- Fix 5 (lane L10, 2026-11-22): nothing to change. The `6:fraser-river` water record stays (the owner's note 2026-11-03:
  "the same river as region 2's Fraser River, so R6's header exception (adult chinook \"other than Fraser River Watershed\")
  finds the Fraser-side waters of Region 6. No R6 DFO place names it."); both active Region 2 Fraser duplicates are
  `duplicate_confirmed: true`.

Owner 2026-11-12: Live straddler: "If python pair table is safe that sounds good we want these to always resolve on their own" - option 2, the Python pair table, provided it measures safe (no longer default 3). Card wording: accept as built.

## Left
- Not done (d199f0d4d): the stretch join key (`salmonTimelineKey`) still ignores which place answers, so two stretches with
  the same words from different regions join and their rows list both places.
- The owner's phone check (## Owner).

## Acceptance
- The owner can read, for every such place, what the app says today and why; each defect has a ticket.

Depends: T164, T549. Size: M. For the next chat.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-11; audit in git history (T668-audit.md); fixes 1–3, Z4a, app half + fix 4 and option 2 (c9488453c, RULINGS Z4b) built; pairs live (staging 2026-11-18 'dfo-pairs ok'); left (Claude): fix 5 hygiene; owner: see ## Owner (phone check of that release)

## Owner

- [ ] 2026-11-11 — After the release that carries the straddle rule, phone-check a stretch in two DFO regions: the card names the region and the Skeena section letter, the stricter answer per fish speaks. All rulings answered ("Yes and yes", "straddles should always take the most strict rules from both regions").
