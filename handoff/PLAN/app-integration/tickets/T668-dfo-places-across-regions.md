# T668 — Audit: a DFO place that covers one river stretch in more than one region (Fraser, Regions 2 and 3)

Status: PARTIAL — audit done; fixes 1–3 built in 20afd21e6; RULINGS Z4a (owner 2026-11-11: "straddles should always take the most strict rules from both regions") built in dfb4ea7c1 with the freshwater comparison primitives, the rest of unification is T669 on D1 + the owner's straddler answer to D2/D3 (2026-11-11: "some streams genuinely have 2 regions" — a straddler keeps both regions' places, strictest per fish); measured in T668-audit.md "Fixes 1–3 as built" (432 of 493 sections change, 197 straddlers keep both, 84 status flips toward closed); app half + fix 4 built in d199f0d4d (D1 and D4 confirmed by the owner 2026-11-11, "Yes and yes", with the Skeena addition: a row names DFO's lettered section as printed, "Region 6 · Skeena River · B(ii)"; see "App half" below); shows after the next deliver; option 2 (the Python pair table, owner 2026-11-12) measured safe and built in c9488453c; left (Claude): deploy the feeds worker that reads dfo-pairs/1, then sync the inputs; fix 5 hygiene; owner: OWNER-TODO "T668 salmon places from two regions on one stretch"

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

## App half (2026-11-11, d199f0d4d)
- Fix 1 on the card: the "in part / not yet mapped" caveat lists only the undrawn places the page's chains name (`u`),
  so Prince George no longer lists Mission (`core/salmon.ts salmonCard`). A release before 20afd21e6 named every
  region's undrawn places in `u`, so it reads as it did.
- `withSalmonFeed` composes a chain's `s` groups as `live.compose` does: the most strict status per fish, both regions'
  gear lines, a merged both-words verdict added to a copy of the feed's tables. Parity is pinned by
  `core/src/salmonStraddle.golden.ts`, cut by `python -m pipeline.tests.test_t668_salmon_who --write` (`--check`, and
  its pytest, fail when stale). Known difference, not ported on purpose (the cascade is resolved in Python): two
  READABLE live openings of different limits — the release keeps the stricter (Z4 ladder), the app under a live feed
  shows both regions' words, no number. Removing it needs the worker to publish composed straddler chains.
- Fix 4 (D4 + owner's Skeena addition): every chain carries `a` (per species, the place that answers: an index into
  `l`, -1 none, -2 both regions together; `pipeline/deliver/salmon_who.py`, read off the composed answer, never
  re-decided) and the payload `p` (each place's region, water and DFO section exactly as scraped). Each species row
  says "Region 6 · A", "Region 6 · Skeena River · B(ii)", "Region 7", "Region 6 and Region 7 together". Region 6 is the
  only page with lettered sections in the scrape (137 active places carry one; elsewhere 2 places in R4/5B do, the
  rest show region and water only). A release without `a`/`p` (r-303b4007a70a) names nothing — the card as it was.
- Chain `h` (dfb4ea7c1): the card's hours line comes from `h` where a chain has it.
- Not done here: the stretch join key (`salmonTimelineKey`) still ignores which place answers, so two stretches with
  the same words from different regions join and their rows list both places.

## Worker lane 2026-11-11: the live two-openings difference is NOT closed (pinned in 55cfffde4)
The release keeps the stricter of two readable openings on a straddler (`deliver.salmon.stricter_opening`: the freshwater
`ladder.stricter` + `rules.statement`); under a live feed the app shows both regions' words, no number (never looser).
The worker cannot publish the composed answer today: it would need a TypeScript copy of that ladder (refused: the cascade
is resolved in Python) and the release's chains (`chains[].s`), which it does not hold. Pinned:
`app/workers/feeds/test/dfo.worker.test.ts` "(T668) the feed answers per place". Options, none built:
1. Python composes at feed time: the worker starts a GitHub workflow (as it does for HYDAT) when the feed's verdicts on
   straddler places change; the workflow runs `live.compose` over the published feed and the live release's chains and
   writes `feeds/dfo/straddlers.json` (chain places -> the composed answer, stamped with the feed's `at`); the app reads
   it when its stamp matches the feed and keeps today's both-words answer otherwise. Needs the branch on GitHub, the
   dispatch token, and an app reader. Exact, Python only.
2. Python ships a pair table with the inputs (`sync_inputs`): the stricter of every pair of readable openings in the
   curated places' current rows. The worker carries it into the feed; a pair it does not hold stays both words. No new
   runner, but it is only as new as the last sync from the Mac, so a limit DFO changes in season falls back to both words.
3. Leave it: both regions' words under a live feed. Never looser than the release, and the next release is exact again.
Default taken: 3, until the owner picks.

## Owner 2026-11-12
Live straddler: "If python pair table is safe that sounds good we want these to always resolve on their own" - option 2, the Python pair table, provided it measures safe (no longer default 3). Card wording: accept as built.

## Option 2 built 2026-11-12 (c9488453c), after it measured safe
Measure first (`deliver.salmon_pairs.pairs` over r-483b295de57c and today's typed rows): 17 two-region chains, 197
sections; per species and day where both regions' places open with different verdicts there are **2 distinct pairs**
(one chain, 19 sections): "4 per day, only 1 over 50 cm" beside "4 per day, only 1 over 65 cm", and "4 per day, only 2
over 50 cm" beside "4 per day, only 1 over 50 cm". The Python resolves BOTH to both regions' words (the caps mix), so
the table's answer equals what the app already shows: **nothing moves today, nothing toward open.** Checks: 0 answers
looser than either input; 0 differ from the release's own chain verdict; the only order dependence is the order of the
two regions' words in a both-words answer, and the table keys the fold order the app uses (regions by number), so it is
not ambiguous.
Built: `pipeline/deliver/salmon_pairs.py` (the table: `[a, b, stricter_opening(a, b)]`, keyed by both verdicts' whole
content); `sync_inputs` input `dfo-pairs` -> `private/feeds/dfo/pairs.json` (`dfo-pairs/1`); the worker reads it and
carries it into `feeds/dfo/index.json` as `pairs` (robust mode; missing: logged, no pairs; not a table: the run stops);
the app (`salmonFeed.ts` `strictestIx`) looks the pair up, and a pair it does not hold (a limit changed since the last
sync) keeps both regions' words — never looser. Shared pin: `salmonStraddle.golden.ts` now has two READABLE coho
openings (Region 6 E 4 a day, Region 7 2 a day) composed by `live.compose` with the table; the app reproduces it, and
without the table shows both words (mutation). RULINGS Z4b.
Order: deploy the feeds worker first (its /status lists `dfo-pairs/1`); until then the input upload refuses
pairs.json and says so.
