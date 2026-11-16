# T796 — A stretch the book closes still shows an open salmon card; a DFO "Finfish closure" closes only the salmon card

Status: OPEN (owner: OWNER-TODO "2026-11-16 morning list" questions 29 and 30; app core salmon card + pipeline).

Source: general audit 2026-11-16, answers checker A5 and A6 (`handoff/PLAN/audit-2026-11-16/answers.md`).

## A5. Closed stretch, open salmon card (errs OPEN)
Rule book p.77: "when fresh waters are closed to fishing, or have gear restrictions outlined in this Synopsis, the
regulations apply to fishing for salmon as well". Seen: Skeena parts 2 and 4 (ecological reserve) on Aug 15, "Closed
all year" beside "Open today" for sockeye; Atnarko parts 2 to 4 on Jul 15 (chinook); Cowichan below Mile 66 on Oct 10
(closed beside "Release only today"). The page's gear rules reach the salmon card (`gearProvincial`); its closures do
not. Read from the app's view model, not seen on a device.
Recommended: the salmon card reads closed when the page is closed by a full closure, with DFO's row kept as text.

## A6. DFO "Finfish closure" (errs OPEN for trout)
Ten DFO rows (Stamp 4, Nitinat 3, Somass, Sproat, Tahltan) say "Finfish closure"; the app closes salmon only. The
Stamp hatchery-lagoon stretch on Oct 10 shows open with a trout card. `typed.py` reads the row as a salmon closure.
Recommended: a finfish closure closes the whole page, trout included, on its dates.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 29, typed: "yes we want to do this but can you bring the example changes to me so i can see why it happened etc? but yes we want it to do this". Question 30: "Closes the whole page (Recommended)". Split agreed with the app chat (11:55): A5 = pipeline adds the book's full closure as the first member of the stretch's salmon chains (a release-side location marked static), the app keeps a static location under a live feed and words the card; A6 = pipeline `typed.py` + deliver + vectors first, then the worker's twin, then core closes the page from the feed. The before / after examples (Skeena reserve sockeye Aug 15, Atnarko chinook Jul 15, Cowichan below the trestle Oct 10) go to him with the why.

## Lane result A6 (lane/salmon-rules-1116, 2026-11-16)
- `typed.typed_rules` (the one way a row is typed: `feed.type_rows` and the vectors' hand rows): a row whose limits
  cell (less a part-closure sentence) says "Finfish closure" keeps its salmon closure (retention_limit, take 0,
  may_target false, species SA) and that rule carries `finfish: true`. `deliver.salmon.verdict_on`: a closed verdict
  answered by such a rule carries `f: 1`. `typed.as_catalogue` strips the DFO marks (`hidden`, `finfish`) where a
  reader validates against `CatalogueRule` (splitwork, the review detail).
- THE FIELD FOR THE APP CHAT: `f: 1` on a verdict object (payload `verdicts[i]`, and the live feed's `verdicts`),
  present only on `s: "closed"` verdicts from a finfish closure; absent otherwise. Meaning: on that day ALL fishing is
  closed on the chain's stretch, trout included. Read it from the same per-species verdict runs the salmon card reads:
  the page is closed on a day when any species' verdict of the page's drawn chain carries `f`.
- Worker twin: `typed.ts` adds `finfish: true` on the retention_limit closure (may_target false, no `within`) of a
  row matching /\bfinfish\s+closure\b/i outside a part-closure sentence; `salmon.ts` `verdictOn` adds `f: 1` when the
  verdict is closed and a speaking rule has `finfish`.
- Vectors moved: pages 1 (9 locators) and 6 (1 locator): `rules[0].finfish: true`, `calendar.verdicts[0].f: 1`;
  `live.verdicts` 31, 37, 38, 50, 51, 56, 57, 58, 218 gain `f: 1`. Nothing else moved.
- Release (salmon step rebuilt): 11 chains, 2,638 sections gain `f` on the closure's days, no other verdict changes:
  Tahltan including tributaries (2,595 sections, Jun 1-Aug 31), Stamp Ash River to the Great Central Lake dam (33,
  Jan 1-Apr 30), Stamp above Girl Guide Falls (2, Jan 1-Apr 30), Stamp hatchery lagoon (1, Jan 1-Apr 30 and
  Aug 25-Oct 31), Stamp powerline to the lagoon (1, Jan 1-Apr 30 and Oct 1-14), Stamp Falls (1, Jun 15-Nov 15),
  Somass below the gravel pit signs (1, Aug 25-Nov 15), Sproat below the lake (1, Jun 15-Nov 15), Nitinat Red Rock
  Pool (1, Aug 25-Dec 31), hatchery intake to the Little Nitinat (1, all year), bridge (1, Aug 25-Oct 15).
- Tests: `pipeline/tests/test_t796_finfish_closure.py` (3; mutations: no `f` fails 2, no `finfish` fails 3).
