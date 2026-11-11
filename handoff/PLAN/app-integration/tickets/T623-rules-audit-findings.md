# T623 — Rules as shown vs the sources: 21 findings from the 48-water audit

Status: PARTIAL — left (Claude): after the T637/T660 atlas rebuild, reach + deliver (F5 incl. Bowron/Willow, F6, F8, F12, F14, F15 then show; the F5 bundle tests are red until then); feeds-chat hand-over F3/F13; owner: tick the F2/F9 cuts in the review app

Source: T608 part A, `handoff/PLAN/app-integration/T608A-findings.md` (commit ef14f3b83): 48 waters, 34 clean, no wrong
value in any regional table; 21 findings F1–F21 (5 high, 7 medium, 9 low), each with the rule as shown, the source
text and printed page. Only 5 are certain; 16 need the owner's reading. 15 more waters are listed apart as DFO
placement/coverage, waiting on the owner's labelling: not part of this ticket.

## Work
1. **Certain ones first** (the findings file marks them): among them F3 (Campbell River salmon: DFO's chinook-only
   "to be determined" shown for all five species) and F13 (Squamish coho short line drops "hatchery marked only").
   F3 and F13 are in the DFO feed's modules, which the review-app chat owns: hand those two over with the evidence.
2. **Owner's reading** for the rest, as proposals in the review app or a short list he answers, never changed on
   Claude's reading alone. The high ones: F1 (Campbell River closed Jul 15–Aug 31 under the Region 1 summer closure
   though the book lists it in unit 1-10), F2 (Campbell parts 4–5 at Elk Falls), F4 (Shuswap and Quesnel bay/arm
   closures undrawn: keep limits shown lake-wide, the closure only a "not yet mapped" note; Salmon Arm Bay is closed
   today), F5 (Nechako part 5 and the Region 6 Fraser-watershed spring closure).
3. For each answered finding: fix at its cause (parse, placement, cascade, feed wording, display), with a test that
   pins the printed page; where one finding stands for a class (F4: part-of-lake closures; F15: "on parts" lost from
   vessel rule titles; F11: an in-season closure that is only a banner), fix the class and count how many waters move.
4. After the owner finishes DFO labelling, re-run the audit's DFO placement list.

## Acceptance
- Every F row is fixed with a pinned test, or answered "as shown is right" by the owner with his words recorded.

Depends: T553 (base rules pinned), T546 (reach run). Size: L. Owner: yes (16 readings).

## Owner readings 2026-11-11 (12 of 16; F2, F9, F16, F21 wait for map shots)
- **F1 Campbell River:** the river straddles the unit boundary for a stretch. Only the stretch that straddles a unit in
  1-1 to 1-6 takes the Jul 15-Aug 31 summer closure; the rest (unit 1-10 only) does not. Show which parts straddle, on a map.
- **F4 part-of-lake closures (Shuswap, Quesnel, every lake like them):** a clear warning on the card for all such lakes now.
  Splitting the lakes properly is far future.
- **F5 Nechako inside Region 6:** closed Apr 1-Jun 30. The "Exempt from spring closure" on the Region 7 row lifts Region 7's only.
- **F7 Fraser steelhead notice (Region 3):** the banner stops at the Region 3 boundary.
- **F8 Fraser, Regions 5/7:** upstream of the Cottonwood River the exemption LIFTS the bait ban, Region 5's too ("typically I
  would say it holds but this is explicit"). Needs a cut at the Cottonwood confluence; below it Region 5's ban holds.
- **F10 Babine above the Fort Babine Bridge:** not fly only. As shown is right.
- **F11 in-season closures:** parse in-season notices the same way as normal regulations, so a closure changes the card.
  That needs a rebuild; between a new notice and the rebuild that carries it parsed, a good banner shows so it is not lost
  (an unparsed notice is the banner). Design ticket needed; the parser run is the owner's.
- **F12 Stellako:** release all rainbow. General rule: a rainbow rule with no size constraint applies to both steelhead and
  ordinary rainbow, since it did not specify.
- **F17 Lakelse Lake:** no bait ban. By the book's definitions page, tributaries are streams.
- **F18 Babine salmon:** sockeye only. The notice itself says "closed to recreational fishing for Sockeye Salmon". As shown is right.
- **F19 Stellako top stretch:** nothing to add. The river already carries "No Fishing Nov 15-May 14; Class II water when
  open"; the book's pointer is a note for people looking at the lake.
- **F20 Fraser tidal stretch:** a freshwater notice starts at Mission, not on the tidal stretch.

## Owner readings 2026-11-11, the four map ones (maps in handoff/PLAN/app-integration/T623-shots/)
- **F2 Campbell at Elk Falls:** the no-fishing water is Elk Falls down to the John Hart generating station (1.86 km below the
  falls). Fly only starts at the station and runs down to the power line near the Quinsam. Above the falls: the river's ordinary rules.
- **F9 Campbell power line:** the cut goes to the cleared power-line strip about 320 m above the Quinsam mouth (today it sits
  1,332 m up, beside the station).
- **F16 Little Qualicum:** Upper Waterfall, as today. The book says only "the falls in Little Qualicum Falls Provincial Park".
- **F21 Peace at Clayhurst reserve:** closed, as today (the reserve covers the north half of the channel).
All 16 readings are now answered. The cuts for F2/F9 (and F8's Cottonwood cut) are proposals for the owner's tick in the review app.

## Close-out 2026-11-11 (per finding)

Pending patches are in `handoff/PLAN/app-integration/T623-pending/`. Each passes `git apply --check` on HEAD. None of them
touches an atlas-recorded input or pipeline/atlas/. Apply order: F7-F20-review-save-region now (commit a0f8adb49 refuses
In-season saves without it). Then, after the atlas rebuild: F5, F6, F8 (back up region-*.json first, AGENTS A2), F7-F20-notice-waters
(or re-run `python -m pipeline.regs.inseason.resolve` on the new release), F2-F9 proposals. Then reach + deliver.

| F | Outcome | Where / test | Waters that move | Shows in the app after |
|---|---|---|---|---|
| F1 | As shown is right by the owner's own reading. The centre line IS the 1-6/1-10 line from km 0.95 to 49.5 (within 1 mm), so parts 1-5 and 7-10 straddle and are closed. Part 6 (331 m below Ladore Dam) is 1-10 only and is open. Map: `T623-shots/F1-campbell-units.png` | d8419a5c2 `test_border.py::test_p13_a_river_drawn_on_the_mu_group_line_straddles_it_and_takes_the_summer_closure` | 0 | already shown |
| F2 | Proposal awaiting tick: sg1 = John Hart generating station at 5644 m (1835 m below Elk Falls, 7479 m). No fishing from the falls down to there | patch `F2-F9-campbell-cut-proposals.patch` (split_suggest RESEARCH r1/r7, confidence high, the owner's words quoted) | after tick | tick → cuts.json → atlas → reach → deliver |
| F3 | Handed over (feeds chat), see below | — | 3 locations | worker deploy + DFO_REV bump; bundle for the release fallback |
| F4 | Fixed: a "Part of this lake is closed today / closes at times" block first on the Keep tab, with the rule's words, dates and part (core `caveats.partClosedOf`, ui-native `CaveatBlock`, the same on web and native) | 682c884f4, `caveats.test.ts` (T623 F4), `keep.test.ts` | 38 waters (37 lakes + Little River), 43 rules | next app build |
| F5 | Code fixed: `Exempts.own_region_only` (no carry under G3). Corpus flag pending. RULINGS L22 | c577f682b, `test_region_homes.py::test_f5_*`; patch `F5-nechako-own-region-only.patch` (+ `test_lift_decisions.py::test_f5_*`) | 18 sections (Nechako part 5) + Bowron 11 + Willow 5 (Region 5) closed Apr 1-Jun 30 | reach → deliver |
| F6 | Certain. Pending: z7b kokanee stream release `outside_items: [gnis:14619]` | patch `F6-peace-outside-zone-b-kokanee-stream-release.patch` (+ `test_rulings_pinned.py::test_f6_*`, RULINGS Z2 note) | 1 section (Peace part 9): keep 10 | patch → reach → deliver |
| F7 | Fixed: a notice covers only fresh water of its own region (`inseason/resolve.py`). Stored ranges pending | a0f8adb49, `test_inseason_publish.py::test_f7_*`; patch `F7-F20-notice-waters.patch` | 3 notices: is-14c1a6f972a3 183→28 sections, is-06e7bf41d7fc 113→109, is-6a32433916ac 561→512 | patch or re-resolve → inseason publish |
| F8 | Cut already built (Fraser 673.61 km). Pending: r7 fraser r2 also lifts `r5:fraser_river@5-2` r3. RULINGS L23 | patch `F8-fraser-cottonwood-lifts-region-5-bait-ban.patch` (+ `test_displacement_rulings.py::test_f8_*`) | 2 sections (part 18) | patch → reach → deliver |
| F9 | Proposal awaiting tick: sg2 = power line about 320 m above the Quinsam mouth, at 4519 m ([-125.298018, 50.038902]), replacing the curated 5534 m as fly-only's lower end | patch `F2-F9-campbell-cut-proposals.patch` (r5) | after tick | tick → atlas → reach → deliver |
| F10 | As shown is right | 3884405fd `test_rulings_pinned.py::test_t623_f10_*` | 0 | — |
| F11 | Design below (no build) | — | 236+ waters under 5 notices | — |
| F12 | Fixed: a size-free rainbow rule speaks for steelhead too (`read.size_free_rainbow`). RULINGS S12 | 89206b670, `test_competition.py::test_f12_*` | 28 named waters / 713 sections (Similkameen + R2 tribs, Stellako, Shuswap tribs) | deliver |
| F13 | Handed over (feeds chat), see below | — | 10 verdicts / 11 locations | worker deploy + DFO_REV bump |
| F14 | Fixed: a two-table card names both regions in one voice (`groups.tables_words`) | fc832cae0, `test_t183_two_regions_card.py::test_f14_*` | 51 rows, 10 region pairs | deliver |
| F15 | Fixed: "on parts"/"on part" kept on the title and the boat chip (`labels.on_parts_words`) | 19580fc9a, `test_catalogue.py::test_f15_*`, `boats.test.ts` | 30 rules on 26 waters | deliver |
| F16 | As shown is right (Upper Waterfall) | 3884405fd `test_t623_f16_*` | 0 | — |
| F17 | As shown is right (the Skeena bait ban is streams only) | 3884405fd `test_t623_f17_*` | 0 | — |
| F18 | As shown is right (sockeye only). DFO feed; no pin | — | 0 | — |
| F19 | As shown is right; already pinned by `test_rulings_pinned.py::test_l14_*` (every Stellako section closed Nov 15-Jun 30) | — | 0 | — |
| F20 | Fixed with F7: a freshwater notice never covers tidal water | a0f8adb49, `test_f20_*`; same patch | 2 notices: is-8cd37f6e5bf9 54→34, is-a8dcfeb1e3c6 61→40 | as F7 |
| F21 | As shown is right | 3884405fd `test_t623_f21_*` | 0 | — |

**Owner answers 2026-11-11** (to the close-out's questions):
- F1: "Yes, as shown."
- F5 class, Bowron and Willow: "Yes, same rule". Both Region 7 rows' "Exempt from spring closure" carry `own_region_only` (region-7.json, flag lines only). Their Region 5 sections close Apr 1-Jun 30 under Region 5's spring stream closure: Bowron 11 sections, Willow 5. RULINGS L22 extended; `test_lift_decisions.py::test_f5_bowron_and_willow_*` (needs the next reach + deliver).
- F6: "Keep it" (the Peace exclusion stays encoded).
- F20: "Right, tidal is out."
- F12 (Stellako part 0 still shows the provincial "10 hatchery steelhead" line): not asked yet; left as is.

The pending patches were applied in ea1c74d67 and 2e68ff4c9 (the owner allowed both review-app patches).

**Not verified:** no reach run or deliver, so F5/F6/F8/F12/F14/F15 were measured with the reader on the live bundle, not on a build. The F4 look was not seen on a device. Not run: `test_export_codec.py` and `test_export_ui_rules.py`. Failing before T623 (still red): `test_t183` copper/mutation (fixed by F12's flag), `test_answers_r14_wording::test_3_*`, `test_answers_r18_tree::test_t203_*`, and `app/tools/one-colour-pathway.test.ts`.

## Hand-over to the DFO feed chat (F3, F13; pipeline/regs/dfo_salmon + app/workers/feeds/src/dfo)

**F3, a one-species "To be determined" row answers all five species.**
- DFO Region 1 (cache 2026-11-06): "downstream of the confluence with the Quinsam River to the fishing boundary sign at the end of Maple Street. | Chinook | To be determined".
- Cause: the TBD advisory is typed with no `species` (`typed.py:to_rules` ~L383; `typed.ts:toRules` ~L611). `verdict_on` / `verdictOn` treat no species as every species, so the chain never reaches the wider "downstream of the Quinsam" place (Coho Sep 1-Dec 31 4 hatchery-marked; Pink Jul 15-Oct 15 4/day).
- Same bug: Courtenay (`1:courtenay-river-the-waters-below-t:whole`) and Puntledge (`1:puntledge-river:downstream-from-the-base-of-stotan`), where a chum TBD row speaks for the other four.
- Change: pass `species=codes` on that advisory in both files. "All | TBD" stays SA. Bump `live.ts` `DFO_REV` (otherwise unchanged pages are not re-typed).
- Tests: `test_dfo_salmon.py::test_a_one_species_tbd_row_speaks_only_for_its_species` (CH check, PK open Jul 15, CO open Nov 10, SK None). A worker vitest in `dfo.dates.test.ts`.

**F13, the short line drops "hatchery marked only".**
- DFO Region 2: "Squamish River (including Powerhouse Channel) | Coho | Sep 15 to Jan 31 | 1 per day, hatchery marked only".
- Cause: the quota quote regex stops at "per day" (`typed.py` ~L436; `typed.ts` `RE_QUOTA_SPAN` ~L252). The app's short line is the verdict `t`.
- Change: append `(?:,?\s+hatchery[- ]marked(?:\s+fish)?\s+only)?` to both patterns. Checked on every cached "per day" cell: only the 5 "hatchery marked [fish] only" cells change.
- Affects 10 verdicts at 11 locations: Cluxewe, Conuma, Little Qualicum, Nanaimo, Quatse, Somass, Stamp, Cheakamus, Fraser, Mamquam, Squamish.
- Tests: `test_dfo_salmon.py::test_the_quota_quote_keeps_hatchery_marked_only`, and `dfo.quota.test.ts` (T623 F13).

Ship both in one worker deploy with the DFO_REV bump, then the pipeline change before the next bundle. No app change is needed. The exact diffs:

```diff
--- a/pipeline/regs/dfo_salmon/typed.py   (to_rules, the TBD branch)
     if _RE_TBD.search(gear):
         return [CatalogueRule(rule_id=rid, type=RuleType.advisory,
                               review_reason="DFO has not set this row yet ('to be determined')",
-                              **base())]
+                              species=codes, **base())]
--- a/pipeline/regs/dfo_salmon/typed.py   (to_rules, the quota quote)
-        quoted = _span(gear, re.compile(r"\b\d+\s+(?:hatchery[- ]marked\s+)?per\s+day", re.I))
+        quoted = _span(gear, re.compile(r"\b\d+\s+(?:hatchery[- ]marked\s+)?per\s+day"
+                                        r"(?:,?\s+hatchery[- ]marked(?:\s+fish)?\s+only)?", re.I))
--- a/app/workers/feeds/src/dfo/typed.ts
-const RE_QUOTA_SPAN = pyRe("\\b\\d+\\s+(?:hatchery[- ]marked\\s+)?per\\s+day", "i");
+const RE_QUOTA_SPAN = pyRe("\\b\\d+\\s+(?:hatchery[- ]marked\\s+)?per\\s+day(?:,?\\s+hatchery[- ]marked(?:\\s+fish)?\\s+only)?", "i");
   if (search(RE_TBD, gear) || cell?.kind === "tbd")
-    return [make({ rule_id: rid, type: "advisory", review_reason: "DFO has not set this row yet ('to be determined')" })];
+    return [make({ rule_id: rid, type: "advisory", species: codes, review_reason: "DFO has not set this row yet ('to be determined')" })];
--- a/app/workers/feeds/src/dfo/live.ts
-export const DFO_REV = "2026-11-09.1";
+export const DFO_REV = "2026-11-11.1"; // T623: a TBD row carries its species; the quota quote keeps "hatchery marked only"
```
These were checked in memory against the real `to_rules` / `verdict_on`. The TS side was not run.

## F11 design: an in-season closure changes the card (owner 2026-11-11)

Owner: "parse in-season notices the same way as normal regulations, so a closure changes the card. That needs a rebuild;
between a new notice and the rebuild that carries it parsed, a good banner shows so it is not lost (an unparsed notice is
the banner)."

1. **Notice → catalogue entry.** Each matched notice (`data/curated/inseason/notice_waters.json`, its range already region- and tidal-clipped since F7/F20) becomes an entry in a new corpus file `synopsis/region-inseason.json`. It goes through the same parser prompt as a book row (the owner runs it: `run_parse.sh`, never Claude). It is keyed `is:<notice id>`, with `source: "inseason"`, the notice's own dates, and extents = the match's range (a `between` on its cuts, or `whole`).
2. **Authority.** An in-season rule ranks above a water row (a superior closure, like an ecological reserve). It lifts nothing unless the notice says so ("rescinded" removes the entry, never adds a lift). The reader (`effective_rules`) needs no new step if it ranks as an `Authority`. Add a `RULINGS` line when built.
3. **Carried state.** The bundle records each notice's id + content hash it was parsed from. The feeds worker publishes the live notice list with the same hashes.
4. **The app, per notice:** hash in the bundle = parsed, so the card speaks it and no banner shows (or a slim "in-season notice" source tag on the rule). Not in the bundle (new or changed) = the banner, as today, with its range. Withdrawn (in the bundle, gone from the feed) = a banner "this in-season closure has ended; the card catches up at the next update", and the card's rule is suppressed by id. The app composes nothing; the worker ships `{id, hash, state}` and core picks banner vs card.
5. **Gates.** A notice parsed into the bundle whose hash differs from the feed's shows the banner (never both silently). A test pins the three states. `check_corpus` covers the new file. Count to measure first: the 5 window notices (Similkameen, Okanagan, Salmon R3, Bessette, Granby).
6. **Steps (future ticket, owner to file):** schema + entry file; parser prompt for notices; reader authority + RULINGS; bundle hash record; worker `{id, hash}`; app three-state; fast-fail item.

## Verification against the build 27 reach run (2026-11-11, read-only)
The new reach tables were compared with the live run, filtered to T623's rules.
- **F6:** `z7b:daily_quotas` species_quotas.r5 went from 207,669 to 207,566 sections (-103, +0). All 103 are Peace River (`gnis:14619`) sections, which is every Peace section the release bound (104 sections; one was never bound). That is right: `outside_items` takes the whole river out ("except Peace River", p.64).
  - The ANSWER changes on part 9 only (1 section): kokanee 10 instead of release.
  - On the other 102 the release was already silenced. A Peace row's own "Kokanee daily quota = 10" beat it, or it stood under a closure. This was simulated on the live bundle by removing r5 from every Peace rule set; it has not yet been seen on a bundle.
- **F5 (Nechako 18, Bowron 11, Willow 5), F8 (2), F12 (713):** no reach change, as expected. The rows bind the same sections (Nechako r1 92, Bowron r1 97, Willow r1 59, Fraser r7 r2 114). The no-carry flag, the extra `exempts` target and the size-free-rainbow rule all act when the bundle is built (`bundle.rules.lifts`, the reader). Proof after the build, on the new bundle (the needs_bundle tests are red on today's bundle):
  - `test_lift_decisions.py -k f5`
  - `test_displacement_rulings.py -k f8`
  - `test_competition.py -k f12`
  - `test_rulings_pinned.py -k "f6 or t623"`
