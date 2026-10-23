# T109 — One entry-id grammar: tier, region and zone parsed about 10 ways (z7a shows as "Region 7")

Status: DONE — 6c03f5fe (step 1), 89fea9c0 (steps 2 + 3, Python), 1ca9bc00 (left item: shipped region). Lanes wf_754f5fcd-d71-3, lane-T125. Needs deliver + regs fixture re-cut.
- Step 1 DONE: level.ts reads z7a/z7b as "Region 7A/7B rule" (23 entries, whole subs pinned in level.test.ts); the
  golden is held to the ruling by one counted rewrite at load (oracles.zone7Ruled: 54 level words in 8 states;
  exclusions.test.ts pins the count). No rebuild.
- Step 2 DONE (Python, pure refactor): pipeline/common/entry_id.py `parse(eid, registry=None) -> EntryId(head, kind,
  region, slug, mus, regions)` + `book_region`, `is_province/is_zone/is_table/is_water_row`, `region_of/zone_region`,
  ONE `region_words` (read.region_words IS it; display.regions_label words through it), `mint_water_row` (batch_exporter
  mints through it, format unchanged). ~70 inline prefix tests and 9 parsers (read.base_region/_entry_region/_book_region,
  rules._zone_region/_book_region, export_codec._region_of, display._region/_region_of + chapter regex, glossary,
  unnamed_trib_sweep, outside._ENTRY_REGION/entry_regions) now go through it. Answers diff EMPTY: export + answers +
  card_check from the live bundle are byte-identical to the live files (answers sha256 0c52b8ce…, card gate 0 problems).
- Step 3 DONE: pipeline/tests/test_entry_id.py scans every pipeline module (AST) for an inline
  startswith("z"|"r"|"zp"|"zp:"|…) or an entry-id regex outside entry_id.py (mutation: each removed spelling is caught),
  and pins level.ts as the app's one entry-id reader.
- Review (adversarial): three re-inlined `book_region` copies in bundle/rules.py (`zr[:-1] == region and zr[-1:] in
  ("a", "b")`, lifts/size lifts/exempt lifts) and two in export_ui_rules (`_zone_words`, the `fam` lambda) now go through
  `entry_id.book_region`/`zone_region`/`region_words`; the scan also catches a sliced zone-letter test (mutation-pinned).
  Old-vs-new equivalence checked on all 1534 bundle entry ids; export/guide/answers byte-identical again, card gate 0.
- Owner question (default kept): `_zone_words` words a 7A closure "Zone A" while the level tag says "Region 7A rule" —
  both are book words; unify on one? Default: keep both (sentence vs tag).
- Left item DONE (1ca9bc00, lane lane-T125): the export's rule `provenance.region` is
  `entry_id.region_of(entry_id)` ("7a", "6", null for zp:), in the field dictionary; the codec drops it on the wire as
  derivable and `expand` refills it from the same function; the store keeps it (`store.common.RULE_PROV_KEEP`).
  `level.ts` reads `prov.region` and holds no regex: no app code reads an entry id (`test_no_ts_reader_of_an_entry_id_
  region`, mutation-pinned on the old regex; `test_the_export_and_the_store_ship_the_parsed_region`; level.test.ts
  "reads the region the pipeline shipped"). On the live export all 218 rank-3 rules are zone tables, so every level tag
  reads as before. Ships with deliver + a regs fixture re-cut (the committed fixture predates the field: until it is
  re-cut, rank-3 tags read "Region rule" in the fixture-backed app tests). Measured on a scratch deliver (this lane's code, live atlas + batch-5 reach run): the store's 3,405 rule blobs differ only by the added region; on all 218 rank-3 rules the shipped region gives the same tag the regex did; export data and answers byte-identical, guide +2 dictionary texts, card gate 0. MERGER: after deliver, re-cut the fixture — `python -m pipeline.tools.regs_fixture_release` (newest data/release) into app/packages/data/dev/regs-release — then `UPDATE_REGS_FIXTURES=1 pnpm -C app exec vitest run conformance/src/regs/harness` only if the frozen VM fixtures still read stale (they should come back identical: same tags). A scratch re-cut from this lane's release is r-151bedfb7f0e (not committed). Minting NOT changed: `matcher.region_num` still reads "REGION 7A" as "7", so
  r7 rows carry no zone in their id (EntryId.regions recovers it from the MUs + registry). Changing it moves every
  Region 7 entry id and needs the registry → entry resync (memory registry-change-entry-resync) — its own ticket.
- Owner question (default built with): mint r7a:/r7b: ids for Zone A/B rows? Default: no — keep r7: and read the
  zone from the MUs (no resync).

## Adversarial review of the left item (2026-10-23, lane-T125)
No code defect found; nothing changed for T109 (the T125 review commits do not touch it).
- End to end on a scratch deliver of this lane's code (live atlas + reach run): the export carries `provenance.region`,
  the codec drops it on the wire and `expand` refills it (export data file identical to the base's but for the atlas
  path string), regs.sqlite's 3,405 rule blobs differ from the base's ONLY by `prov.region`, and the RELEASE's
  `water` store (what the app opens) carries it on all 218 rank-3 rules (R1 19, R2 21, R3 21, R4 22, R5 20, R6 31,
  7A 28, 7B 36, R8 20). On every one the shipped region equals what the removed regex read; no rank-3 rule is a water
  row (an `r…` rank-3 rule would now say "Region N rule" where the regex said "Region rule").
- Mutation: level.ts with the regex back → level.test "reads the region the pipeline shipped" fails; codec `expand`
  without `region` → 4 codec tests + 2 entry-id pins fail.
- Not verified here: the app's regs parity on a re-cut fixture. The release's answers digest carries the bundle's atlas
  path, so a scratch release never pairs with the oracles' store and neither province nor fixture mode can run against
  one. The merger's re-cut after deliver is the check; until then 284 regs-parity tests fail on the stale fixture, as
  the builder said.
- Hand-built test rows (`core/src/regs/testing/ruleRows.ts`, copied from page_v36) carry no `region`: 7 rank-3 rows would
  tag "Region rule"; no test asserts their tag today.
- Owner question (default built: no bump): an app reading a release from BEFORE this field (an older release on a device)
  tags every rank-3 rule "Region rule" — less specific, never a wrong region. Bump the regulations store's section
  version (`KNOWN_VERSIONS`) so the app refuses such a release instead? Default: no bump.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, findings matching 4,
answers 5, answers 7 and cross 5 (CONFIRMED).

## Decision being unified
"What does this entry id say: kind (zone / zone-province / region / water), region, zones, book region, MUs, slug?"

## Where it is decided today (about 10 parsers plus about 78 inline prefix tests)
- `app/packages/core/src/regs/level.ts:12` `/^z(\d+)/`: turns `z7a:`/`z7b:` (23 entries) into "Region 7 rule". The sub
  then appends "· Region 7A", and Python says "Region 7A". **Live wrong text.**
- `matcher.region_num` (`\b(\d+)\b`) cannot read "REGION 7A", so all r7 rows are minted `r7:` with no zone. Only
  `pipeline/regs/reach/outside.py:92` `entry_regions` recovers the zone.
- `zp:` counts as a zone in some readers (`startswith("z")`) and not in others (`rules.py:138/517/1079`,
  `glossary.py:117`).
- Region word order differs: `read.region_words` sorts, `display.regions_label` keeps input order.

## Work
1. Now, no rebuild: fix `level.ts` to read `z7a`/`z7b` as Region 7A/7B (test on the 23 entries).
2. `pipeline/common/entry_id.py`: `parse(eid, registry=None) -> EntryId(kind, region, zones, book_region, mus, slug)`
   and one `region_words`. Every reader and `batch_exporter` minting go through it. The bundle exports the parsed
   region, so TypeScript stops using a regex.
3. Pin: an AST/grep test against inline `startswith("z"|"r"|"zp")` and regexes on entry ids outside `entry_id.py`
   (and outside one TS reader of the exported field).

## Acceptance
- Step 1: a rule-sheet tag diff over the 23 z7a/z7b entries, nothing else.
- Step 2 as a pure refactor: an empty answers diff. If minting changes, entry_ids change: run the registry → entry resync
  (memory `registry-change-entry-resync`) and review that diff separately.
- The pin fails on a re-inlined `startswith("z")`.

Rebuild: none for step 1; deliver + app for step 2. Depends: none. Size: M. Owner: no.
