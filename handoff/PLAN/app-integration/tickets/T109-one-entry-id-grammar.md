# T109 — One entry-id grammar: tier, region and zone parsed about 10 ways (z7a shows as "Region 7")

Status: PARTIAL — 6c03f5fe (step 1), 89fea9c0 (steps 2 + 3, Python). Lane wf_754f5fcd-d71-3.
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
- Left (follow-up, needs deliver + app): the bundle/export ships the parsed region so level.ts stops using its regex
  (store field + regs fixture regenerate). Minting NOT changed: `matcher.region_num` still reads "REGION 7A" as "7", so
  r7 rows carry no zone in their id (EntryId.regions recovers it from the MUs + registry). Changing it moves every
  Region 7 entry id and needs the registry → entry resync (memory registry-change-entry-resync) — its own ticket.
- Owner question (default built with): mint r7a:/r7b: ids for Zone A/B rows? Default: no — keep r7: and read the
  zone from the MUs (no resync).

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
