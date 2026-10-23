# T109 — One entry-id grammar: tier, region and zone parsed about 10 ways (z7a shows as "Region 7")

Status: OPEN.

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
