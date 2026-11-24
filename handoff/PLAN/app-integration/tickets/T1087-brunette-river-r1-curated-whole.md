# T1087 — `brunette_river.r1` "No Fishing upstream of Burnaby Lake" is curated as `whole` on the Brunette itself: is the river wrongly closed?

Status: OPEN — step 1 done (lane L21, 2026-11-24): the binding is RIGHT, nothing to propose; the planning chat closes it.

Source: the 0.9.1 chat's T1053 lane, 2026-11-24: the rule reads "No Fishing upstream of Burnaby Lake …" but its extent is curated `whole` on the Brunette River, so it may be bound where it does not apply (the Brunette runs downstream of Burnaby Lake). It is also why 94 Brunette tributaries quote that extent line in T1052. Evidence: the T1053 lane's before/after list, `head-lakes-before-after.md` in the T1053-shots folder on branch feat/T1053-walk-climbs-head-lakes (not pushed when reported).

## Work
1. Read the printed row (cite the printed page) and say what the app shows today for the Brunette below Burnaby Lake and for the streams above the lake.
2. If the binding is wrong: correct the extent as a proposal in the review app (everything added is a proposal; the owner's marks are never edited), and list every water whose answer changes.

## Acceptance
the ticket states right or wrong with the page; if wrong, the corrected extent is proposed and the changed waters are listed.

Severity: HIGH until traced (a river may show closed, or open, against the book). Release: 0.9.1 if wrong and the fix is a curation proposal the owner can accept in time; else 0.9.2. Model: Opus. Size: S. Owner: yes (accepts the corrected extent).

## Step 1 (lane L21, 2026-11-24): RIGHT

The printed row, PRINTED p.22 (the repo PDF's page 24), Region 2 table: "BRUNETTE RIVER 2-8 No Fishing upstream of Burnaby
Lake or from Cariboo Dam to Salamander Creek; steelhead catch and release". The corpus splits it into two rules:
`brunette_river.r1` = `{op: whole, item_id: wbk:329291868}` (Burnaby Lake) with `tributaries_only: true` and
`extent_text` "upstream of Burnaby Lake" — the streams feeding Burnaby Lake, never the lake and never the Brunette (live
run: 102 sections, all walked; Burnaby Lake answers by its own row, Single barbless hook / No powered boats);
`brunette_river.r1b` = `between` Cariboo Dam and Salamander Creek. Today the app shows the Brunette below Burnaby Lake
open except Cariboo Dam to Salamander Creek (closed all year), and the streams above the lake closed (also by "Burnaby
Lake's tributaries No Fishing" and Still Creek's own row). The "curated whole on the Brunette" reading came from the T1053
triage probe, which paired each walking rule with every water its ENTRY matches (the Brunette) instead of the rule's own
extent (the lake). Not a curation error. (With T1053, `brunette_river_s_tributaries.r1` "No Fishing" also climbs Burnaby
Lake: the same streams, already closed; no answer changes.)
