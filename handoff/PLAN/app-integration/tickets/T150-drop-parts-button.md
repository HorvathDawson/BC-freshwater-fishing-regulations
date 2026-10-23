# T150 — Drop the regulations header's "Parts" button; the part list lives in the Map view

Status: OPEN — after T147 (same header).

Source: owner 2026-10-23: "dont we want to remove 'parts' from reg page since map is now at the top tabs?" Follows owner
call 3 (decisions.md 2026-10-22, T71: the view strip is the one Map entry; the header lost its Map button but kept
Parts when the picker has a choice — core/src/regs/header.ts).

## Work
1. The regs header offers no Parts action (core header.ts, ui + ui-native); it keeps the part's NAME, so the reader
   always knows which part's rules are shown.
2. The "Parts of this water" list moves into the Map view (compact, under/over the map when the water has > 1 part):
   picks a part by name — tiny parts are hard to tap and a screen reader cannot tap a map. Tapping a part on the map
   still works.
3. Golden/conformance: the golden header's Parts action becomes an exclusion with this decision as its reason (like
   GOLDEN_MAP_ACTION_IDS for the Map button); header.parity asserts the new rule.
4. decisions.md: record it under owner call 3.

## Acceptance
No Parts button on any regs page; every part reachable by name from the Map view (web + native, light + dark, screen
reader label); shots of a many-part water (Fraser, Kootenay Lake). Size: S–M.

Owner question: the list's placement in the Map view (default: a collapsible list under the map).
