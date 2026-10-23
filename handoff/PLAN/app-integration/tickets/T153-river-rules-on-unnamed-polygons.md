# T153 — A river's rules stop at every unnamed lake polygon on its course

Status: OPEN

Source: T148 census structural finding (handoff/PLAN/sloughs/v2/census.md, signals 6–7), 2026-10-23.

## Problem
A river entry's reach rule almost never binds a lake polygon on the river's own course: province-wide only 7 lake
polygons carry a rule from a stream entry. 456 unnamed lake/manmade polygons sit on rivers regulated by a named entry
(439 Lake–Definite, 9 Indefinite, 8 reservoir); each is a hole where the river's rules (often stricter: closures, bait
bans) fall back to zone rules. 180 of them were not in the first slough sweep. T55/T149 decide which pieces ARE the
river (fold → stream item); this ticket is the rule for the ones that stay lakes on the river's line.

## Work
1. The decision, with the owner: does a river row's rule apply to an unnamed lake polygon its main flow passes through
   (the book's "river" includes its widenings unless it names the lake)? Default proposal: yes for UNNAMED polygons the
   river's main flow runs through, when the rule's extent covers both sides; a NAMED lake never takes the river's row.
2. One function in the reach walk (may_enter / extent) deciding it, with a pin and a measured binding diff (every newly
   bound polygon named by river).

Rebuild: reach rerun + binding diff + deliver. Owner question Q124.
