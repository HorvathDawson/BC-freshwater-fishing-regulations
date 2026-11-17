# T766 — Upstream after a salmon merge can land on the side channel, and the step into the next water is lost

Status: OPEN (filed 2026-11-16 by the app chat from the general audit; found by reading, not yet reproduced).

Source: `handoff/PLAN/audit-2026-11-16/code.md` (the main chat's audit, 2026-11-16). Area: app core (T750's code).

## What the audit read
`app/packages/core/src/waterNav.ts` `crossUp` (about lines 348-349): when the live salmon reads parts 1, 3 and 4 of a
water alike and they join, while the side channel (part 2) does not, the map's entries become [0], [1+3+4], [2].
Upstream from the joined entry then goes to the side channel, and the step from the water's top part into the next
water (`water_continue`) cannot be reached.

## Work
1. Reproduce in a core test with a made-up group of that shape (a joined run around a part that does not join, on a
   water with a continuation). If it does not reproduce, close this with the test kept.
2. Fix so that every entry is reached once going up and once going down, and the top entry's Upstream still crosses.
3. A property test over every fixture water with every possible join pattern: ▲ from the lowest entry walks every
   entry once and ends on the crossing or on "Highest part"; ▼ walks it back.

## Done when
The made-up case and the property test pass; the Android step bench (T750's flows) still passes.
