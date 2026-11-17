# T766 — Upstream after a salmon merge can land on the side channel, and the step into the next water is lost

Status: DONE 2cdce6538 (landed 2026-11-17, CI green, 6/6 Android benches; was: IN PROGRESS (built on branch `app-fix/t766-t767`, 2026-11-17; reproduced in a core test; Android step bench owed before merge).)

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

## Built (2026-11-17, branch `app-fix/t766-t767`)
- **Reproduced** (core test, made-up water: parts 0, 1, side channel 2, 3, 4, going on as one water below and from one above;
  live salmon reads 1, 3, 4 alike): entries were [0], [1+3+4], [2]. ▲ from the joined entry stepped to the side channel, and
  the side channel offered "Highest part": the step into the water above was unreachable.
- **Fix** (`core/src/waterNav.ts`): `mergeNav` keeps the line's ends its ends: the entry holding the top part goes last
  (the mouth's entry is already first). `crossUp` is now "▲ past the last entry" (`at === parts.length - 1`), not "the entry
  whose index is `prev.at`": where the live salmon joins the top part with the mouth's and a piece apart stands alone (the
  whole main stem one entry, a side channel another) the joined entry must be first, and ▲ still crosses from the end of the
  line. `crossDown` is unchanged (the mouth's entry is always first).
- **Tests** (`core/src/waterNav.test.ts`, describe "(T766) …"): the made-up case, and a property test over every fixture
  water (Side, Chilliwack, Vedder, Cowichan, Zymoetz, both Fraser cuts, Kootenay Lake) × every join pattern (all set
  partitions of the parts) × going on as / from another water or not (2,000+ cases): ▲ from entry 0 walks every entry once to
  the cross or "Highest part", ▼ walks it back to the cross or "Lowest part", nothing in between crosses, and the top part's
  entry is last unless it is also the mouth's. Mutation-checked: drop the reorder (the made-up case and the property fail);
  make `crossUp` need `prev.at === at` (the property fails on a whole-stem join with a side channel apart).
- Left: the Android step bench (T750's flows) at merge.

