# T116 — One water noun: a wetland is called a "river" on its page

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding app 2 and the
TypeScript half of cross 8 (CONFIRMED).

## Decision being unified
"What noun names this kind of water (river, lake, wetland, …)?"

## Where it is decided today
- `lake ? lake : river` in `app/packages/core/src/regs/decided.ts:102`, `caveats.ts:89` and `waterNav.ts:338`.
- `app/packages/core/src/search.ts:169` `KIND_WORD`: knows every kind.
- (`header.ts:72` is fine: an unnamed-key stream/else test, not a noun.)

How they differ: 54 wetland items (marshes, swamps, oxbows) get "this river's rule" and "Whole river".

## Work
- An exhaustive `waterNoun(kind)` (a `switch` with a `never` default) in `app/packages/core/src/regs/words.ts`; the
  four places call it.
- Pin: an app grep test (one-* style) that no core module writes `? "lake" : "river"` or its own kind-to-word table.

## Acceptance
A wetland fixture page reads "this wetland's rule" / "Whole wetland" (or the agreed noun); search wording unchanged.

Rebuild: none (app only). Depends: none. Size: S. Owner: no.
