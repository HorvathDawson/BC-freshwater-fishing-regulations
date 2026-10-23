# T116 — One water noun: a wetland is called a "river" on its page

Status: DONE — 4cf77c7a, review fix 0c533d4a.

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

## As built (4cf77c7a)
- `waterNoun(kind: string, form: "page" | "row")` in `app/packages/core/src/regs/words.ts`: a `switch` over
  `WaterKind = keyof PlaceNames` (the atlas's `stream | lake | wetland`) with a `never` default, so a kind added to the
  generated `PlaceNames` is a compile error there. `page` is the in-sentence noun (river / lake / wetland); `row` is the
  search row label (Stream / Lake / Wetland). A kind the build does not know at run time is returned as the atlas spells
  it (search's existing "never guessed" rule, e.g. `canal`). The words live in `WORDS` (`WATER_RIVER/LAKE/WETLAND`,
  `ROW_STREAM/LAKE/WETLAND`), so `ALL_WORDS` (the no-hand-facts set) holds them.
- Callers: `decided.ts` byName (`BY_WATER`), `caveats.ts` inPartOf (`CALM_BOAT`), `waterNav.ts` wholeAria (new
  `WATER_MAP_WORDS.whole(noun)` → "Whole wetland"), `search.ts` kindWord (row form). `KIND_WORD` is deleted (no other
  user). A missing item (`inp.item` null) still says "river", as before (`?? "stream"`).
- Pin: `app/tools/one-water-noun.test.ts` scans every non-test `.ts/.tsx` under `app/packages` and `app/apps`
  (comments stripped) for a noun ternary, a kind-to-word table, a `"Whole <noun>"` literal, or a `WORDS.WATER_*/ROW_*`
  read outside `waterNoun`'s own body; checks the four callers call `waterNoun`; and feeds the old code of each place to
  prove each pattern fires.
- Whole-string tests: words.test (every kind × form, `BY_WATER`/`CALM_BOAT` composed, nouns ⊆ `ALL_WORDS`),
  decided.test ("Replaced by this wetland’s rule" for stream/lake/wetland), caveats.test ("…the same everywhere on
  this wetland."), waterNav.test ("⤢ Whole wetland" / "Whole wetland"). Search wording is unchanged (search.test's
  existing Stream/Lake/Wetland/canal pins pass untouched).
- Mutations: reverting decided.ts to the lake-else-river ternary fails one-water-noun (patterns + callers) and the
  decided wetland test; mapping wetland → `WATER_RIVER` in `waterNoun` fails words.test ×2, decided, caveats and
  waterNav tests.
- Checks: targeted vitest green; `REGS_STORE_REQUIRED=1 pnpm -C app check` (heavy.sh): all steps through typecheck
  pass, tests 7218 passed / 5 failed — the 5 are the store-paired conformance tests (blob.release, card/inputs/settle
  parity, store.release) failing on a digest mismatch in the shared `data/generated/bundle/regs.sqlite` vs taps.json
  oracles (environment, unrelated to this change).

## Adversarial review (0c533d4a)
Fixed:
- **An unnamed section's page still said "river".** `decided.ts` and `caveats.ts` fell back from a missing item
  (`inp.item` null — an unnamed section tap) straight to `"stream"`, though the section's key carries its kind (the
  same kind its place name is read by, `inputs.ts`). An unnamed lake or marsh read "Replaced by this river’s rule" /
  "…the same everywhere on this river." New `pageWaterNoun(inp)` in `words.ts` (item's kind, else the key's, else a
  stream → river) is what both call. Tests: decided.test (unnamed lake / wetland / stream / no key, whole strings),
  caveats.test (unnamed lake → "…everywhere on this lake."), words.test (item wins over key; key; neither → river).
  one-water-noun.test now requires `pageWaterNoun(inp)` in decided/caveats and refuses an `item?.kind ?? "<literal>"`
  fallback anywhere outside `words.ts`'s two noun functions (its old-code sample proves the pattern fires).
- Mutations: dropping the key from `pageWaterNoun` fails words, decided and caveats tests; passing `key: null` from
  decided.ts fails one-water-noun (callers) and the decided unnamed test; the same in caveats.ts fails one-water-noun
  and caveats.test.

Checked and sound:
- No other copy: grepped `kind ===/!== "<kind>"`, `case "<kind>"`, `"river"/"lake"/"wetland"` literals and
  "this/Whole <noun>" across app/packages and app/apps (non-test). Only `near.ts:113` (a lake's area signal, not a
  noun), `header.ts:72` (tributary-of vs flows-into, not a noun), `tree.ts dutyWords` / `grouphead.ts shortOf` (parse
  pipeline sentences, not decide a noun) and map layer lists remain. `KIND_WORD` had no other user.
- The store's item kinds are exactly `stream | lake | wetland` (regs.sqlite: 11963 / 7624 / 54), the `PlaceNames` keys.
- `never` default is real: adding `"canal"` to `PlaceNames` makes `tsc -p packages/core` fail at `words.ts`
  ("Type '"canal"' is not assignable to type 'never'").
- Builder's mutations re-run plus: wetland row → `ROW_STREAM` fails words.test and search.test; default → `WATER_RIVER`
  fails words.test (canal) and search.test; caveats using the `row` form fails caveats.test; search using `page` fails
  search.test ×2; a waterNav wetland special case fails the waterNav wetland test.
- No golden tap is a wetland, so the conformance oracles (page v36, lake-else-river) are not contradicted.
- Platform: `check-platform` clean (18 variant sets). Strict check (`REGS_STORE_REQUIRED=1 pnpm -C app check`, heavy.sh,
  worktree `.venv` symlinked for the gauge-policy step then removed): every step through typecheck passes; tests 7220
  passed / 5 failed — the same 5 store-pairing failures (blob/store.release, card/inputs/settle parity: shared
  regs.sqlite store 43115d8c… vs this branch's taps.json a2307766…), which also fail with the review fix stashed.

Left / follow-ups:
- The pipeline's own `whose` strings ("this river’s rule", `pipeline/deliver/answers/tree.py`) are the Python half of
  audit cross 8, not this ticket; a wetland's tree tier still reads "river" until that lands (needs a deliver rebuild).

## Owner questions
- Should a page call a stream "stream" rather than "river" ("this stream’s rule", "Whole stream")? Built default: the
  page keeps "river" (unchanged wording); search rows keep "Stream". Changing it is one line in `waterNoun`.
