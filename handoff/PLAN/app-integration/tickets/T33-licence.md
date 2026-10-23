# T33 — presenter: licence (who is fishing, label, answer, sources)

Status: DONE — b3ab85c5, a43a0de2, cd0a2a59.

Phase 3 · Depends on T00, T04 (`licence_*` codecs, statics `licence.*`, `export.licences`, `lic` words), T05
(`profileIndex`), T06 (species words), T08 (rows: `not_kept` for the steelhead drop), T09 (`frames.licence`, `lics`,
`part.paper_licence`), T12 · Blocks T35, T36 (`licsrc:` sheet body) · Decisions **D8** (profile is per session; this
presenter only takes a `Who`) · Size ≤ 1 day (+½ if the classbox/under-16 words run long)

## Goal
`core/src/regs/licence.ts` builds the Licence tab for one `Who` (age × residency; guidance fixed `non_guided`, status
`none`): the question "Who is fishing? ⓘ", two radio groups (16 or older | Under 16; B.C. resident | Canadian, other
province | From outside Canada), the profile's label ("Canadian, other province, under 16"), then the answer in draw order —
classified-water boxes, the under-16 "fish with a licensed adult" box, the **You need** list (name, ⓘ, when, price;
"Also, only if…" sub-list; the price note), the paper-licence line, the not-yet-mapped box, the guiding fold drawn open —
and the source line "Province · This water ›" that opens the `licsrc:` sheet ("Where each one comes from").

## Files
- create `app/packages/core/src/regs/licence.ts`, `app/packages/core/src/regs/licence.test.ts` (unit)
- create `app/conformance/src/regs/licence.parity.test.ts` (golden, block `licence`, six `Who`s per state via `page(who)`)
- modify `app/packages/core/src/regs/words.ts` — add `LIC`
- modify `app/packages/core/src/regs/model.ts` — CONTRACT CHANGE below

## Signatures
```ts
export type ProfileKey = "adult" | "adult_non" | "adult_alien" | "u16_res" | "u16_non" | "u16_alien";
export function keyOf(who: Who): ProfileKey;                                   // prototype KEYOF 1427–1432
export function profileOf(who: Who, s: Statics): number;                       // T05 profileIndex({residency, age: who.age==="u16"?"under_16":"16_plus", guidance:"non_guided", status:"none"}, s.profileDims, s.profiles)
                                                                               // resident 16+ 0 · resident u16 10 · non_resident 16+ 20 · u16 30 · alien 16+ 40 · u16 50
export interface LicWords { key: string; kind: string; label: string; verbatim: string; placement: string; fields: Record<string, any>;
  parts?: Record<string, string>; prov: { entry_name?: string; why?: string | null; uncertain?: boolean };
  period?: { says: string; dates?: unknown[] } | null; stamp_waiver?: { says: string } | null; not_yet_mapped?: { part: string; says: string } | null }
export function lx(inp: RegsInputs, lix: number): LicWords;                    // page v36 LX: inp.lics.get(lix), key = `${entry_id}::${record_id}`
export function actTxt(when: When, species: SpeciesWords): string;              // page v36 actTxt L1275–1279
export function whoTxt(who: Record<string, string | string[]> | null): string;  // page v36 whoTxt L1274
export function priceTxt(p: Prices): string;                                    // page v36 priceTxt L1282
export function srcOf(l: LicWords): string;                                     // page v36 srcOf L1290
export function licenceWho(inp: RegsInputs, p: ProfileAnswer, h: Holds): readonly string[];  // the source-line words, deduped
export function licenceSources(inp: RegsInputs, p: ProfileAnswer, h: Holds): readonly { act: string; cards: readonly ReqCardVM[] }[];  // T36 draws the licsrc: sheet from this
export function buildLicence(inp: RegsInputs, who: Who, ctx: { closed: boolean; rows: RowsValue; species: SpeciesWords; glossary: ReadonlySet<string> }): LicenceVM;
```
`words.ts` `LIC` (verbatim page v36 L1273–1366 and prototype 1381–1426 / render 1597–1608):
```ts
export const LIC = {
  QUESTION: "Who is fishing?", Q_TERM: "under_16",
  AXES: { residency: [["resident","B.C. resident"],["non_resident","Canadian, other province"],["non_resident_alien","From outside Canada"]],
          age: [["16_plus","16 or older"],["under_16","Under 16"]],
          guidance: [["non_guided","Not guided"],["guided","Guided"]],
          status: [["none","No status"],["indian_bc_resident","Status First Nations, living in B.C."],["metis","Métis"],["disabled","Disabled"],["aged_65_plus","65 or older"]] },
  YOU_NEED: "You need", ONLY_IF_HEAD: "Also, only if…", ONLY_IF_TEST: /^Only if\b/i,
  PRICE_NOTE: "Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence",
  NONE_HEAD: "No licence needed to fish here", NONE_U16: "Anglers under 16 who live in B.C. don’t need a basic angling licence.",
  NONE_EXEMPT: "You’re exempt from the basic angling licence.", NONE_ANY: "No licence rule applies to this angler.",
  NONE_TAIL: "The fishing rules and limits still apply.",
  YOUTH_HEAD: "Under 16: no licence needed if you fish with a licensed adult", YOUTH_NOTHING: "Nothing, if you fish with a licensed adult (16 or older)",
  YOUTH_COUNTS: "What you keep counts toward their limit", YOUTH_HOLD: "They must hold what this fishing needs",
  YOUTH_OWN: "Or, for your own limit: what a 16+ angler buys", /* + the classbox youth sentence parts, page v36 L1351 */
  PAPER: ["Keep ", "? ", "Carry your paper licence", " (16 and over) and record it right away."],
  NYM_HEAD: "In one part of this water", NYM_NOTE: "That part isn’t drawn on the map yet. It doesn’t apply to the rest of the water.",
  GUIDING: "If you are guiding other anglers", FOR: (w: string) => `For ${w}:`, OR: "or",
  CONTESTED: "Check: part of this water is also marked “not a Classified Water”.",
  NOTHING: "Nothing is required of this angler here.", SOURCE: "Source",
  NOT_KOKANEE: "(not kokanee)", STEELHEAD_DOC: /Steelhead Conservation Surcharge Stamp/,
  SRC: { province: "Province", on_designation: "Classified water", superior: "Federal / parks", water: "This water" },
} as const;
```

## Ports (@04765ab5; page v36 @ same commit)
The prototype draws page v36's licence HTML (snapshotted per profile) and then edits it in three places. The port builds
the result directly, in this order:
1. **page v36 `licParts` L1309–1370** for profile `pix = profileOf(who)` over `LicenceValue{holds: H, profiles[pix]: P,
   under_16}` (closed day → see 6). Port each block verbatim: designation classboxes (`H.designations` → `lx`: head
   `period.says ?? \`Class ${classified} Classified Water\``, term `classified_waters`; body `"Licence unit: " + (unit_name ?? unit)`
   + steelhead-stamp clause (`whenDates`/`rangeTxt`, "(in force today)" when `H.stamp_period`) + `stamp_waiver.says` (trailing
   "." stripped) or the waived sentence + "."; link "Source"); `H.contested` flag; unresolved flags (`item.unresolved` lix,
   `friendlyWhy`); `P.exempt` scopenote; `P.none_needed` classbox (NONE_U16 when age u16, NONE_EXEMPT when exempt, else
   NONE_ANY; + NONE_TAIL); the **youth** path (`youthReq` = no documents and a `fishing` requirement with an
   `accompanied_by`/`as` path; adult docs from `profiles[profileIx({...who, ...as})]`; classbox youth sentence; the two
   buy rows); the **buy** list (`name = cap([DOC(doc), ...alts].join(" or "))`, terms via `docGi` — steelhead_stamp,
   classified_waters, `/conservation surcharge/i` → conservation_surcharge, filtered by the glossary; `when = [whenTxt(when),
   ...also_when.map(alsoTxt)].join("; ") + (alts ? ". Either one does" : "")`; `price = priceTxt(prices)`); the price note
   when any row has a price; the **paper** line (`P.requirements` has act `retaining_recorded` and the part's
   `paper_licence` rixs are non-empty → `fishOf` per rule, unique, `join(fish, " or ")`, article a/an by `/^[aeiou]/`);
   `H.not_yet_mapped` nymboxes (`cap(parts.in_part ?? not_yet_mapped.part) + ": you need " + (parts.need ?? DOC(fields.satisfied_by[0].hold[0])) + " " + (parts.doing ?? "to fish") + "."`);
   NOTHING when no requirements and not none_needed; the guiding fold (`P.guiding` → otherCards: `FOR(whoTxt(l.fields.who))`
   (+ " (not …)" for `who_except`), the label, each path's docs joined " + ", paths separated by OR).
   Dropped (the prototype removes them): `profileHtml`, "Showing the answer for:", the "Under 16, by where you live" list,
   the "Rules for other anglers (N)" fold, the "All licence sources" button.
2. **prototype `licence.asNeeds` 1407–1419**: when the answer has no buy list, every classbox whose normalised text is not
   also a classbox of the ADULT resident answer (profile 0) becomes an item of a "You need" list placed where the first such
   box was (`name` = its head, `when` = its body, no price) — e.g. u16 resident's "No licence needed to fish here".
3. **prototype `render` 1597–1608 (v33)**, on the needs list, in this order: (a) drop items whose text matches
   `STEELHEAD_DOC` when any `rows.rows[].group.not_kept[]` has `status === "closed"` and `species` includes `"ST"`;
   (b) an item whose text matches `/salmon/i` and whose `when` lacks "not kokanee" gets `when.replace(/\.?$/, "") + " (not kokanee)"`
   when any requirement label of any profile in the frame contains "(not kokanee)"; (c) split: items whose `when` matches
   `ONLY_IF_TEST` move to `onlyIf` (head "Also, only if…") unless none or all match.
4. **prototype `licence.one` 1394–1406**: the source words = `.lhead .src` texts of the WHOLE answer before folds move —
   i.e. for each requirement card (groups by `actTxt`, stable-sorted by `orderAct`: "To fish here" 0, "To fish for…" 1,
   "To keep…" 2, else 3) `srcOf(l) + (H.also_printed[req].length ? " +" + n : "")`, then each guiding card's `srcOf`, then
   each `P.others` card's `srcOf`; deduped first-seen. `src = words.length ? {who: words.join(" · ") + " ›", open:
   \`licsrc:${profileKeyOf(who)}\`} : null`. The "Where each one comes from" fold becomes `licenceSources()` (T36's sheet;
   its title is the fold's summary "Where each one comes from").
5. **prototype `licence` selectors 1420–1426**: `question` "Who is fishing?", `questionTerms` `["under_16"]` (when the
   glossary has it); `axes` = `[{axis:"age", aria:"Who is fishing?", options: AXES.age → {value: "adult"|"u16", text, checked}},
   {axis:"res", aria, options: AXES.residency → {value, text, checked}}]`; `label = statics.profileLabels[pix]`.
6. **closed day** (v36 `licParts` returns null; the prototype then shows an empty section): `{question: null, questionTerms: [],
   axes: [], label: "", needs: [], blocks: [], src: null}` — parity; see Notes.

## Reads
`inp.frames.licence` → `LicenceValue{holds: Holds{designations, contested, stamp_period, not_yet_mapped, also_printed, considered},
profiles[60]: ProfileAnswer{documents[{doc, when, base, prices, also_when, or}], none_needed, requirements[{req, when, paths,
displaced_by, presumes_freed, presumes_by, terms}], exempt, others, guiding}, under_16}`; `inp.lics` (export licensing words);
statics `profileDims`, `profiles`, `profileLabels`, `exportLicences` (DOC names), `exportConduct`; `inp.item.parts[partIx].paper_licence`
+ `inp.rules` (fishOf: `fields.lengths`, `life_stage`, `origin`, `species`); `inp.item` unresolved licensing lix;
`inp.frames.rows.rows[].group.not_kept[]{status, species}`.

## Tests (golden v33; 134 states × 6 profiles = 804 objects)

**Where the tests live** (tests.md §1.2): core may not import `@app/data` or `node:fs`, so every golden-driven case below
is in `app/conformance/src/regs/licence.parity.test.ts` over T12's harness — `for await (const {id, g, page} of cases("licence"))`,
`requestOf(id)` (today Oct 10, keys compared via `goldenKeyOf(key, id)`), `eqPlain(plainLines(vm), goldenLines(g))`, one `it(id)` per state, skipped
when `provinceStore()` is null. ``licence.test.ts`` keeps only unit cases over small hand-built `GearAnswer`/`LicenceValue` literals
(cases marked *unit* below).
Cases 1–10 are parity; 11 is *unit*, plus `priceTxt`, `actTxt`, `whoTxt`, `keyOf`, and `patchNeeds` (steelhead drop / kokanee / only-if split) over hand lists.

1. **Parity, all 804** vs `golden.states[id].licence["<age>|<res>"]`: `question` (`plain` = `"Who is fishing? ⓘ"`),
   `questionTerms`, `axes[].options` (`text`, `checked`, value = golden `age`/`res`), `label`, `blocks` as `{cls, lines}`
   via `licenceBlockLines(vm)` (plain lines exactly as golden `lines()`: one line per block element, inline spaces), `src`.
   Tallies: 642 answers (107 open × 6) + 162 closed-empty; block classes: needs 642, `lostlist lopen` 642, paper 321,
   `classbox youth` 212, classbox 114, nymbox 6; `src.who` 9 distinct values ("Province · This water ›" 318, "Province ›" 198, …).
2. **Labels, vedder_1010** — `adult|resident` "B.C. resident, 16 or older"; `adult|non_resident` "Canadian, other province, 16 or older";
   `adult|non_resident_alien` "From outside Canada, 16 or older"; `u16|resident` "B.C. resident, under 16";
   `u16|non_resident` "Canadian, other province, under 16"; `u16|non_resident_alien` "From outside Canada, under 16";
   `src.open` `licsrc:adult` … `licsrc:u16_alien` (golden `licsrc:lic|vedder_1010|adult` …), `src.who` "Province · This water ›" for all six.
3. `vedder_1010 adult|resident` needs: `[{name:"Basic angling licence", when:"Needed to fish at all", price:"$41.15 a year · $11.43 a day · $22.86 for 8 days"}]`,
   onlyIf `["Steelhead Conservation Surcharge Stamp" (terms ["steelhead_stamp"]) "Only if you fish for steelhead, even to release them" "$28.57 a year",
   "White Sturgeon Conservation Licence" …, "Conservation Surcharge Stamp for salmon" "Only if you keep salmon (not kokanee)" "$17.14 a year"]`;
   paper line `"Keep an adult chinook or hatchery steelhead? Carry your paper licence (16 and over) and record it right away."`;
   guiding lines `["If you are guiding other anglers","For every angler:","You need an angling guide licence or an assistant angling guide licence to guide anglers.","angling guide licence","or","assistant angling guide licence"]`.
4. **Under 16, `vedder_1010 u16|non_resident`**: blocks `classbox youth` `"Under 16: no licence needed if you fish with a licensed adult ⓘ Fish with someone 16 or older who holds what this fishing needs (Basic angling licence). What you keep counts toward their limit. Or buy the Basic angling licence yourself to have your own limit."`,
   then needs `["Nothing, if you fish with a licensed adult (16 or older) What you keep counts toward their limit", "Basic angling licence Or, for your own limit: what a 16+ angler buys $62.87 a year · $22.86 a day · $41.15 for 8 days", "Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence"]` (no "Also, only if…").
5. `vedder_1010 u16|resident` (asNeeds): needs one item `"No licence needed to fish here"` / `"Anglers under 16 who live in B.C. don’t need a basic angling licence. The fishing rules and limits still apply."`.
6. `bc1_1010 adult|resident`: the steelhead stamp is dropped (needs = Basic + onlyIf [salmon stamp]).
7. `x5p1_1010 adult|resident`: classbox `"Classified (Class II) whenever this water is open ⓘ Licence unit: Elk River. Source"`;
   needs Basic + `"Classified Waters Licence ⓘ Needed while it’s classified: class II whenever this water is open $17.15 a year"`;
   src `"Province · Classified water +1 ›"`.
8. `klmb_1010 adult|resident`: nymbox `["In one part of this water","Only the south end, within the Creston Valley Wildlife Management Area: you need a Creston Valley Wildlife Management Area permit to fish.","That part isn’t drawn on the map yet. It doesn’t apply to the rest of the water."]`; src `"Province · This water +1 ›"`.
9. `x13p0_1010 u16|non_resident`: the stamp-waiver classbox text and youth docs `"(Basic angling licence, Classified Waters Licence)"`.
10. `vedder_615` (closed), all six: `{question:null, axes:[], label:"", blocks:[], src:null}` (golden keeps two option-less
    groups `{axis:"age", aria:"age", options:[]}` — the parity helper compares `axes.flatMap(a => a.options)`).
11. `profileOf` for the six `Who`s = `[0, 20, 40, 10, 30, 50]` (adult res/non/alien, u16 res/non/alien).

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/core/src/regs/licence conformance/src/regs/licence.parity
```
804/804 equal.

## Notes
- CONTRACT CHANGE: applied to contracts.md (§2.5 `LicenceVM`, `NeedItemVM`, `LicenceBlockVM`, `licenceBlockLines`; golden `needs` derivation noted there).
- **Closed day**: the prototype's empty Licence section is an artefact (v36 hides licensing while closed; UR2 6 removed
  the "no licence is needed while fishing is closed" sentence). Parity is the default; owner question: show the answer
  anyway (the frame exists on closed days)?
- The classbox "Source" button is dead in the prototype (no `data-lic` handler). The app makes it open the profile's
  `licsrc:` sheet (closest working target) — record in decisions.md.
- The kokanee rule (3b) and the steelhead drop (3a) are page-side patches over data words (v33 UR4 fix 7, owner O3); keep
  them isolated in one function `patchNeeds()` so a data fix can delete it.
- D8: `who` comes from `useAnglerProfile` (T11); the presenter is pure in `who`. Switching profile rebuilds only `licence`.
