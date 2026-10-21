/**
 * T33 unit cases over hand-built licence answers (the golden parity is conformance/src/regs/licence.parity.test.ts):
 * the word helpers (priceTxt, actTxt, whoTxt, srcOf), the angler (keyOf, profileOf), patchNeeds (the kokanee words and the
 * "Also, only if…" split — and that it never DROPS a document), and buildLicence over a small LicenceValue: draw order,
 * the youth path selecting the stored 16+ profile, asNeeds, the source line, closed / outside / tidal.
 */
import { describe, expect, it } from "vitest";
import type { LicenceValue, ProfileAnswer } from "../answers.generated";
import type { RegsInputs, RuleView } from "./inputs";
import {
  actTxt, buildLicence, keyOf, licenceBlockLines, licenceSourceLines, licenceSources, licenceWho, lx, needLine, patchNeeds, priceTxt,
  profileOf, srcOf, whoTxt,
} from "./licence";
import type { NeedItemVM, Who } from "./model";
import type { SpeciesBook } from "./names";
import { ALL_WORDS, LIC } from "./words";

const SP: SpeciesBook = { fish: { ST: { name: "Steelhead" }, SALMON: { name: "Salmon" }, RB: { name: "Rainbow trout" }, CH: { name: "Chinook" },
  WSG: { name: "White sturgeon" } }, groups: {} };

const DIMS = [["residency", ["resident", "non_resident", "non_resident_alien"]], ["age", ["16_plus", "under_16"]],
  ["guidance", ["non_guided", "guided"]], ["status", ["none", "indian_bc_resident", "metis", "disabled", "aged_65_plus"]]] as const;
const PROFILES = DIMS[0][1].flatMap((r) => DIMS[1][1].flatMap((a) => DIMS[2][1].flatMap((g) => DIMS[3][1].map((s) => `${r}/${a}/${g}/${s}`))));
const LABELS = PROFILES.map((p) => `label ${p}`);
const WHO: Record<string, Who> = {
  adult: { age: "adult", residency: "resident" }, adult_non: { age: "adult", residency: "non_resident" }, adult_alien: { age: "adult", residency: "non_resident_alien" },
  u16_res: { age: "u16", residency: "resident" }, u16_non: { age: "u16", residency: "non_resident" }, u16_alien: { age: "u16", residency: "non_resident_alien" },
};

const LICS = new Map<number, unknown>([
  [1, { entry_id: "zp:basic_licence", record_id: "basic_licence", kind: "requirement", label: "Anglers 16 and over need a basic angling licence to fish.", placement: "province", fields: { who: { age: ["16_plus"] } }, prov: {} }],
  [2, { entry_id: "zp:salmon_stamp", record_id: "salmon_stamp", kind: "requirement", label: "Anglers 16 and over need a Conservation Surcharge Stamp for salmon to keep salmon (not kokanee).", placement: "province", fields: {}, prov: {} }],
  [3, { entry_id: "zp:steelhead", record_id: "steelhead_targeting", kind: "requirement", label: "Steelhead stamp to fish for steelhead.", placement: "water", fields: {}, prov: {} }],
  [4, { entry_id: "zp:conduct", record_id: "angling_guide_licence", kind: "requirement", label: "You need an angling guide licence to guide anglers.", placement: "province", fields: {}, prov: {} }],
  [5, { entry_id: "zp:basic_licence", record_id: "under_16_non_resident", kind: "requirement", label: "Under 16 non-residents: be accompanied.", placement: "province", fields: {}, prov: {} }],
  [6, { entry_id: "zp:licence_administration", record_id: "carry_paper_licence", kind: "requirement", label: "Carry your paper licence.", placement: "province", fields: { conduct: ["carry_paper_licence"] }, prov: {} }],
]);
const BASIC = { doc: "basic_licence", when: { act: "fishing" }, base: true, prices: { year: 41.15, day: [11.43], eight_days: [22.86] } } as const;
const ADULT: ProfileAnswer = {
  documents: [BASIC, { doc: "steelhead_stamp", when: { act: "targeting", species: ["ST"] }, base: false, prices: { year: 28.57 } },
    { doc: "salmon_stamp", when: { act: "retaining", species: ["SALMON"] }, base: false, prices: { year: 17.14 } }],
  none_needed: false,
  requirements: [{ req: 3, when: { act: "targeting", species: ["ST"] }, paths: [{ need: ["steelhead_stamp"] }] },
    { req: 1, when: { act: "fishing" }, paths: [{ need: ["basic_licence"] }] },
    { req: 6, when: { act: "retaining_recorded" }, paths: [] },
    { req: 2, when: { act: "retaining", species: ["SALMON"] }, paths: [{ need: ["salmon_stamp"] }] }],
  guiding: [{ req: 4, paths: [{ need: ["angling_guide_licence"] }, { need: ["assistant_angling_guide_licence"] }] }],
};
const ADULT_NON: ProfileAnswer = { ...ADULT, documents: [{ ...BASIC, prices: { year: 62.87, day: [22.86], eight_days: [41.15] } }] };
const U16_NON: ProfileAnswer = { documents: [], none_needed: false, guiding: ADULT.guiding,
  requirements: [{ req: 5, when: { act: "fishing" }, paths: [
    { accompanied_by: { holding: "what_this_fishing_requires", who: { age: ["16_plus"] } }, quota: "counts_to_companion" },
    { as: { age: ["16_plus"], residency: ["non_resident", "non_resident_alien"] }, quota: "own" }] }] };
const U16_RES: ProfileAnswer = { documents: [], none_needed: true, requirements: [] };

/** the stored under-16 summary (pipeline licence.under_16_summary over the profiles above) */
const U16: LicenceValue["under_16"] = [
  { residency: "resident", label: "B.C. resident", none_needed: true, with_adult: null, buy: [], own_limit: true, says: "No licence or stamp needed …" },
  { residency: "non_resident", label: "Canadian, other province", none_needed: false, with_adult: { who: "16 or older", counts: true },
    buy: [{ doc: "basic_licence", name: "Basic angling licence", prices: { year: 62.87, day: [22.86], eight_days: [41.15] } }], own_limit: true, says: "Fish with …" },
  { residency: "non_resident_alien", label: "From outside Canada", none_needed: false, with_adult: null, buy: [], own_limit: false, says: "No licence needed." }];

const frame = (over: Partial<Record<keyof typeof WHO, ProfileAnswer>> = {}): LicenceValue => {
  const ps: ProfileAnswer[] = PROFILES.map(() => ({ documents: [], none_needed: false, requirements: [] }));
  const at = (k: keyof typeof WHO, p: ProfileAnswer) => { ps[PROFILES.indexOf(`${WHO[k]!.residency}/${WHO[k]!.age === "u16" ? "under_16" : "16_plus"}/non_guided/none`)] = p; };
  at("adult", ADULT); at("adult_non", ADULT_NON); at("u16_res", U16_RES); at("u16_non", U16_NON);
  for (const [k, p] of Object.entries(over)) at(k as keyof typeof WHO, p!);
  return { holds: { holds: [], designations: [], stamp_period: false, contested: false, considered: [], wrong_water: [], waived: [], not_yet_mapped: [],
    displaced: {}, also_printed: { "3": [9] } }, profiles: ps, under_16: U16 };
};
const RULE = { f: { species: ["CH"], life_stage: "adult" } } as unknown as RuleView;
const INP = (o: { licence?: LicenceValue; key?: unknown; paper?: number[]; lics?: Map<number, unknown> } = {}): RegsInputs => ({
  key: o.key === undefined ? { tidal: false } : o.key, frames: { licence: o.licence ?? frame() }, lics: o.lics ?? LICS, partIx: 0,
  item: { parts: [{ paper_licence: o.paper ?? [7] }] }, rules: { get: () => RULE, has: () => true, size: 1 },
  statics: { profiles: PROFILES, profileDims: DIMS, profileLabels: LABELS, exportSpecies: SP, exportConduct: { carry_paper_licence: "Carry your paper licence" },
    exportLicences: { basic_licence: { name: "basic angling licence" }, salmon_stamp: { name: "Conservation Surcharge Stamp for salmon" },
      steelhead_stamp: { name: "Steelhead Conservation Surcharge Stamp" }, angling_guide_licence: { name: "angling guide licence" },
      assistant_angling_guide_licence: { name: "assistant angling guide licence" } } },
}) as unknown as RegsInputs;
const GLOSS = new Set(["under_16", "steelhead_stamp", "conservation_surcharge", "classified_waters"]);
const onlyIfOf = (vm: ReturnType<typeof buildLicence>): readonly NeedItemVM[] => {
  const b = vm!.blocks[0];
  if (b?.cls !== "needs" || !b.onlyIf) throw new Error("no 'Also, only if…' list first");
  return b.onlyIf.items;
};
const lines = (vm: ReturnType<typeof buildLicence>) => vm!.blocks.map((b) => [b.cls, ...licenceBlockLines(b)]);

describe("words", () => {
  it("priceTxt: year, each day price, each 8-day price, joined by ' · '; nothing for no prices", () => {
    expect(priceTxt({ year: 41.15, day: [11.43], eight_days: [22.86] })).toBe("$41.15 a year · $11.43 a day · $22.86 for 8 days");
    expect(priceTxt({ year: 17.1 })).toBe("$17.10 a year");
    expect(priceTxt({})).toBe("");
  });
  it("actTxt: each act; species by name; lengths over / under / between", () => {
    expect(actTxt({ act: "fishing" }, SP)).toBe("To fish here");
    expect(actTxt({ act: "targeting", species: ["ST"] }, SP)).toBe("To fish for steelhead");
    expect(actTxt({ act: "retaining", species: ["RB"], lengths: [{ min_cm: 50 }] }, SP)).toBe("To keep rainbow trout over 50 cm");
    expect(actTxt({ act: "retaining", lengths: [{ max_cm: 30 } as never, { min_cm: 30, max_cm: 50 } as never] }, SP)).toBe("To keep fish under 30 cm, 30–50 cm");
    expect(actTxt({ act: "retaining_recorded" }, SP)).toBe("When you keep a fish you must record");
    expect(actTxt({ act: "guiding" }, SP)).toBe("To guide anglers");
  });
  it("whoTxt: every angler; axis labels mid-sentence; the status words; lists joined by 'or'", () => {
    expect(whoTxt(null)).toBe("every angler");
    expect(whoTxt({ age: ["16_plus"] })).toBe("16 or older");
    expect(whoTxt({ residency: ["non_resident", "non_resident_alien"] })).toBe("canadian, other province or from outside Canada");
    expect(whoTxt({ residency: "resident", status: ["indian_bc_resident"] })).toBe("B.C. resident, a status First Nations person living in B.C.");
  });
  it("srcOf: Province / Classified water / Federal / parks / This water", () => {
    expect([1, 3].map((i) => srcOf(lx(INP(), i)))).toEqual(["Province", "This water"]);
    expect(srcOf({ ...lx(INP(), 3), placement: "on_designation" })).toBe("Classified water");
    expect(srcOf({ ...lx(INP(), 3), fields: { authority: "superior" } })).toBe("Federal / parks");
  });
  it("lx: the record's key; a stand-in for a missing record", () => {
    expect(lx(INP(), 2).key).toBe("zp:salmon_stamp::salmon_stamp");
    expect([lx(INP(), 99).key, lx(INP(), 99).label]).toEqual(["#L99", "(licensing record 99: missing from the page’s data)"]);
  });
  it("every fixed licence word is in ALL_WORDS (no-hand-facts)", () => {
    for (const w of [LIC.YOU_NEED, LIC.ONLY_IF_HEAD, LIC.PRICE_NOTE, LIC.NOT_KOKANEE, "B.C. resident", "Under 16", " a year", "Keep ", "? ", " for 8 days"])
      expect(ALL_WORDS.has(w), w).toBe(true);
    expect(ALL_WORDS.has("16_plus")).toBe(false);   // a dimension value is a key, never said
  });
});

describe("the angler", () => {
  it("keyOf: the prototype's six answer keys", () => {
    expect(Object.values(WHO).map(keyOf)).toEqual(["adult", "adult_non", "adult_alien", "u16_res", "u16_non", "u16_alien"]);
  });
  it("11. profileOf: adult res/non/alien, u16 res/non/alien = 0, 20, 40, 10, 30, 50", () => {
    const s = INP().statics;
    expect([WHO.adult, WHO.adult_non, WHO.adult_alien, WHO.u16_res, WHO.u16_non, WHO.u16_alien].map((w) => profileOf(w!, s))).toEqual([0, 20, 40, 10, 30, 50]);
  });
});

describe("patchNeeds", () => {
  const it_ = (name: string, when: string): NeedItemVM => ({ name, terms: [], when, price: null });
  const BASIC_I = it_("Basic angling licence", "Needed to fish at all"), ST_I = it_("Steelhead Conservation Surcharge Stamp", "Only if you fish for steelhead, even to release them"),
    SA_I = it_("Conservation Surcharge Stamp for salmon", "Only if you keep salmon");
  it("moves the 'Only if' items under 'Also, only if…' and keeps every item (never drops a document)", () => {
    const p = patchNeeds([ST_I, BASIC_I, SA_I], () => false);
    expect([p.items.map((x) => x.name), p.onlyIf?.label, p.onlyIf?.items.map((x) => x.name)]).toEqual([
      ["Basic angling licence"], "Also, only if…", ["Steelhead Conservation Surcharge Stamp", "Conservation Surcharge Stamp for salmon"]]);
  });
  it("no split when none or all are conditional", () => {
    expect(patchNeeds([BASIC_I], () => false).onlyIf).toBeNull();
    expect(patchNeeds([ST_I, SA_I], () => false)).toEqual({ items: [ST_I, SA_I], onlyIf: null });
  });
  it("(not kokanee): only on a salmon item whose record says so, once", () => {
    expect(patchNeeds([BASIC_I, SA_I], (x) => x === SA_I).onlyIf!.items[0]!.when).toBe("Only if you keep salmon (not kokanee)");
    expect(patchNeeds([BASIC_I, SA_I], () => false).onlyIf!.items[0]!.when).toBe("Only if you keep salmon");
    const done = it_("Conservation Surcharge Stamp for salmon", "Only if you keep salmon (not kokanee)");
    expect(patchNeeds([BASIC_I, done], () => true).onlyIf!.items[0]!.when).toBe("Only if you keep salmon (not kokanee)");
    expect(patchNeeds([it_("Basic angling licence", "Needed to fish at all.")], () => true).items[0]!.when).toBe("Needed to fish at all.");
  });
});

describe("buildLicence", () => {
  it("adult resident: choices, label, You need / Also, only if…, paper line, guiding fold; source line", () => {
    const vm = buildLicence(INP(), WHO.adult!, { closed: false, glossary: GLOSS })!;
    expect([vm.question, vm.questionTerms, vm.label]).toEqual(["Who is fishing?", ["under_16"], "label resident/16_plus/non_guided/none"]);
    expect(vm.axes.map((a) => [a.axis, a.aria, a.options.map((o) => `${o.value}:${o.text}:${o.checked}`)])).toEqual([
      ["age", "Who is fishing?", ["adult:16 or older:true", "u16:Under 16:false"]],
      ["res", "Who is fishing?", ["resident:B.C. resident:true", "non_resident:Canadian, other province:false", "non_resident_alien:From outside Canada:false"]]]);
    expect(lines(vm)).toEqual([
      ["needs", "You need", "Basic angling licence Needed to fish at all $41.15 a year · $11.43 a day · $22.86 for 8 days", "Also, only if…",
        "Steelhead Conservation Surcharge Stamp ⓘ Only if you fish for steelhead, even to release them $28.57 a year",
        "Conservation Surcharge Stamp for salmon ⓘ Only if you keep salmon (not kokanee) $17.14 a year",
        "Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence"],
      ["paper", "Keep an adult chinook? Carry your paper licence (16 and over) and record it right away."],
      ["lostlist lopen", "If you are guiding other anglers", "For every angler:", "You need an angling guide licence to guide anglers.",
        "angling guide licence", "or", "assistant angling guide licence"]]);
    expect(vm.src).toEqual({ who: "Province · This water +1 ›", aria: "Sources: Province, This water +1", open: "licsrc:adult" });
  });
  it("(not kokanee) only where the salmon stamp's own stored record says it", () => {
    const lics = new Map(LICS); lics.set(2, { ...(LICS.get(2) as object), label: "Anglers 16 and over need a Conservation Surcharge Stamp for salmon to keep salmon." });
    const only = (inp: RegsInputs) => onlyIfOf(buildLicence(inp, WHO.adult!, { closed: false, glossary: GLOSS })).map((x) => x.when);
    expect(only(INP({ lics }))).toEqual(["Only if you fish for steelhead, even to release them", "Only if you keep salmon"]);
    expect(only(INP())).toEqual(["Only if you fish for steelhead, even to release them", "Only if you keep salmon (not kokanee)"]);
  });
  it("the glossary filters every ⓘ", () => {
    const vm = buildLicence(INP(), WHO.adult!, { closed: false, glossary: new Set() })!;
    expect([vm.questionTerms, needLine(onlyIfOf(vm)[0]!)])
      .toEqual([[], "Steelhead Conservation Surcharge Stamp Only if you fish for steelhead, even to release them $28.57 a year"]);
  });
  it("no paper line without the part's paper_licence rules", () => {
    expect(lines(buildLicence(INP({ paper: [] }), WHO.adult!, { closed: false, glossary: GLOSS })).map((l) => l[0])).toEqual(["needs", "lostlist lopen"]);
  });
  it("under 16, non-resident: the youth box words the stored under_16 entry (who, counts, buy names, first price)", () => {
    const vm = buildLicence(INP(), WHO.u16_non!, { closed: false, glossary: GLOSS })!;
    expect(lines(vm).slice(0, 2)).toEqual([
      ["classbox youth", "Under 16: no licence needed if you fish with a licensed adult ⓘ Fish with someone 16 or older who holds what this fishing needs (Basic angling licence). What you keep counts toward their limit. Or buy the Basic angling licence yourself to have your own limit."],
      ["needs", "You need", "Nothing, if you fish with a licensed adult (16 or older) What you keep counts toward their limit",
        "Basic angling licence Or, for your own limit: what a 16+ angler buys $62.87 a year · $22.86 a day · $41.15 for 8 days",
        "Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence"]]);
    expect(vm.src!.who).toBe("Province ›");
  });
  it("under 16, resident (none needed): the box becomes the one 'You need' item; 'Nothing is required' is not said", () => {
    expect(lines(buildLicence(INP(), WHO.u16_res!, { closed: false, glossary: GLOSS }))).toEqual([["needs", "You need",
      "No licence needed to fish here Anglers under 16 who live in B.C. don’t need a basic angling licence. The fishing rules and limits still apply."]]);
  });
  it("a profile with no requirement and nothing needed: 'Nothing is required of this angler here.'; the source line says 'Sources'", () => {
    const vm = buildLicence(INP(), WHO.adult_alien!, { closed: false, glossary: GLOSS })!;
    expect([lines(vm), vm.src!.who]).toEqual([[["muted", "Nothing is required of this angler here."]], "Sources ›"]);
  });
  it("closed day: the empty section (D16 a); outside B.C.: null; tidal: the stored tidal words", () => {
    expect(buildLicence(INP(), WHO.adult!, { closed: true, glossary: GLOSS })).toEqual({ question: null, questionTerms: [], axes: [], label: "", blocks: [], src: null });
    expect(buildLicence(INP({ key: null }), WHO.adult!, { closed: false, glossary: GLOSS })).toBeNull();
    const tidal = { holds: { tidal: { tidal: true, note: "n", see: [], licence: "federal Tidal Waters Sport Fishing Licence" } }, profiles: [] } as unknown as LicenceValue;
    expect(lines(buildLicence(INP({ licence: tidal }), WHO.adult!, { closed: false, glossary: GLOSS }))).toEqual([["classbox", "Tidal water federal Tidal Waters Sport Fishing Licence"]]);
  });
  it("the sources: grouped by act, ordered to fish here → to fish for → to keep → the rest; the +N the book prints", () => {
    const inp = INP(), F = inp.frames!.licence, h = F.holds as never;
    const g = licenceSources(inp, ADULT, h);
    expect(g.map((x) => x.act)).toEqual(["To fish here", "To fish for steelhead", "To keep salmon", "When you keep a fish you must record"]);
    expect(licenceWho(inp, ADULT, h)).toEqual(["Province", "This water +1"]);
    expect(licenceSourceLines(g).slice(0, 4)).toEqual(["To fish here", "Anglers 16 and over need a basic angling licence to fish. Province", "basic angling licence", "To fish for steelhead"]);
    expect(licenceSourceLines(g)).toContain("Carry your paper licence");
  });
});
