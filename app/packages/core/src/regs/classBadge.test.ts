/**
 * T841 — the header's classified-water badge over hand-built licence answers shaped like the 2026 release's (Skeena River
 * Section 4: Class II Jul 1–Dec 31; the Dean's Class I and Class II units; the Elk: Class II whenever open; lower Coal
 * Creek: "Not a Classified Water").
 */
import { describe, expect, it } from "vitest";
import { classBadgeOf } from "./classBadge";
import { headerOf } from "./header";
import type { RegsInputs } from "./inputs";

const dated = (cls: "I" | "II", unit: string, fm: number, fd: number, tm: number, td: number) => ({
  entry_id: unit, record_id: "r1", kind: "designation", label: `Class ${cls} Classified Water`, verbatim: "", placement: "sections",
  fields: { classified: cls, unit_name: unit },
  period: { kind: "dates", dates: [{ from_month: fm, from_day: fd, to_month: tm, to_day: td }], says: `Classified (Class ${cls}) …` },
  prov: {},
});
const LICS = new Map<number, unknown>([
  [77, dated("II", "Skeena River Section 4", 7, 1, 12, 31)],
  [51, dated("I", "Dean River Class I", 6, 1, 9, 30)],
  [53, dated("II", "Dean River Class II", 6, 16, 10, 31)],
  [23, { entry_id: "elk", record_id: "r1", kind: "designation", label: "", verbatim: "", placement: "sections",
         fields: { classified: "II", unit_name: "Elk River" }, period: { kind: "when_open", says: "Classified (Class II) whenever this water is open" }, prov: {} }],
  [21, { entry_id: "coal", record_id: "r1", kind: "not_classified", label: "Not a Classified Water.", verbatim: "", placement: "sections", fields: {}, prov: {} }],
  [89, { entry_id: "p", record_id: "r9", kind: "requirement", label: "Anglers 16 and over need a Classified Waters Licence", verbatim: "",
         placement: "province", fields: {}, prov: {} }],
  // a requirement placed on a designation, its fields naming the class it needs the licence on: a requirement, not a designation
  [92, { entry_id: "d", record_id: "r2", kind: "requirement", label: "Anglers 16 and over need a Classified Waters Licence", verbatim: "",
         placement: "on_designation", fields: { classified: "I" }, prov: {} }],
]);
const GLOSS = { version: 1, terms: [{ id: "classified_waters", term: "Classified waters" }] };

function inputs(designations: number[], considered: number[], o: { glossary?: boolean } = {}): RegsInputs {
  return {
    day: { month: 8, day: 15 }, weekday: "Saturday", partIx: 0, item: null, place: { label: "Region 6", name: "Unnamed stream" },
    frames: { licence: { holds: { holds: [], designations, considered, stamp_period: false, contested: false, wrong_water: [], waived: [],
                                  not_yet_mapped: [], displaced: {}, also_printed: {} }, profiles: [] } },
    lics: LICS,
    statics: { glossary: o.glossary === false ? { version: 1, terms: [] } : GLOSS, ends: {} },
  } as unknown as RegsInputs;
}

describe("look A (in period only)", () => {
  it("a classified day: the badge says the class, opens the classified-waters term", () => {
    expect(classBadgeOf(inputs([77], [77, 89]), "in_period"))
      .toEqual({ cls: "II", label: "Class II", period: null, inForce: true, term: "classified_waters", aria: "Class II" });
  });
  it("out of period (the designation only considered): no badge", () => {
    expect(classBadgeOf(inputs([], [77, 89]), "in_period")).toBeNull();
  });
  it("not classified (lower Coal Creek: a not_classified record, the province's requirement): no badge", () => {
    expect(classBadgeOf(inputs([], [21, 89]), "in_period")).toBeNull();
  });
  it("only a designation makes a badge: a requirement naming a class does not", () => {
    expect(classBadgeOf(inputs([92], [92, 89]), "in_period")).toBeNull();
    expect(classBadgeOf(inputs([], [92, 89]), "year_round")).toBeNull();
  });
  it("two units in force with different classes: the highest, Class I, is said", () => {
    expect(classBadgeOf(inputs([53, 51], [51, 53]), "in_period")?.label).toBe("Class I");
    expect(classBadgeOf(inputs([53], [51, 53]), "in_period")?.label).toBe("Class II");
  });
  it("no glossary term in the release: the badge opens nothing", () => {
    expect(classBadgeOf(inputs([77], [77], { glossary: false }), "in_period")?.term).toBeNull();
  });
  it("no frame (outside B.C., a tidal piece, a hand-built page): no badge", () => {
    expect(classBadgeOf({ ...inputs([77], [77]), frames: null }, "in_period")).toBeNull();
    expect(classBadgeOf({ ...inputs([77], [77]), frames: { licence: { holds: { tidal: {} }, profiles: [] } } } as unknown as RegsInputs, "in_period")).toBeNull();
  });
});

describe("look B (year round, with the period)", () => {
  it("in period: the class with the book's dates, in force", () => {
    expect(classBadgeOf(inputs([77], [77, 89]), "year_round"))
      .toEqual({ cls: "II", label: "Class II", period: "Jul 1–Dec 31", inForce: true, term: "classified_waters", aria: "Class II, Jul 1–Dec 31" });
  });
  it("out of period: the same words, not in force (drawn muted)", () => {
    expect(classBadgeOf(inputs([], [77, 89]), "year_round"))
      .toEqual({ cls: "II", label: "Class II", period: "Jul 1–Dec 31", inForce: false, term: "classified_waters", aria: "Class II, Jul 1–Dec 31" });
  });
  it("a period with no dates (the Elk: whenever this water is open): the class alone", () => {
    expect(classBadgeOf(inputs([23], [23, 89]), "year_round")).toMatchObject({ label: "Class II", period: null, inForce: true, aria: "Class II" });
  });
  it("the Dean: Class II in force beats Class I out of period; neither in force, Class I with its dates", () => {
    expect(classBadgeOf(inputs([53], [51, 53]), "year_round")).toMatchObject({ label: "Class II", period: "Jun 16–Oct 31", inForce: true });
    expect(classBadgeOf(inputs([], [51, 53]), "year_round")).toMatchObject({ label: "Class I", period: "Jun 1–Sep 30", inForce: false });
  });
  it("not classified: no badge on any day", () => {
    expect(classBadgeOf(inputs([], [21, 89]), "year_round")).toBeNull();
  });
});

describe("the header carries it (look A, the app's default)", () => {
  const h = (i: RegsInputs) => headerOf(i, { today: { month: 8, day: 15 }, status: "open" });
  it("a classified day: `classed` set", () => {
    expect(h(inputs([77], [77])).classed?.label).toBe("Class II");
  });
  it("an unclassified day: no `classed` key at all (the frozen headers stay equal)", () => {
    expect("classed" in h(inputs([], [77]))).toBe(false);
  });
});
