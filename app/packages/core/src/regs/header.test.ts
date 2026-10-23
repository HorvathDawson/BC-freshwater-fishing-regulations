/**
 * header.ts over hand-built inputs (no store, no golden). The 134-state parity against the golden header is
 * app/conformance/src/regs/header.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import { headerOf, partLabels, subtitleOf } from "./header";
import type { Choice, PartFacts, RegsInputs } from "./inputs";
import type { Day } from "./model";

const D = (month: number, day: number): Day => ({ month, day });
const part = (label: string, short: string, o: Partial<PartFacts> = {}) =>
  ({ label, short, runs: "", place: null, ...o }) as unknown as PartFacts;
const choice = (parts: number[], text: string, sections: number, closed = false) =>
  ({ parts, text, sections, closed, heading: "" }) as Choice;
function inputs(o: { parts?: (PartFacts | null)[]; choices?: Choice[]; partIx?: number; day?: Day; weekday?: string;
                     name?: string; kind?: string; sections?: number; partSections?: number[]; unnamed?: boolean } = {}): RegsInputs {
  const parts = o.parts ?? [part("From the mouth up to the falls", "Mouth → falls", { runs: "From the mouth up to the falls" }),
                            part("From the falls up to the lake", "Falls → lake…", { runs: "From the falls up to the lake" })];
  return {
    day: o.day ?? D(10, 10), weekday: o.weekday ?? "Saturday", partIx: o.partIx ?? 0,
    item: o.unnamed ? null : { name: o.name ?? "Test River", kind: o.kind ?? "stream", sections: o.sections ?? 9,
      parts, partSections: o.partSections ?? parts.map(() => 3),
      picker: o.choices === undefined ? { choices: [choice([0], "From the mouth up to the falls", 4), choice([1], "Upper river", 5)], headed: false }
        : o.choices.length ? { choices: o.choices, headed: false } : null },
    place: o.unnamed ? { label: "Region 5", name: "Unnamed stream" } : null,
  } as unknown as RegsInputs;
}
const today = D(10, 10);

describe("subtitleOf (page v36's subtitle)", () => {
  it("several parts: the choice's sections, then the part's runs", () => {
    expect(subtitleOf(inputs())).toBe("stream · 4 sections · From the mouth up to the falls");
  });
  it("a place before the runs; runs cut at 150 characters with an ellipsis", () => {
    const long = "word ".repeat(40).trim();
    expect(subtitleOf(inputs({ parts: [part("a", "a", { place: "Upstream of X", runs: long }), part("b", "b")] })))
      .toBe(`stream · 4 sections · Upstream of X · ${long.slice(0, 150).replace(/\s+\S*$/, "")}…`);
  });
  it("a closed choice of stretches: 'closed all year'; one part: the part's sections, no tail; 1 section", () => {
    expect(subtitleOf(inputs({ choices: [choice([0, 1], "Closed all year · 2 stretches", 8, true)] }))).toBe("stream · 8 sections · closed all year");
    expect(subtitleOf(inputs({ parts: [part("Whole water", "Whole water")], choices: [], kind: "lake", partSections: [1] }))).toBe("lake · 1 section");
  });
  it("an OPEN choice of two stretches says the part's runs; a one-part water counts its own sections, not its choice's", () => {
    expect(subtitleOf(inputs({ choices: [choice([0, 1], "Both stretches", 8)] }))).toBe("stream · 8 sections · From the mouth up to the falls");
    expect(subtitleOf(inputs({ parts: [part("Whole water", "Whole water")], choices: [choice([0], "Whole water", 7)], partSections: [1] })))
      .toBe("stream · 1 section");
  });
});

describe("partLabels (prototype hereLabel / shortLabel)", () => {
  it("the subtitle says the long label: full = long; short = the first part's short, uncut", () => {
    expect(partLabels(inputs())).toEqual({ full: "From the mouth up to the falls", short: "Mouth → falls" });
  });
  it("the subtitle does not say it: full = the uncut short; a cut short falls back to the long", () => {
    expect(partLabels(inputs({ partIx: 1 }))).toEqual({ full: "Upper river", short: "Upper river" });
    const p = inputs({ partIx: 1, parts: [part("a", "A"), part("b", "B → C", { runs: "the runs" })] });
    expect(partLabels(p)).toEqual({ full: "B → C", short: "B → C" });
  });
  it("no picker: the part's uncut short label, else its label (T16: the slot is empty only when it is the water's name)", () => {
    const one = inputs({ parts: [part("Mouth to source", "Mouth → Source")], choices: [] });
    expect(partLabels(one)).toEqual({ full: "Mouth → Source", short: "Mouth → Source" });
    expect(headerOf(one, { today, status: "open" }).place).toEqual({ full: "Mouth → Source", short: "Mouth → Source" });
    expect(partLabels(inputs({ parts: [part("Mouth to source", "Mouth…")], choices: [] }))).toEqual({ full: "Mouth to source", short: "Mouth to source" });
    expect(partLabels(inputs({ unnamed: true }))).toBeNull();
  });
});

describe("headerOf", () => {
  it("today: 'Today' + the date; another day: its weekday; the dot from the strip's status", () => {
    const h = headerOf(inputs(), { today, status: "open" });
    expect([h.name, h.dayWord, h.date, h.dateAria, h.dot]).toEqual(["Test River", "Today", "Oct 10", "Change date, now Oct 10", "open"]);
    const h2 = headerOf(inputs({ day: D(6, 15), weekday: "Monday" }), { today, status: "closed" });
    expect([h2.dayWord, h2.date, h2.dot]).toEqual(["Mon", "Jun 15", "closed"]);
    expect(["tidal", "outside", "unknown"].map((s) => headerOf(inputs(), { today, status: s as "tidal" }).dot))
      .toEqual(["faint", "faint", "faint"]);
  });
  it("(T71, owner call 3: one Map entry, the view strip's) the action is Parts for several choices, else none — never Map", () => {
    expect(headerOf(inputs(), { today, status: "open" }).action)
      .toEqual({ kind: "parts", label: "Parts", aria: "Other parts — now From the mouth up to the falls" });
    expect(headerOf(inputs({ parts: [part("Whole water", "Whole water")], choices: [] }), { today, status: "open" }).action).toBeNull();
    expect(headerOf(inputs({ parts: [part("Whole water", "Whole water")], choices: [choice([0], "Whole water", 1)] }),
                    { today, status: "open" }).action).toBeNull();
    // no option can bring the Map button back: the header has no `canMap` (an old caller's extra key changes nothing)
    expect(headerOf(inputs(), { today, status: "open", canMap: true } as Parameters<typeof headerOf>[1]).action?.kind).toBe("parts");
  });
  it("a label that is the water's own name leaves the slot empty; the Parts words keep it", () => {
    const h = headerOf(inputs({ name: "Kootenay Lake — Lower West Arm", parts: [part("x", "Kootenay Lake — Lower West Arm"), part("y", "y")] }),
                       { today, status: "open" });
    expect([h.place, h.action]).toEqual([null,
      { kind: "parts", label: "Parts", aria: "Other parts — now Kootenay Lake — Lower West Arm" }]);
  });
  it("a one-part water: the label slot is empty only when the label is the water's name, and there is no action", () => {
    const h = headerOf(inputs({ name: "Wilkes Creek", parts: [part("Wilkes Creek", "Wilkes Creek")], choices: [] }),
                       { today, status: "open" });
    expect([h.place, h.action]).toEqual([null, null]);
  });
  it("an unnamed section: the place's name, no label, no action (D11); always the four tabs", () => {
    const h = headerOf(inputs({ unnamed: true }), { today, status: "open" });
    expect([h.name, h.place, h.action, h.tabs.map((t) => `${t.tab}:${t.label}`)]).toEqual(["Unnamed stream", null, null,
      ["keep:What you can keep", "gear:Gear", "methods:Methods", "licence:Licence"]]);
  });
  it("(T53) an unnamed section that flows into a named water: the label slot says which — a stream is its tributary", () => {
    const of = (kind: string | null, downstream: string | null | undefined, unnamed = true) => {
      const inp = { ...inputs({ unnamed }), key: kind === null ? null : { kind } } as unknown as RegsInputs;
      return headerOf(inp, { today, status: "open", downstream }).place;
    };
    expect([of("stream", "Eve River"), of("lake", "Salmon River"), of("wetland", "Fraser River"), of(null, "Eve River"),
            of("stream", null), of("stream", undefined)]).toEqual([
      { full: "Tributary of Eve River", short: "Tributary of Eve River" },
      { full: "Flows into Salmon River", short: "Flows into Salmon River" },
      { full: "Flows into Fraser River", short: "Flows into Fraser River" },
      { full: "Flows into Eve River", short: "Flows into Eve River" },
      null, null]);
    // a named water's label slot is its part's, never the downstream water's
    expect([of("stream", "Eve River", false), of("stream", null, false)]).toEqual([
      { full: "From the mouth up to the falls", short: "Mouth → falls" },
      { full: "From the mouth up to the falls", short: "Mouth → falls" }]);
  });
});
