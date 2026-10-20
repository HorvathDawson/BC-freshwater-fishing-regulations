/**
 * The contract as a compile-time check: one minimal RegsPage built field by field with `satisfies`, so a contract edit
 * that adds a required field (or renames one) fails `tsc -b` here before any presenter is written. The runtime
 * assertions only keep vitest from calling the file empty.
 */
import { describe, expect, it } from "vitest";
import { buildRegsPage, type RegsPage } from "./model";

const page = {
  version: "regs-v38",
  target: { kind: "part", item: "vedder", partIx: 0 },
  day: { month: 5, day: 15 }, weekday: "Friday", today: false, who: { age: "adult", residency: "resident" },
  status: "open",
  header: {
    name: "Vedder River", dayWord: "Fri", date: "May 15", dateAria: "Change date, now May 15", dot: "open",
    place: { full: "Mouth → Vedder Crossing Bridge", short: "Mouth → Vedder Crossing Bridge" },
    action: { kind: "map", label: "Map", aria: "View on map" },
    tabs: [{ tab: "keep", label: "What you can keep" }],
  },
  keep: {
    order: ["strip", { group: 0 }, "otherFish"],
    strip: {
      status: "open", line: [{ kind: "status", text: "Open" }, { kind: "when", text: "today" }],
      runs: [{ bits: null, days: 365 }], todayAt: 0.37, months: ["J"], legend: [{ bit: 2, mark: null, label: "release" }],
    },
    inherited: null, banner: null,
    groups: [{
      row: 0, title: "Trout and char", scope: null,
      head: { form: "a", rootTier: "t1", lead: [{ t: "num", s: "2", size: "big" }], leadN: 2, words: [], ofTotal: true,
        whyHere: null, shareOf: null, terms: [], nolimit: false },
      origin: { says: [], tag: null, howtell: true, book: [{ t: "book", printed: 21, pdf: 23 }] },
      tree: [{ t: "tier", tier: "size", tid: "t3", kicker: null, line: [], sub: null, origin: null, band: true, short: "1 over 50 cm",
        children: [{ t: "kind", kind: "Brook trout", species: ["EB"], folded: false, zero: false, says: null,
          size: { text: "keep 30 cm (12 in) or longer", rule: false }, qualifiers: null,
          pictures: [{ code: "EB", name: "Brook trout", caption: "Brook trout ›", open: "sp:EB", sil: false }],
          also: [], duty: null, presence: null }] }],
      notKept: [], worked: null, quiet: null, more: { text: "See every regulation line ›", open: "src:r0" },
    }],
    plainRows: [], otherFish: [{ thumb: null, name: "Kokanee", value: "release", tone: "release", week: null, sizeHint: null,
      open: "other:o0" }],
    caveats: [], possession: null, whereNot: null, outside: null,
  },
  gear: { closed: null, day: "Fri, May 15", mode: null, lines: [], bait: null },
  methods: { closed: null, ways: null, boats: null, reference: null },
  licence: null,
  sheet: () => null,
} satisfies RegsPage;

describe("RegsPage", () => {
  it("names the port target the goldens were taken from", () => {
    expect(page.version).toBe("regs-v38");
  });

  it("buildRegsPage is declared and not yet built (T21)", () => {
    expect(() => buildRegsPage({} as never, { today: { month: 1, day: 1 }, who: page.who }))
      .toThrow("buildRegsPage: T21");
  });
});
