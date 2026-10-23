/**
 * The contract as a compile-time check: one minimal RegsPage built field by field with `satisfies`, so a contract edit
 * that adds a required field (or renames one) fails `tsc -b` here before any presenter is written. The runtime
 * assertions only keep vitest from calling the file empty.
 */
import { describe, expect, it } from "vitest";
import type { RegsPage } from "./model";

const page = {
  version: "regs-v38",
  target: { kind: "part", item: "vedder", partIx: 0 },
  day: { month: 5, day: 15 }, weekday: "Friday", today: false, who: { age: "adult", residency: "resident" },
  status: "open",
  header: {
    name: "Vedder River", dayWord: "Fri", date: "May 15", dateAria: "Change date, now May 15", dot: "open",
    place: { full: "Mouth → Vedder Crossing Bridge", short: "Mouth → Vedder Crossing Bridge" },
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
      pinLine: { cls: "g two", text: "Trout and char · 2 a day from streams", sub: "(of 4 across Region 2)" },
      origin: { says: [], tag: null, howtell: true, book: [{ t: "book", printed: 21, pdf: 23 }] }, year: null,
      tree: [{ t: "tier", tier: "size", tid: "t3", kicker: null, line: [], folded: false, relation: null, sub: null, origin: null, band: true, short: "1 over 50 cm", n: 1,
        children: [{ t: "kind", kind: "Brook trout", species: ["EB"], folded: false, zero: false, says: null,
          size: { text: "keep 30 cm (12 in) or longer", rule: false }, qualifiers: null,
          pictures: [{ code: "EB", name: "Brook trout", caption: "Brook trout ›", open: "sp:EB", sil: false }],
          also: [], duty: null, stamps: [], presence: null }] }],
      notKept: [], worked: null, quiet: null, more: { text: "See every regulation line ›", open: "src:r0" },
    }],
    plainRows: [], otherFish: [{ thumb: null, name: "Kokanee", value: "release", tone: "release", week: null, sizeHint: null,
      open: "other:o0", stamps: [] }],
    otherFishKey: [{ status: "release", says: "You may fish for it, but every one you catch must go back.", term: "catch_and_release" }],
    caveats: [], possession: null, whereNot: null, outside: null,
  },
  gear: { closed: null, closedMore: [], day: "Fri, May 15", mode: null, lines: [], bait: null },
  methods: { closed: null, closedMore: [], ways: null, boats: null, reference: null },
  licence: null,
  terms: { catch_and_release: "Catch and release (release)" },
  sheet: () => null,
} satisfies RegsPage;

describe("RegsPage", () => {
  it("names the port target the goldens were taken from", () => {
    expect(page.version).toBe("regs-v38");
  });

  it("leaves Gear, Methods and Licence to their tickets: each may be null until built (T21)", () => {
    const stub = { ...page, gear: null, methods: null, licence: null } satisfies RegsPage;
    expect([stub.gear, stub.methods, stub.licence]).toEqual([null, null, null]);
  });
});
