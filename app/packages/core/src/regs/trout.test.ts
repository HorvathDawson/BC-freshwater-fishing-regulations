/**
 * (T810 N) trout.ts and the dev switch's split, store-free. The vectors, the hand cases, the a11y golden, the "drawn
 * exactly once" proof over the trout fixture release and trout.ts's imports test (it reads the source: core imports no
 * platform) are app/conformance/src/regs/trout.release.test.ts.
 */
import { describe, expect, it } from "vitest";
import { troutGlance } from "../glance";
import { keepOrder } from "./keep";
import { listRows, looseRows, splitRows } from "./others";
import { dayPhrase } from "./strip";
import { a11yOf, checkTrout, DAY_SLOT, TroutRefused, troutOf } from "./trout";
import type { Band, TRow, TroutValue } from "../answers.generated";
import type { RegsInputs } from "./inputs";
import type { TroutVM } from "./model";
import { TROUT_NAV } from "./words";
import { cardOf, rowOf, TROUT_ROW } from "./testing/keepFixtures";

describe("dayPhrase: the strip's day words, shared with the trout table's slot", () => {
  it("today, else on <day>", () => {
    expect(dayPhrase({ month: 7, day: 15 }, { month: 7, day: 15 })).toBe("today");
    expect(dayPhrase({ month: 7, day: 15 }, { month: 7, day: 16 })).toBe("on Jul 15");
    expect(DAY_SLOT).toBe("{day}");
  });
});

describe("a11yOf: payload texts in payload order, joined by \". \", nothing else", () => {
  it("drops the empty parts only", () => {
    expect(a11yOf(["Steelhead", null, "Hatchery only", undefined, "only 3", "until Oct 31, then release"]))
      .toBe("Steelhead. Hatchery only. only 3. until Oct 31, then release");
  });
  it("adds no second stop after a text that already ends a sentence (the steelhead quiet line never reads \"..\")", () => {
    const got = a11yOf(["Steelhead", "Steelhead are known to be in this water.", "only 1 of these 4", "Ask first!", "Closed?", "x"]);
    expect(got).toBe("Steelhead. Steelhead are known to be in this water. only 1 of these 4. Ask first! Closed? x");
    expect(got).not.toContain("..");
  });
});

describe("the split by covers (others.ts; T810 S: the only split)", () => {
  const KO = rowOf({ kind: "keep", pool: 3, daily: 5, members: ["KO"], all_members: ["KO"] });
  const BASS = rowOf({ kind: "no_limit", pool: 3, members: ["LMB", "SMB"], all_members: ["LMB", "SMB"] });
  // (Nahwitti) Region 1's "Release all char": a row with no pool and no group, beside the trout keep pool
  const CHAR = rowOf({ kind: "release", members: ["DV", "LT", "EB"], all_members: ["DV", "LT", "EB"] });
  // an other fish with no pool (a plain row in full; a pooled one with no card is a list line, as today: T670 K4)
  const WSG = rowOf({ kind: "release", win: 3, members: ["WSG"], all_members: ["WSG"] });

  it("covered rows are the table's; two or more others are the list", () => {
    expect(splitRows(cardOf([KO, TROUT_ROW, BASS]), [1])).toEqual({ main: [], rest: [0, 2] });
    expect(listRows(cardOf([KO, TROUT_ROW, BASS]), [1])).toEqual([0, 2]);
  });
  it("a lone other fish stays as today: drawn in full, no list", () => {
    expect(splitRows(cardOf([TROUT_ROW, WSG]), [0])).toEqual({ main: [1], rest: null });
    expect(listRows(cardOf([TROUT_ROW, WSG]), [0])).toBeNull();
    expect(listRows(cardOf([TROUT_ROW, BASS]), [0])).toEqual([1]);
  });
  it("Nahwitti: a pool-less release row the table covers is drawn by nothing else (no plain row, no list line)", () => {
    const card = cardOf([TROUT_ROW, CHAR, WSG]);
    expect([splitRows(card, [0, 1]), listRows(card, [0, 1]), looseRows(card, [0, 1])]).toEqual([{ main: [2], rest: null }, null, []]);
    // mutation: a covers that leaves the char row out draws it beside the table (an Other fish line)
    expect(splitRows(card, [0]).rest).toContain(1);
  });
  it("no covered row (the cell has no table): every row drawn in order", () => {
    expect(splitRows(cardOf([KO, BASS]), [])).toEqual({ main: [0, 1], rest: null });
  });
  it("a closed day: no rows", () => {
    expect(splitRows(cardOf([TROUT_ROW, KO, BASS], { status: "closed" }), [0])).toEqual({ main: [], rest: null });
  });
});

describe("keepOrder and the fold row", () => {
  const none = { strip: false, inherited: false, banner: false, caveats: [], rows: [], otherFish: false, possession: false, whereNot: false };
  it("one trout block, where the trout rows stood: before the full rows", () => {
    expect(keepOrder({ ...none, strip: true, trout: true, rows: [{ plain: 0 }], otherFish: true }))
      .toEqual(["strip", "trout", { plain: 0 }, "otherFish"]);
    expect(keepOrder({ ...none, strip: true })).not.toContain("trout");
  });
  it("the trout fold: value, tone and date as shipped (the no-count rule lifted for trout only)", () => {
    const t = { boxes: [{ rows: [{ fish: "DV" }, { fish: "RB" }] }],
                fold: { name: TROUT_NAV.ENTRY, value: "Up to 2 or 5", tone: "none", date: "until Mar 31" } } as unknown as TroutVM;
    expect(troutGlance(t)).toEqual({ source: "bc", thumb: "RB", name: "Trout and char", value: "Up to 2 or 5", tone: "none", date: "until Mar 31" });
    expect(troutGlance({ ...t, fold: { ...t.fold, date: null } })).not.toHaveProperty("date");
  });
});

// ── (T810 S, trout 4) the new shapes: the top line's sub, nested bands, merged twins, a date said once ──
const SHEET = { head: "x", here: [], src: [0] };
const row = (key: string, name: string, lines: readonly string[] = []): TRow =>
  ({ key, fish: key.split("/")[0], name, origin: null, label: null, sub: null, lines, quiet: [], src: [0], sheet: SHEET }) as unknown as TRow;
/** the Vedder's shape, cut down: four rows in one shared band (0..3), the last two with a count between them, then an
 *  `upto` row (4) */
function vedder(over: { inner?: readonly Band[]; bands?: readonly Band[]; dates?: TroutValue["dates"]; sub?: string | null } = {}): TroutValue {
  return {
    table: {
      top: { lead: "Keep 2 trout and char {day}", sub: over.sub === undefined ? "or 4 if they're hatchery rainbow trout 50 cm (20 in) or less" : over.sub, second: null },
      fold: { value: "2 or 4 a day", tone: "keep" }, hatch_link: false,
      boxes: [{
        kind: "keep", head: "Keep 4 in all", label: null, strip: null, possession: null, tag: null,
        rows: [row("EB", "Brook trout"), row("CT", "Cutthroat trout"), row("DV", "Dolly Varden/bull trout"), row("LT", "Lake trout"),
               row("RB", "Rainbow trout", ["50 cm (20 in) or less if wild"])],
        bands: over.bands ?? [{ from: 0, to: 3, text: "2 total from these 4", kind: "shared" }, { from: 4, to: 4, text: "counts toward the 4", kind: "upto" }],
        inner: over.inner ?? [{ from: 2, to: 3, text: "only 1 between them", kind: "shared" }],
      }],
      together: null, year: null, elsewhere: [], src: [0],
    },
    dates: over.dates === undefined ? null : over.dates,
    covers: [0],
  };
}
const BOUNDS = { hasRule: (i: number) => i === 0, rows: 1 };
const why = (f: TroutValue): string | null => { try { checkTrout(f, BOUNDS); return null; } catch (e) { if (e instanceof TroutRefused) return e.why; throw e; } };
const DATES = { top: null, card: null, fold: null, box: {}, band: {}, row: {}, sheet: {} };

describe("checkTrout (trout 4): the new shapes held, the bad ones refused", () => {
  it("accepts the Vedder's shape: a sub, a nested band inside the shared band, a merged twin (key = fish, origin null)", () => {
    expect(why(vedder())).toBeNull();
    expect(why(vedder({ inner: [{ from: 0, to: 1, text: "only 1 over 50 cm (20 in) between them", kind: "size" },
                                { from: 2, to: 3, text: "only 1 between them", kind: "shared" }] }))).toBeNull();
  });
  it("refuses a nested band that is its band whole", () => {
    expect(why(vedder({ inner: [{ from: 0, to: 3, text: "only 1 between them", kind: "shared" }] }))).toMatch(/inside no band, or is one whole/);
  });
  it("refuses a nested band across two bands, or inside none", () => {
    expect(why(vedder({ inner: [{ from: 3, to: 4, text: "only 1 between them", kind: "shared" }] }))).toMatch(/inside no band/);
    expect(why(vedder({ inner: [{ from: 5, to: 6, text: "only 1 between them", kind: "shared" }] }))).toMatch(/inside no band/);
  });
  it("refuses a nested band of one row", () => {
    expect(why(vedder({ inner: [{ from: 2, to: 2, text: "only 1", kind: "shared" }] }))).toMatch(/spans two rows or more/);
  });
  it("refuses nested bands that overlap", () => {
    expect(why(vedder({ inner: [{ from: 0, to: 2, text: "only 1 between them", kind: "shared" },
                                { from: 2, to: 3, text: "only 1 over 50 cm (20 in) between them", kind: "size" }] })))
      .toMatch(/nested bands overlap at row 2/);
  });
  it("refuses a nested band of a kind but shared or size", () => {
    expect(why(vedder({ inner: [{ from: 2, to: 3, text: "only 1 between them", kind: "possession" }] }))).toMatch(/a possession band is one row's/);
    expect(why(vedder({ inner: [{ from: 4, to: 4, text: "up to the 4", kind: "upto" }] }))).toMatch(/nested band is shared or size, not upto/);
  });
  it("refuses a nested band on a release box", () => {
    const f = vedder(), b = f.table.boxes[0]!;
    const rel = { ...f, table: { ...f.table, fold: { value: "Release", tone: "release" as const },
      boxes: [{ ...b, kind: "release" as const, head: "Release", bands: [], inner: [{ from: 0, to: 1, text: "only 1 between them", kind: "shared" as const }] }] } };
    expect(why(rel)).toMatch(/a release box has no bands/);
  });
  it("refuses a bad sub (the same Txt as every phrase)", () => {
    expect(why(vedder({ sub: " or 4" }))).toMatch(/top.sub: a leading or trailing space/);
    expect(why(vedder({ sub: "or 4 {day}" }))).toMatch(/top.sub/);
  });
  it("a date said once: a card date with box, band and row dates under it, none repeating the level above", () => {
    expect(why(vedder({ dates: { ...DATES, card: "Until May 31, then closed to all fishing", band: { "0:0": "until Apr 30, then release" },
                                 row: { "0:4": "until Apr 30, then release" } } }))).toBeNull();
    // the most specific level repeating the level above: refused (the card's, its first letter lowered, as the model compares)
    expect(why(vedder({ dates: { ...DATES, card: "Until May 31, then closed to all fishing", box: { "0": "until May 31, then closed to all fishing" } } })))
      .toMatch(/row 0:0 carries one date at two levels/);
    expect(why(vedder({ dates: { ...DATES, band: { "0:0": "until Apr 30, then release" }, row: { "0:1": "until Apr 30, then release" } } })))
      .toMatch(/row 0:1 carries one date at two levels/);
  });
});

describe("troutOf (trout 4): the sub, the nested bands and the row labels", () => {
  const inputs = (f: TroutValue) => ({ trout: f, rules: { has: (i: number) => i === 0 }, frames: { rows: { rows: [{}] } } }) as unknown as RegsInputs;
  const day = { month: 4, day: 2 };
  it("carries the sub and the nested bands as given; a nested band has no date", () => {
    const vm = troutOf(inputs(vedder({ dates: { ...DATES, card: "Until May 31, then closed to all fishing" } })), day, day) as TroutVM;
    expect(vm.top).toEqual({ lead: "Keep 2 trout and char today", sub: "or 4 if they're hatchery rainbow trout 50 cm (20 in) or less", second: null, date: null });
    expect(vm.card).toBe("Until May 31, then closed to all fishing");
    expect(vm.boxes[0]!.inner).toEqual([{ from: 2, to: 3, text: "only 1 between them", kind: "shared", date: null }]);
  });
  it("a row's label reads its band's words, then its nested band's (nested after outer), once each", () => {
    const vm = troutOf(inputs(vedder({ dates: { ...DATES, band: { "0:0": "until Apr 30, then release" } } })), day, day) as TroutVM;
    const a11y = vm.boxes[0]!.rows.map((r) => r.a11y);
    expect(a11y).toEqual([
      "Brook trout. 2 total from these 4. until Apr 30, then release",
      "Cutthroat trout. 2 total from these 4. until Apr 30, then release",
      "Dolly Varden/bull trout. 2 total from these 4. only 1 between them. until Apr 30, then release",
      "Lake trout. 2 total from these 4. only 1 between them. until Apr 30, then release",
      "Rainbow trout. 50 cm (20 in) or less if wild. counts toward the 4",
    ]);
    // the merged twin: origin null, nothing read for it
    expect(vm.boxes[0]!.rows[4]!.origin).toBeNull();
  });
});

describe("(T892, T907) a row's tributary source: `TRow.down` (the line, no longer drawn) and `TRow.tag` (the tag beside the name)", () => {
  const inputs = (f: TroutValue) => ({ trout: f, rules: { has: (i: number) => i === 0 }, frames: { rows: { rows: [{}] } } }) as unknown as RegsInputs;
  const day = { month: 4, day: 2 };
  const LINE = "Kootenay Lake — Main Body’s rule for its tributaries";
  const TAG = "Kootenay Lake — Main Body’s tributaries";
  const withDown = (down: string | null, tag: string | null = null, at = 2): TroutValue => {
    const f = vedder(), b = f.table.boxes[0]!;
    const put = (r: object) => ({ ...r, ...(down !== null ? { down } : {}), ...(tag !== null ? { tag } : {}) });
    return { ...f, table: { ...f.table, boxes: [{ ...b, rows: b.rows.map((r, i) => (i === at ? put(r) : r)) as typeof b.rows }] } };
  };
  it("absent (a release from before T892): null on every row, the label unchanged", () => {
    const vm = troutOf(inputs(vedder()), day, day) as TroutVM;
    expect(vm.boxes[0]!.rows.map((r) => r.tag)).toEqual([null, null, null, null, null]);
    expect(vm.boxes[0]!.rows[2]!.a11y).toBe("Dolly Varden/bull trout. 2 total from these 4. only 1 between them");
  });
  it("a release from T892 to T907 (the line alone): read, nothing drawn or spoken for it", () => {
    const vm = troutOf(inputs(withDown(LINE)), day, day) as TroutVM;
    expect(vm.boxes[0]!.rows.map((r) => r.tag)).toEqual([null, null, null, null, null]);
    expect(vm.boxes[0]!.rows[2]!.a11y).toBe("Dolly Varden/bull trout. 2 total from these 4. only 1 between them");
  });
  it("the tag: on its row only, the payload's words; the label reads it after the name, before the band, once", () => {
    const vm = troutOf(inputs(withDown(LINE, TAG)), day, day) as TroutVM;
    expect(vm.boxes[0]!.rows.map((r) => r.tag)).toEqual([null, null, TAG, null, null]);
    expect(vm.boxes[0]!.rows[2]!.a11y).toBe(`Dolly Varden/bull trout. ${TAG}. 2 total from these 4. only 1 between them`);
    expect(vm.boxes[0]!.rows[2]!.a11y.includes(LINE)).toBe(false);                      // the line is not read
  });
  it("two waters, and a name ending in s, are the model's forms; any other text is refused as the model refuses it", () => {
    expect(why(withDown("Duck Lake’s and Twin Lakes’ rules for their tributaries", "Duck Lake’s and Twin Lakes’ tributaries"))).toBeNull();
    expect(why(withDown("Twin Lakes’ rule for its tributaries", "Twin Lakes’ tributaries"))).toBeNull();
    expect(why(withDown("from Dutch Creek"))).toMatch(/row 0:2: the tributary line from Dutch Creek is not one of its forms/);
    expect(why(withDown(" Dutch Creek’s rule for its tributaries"))).toMatch(/row 0:2 down: a leading or trailing space/);
    // (T907) the tag's one form, and never without its line
    expect(why(withDown(LINE, LINE))).toMatch(/row 0:2: the tributary tag .* is not "<water>’s tributaries"/);
    expect(why(withDown(LINE, ` ${TAG}`))).toMatch(/row 0:2 tag: a leading or trailing space/);
    expect(why(withDown(null, TAG))).toMatch(/row 0:2: a tributary tag without its line/);
  });
  it("(T907 round 2, owner \"Drop the repeat\") a name already ending in Tributaries is said once; the doubled words are refused", () => {
    expect(why(withDown("Lake Revelstoke’s rule for its tributaries", "Lake Revelstoke’s tributaries"))).toBeNull();
    expect(why(withDown("Lake Revelstoke's Tributaries’ rule for its tributaries", "Lake Revelstoke’s tributaries")))
      .toMatch(/row 0:2: the tributary line .* says "tributaries" twice/);
    expect(why(withDown("Lake Revelstoke’s rule for its tributaries", "Lake Revelstoke Tributaries’ tributaries")))
      .toMatch(/row 0:2: the tributary tag .* says "tributaries" twice/);
    expect(why(withDown("Duck Lake’s rule for its tributaries", "Lake Revelstoke Tributaries’ and Duck Lake’s tributaries")))
      .toMatch(/says "tributaries" twice/);
  });
});
