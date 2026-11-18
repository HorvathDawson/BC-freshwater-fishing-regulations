/**
 * T138 — the reader shooter's row-frame target (app/tools/regs-web/rows.mjs `rowOf` + `blockOf`) over real Keep tabs.
 * The Vedder's frozen page has Chilliwack p2's group: a size box ("Of these, only 1 may be over 50 cm …") holding the
 * Dolly Varden and steelhead rows, and the rainbow row under its own tier head beside the box. Glenn's rainbow row opens
 * the Duncan River's tier, whose head carries "Its row covers its tributaries, so it applies here" (reader F4).
 * Mutations: dropping `testID="krow"` from GroupCard's Kind row fails every `rowOf` case; the old three-parent climb
 * (picture → strip → row → the box or the group's body) fails "RB" and "ST"; dropping `testID="khead"` from the Tier
 * head fails the two head cases (the frame would open under the head, cutting Glenn's F4 line); `blockOf` returning
 * the head for every row fails "ST" (the steelhead row is not the first in its box).
 *
 * (T810 R) The same functions over the trout table (`TroutCard`, behind the dev switch until the cutover), so the browser
 * tools hold before (the old card) and after (the table) with no flag: a table row is the `keep-row` whose picture is
 * `pic:<code>` or a twin's `pic:<code>/<origin>`; a row that opens its keep box or its release group frames that whole
 * block; `cardGroups` never counts KeepBox's own `keep-group` (a band of rows) as a group card; `IN_PAGE` (the source the
 * walk evaluates in the page) answers as the module does.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { troutOf, type KeepVM, type RegsInputs, type RegsPage, type TroutValue } from "@app/core";
import { KeepTab } from "./KeepTab";
import { TroutCard } from "./TroutCard";
import vectors from "../../../../fixtures/trout/trout-vectors.generated.json";
import { LIGHT } from "../theme";
// @ts-expect-error — a plain .mjs module of the node tools (no declaration file)
import { HEAD_ID, IN_PAGE, ROW_ID, blockOf, cardGroups, rowOf, troutShown } from "../../../../tools/regs-web/rows.mjs";
import vedder1010 from "./fixtures/vedder_1010.json";
import glenn615 from "./fixtures/glenn_615.json";

afterEach(cleanup);

const mount = (f: unknown) => render(<KeepTab keep={(f as RegsPage).keep as KeepVM} dateLabel="Oct 10" palette={LIGHT}
                                              actions={{ onSheet() {}, onDay() {}, onTab() {}, onBook() {} }} />).container;
const picsOf = (row: Element) => [...row.querySelectorAll("[data-testid^='pic:']")].map((p) => p.getAttribute("data-testid"));
const words = (e: Element) => e.textContent!.replace(/\s/g, " ");   // the chips' spaces are no-break

describe("row frame target (T138)", () => {
  it("every Keep row is a krow; each holds only its own pictures", () => {
    const c = mount(vedder1010);
    expect([ROW_ID, HEAD_ID]).toEqual(["krow", "khead"]);
    expect([...c.querySelectorAll("[data-testid='krow']")].map(picsOf)).toEqual([["pic:EB", "pic:GB", "pic:CT"], ["pic:DV", "pic:LT"], ["pic:ST"], ["pic:RB"]]);
  });
  it("RB (Vedder): the rainbow row, opened by its own tier head — not the steelhead row, the box or the group's body", () => {
    const c = mount(vedder1010), row = rowOf(c, "RB") as Element;
    expect(row).toBe(c.querySelector("[data-testid='pic:RB']")!.closest("[data-testid='krow']"));
    expect(words(row)).toBe("50 cm (20 in) or lessRainbow trout ›");
    const block = blockOf(row) as Element;
    expect(block.getAttribute("data-testid")).toBe("khead");
    // T479: the own tier's "Exception to regional quota" kicker heads its block
    expect(words(block)).toMatch(/^Exception to regional quotaThis river’s rule: up to 4 hatchery rainbow trout a day/);
  });
  it("ST (Vedder): the steelhead row itself — the second row in its box, so not the box head", () => {
    const row = rowOf(mount(vedder1010), "ST") as Element;
    expect(picsOf(row)).toEqual(["pic:ST"]);
    expect(blockOf(row)).toBe(row);
    expect(words(row)).not.toMatch(/Of these, only/);
  });
  it("RB (Glenn Jun 15): the Duncan River's tier head, which says why another river's rule applies (reader F4)", () => {
    const block = blockOf(rowOf(mount(glenn615), "RB")) as Element;
    expect(block.getAttribute("data-testid")).toBe("khead");
    expect(words(block)).toMatch(/Duncan River’s rule: up to 5 rainbow trout a day.*Its row covers its tributaries, so it applies here/);
  });
  it("a fish with no row on the page: null (the shooter throws, naming it)", () => {
    expect(rowOf(mount(vedder1010), "ZZ")).toBeNull();
  });
});

/** a vector's trout table, drawn alone (as `TroutCard.test.tsx` does: its own day, the store's bounds held open) */
function table(name: string) {
  const c = (vectors as unknown as { cases: { name: string; day: string; frame: TroutValue | null }[] }).cases.find((x) => x.name === name)!;
  const [month, day] = c.day.split("-").map(Number) as [number, number];
  const vm = troutOf({ trout: c.frame, rules: { has: () => true }, frames: { rows: { rows: { length: 1e6 } } } } as unknown as RegsInputs, { month, day }, { month, day });
  if (vm?.kind !== "table") throw new Error(name);
  return render(<TroutCard t={vm} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onTab() {}, onBook() {} }} />).container;
}
const tid = (e: Element | null) => e?.getAttribute("data-testid") ?? null;

describe("(T810 R) the same targets over the trout table", () => {
  it("Kitimat Jul 15: ST opens keep box 0 (its first row); EB is a row inside it; RB is the first twin (RB/wild)", () => {
    const c = table("kitimat-jul15");
    const st = rowOf(c, "ST") as Element;
    expect(tid(st)).toBe("keep-row");
    expect(picsOf(st)).toEqual(["pic:ST"]);
    expect(tid(blockOf(st))).toBe("trout-box:0");
    const eb = rowOf(c, "EB") as Element;
    expect(picsOf(eb)).toEqual(["pic:EB"]);
    expect(blockOf(eb)).toBe(eb);
    expect(picsOf(rowOf(c, "RB") as Element)).toEqual(["pic:RB/wild"]);
  });
  it("Kitimat Jul 15: DV is the release group's first row, so the frame is the whole group (its column word)", () => {
    const c = table("kitimat-jul15"), dv = rowOf(c, "DV") as Element;
    expect(picsOf(dv)).toEqual(["pic:DV"]);
    expect(tid(blockOf(dv))).toBe("trout-release");
  });
  it("a fish the table does not hold: null; a twin's origin is not a code (\"RB/w\" is no row)", () => {
    const c = table("kitimat-jul15");
    expect(rowOf(c, "ZZ")).toBeNull();
    expect(rowOf(c, "RB/w")).toBeNull();
  });
  it("cardGroups: the old cards' articles, never a KeepBox band; troutShown names what the screen shows", () => {
    const t = table("fraser-r2-mission-jul15");
    expect(t.querySelectorAll("[data-testid='keep-group']").length).toBeGreaterThan(0);
    expect(cardGroups(t)).toEqual([]);
    expect(troutShown(t)).toBe("table");
    cleanup();
    const v = mount(vedder1010);
    expect(cardGroups(v).length).toBe(v.querySelectorAll("[data-testid='keep-group']").length);
    expect(cardGroups(v).length).toBeGreaterThan(0);
    expect(troutShown(v)).toBe("card");
  });
  it("IN_PAGE (the walk's in-page copy) answers as the module", () => {
    const page = new Function(`${IN_PAGE}\nreturn { rowOf, blockOf, cardGroups, troutShown };`)() as
      { rowOf: typeof rowOf; blockOf: typeof blockOf; cardGroups: typeof cardGroups; troutShown: typeof troutShown };
    const c = table("kitimat-jul15");
    for (const code of ["ST", "EB", "RB", "DV", "ZZ"]) expect(page.rowOf(c, code), code).toBe(rowOf(c, code));
    expect(page.blockOf(rowOf(c, "DV"))).toBe(blockOf(rowOf(c, "DV")));
    expect(page.troutShown(c)).toBe("table");
    cleanup();
    const v = mount(vedder1010);
    expect(page.blockOf(page.rowOf(v, "RB"))).toBe(blockOf(rowOf(v, "RB")));
    expect(page.cardGroups(v)).toEqual(cardGroups(v));
  });
});
