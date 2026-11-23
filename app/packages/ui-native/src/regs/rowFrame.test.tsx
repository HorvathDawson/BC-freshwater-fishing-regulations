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
 * (T810 cutover) Since the cutover no Keep tab draws the group card: the old-card cases are retired (see below).
 * (T810 R) The same functions over the trout table (`TroutCard`), so the browser
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
import { IN_PAGE, blockOf, cardGroups, rowOf, troutShown } from "../../../../tools/regs-web/rows.mjs";
import vedder1010 from "./fixtures/vedder_1010.json";
import { TEST_TROUT_SOURCES } from "./testing/sources";

afterEach(cleanup);

const mount = (f: unknown) => render(<KeepTab troutSources={TEST_TROUT_SOURCES} keep={(f as RegsPage).keep as KeepVM} dateLabel="Oct 10" palette={LIGHT}
                                              actions={{ onSheet() {}, onDay() {}, onTab() {}, onBook() {} }} />).container;
const picsOf = (row: Element) => [...row.querySelectorAll("[data-testid^='pic:']")].map((p) => p.getAttribute("data-testid"));

describe("row frame target (T138)", () => {
  // (T810 cutover) retired, the four old-card cases over the Vedder and Glenn ("every Keep row is a krow …", "RB (Vedder):
  // … its own tier head", "ST (Vedder): …", "RB (Glenn Jun 15): the Duncan River's tier head …"): no Keep tab draws the
  // group card since the cutover, so no page has a `krow` or `khead`. The same targets over the table are the "(T810 R)"
  // cases below (a row, its keep box, its release group, a twin), and the Vedder's rainbow row through the in-page copy
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
  return render(<TroutCard t={vm} sources={TEST_TROUT_SOURCES} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onTab() {}, onBook() {} }} />).container;
}
const tid = (e: Element | null) => e?.getAttribute("data-testid") ?? null;

describe("(T810 R) the same targets over the trout table", () => {
  // (T810 S, trout 4) the re-cut Kitimat puts the hatchery rainbow first (RB/hatchery, ST, EB, …, RB/wild)
  it("Kitimat Jul 15: RB (its first twin, RB/hatchery) opens keep box 0 (its first row); ST and EB are rows inside it", () => {
    const c = table("kitimat-jul15");
    const rb = rowOf(c, "RB") as Element;
    expect(tid(rb)).toBe("keep-row");
    expect(picsOf(rb)).toEqual(["pic:RB/hatchery"]);
    expect(tid(blockOf(rb))).toBe("trout-box:0");
    for (const code of ["ST", "EB"]) {
      const r = rowOf(c, code) as Element;
      expect(picsOf(r)).toEqual([`pic:${code}`]);
      expect(blockOf(r)).toBe(r);
    }
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
    expect(t.querySelectorAll("[data-testid='box-group']").length).toBeGreaterThan(0);   // KeepBox's bands of rows
    expect(cardGroups(t)).toEqual([]);
    expect(troutShown(t)).toBe("table");
    cleanup();
    // (T810 cutover) the Vedder's real page: the table, and no group card (no Keep tab draws one since the cutover)
    const v = mount(vedder1010);
    expect([cardGroups(v), troutShown(v), rowOf(v, "RB") === null]).toEqual([[], "table", false]);
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
