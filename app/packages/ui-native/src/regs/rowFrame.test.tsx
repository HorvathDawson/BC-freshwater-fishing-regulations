/**
 * T138 — the reader shooter's row-frame target (app/tools/regs-web/rows.mjs `rowOf` + `blockOf`) over real Keep tabs.
 * The Vedder's frozen page has Chilliwack p2's group: a size box ("Of these, only 1 may be over 50 cm …") holding the
 * Dolly Varden and steelhead rows, and the rainbow row under its own tier head beside the box. Glenn's rainbow row opens
 * the Duncan River's tier, whose head carries "Its row covers its tributaries, so it applies here" (reader F4).
 * Mutations: dropping `testID="krow"` from GroupCard's Kind row fails every `rowOf` case; the old three-parent climb
 * (picture → strip → row → the box or the group's body) fails "RB" and "ST"; dropping `testID="khead"` from the Tier
 * head fails the two head cases (the frame would open under the head, cutting Glenn's F4 line); `blockOf` returning
 * the head for every row fails "ST" (the steelhead row is not the first in its box).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { KeepVM, RegsPage } from "@app/core";
import { KeepTab } from "./KeepTab";
import { LIGHT } from "../theme";
// @ts-expect-error — a plain .mjs module of the node tools (no declaration file)
import { HEAD_ID, ROW_ID, blockOf, rowOf } from "../../../../tools/regs-web/rows.mjs";
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
    expect(words(block)).toMatch(/^This river’s rule: up to 4 hatchery rainbow trout a day/);
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
