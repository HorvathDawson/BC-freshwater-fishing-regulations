/**
 * (T868, owner 2026-11-18: "Has duplicate images here too") EACH FISH'S PICTURE ONCE PER SHEET. The Coquitlam River's
 * Whitefish sheet (an Other fish row drawn as its group card) drew Lake whitefish and Mountain whitefish twice: once as the
 * sheet's head row, then again on the card's keep line ("Up to 15 whitefish combined · any size"). The keep line keeps
 * them; the head row keeps only a fish the card does not picture.
 *
 * Every Other fish sheet of every water in the committed regs fixture, built by core and drawn by SheetBody: no `pic:` is
 * drawn twice, and the group sheets still picture each of their fish (on the card).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { buildRegsPage, type RegsInputs, type SheetVM } from "@app/core";
import { loadRegsInputs, targetFor } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { loadRegsStore } from "@app/ui";
import { SheetBlockView, SheetBody } from "./SheetBody";
import { LIGHT } from "../theme";

afterEach(cleanup);

const DAY = { month: 10, day: 10 };

/** Every water of the fixture with a part (the fixture holds the names of waters it cuts out), on Oct 10 2026. */
async function everyWater(): Promise<RegsInputs[]> {
  const st = await loadRegsStore(openRegsFixture());
  const items = await (await st.core()).all("SELECT item_id FROM item ORDER BY ord");
  const out: RegsInputs[] = [];
  for (const it of items) {
    const ord = await st.itemOrd(String(it.item_id));
    if (ord === null || !(await st.parts(ord)).length) continue;
    out.push(await loadRegsInputs(st, await targetFor(st, String(it.item_id), null), DAY, "Saturday", 2026));
  }
  expect(out.length).toBeGreaterThan(1);
  return out;
}

/** Every Other fish sheet of every water: its key, its VM. */
async function otherSheets(): Promise<{ key: string; vm: SheetVM }[]> {
  const out: { key: string; vm: SheetVM }[] = [];
  for (const inp of await everyWater()) {
    const page = buildRegsPage(inp, { today: DAY, who: { age: "adult", residency: "resident" } });
    for (const o of page.keep.otherFish ?? []) {
      const vm = page.sheet(o.open);
      if (vm) out.push({ key: `${inp.item?.name ?? "?"} ${o.open}`, vm });
    }
  }
  return out;
}

/** The fish codes the drawn sheet pictures, one per drawn picture. */
function drawnPics(vm: SheetVM): string[] {
  const r = render(<SheetBody vm={vm} palette={LIGHT} onOpen={() => {}} />);
  const codes = [...r.container.querySelectorAll("[data-testid^='pic:']")].map((e) => e.getAttribute("data-testid")!.slice(4));
  cleanup();
  return codes;
}

describe("<SheetBody>: each fish pictured once per sheet (T868)", () => {
  it("no Other fish sheet draws a fish's picture twice; a group sheet pictures its fish on the card", { timeout: 60_000 }, async () => {
    const sheets = await otherSheets();
    const groups = sheets.filter((s) => s.vm.blocks.some((b) => b.k === "group"));
    expect(groups.length).toBeGreaterThan(0);                              // the fixture holds group sheets (whitefish)
    const twice: string[] = [];
    for (const s of sheets) {
      const codes = drawnPics(s.vm);
      const dup = codes.filter((c, i) => codes.indexOf(c) !== i);
      if (dup.length) twice.push(`${s.key}: ${[...new Set(dup)].join(", ")}`);
    }
    expect(twice).toEqual([]);
    // the card keeps the pictures: a group sheet with pictured fish still draws them, inside the card's keep line
    for (const s of groups) {
      const r = render(<SheetBody vm={s.vm} palette={LIGHT} onOpen={() => {}} />);
      for (const p of r.container.querySelectorAll("[data-testid^='pic:']"))
        expect(p.closest("[data-testid='krow']"), `${s.key} ${p.getAttribute("data-testid")}`).not.toBeNull();
      cleanup();
    }
  });

  it("a group card that opens its sheet draws no heavy rule of its own under the sheet's title rule; elsewhere it keeps it", { timeout: 60_000 }, async () => {
    const s = (await otherSheets()).find((x) => x.vm.blocks[0]?.k === "group");
    expect(s, "a group sheet that opens with its card").toBeDefined();
    const heavy = (el: Element) => [...el.querySelectorAll("[data-testid='keep-group'] *")].filter((e) => getComputedStyle(e).borderTopWidth === "1.5px").length;
    // (T883, owner ~23:25) in its sheet the card is the answer alone: no head band, so none of its rule (the examples
    // fold keeps its own line); a card with its head (titled, not first in a sheet) draws the band's rule on top of that
    const inSheet = heavy(render(<SheetBody vm={s!.vm} palette={LIGHT} onOpen={() => {}} />).container);
    cleanup();
    const b = s!.vm.blocks[0]!;
    if (b.k !== "group") throw new Error("not a group block");
    const first = render(<SheetBlockView b={{ ...b, titled: true }} title="Whitefish" palette={LIGHT} onOpen={() => {}} />).container;
    expect(heavy(first)).toBe(inSheet + 1);
    expect(getComputedStyle(first.querySelector("[data-testid='keep-group']")!.firstElementChild!).borderTopWidth).toBe("1.5px");
  });
});
