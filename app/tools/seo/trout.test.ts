/**
 * (T810 T) THE CRAWLER'S TROUT TABLE. On the trout fixture release (a release that carries the optional `trout` section) a
 * water's page model reads N's view model (`troutOf`): the table's words are a "Trout and char" section, the top line's
 * `{day}` slot is filled through `dayPhrase` with the build day as today ("Up to 5 today"), and the model agrees with the
 * app's own page (`verifyAgainstApp`). On the normal fixture (no section) the page is today's: no such section.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage, WEEKDAYS, type RegsPage } from "@app/core";
import { loadRegsInputs, openRegsStore, type RegsStore } from "@app/data/regs";
import { openTroutFixture, TROUT_FIXTURE_ROOT } from "@app/data/release/node";
import { openFixture } from "./fixture";
import { listWaterOrds, waterModel, type BuildDay, type WaterModel } from "./model";
import { verifyAgainstApp } from "./verify";

interface Vector { readonly name: string; readonly item: string; readonly day: string; readonly weekday: string;
                   readonly frame: unknown | null; readonly composed: { readonly lead_today: string } }
const V = JSON.parse(readFileSync(join(TROUT_FIXTURE_ROOT, "trout-vectors.generated.json"), "utf8")) as { readonly cases: readonly Vector[] };
const WEEKDAY = Object.fromEntries(WEEKDAYS.map((w) => [w.slice(0, 3).toLowerCase(), w]));
const dayOf = (c: Vector): BuildDay => {
  const [m, d] = c.day.split("-").map(Number) as [number, number];
  return { day: { month: m, day: d }, weekday: WEEKDAY[c.weekday]!, year: 2026, iso: `2026-${c.day}` };
};
const TITLE = "Trout and char";
const words = (m: WaterModel) => m.parts.flatMap((p) => p.sections.flatMap((s) => s.items.map((i) => i.text)));

let st: RegsStore;
beforeAll(async () => { st = await openRegsStore(openTroutFixture()); });

describe("the trout fixture release", () => {
  it("every case water: the crawler's lead is the vector's lead_today; the page agrees with the app; no slot is left open", async () => {
    const drawn = V.cases.filter((c) => c.frame !== null);
    expect(drawn.length).toBeGreaterThan(20);
    for (const c of drawn) {
      const on = dayOf(c), ord = (await st.itemOrd(c.item))!;
      const m = (await waterModel(st, ord, on))!;
      const leads = m.parts.flatMap((p) => p.sections.filter((s) => s.title === TITLE).flatMap((s) => s.items.map((i) => i.text)));
      expect(leads, `${c.name}: the table's section`).toContain(c.composed.lead_today);
      expect(words(m).filter((t) => /[{}]/.test(t)), c.name).toEqual([]);
      const pages: RegsPage[] = await Promise.all(m.parts.map(async (p) => buildRegsPage(
        await loadRegsInputs(st, { kind: "part", item: m.id, partIx: p.partIx }, on.day, on.weekday, on.year),
        { today: on.day, who: { age: "adult", residency: "resident" }, troutTable: true })));
      expect(verifyAgainstApp(m, pages), c.name).toEqual([]);
    }
  });
});

describe("a release without the section", () => {
  it("draws today's page: no trout section on any water", async () => {
    const { store } = await openFixture();
    for (const ord of await listWaterOrds(store)) {
      const m = await waterModel(store, ord, { day: { month: 10, day: 27 }, weekday: "Tuesday", year: 2026, iso: "2026-10-27" });
      if (m) expect(m.parts.flatMap((p) => p.sections.map((s) => s.title))).not.toContain(TITLE);
    }
  });
});
