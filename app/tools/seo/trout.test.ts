/**
 * (T810 T) THE CRAWLER'S TROUT TABLE. On the trout fixture release a water's page model reads N's view model (`troutOf`):
 * the table's words are a "Trout and char" section, the top line's `{day}` slot is filled through `dayPhrase` with the
 * build day as today ("Up to 5 today"), and the model agrees with the app's own page (`verifyAgainstApp`). Since the
 * cutover (T810 L + S) every release carries the section, the normal fixture too: a water whose cell names a table has the
 * section, and one whose cell is NULL (no table drawn, Q-D3) has none.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage, WEEKDAYS, type RegsPage } from "@app/core";
import { loadRegsInputs, openRegsStore, type RegsStore } from "@app/data/regs";
import { openTroutFixture, TROUT_FIXTURE_ROOT } from "@app/data/release/node";
import { openFixture } from "./fixture";
import { listWaterOrds, troutWords, waterModel, type BuildDay, type WaterModel } from "./model";
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
        { today: on.day, who: { age: "adult", residency: "resident" } })));
      expect(verifyAgainstApp(m, pages), c.name).toEqual([]);
    }
  });
});

describe("the regs fixture release (it carries the section since the cutover)", () => {
  it("a water whose cell names a table has the section, and every page agrees with the app's", async () => {
    const { store } = await openFixture();
    const on: BuildDay = { day: { month: 10, day: 27 }, weekday: "Tuesday", year: 2026, iso: "2026-10-27" };
    let drawn = 0;
    for (const ord of await listWaterOrds(store)) {
      const m = await waterModel(store, ord, on);
      if (!m) continue;
      if (m.parts.some((p) => p.sections.some((s) => s.title === TITLE))) drawn++;
      const pages: RegsPage[] = await Promise.all(m.parts.map(async (p) => buildRegsPage(
        await loadRegsInputs(store, { kind: "part", item: m.id, partIx: p.partIx }, on.day, on.weekday, on.year),
        { today: on.day, who: { age: "adult", residency: "resident" } })));
      expect(verifyAgainstApp(m, pages), m.id).toEqual([]);
    }
    expect(drawn).toBeGreaterThan(0);
  });
});

describe("(T810 L) the kept-elsewhere list on the site", () => {
  it("a mixed list — the sentence, its row's worked lines, the groups' — is carried whole, as given, in order, after the table", async () => {
    const MIXED = ["This lake's rule: 2 a day here. Trout and char other than steelhead you kept anywhere else count toward those 2.",
      "Kept 1 at a stream? Keep 1 more here.", "Kept 2 elsewhere? Keep 3 more here."];
    const c = V.cases.find((x) => x.frame !== null)!, on = dayOf(c), ord = (await st.itemOrd(c.item))!;
    const m = (await waterModel(st, ord, on))!;
    const part = m.parts.find((p) => p.sections.some((x) => x.title === TITLE))!;
    const page = buildRegsPage(await loadRegsInputs(st, { kind: "part", item: m.id, partIx: part.partIx }, on.day, on.weekday, on.year),
      { today: on.day, who: { age: "adult", residency: "resident" } });
    const t = page.keep.trout;
    if (t?.kind !== "table") throw new Error(`${c.name}: no table`);
    const said = troutWords({ ...t, elsewhere: MIXED }).filter((x) => x !== null).map((x) => x!.text);
    expect(said.slice(-MIXED.length)).toEqual(MIXED);
  });
});
