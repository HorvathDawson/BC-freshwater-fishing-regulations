/**
 * T192: the slug map is stable, and a water's page model is the app's answer — over the committed regs fixture release
 * (packages/data/dev/regs-release), never the province. Mutation-pinned: a planted wrong verdict, a dropped rule and an
 * invented sentence each fail the comparison.
 */
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage, type RegsPage } from "@app/core";
import { loadRegsInputs, type RegsStore } from "@app/data/regs";
import { openFixture, TODAY } from "./fixture";
import { listWaterOrds, waterModel, type WaterModel } from "./model";
import { assignSlugs, slugOf } from "./slug";
import { verifyAgainstApp } from "./verify";

const HERE = dirname(fileURLToPath(import.meta.url));

/** the app's pages for a model's parts, built here (not through model.ts) */
async function appPages(store: RegsStore, m: WaterModel): Promise<RegsPage[]> {
  return Promise.all(m.parts.map(async (p) => buildRegsPage(
    await loadRegsInputs(store, { kind: "part", item: m.id, partIx: p.partIx }, TODAY.day, TODAY.weekday, TODAY.year),
    { today: TODAY.day, who: { age: "adult", residency: "resident" } })));
}

describe("slugs (T192)", () => {
  it("folds names to ASCII, lowercase", () => {
    expect(slugOf("Vedder River")).toBe("vedder-river");
    expect(slugOf("Écho Lake / Lac d’Écho")).toBe("echo-lake-lac-decho");
    expect(slugOf("St. Mary’s Creek #2")).toBe("st-marys-creek-2");
  });
  const A = { id: "gnis:1", name: "Mill Creek", region: 3 }, B = { id: "gnis:2", name: "Mill Creek", region: 5 };
  it("collisions both take the region suffix, whichever was seen first", () => {
    const one = assignSlugs([A, B]).map.waters, two = assignSlugs([B, A]).map.waters;
    expect(one).toEqual(two);
    expect([one["gnis:1"]!.slug, one["gnis:2"]!.slug]).toEqual(["mill-creek-r3", "mill-creek-r5"]);
  });
  it("a slug, once assigned, never moves: a rename keeps it, a newcomer never takes it, a removal is reported", () => {
    const first = assignSlugs([A]).map;
    expect(first.waters["gnis:1"]!.slug).toBe("mill-creek");
    const next = assignSlugs([{ ...A, name: "Mill Brook" }, B], first);
    expect(next.map.waters["gnis:1"]!.slug).toBe("mill-creek");        // renamed, same URL
    expect(next.map.waters["gnis:2"]!.slug).toBe("mill-creek-r5");     // the newcomer cannot take it
    const gone = assignSlugs([B], next.map);
    expect(gone.removed).toEqual([{ id: "gnis:1", slug: "mill-creek", region: 3 }]);
    expect(gone.map.waters["gnis:1"]).toBeDefined();                   // kept: the URL stays reserved for its 301
  });
  it("the same name twice in one region still ends distinct", () => {
    const s = assignSlugs([A, { ...A, id: "gnis:9" }]).map.waters;
    expect(new Set(Object.values(s).map((e) => e.slug)).size).toBe(2);
  });
});

describe("the page model is the app's answer (T192, fixture release)", () => {
  let store: RegsStore, loader: Awaited<ReturnType<typeof openFixture>>["loader"], models: WaterModel[];
  beforeAll(async () => {
    ({ loader, store } = await openFixture());
    models = [];
    for (const ord of await listWaterOrds(store)) { const m = await waterModel(store, ord, TODAY); if (m) models.push(m); }
  }, 120_000);

  it("a page for every water with rules, each with a region and a verdict", () => {
    expect(models.length).toBeGreaterThan(20);
    for (const m of models) {
      expect(m.region, m.id).toBeGreaterThanOrEqual(1);
      expect(m.parts.length, m.id).toBeGreaterThan(0);
      expect(m.parts.some((p) => p.sections.length > 0), m.id).toBe(true);
    }
  });

  it("every water's verdict, rules and words equal the app's presenter on the same bundle", async () => {
    const bad: string[] = [];
    for (const m of models) bad.push(...verifyAgainstApp(m, await appPages(store, m)));
    expect(bad).toEqual([]);
  }, 120_000);

  it("the Vedder says what the app says: open today, the 2 a day from streams, the 4 hatchery rainbow", () => {
    const v = models.find((m) => m.name === "Vedder River")!;
    expect(v.status).toBe("open");
    const text = v.parts.flatMap((p) => p.sections.flatMap((s) => s.items.map((i) => i.text))).join("\n");
    expect(text).toMatch(/2 a day per angler from streams/);
    expect(text).toMatch(/This river’s rule: up to 4 hatchery rainbow trout a day/);
  });

  it("MUTATION: a wrong verdict, a dropped rule and an invented sentence are each reported", async () => {
    const m = models.find((x) => x.name === "Vedder River")!, pages = await appPages(store, m);
    expect(verifyAgainstApp(m, pages)).toEqual([]);
    const flip = { ...m, status: "closed" as const, parts: m.parts.map((p) => ({ ...p, status: "closed" as const })) };
    expect(verifyAgainstApp(flip, pages).join("|")).toMatch(/status closed, the app says open/);
    const dropped = { ...m, parts: m.parts.map((p) => ({ ...p, sections: p.sections.map((s) => ({ ...s, items: s.items.filter((i) => !/hatchery rainbow/.test(i.text)) })) })) };
    expect(verifyAgainstApp(dropped, pages).join("|")).toMatch(/the app says ".*hatchery rainbow.*" and the page does not/);
    const invented = { ...m, parts: m.parts.map((p, i) => i ? p : { ...p, sections: [{ title: "Extra", items: [{ text: "Keep 10 a day of anything.", book: [] }] }, ...p.sections] }) };
    expect(verifyAgainstApp(invented, pages).join("|")).toMatch(/Keep 10 a day of anything/);
    expect(verifyAgainstApp({ ...m, answer: "Closed to fishing today." }, pages).join("|")).toMatch(/does not open with its status/);
  });

  it("reads only the bundle: the release's own files, and no module here names a curated or pipeline path", () => {
    const names = Object.keys(loader.manifest.files);
    expect(loader.opened().every((n) => n === "core" || n === "geo" || n === "water" || n === "regs-sid" || n.startsWith("regs-"))).toBe(true);
    expect(names.length).toBeGreaterThan(5);
    for (const f of readdirSync(HERE).filter((n) => n.endsWith(".ts") && !n.endsWith(".test.ts"))) {
      expect(readFileSync(join(HERE, f), "utf8"), f).not.toMatch(/data\/curated|data\/generated|pipeline\//);
    }
  });
});
