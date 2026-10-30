/**
 * (T256 item 1) THE ANSWER SLICES ANSWER EXACTLY AS THE WHOLE FILES, on a release cut with them ($SLICED_RELEASE, or the
 * live release when it carries them; skipped otherwise). For a sample of each tested region's waters: the page's inputs
 * (`targetFor` + `loadRegsInputs`), the water sheet (`source.water`) and its group (`loadRawWaterGroup`) read through a
 * loader FOCUSED on the region whose source REFUSES the whole core and water — so every read was the slice's — deep-equal
 * the same reads through a plain loader. Unnamed sections of the region: `itemForSection` says none from the slice alone.
 * A water of another region, focused elsewhere, falls back to the whole files (never a wrong answer).
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { tapDay } from "@app/core";
import { verify, type Manifest } from "@app/data";
import { makeReleaseSource, releaseLoader, type ReleaseLoader } from "@app/data/release";
import { loadRawWaterGroup, loadRegsInputs, openRegsStore, targetFor } from "@app/data/regs";
import { liveRelease, nodeOpener } from "../fixtureRelease";

const pick = (): string | null => {
  const env = process.env.SLICED_RELEASE;
  if (env) return env;
  const live = liveRelease();
  if (!live) return null;
  const m = JSON.parse(readFileSync(join(live, "manifest.json"), "utf8")) as Manifest;
  return m.files["water-2"] ? live : null;
};
const DIR = pick();

function loaderOf(dir: string, refuse: readonly string[] = []): { loader: ReleaseLoader; fetched: string[] } {
  const m = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as Manifest;
  const fetched: string[] = [];
  const loader = releaseLoader(m, {
    async bytes(name) {
      if (refuse.includes(name)) throw new Error(`the whole ${name} was asked for`);
      fetched.push(name);
      const f = m.files[name]!;
      return verify(f.url, new Uint8Array(readFileSync(join(dir, "..", f.url))), f);
    },
  }, nodeOpener());
  return { loader, fetched };
}

const DAY = { month: 6, day: 15 }, YEAR = 2026;

/** What a read gave, as plain data: Maps and Sets as entries, functions (a page's closures) left out. */
const plain = (v: unknown): unknown => JSON.parse(JSON.stringify(v, (_k, x: unknown) =>
  x instanceof Map ? { map: [...x.entries()] } : x instanceof Set ? { set: [...x] }
    : x instanceof Uint8Array ? { bytes: [...x] } : typeof x === "function" ? undefined : x));

async function read(loader: ReleaseLoader, item: string) {
  const st = await openRegsStore(loader, { tiles: null }), source = makeReleaseSource(loader);
  const ord = (await st.itemOrd(item))!;
  const sid = (await st.navRows(ord)).sections[0]?.sid;
  const inputs = sid === undefined ? null
    : await loadRegsInputs(st, await targetFor(st, item, sid), DAY, tapDay(DAY, YEAR).weekday, YEAR);
  return plain({ inputs, water: await source.water(item as never), group: await loadRawWaterGroup(st, item),
                 section: sid === undefined ? null : await source.itemForSection(sid as never) });
}

describe.skipIf(!DIR)(`T256 answer slices = the whole files (${DIR ?? "no sliced release"})`, () => {
  const whole = () => loaderOf(DIR!).loader;
  const sample = async (region: string, every: number): Promise<string[]> => {
    const core = await whole().db(`core-${region}` as never);
    return (await core.all("SELECT i.item_id FROM item i JOIN item_area a ON a.ord = i.ord WHERE i.pieces > 0 " +
                           "AND (',' || a.regions || ',') LIKE ? ORDER BY i.ord", `%,${region},%`))
      .map((r) => String(r.item_id)).filter((_, i) => i % every === 0);
  };

  it.each([["2", 40], ["6", 120], ["7b", 60]])("Region %s: every sampled water reads the same from its slices alone", async (region, every) => {
    const items = await sample(region, every);
    expect(items.length).toBeGreaterThan(10);
    for (const item of items) {
      const sliced = loaderOf(DIR!, ["core", "water"]);
      sliced.loader.focus(region as never);
      const got = await read(sliced.loader, item);
      expect(sliced.fetched.filter((f) => f === "core" || f === "water")).toEqual([]);
      expect(got, item).toEqual(await read(whole(), item));
    }
  }, 600_000);

  it("an unnamed section of the region has no water — said from the slice alone", async () => {
    const regs = await whole().db("core-2" as never);
    const blocks = await regs.all("SELECT block, b FROM cover_sid ORDER BY block LIMIT 3");
    const named = new Set((await regs.all("SELECT sid FROM item_section")).map((r) => Number(r.sid)));
    const unnamed: number[] = [];
    for (const { block, b } of blocks) {
      const bytes = b as Uint8Array;
      for (let i = 0; i < 4096 && unnamed.length < 30; i++)
        if ((bytes[i >> 3]! >> (i & 7)) & 1 && !named.has((Number(block) << 12) | i)) unnamed.push((Number(block) << 12) | i);
    }
    expect(unnamed.length).toBe(30);
    const sliced = loaderOf(DIR!, ["core", "water"]);
    sliced.loader.focus("2");
    const source = makeReleaseSource(sliced.loader);
    for (const s of unnamed) expect(await source.itemForSection(s as never)).toBeNull();
  });

  it("a water the focus region's slice does not cover is read from the whole files", async () => {
    const [far] = await sample("8", 50);
    const near = await whole().db("core-2" as never);
    expect(await near.get("SELECT 1 AS x FROM item WHERE item_id = ?", far!)).toBeUndefined();
    const focused = loaderOf(DIR!);
    focused.loader.focus("2");
    const got = await read(focused.loader, far!);
    expect(focused.fetched).toContain("water");
    expect(focused.fetched).toContain("core");
    expect(got).toEqual(await read(whole(), far!));
  });
});
