/**
 * The v1 site's indexed URLs keep working: every one is a single 301 to a page that exists (200), none to another redirect,
 * none to a 404. The committed crosswalk covers every v1 path; the fixture build proves the targets; the generated worker
 * answers exactly as the pure function does.
 */
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";
import { openRegsFixture } from "@app/data/release/node";
import { buildAll, type Built } from "./build";
import { TODAY } from "./fixture";
import { legacyRedirect, legacyRedirects, type Crosswalk } from "./legacy";

const APP = join(dirname(fileURLToPath(import.meta.url)), "../..");
const paths = readFileSync(join(APP, "seo/legacy-paths.txt"), "utf8").split("\n").filter(Boolean);
const committed = JSON.parse(readFileSync(join(APP, "seo/legacy-crosswalk.json"), "utf8")) as Crosswalk;

describe("the committed crosswalk", () => {
  it("covers every v1 path exactly once, and nothing else", () => {
    // the v1 site's path list is frozen (seo/legacy-paths.txt), not a release's count
    expect(paths.length).toBe(19331);
    expect(new Set(paths).size).toBe(paths.length);
    expect(Object.keys(committed).sort()).toEqual([...paths].sort());
  });
  it("resolves almost every path to an item (the rest to a region or the home page)", () => {
    const items = paths.filter((p) => Array.isArray(committed[p]) && (committed[p] as string[]).some((c) => !/^r\d+$/.test(c)));
    // a floor on the frozen v1 list's resolved share, not a release count
    expect(items.length).toBeGreaterThan(19000);
  });
});

describe("v1 redirects on the fixture release", () => {
  let built: Built;
  const FIXTURE_OLD = ["w-one", "w-two", "gone-with-region", "gone-region-text", "unknown"];
  beforeAll(async () => {
    const probe = await buildAll({ loader: openRegsFixture(), on: TODAY });
    const [a, b] = probe.build.models;
    const crosswalk: Crosswalk = {
      "w-one": [a!.id], "w-two": ["nope:1", b!.id, "r3"], "gone-with-region": ["nope:2", `r${a!.region}`],
      "gone-region-text": String(a!.region), "unknown": null,
    };
    built = await buildAll({ loader: openRegsFixture(), on: TODAY, crosswalk });
  }, 240_000);

  it("every v1 path is one 301 to a page that was built (no chain, no 404)", () => {
    const real = new Set(built.build.pages.map((p) => p.path));
    const map = built.legacy!.map;
    expect(Object.keys(map).length).toBe(FIXTURE_OLD.length);
    for (const old of FIXTURE_OLD) {
      const hop = legacyRedirect(map, `/waterbody/${old}/`);
      expect(hop?.status, old).toBe(301);
      expect(real.has(hop!.to) || hop!.to === "/", `${old} -> ${hop!.to}`).toBe(true);
      expect(legacyRedirect(map, hop!.to), `${old} chains`).toBeNull();
    }
    expect(built.legacy!.counts).toEqual({ water: 2, region: 2, home: 1 });
  });

  it("the first candidate that has a page wins; a region is the fallback; the home page the last", () => {
    const m = built.legacy!.map;
    expect(m["/waterbody/w-one"]).toBe(`/water/${built.build.slugMap.waters[built.build.models[0]!.id]!.slug}`);
    expect(m["/waterbody/w-two"]).toBe(`/water/${built.build.slugMap.waters[built.build.models[1]!.id]!.slug}`);
    expect(m["/waterbody/gone-with-region"]).toMatch(/^\/region\/\d+$/);
    expect(m["/waterbody/unknown"]).toBe("/");
  });

  it("the generated worker answers as the pure function does, and passes other paths to the assets", async () => {
    const src = built.files.get("_worker.js")!;
    const mod = await import(/* @vite-ignore */ `data:text/javascript;base64,${Buffer.from(src).toString("base64")}`);
    const env = { ASSETS: { fetch: () => "asset" } };
    const hit = await mod.default.fetch(new Request("https://canifishthis.ca/waterbody/w-one/"), env);
    expect(hit.status).toBe(301);
    expect(new URL(hit.headers.get("location")!).pathname).toBe(legacyRedirect(built.legacy!.map, "/waterbody/w-one")!.to);
    expect(await mod.default.fetch(new Request("https://canifishthis.ca/water/anything"), env)).toBe("asset");
    expect(legacyRedirects({ x: null }, new Map(), new Set()).counts.home).toBe(1);
  });
});
