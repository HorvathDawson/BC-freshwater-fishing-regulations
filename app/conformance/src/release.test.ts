/**
 * THE CONFORMANCE SUITE AGAINST THE RELEASE (P11) — the reader the app ships, over a FIXTURE RELEASE.
 *
 * Until now the suite ran against the hand-written fixture alone, which is the tautology audit 01#13 named: the
 * fixture is written to pass it. This runs the same assertions against `makeReleaseSource` over files cut from the dev
 * fixture bundle by the release builder's OWN code (`python -m pipeline.tools.fixture_release`: paint, flow-area, core,
 * near, conditions), every one fetched through the loader and verified against its manifest.
 *
 * WHY A FIXTURE RELEASE AND NOT THE LIVE ONE. `python -m pipeline.deliver release` cannot run on the dev fixture (it has
 * no regs store), but the bundle half of the builder can, so the fixture release is built here, in `pnpm check`, with
 * no data — and RegsSource reads nothing of the regs half. A run over the live release would need data CI does not
 * hold and would only ever be a Tier 2 report.
 *
 * The suite's two sections are QUERIED from the fixture (each source names its own; handles mean nothing across
 * tables): the lowest reach of gnis:8634 a station speaks for as `good`, and the lowest reach no station speaks for.
 */
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import { httpFeed, parseManifest, verify, type ItemId, type Manifest, type PlaceId, type RegsSource, type SectionId,
         type StationId } from "@app/data";
import { makeBundleSource } from "@app/data/bundle";
import { makeFixtureSource } from "@app/data/fixture";
import { DEV_BUNDLE, openBundle } from "@app/data/bundle/node";
import { makeReleaseSource, releaseLoader, type ReleaseLoader } from "@app/data/release";
import { runConformance, type ConformanceSections } from "./suite";

const REPO = resolve(__dirname, "../../..");
const FEEDS = resolve(__dirname, "../../packages/data/dev/feeds/gauge");

let dir = "", m: Manifest;
const sections = {} as { -readonly [K in keyof ConformanceSections]: SectionId };

/**
 * The dev bundle, plus three `lake_gauge` rows: the fixture has none, and the lake road (section → item → the lowest
 * station in the lake) is the cross-file read most easily got wrong. Two stations in one lake pin "the lowest".
 */
let bundlePath = "";
const LAKE_ROWS = [["wbk:329177889", "08MH103"], ["wbk:329177889", "08MH016"], ["wbk:329177990", "08MH056"]];

beforeAll(async () => {
  const out = mkdtempSync(join(tmpdir(), "fixture-release-"));
  bundlePath = join(out, "bundle.sqlite");
  copyFileSync(DEV_BUNDLE, bundlePath);
  const w = new DatabaseSync(bundlePath);
  for (const [item, station] of LAKE_ROWS) w.prepare("INSERT INTO lake_gauge VALUES (?, ?)").run(item!, station!);
  w.close();
  // the fixture source's edition, so the two fixtures state one (the dev bundle's own is empty)
  const validUntil = (await makeFixtureSource().info()).validUntil!;
  dir = execFileSync(join(REPO, ".venv/bin/python"),
    ["-m", "pipeline.tools.fixture_release", "--bundle", bundlePath, "--out", out, "--valid-until", validUntil],
    { cwd: REPO, encoding: "utf8" }).trim();
  m = parseManifest(JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")));
  const b = openBundle(bundlePath);
  const one = async (sql: string) => Number((await b.get(sql))!.sid) as SectionId;
  sections.lowerChilliwack = await one(
    "SELECT min(s.sid) AS sid FROM item_section s JOIN item i ON i.ord = s.ord " +
    "JOIN section_gauge sg ON sg.sid = s.sid WHERE i.item_id = 'gnis:8634' AND sg.trust = 'good'");
  sections.jeperson = await one(
    "SELECT min(sid) AS sid FROM item_section WHERE sid NOT IN (SELECT sid FROM section_gauge)");
  b.close?.();
}, 60_000);

/** node:sqlite over a verified file, through the loader exactly as the app opens one. */
function loader(): ReleaseLoader {
  const tmp = mkdtempSync(join(tmpdir(), "release-open-"));
  return releaseLoader(m, {
    bytes: async (name) => {
      const spec = m.files[name];
      if (!spec) throw new Error(`no ${name}`);
      return verify(spec.url, new Uint8Array(readFileSync(join(dir, "..", spec.url))), spec);
    },
  }, async (name, bytes) => {
    const p = join(tmp, `${name}.sqlite`);
    writeFileSync(p, await bytes());
    return openBundle(p);
  });
}

/** The fixture feed, served from disk: what `pnpm tiles` serves at /feeds/gauge. */
const feedFetch = (async (url: string) => {
  const name = url.split("/").pop()!;
  if (!readdirSync(FEEDS).includes(name)) return { ok: false, status: 404 };
  const body = readFileSync(join(FEEDS, name), "utf8");
  return { ok: true, status: 200, json: async () => JSON.parse(body) };
}) as unknown as typeof fetch;

runConformance("release", async () => makeReleaseSource(loader(), { feed: httpFeed("fixture:/feeds/gauge", feedFetch) }),
               sections, { noProducer: ["lakes"] });

/**
 * PARITY: the release reader answers EXACTLY as the bundle reader over the bundle the release was cut from. This is
 * what proves the cross-file reads (two keyed reads joined in code) are the joins they replace — the conformance suite
 * above asserts behaviour, this asserts the same rows. The only differences allowed are the release's own: the panel
 * target areas are stored to 0.001 km² (flow-area/1) and the climatology to float16 (clim/2).
 */
describe("the release reader answers like the bundle reader (P11)", () => {
  let bundle: RegsSource, release: RegsSource, L: ReleaseLoader;
  const feed = () => httpFeed("fixture:/feeds/gauge", feedFetch);
  beforeAll(() => {
    bundle = makeBundleSource(openBundle(bundlePath), { feed: feed() });
    L = loader();
    release = makeReleaseSource(L, { feed: feed() });
  });
  const allSections = () => {
    const b = new DatabaseSync(bundlePath, { readOnly: true });
    try { return b.prepare("SELECT DISTINCT sid FROM item_section ORDER BY sid").all().map((r) => Number(r.sid) as SectionId); }
    finally { b.close(); }
  };
  const column = (sql: string) => {
    const b = new DatabaseSync(bundlePath, { readOnly: true });
    try { return b.prepare(sql).all().map((r) => Object.values(r)[0] as string); } finally { b.close(); }
  };

  it("identity, water and search", async () => {
    for (const s of allSections().filter((_, i) => i % 50 === 0)) {
      expect([s, await release.itemForSection(s)]).toEqual([s, await bundle.itemForSection(s)]);
      expect([s, await release.waterFor(s)]).toEqual([s, await bundle.waterFor(s)]);
    }
    for (const id of column("SELECT item_id FROM item ORDER BY ord").filter((_, i) => i % 10 === 0) as ItemId[]) {
      expect([id, await release.water(id)]).toEqual([id, await bundle.water(id)]);
      expect([id, await release.itemExists(id)]).toEqual([id, await bundle.itemExists(id)]);
    }
    for (const q of ["vedder", "creek", "lake", "slough", "chilliwack", "greyell", "a_b", "xx"])
      expect([q, await release.searchNames(q, 10)]).toEqual([q, await bundle.searchNames(q, 10)]);
    for (const q of ["chilliwack", "hope", "a"])
      expect([q, await release.searchPlaces(q, 10)]).toEqual([q, await bundle.searchPlaces(q, 10)]);
    for (const p of column("SELECT DISTINCT place_id FROM place_water ORDER BY place_id") as PlaceId[])
      expect([p, await release.watersNear(String(p) as PlaceId)]).toEqual([p, await bundle.watersNear(String(p) as PlaceId)]);
    const c = await release.counts();
    expect(c).toEqual(await bundle.counts());
  });

  it("the gauge reads, the lake road and the walks", async () => {
    const all = allSections();
    expect(await release.stationsFor(all)).toEqual(await bundle.stationsFor(all));
    const lakes = await release.lakeStationsFor(all);
    expect(lakes).toEqual(await bundle.lakeStationsFor(all));
    expect(new Set(lakes.values())).toEqual(new Set(["08MH016", "08MH056"]));     // the lowest of a lake's two
    for (const s of all.filter((_, i) => i % 40 === 0)) {
      expect([s, await release.gaugeForSection(s)]).toEqual([s, await bundle.gaugeForSection(s)]);
      expect([s, await release.traceToGauge(s)]).toEqual([s, await bundle.traceToGauge(s)]);
      expect([s, await release.panelRoutes(s)]).toEqual([s, await bundle.panelRoutes(s)]);
    }
    for (const id of column("SELECT item_id FROM item ORDER BY ord").filter((_, i) => i % 5 === 0) as ItemId[]) {
      expect([id, await release.gaugeForItem(id)]).toEqual([id, await bundle.gaugeForItem(id)]);
      expect([id, await release.locate(id)]).toEqual([id, await bundle.locate(id)]);
    }
    expect(await release.gaugePoints()).toEqual(await bundle.gaugePoints());
    expect(await release.basinMembers()).toEqual(await bundle.basinMembers());
    for (const st of column("SELECT station FROM gauge ORDER BY station") as StationId[])
      expect([st, await release.gaugeParameters(st)]).toEqual([st, await bundle.gaugeParameters(st)]);
  });

  it("the panels from paint + flow-area are section_panel + panel_member (areas to 0.001 km²)", async () => {
    const all = allSections();
    const [r, b] = [await release.panelsFor(all), await bundle.panelsFor(all)];
    expect([...r.keys()]).toEqual([...b.keys()]);
    for (const [s, p] of b) {
      const q = r.get(s)!;
      expect(Math.abs((q.areaKm2 ?? NaN) - (p.areaKm2 ?? NaN))).toBeLessThanOrEqual(0.0005);
      expect(q.members.map((m) => ({ ...m, areaKm2: Math.round(m.areaKm2 * 1000) })))
        .toEqual(p.members.map((m) => ({ ...m, areaKm2: Math.round(m.areaKm2 * 1000) })));
    }
  });

  it("the series: the same observations over the same envelope, to float16", async () => {
    for (const st of column("SELECT station FROM gauge ORDER BY station") as StationId[])
      for (const span of ["72h", "year"] as const) {
        const [r, b] = [await release.gaugeSeries(st, span), await bundle.gaugeSeries(st, span)];
        expect([st, span, r === null]).toEqual([st, span, b === null]);
        if (!r || !b) continue;
        const { band: rb, yearBand: ry, ...rv } = r.value, { band: bb, yearBand: by, ...bv } = b.value;
        expect(rv).toEqual(bv);
        expect([ry === undefined, ry?.length]).toEqual([by === undefined, by?.length]);
        for (const [got, want] of [[rb, bb], [ry ?? [], by ?? []]] as const)
          got.forEach((x, i) => x?.forEach((v, k) =>
            expect(Math.abs(v - want[i]![k]!)).toBeLessThanOrEqual(Math.max(Math.abs(want[i]![k]!) * 1e-3, 1e-4))));
      }
  });

  it("opens only what it was asked for: a search is core + near, a gauge question conditions", async () => {
    const fresh = loader(), s = makeReleaseSource(fresh, { feed: feed() });
    await s.itemForSection(sections.lowerChilliwack);
    expect(fresh.opened()).toEqual(["core"]);
    await s.searchNames("vedder", 5);
    expect(fresh.opened()).toEqual(["core", "near"]);
    await s.gaugeForSection(sections.lowerChilliwack);
    expect(fresh.opened()).toEqual(["core", "near", "conditions"]);
    await s.panelsFor([sections.lowerChilliwack]);
    expect(fresh.opened()).toEqual(["core", "near", "conditions", "paint", "flow-area"]);
  });
});
