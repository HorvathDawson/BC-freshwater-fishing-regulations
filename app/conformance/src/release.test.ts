/**
 * THE CONFORMANCE SUITE AGAINST THE RELEASE (P11) — the reader the app ships, over a FIXTURE RELEASE.
 *
 * Until now the suite ran against the hand-written fixture alone, which is the tautology audit 01#13 named: the
 * fixture is written to pass it. This runs the same assertions against `makeReleaseSource` over files cut by the
 * release builder's OWN code (`pipeline/deliver/release/build.py` `bundle_files`: paint, flow-area, access, core, near,
 * conditions; cut once, then frozen), committed as the `conformance` fixture release (`./fixtureRelease.ts`), every one fetched through the
 * loader and verified against its manifest.
 *
 * WHY A FIXTURE RELEASE AND NOT THE LIVE ONE. A run over the live release would need data CI does not hold and would
 * only ever be a Tier 2 report; the fixture runs in `pnpm check` with no data — and WaterSource reads nothing of the regs
 * half.
 *
 * The suite's two sections are QUERIED from the fixture (each source names its own; handles mean nothing across
 * tables): the lowest reach of gnis:8634 a station speaks for as `good`, and the lowest reach no station speaks for.
 *
 * Its parity half — the release reader answering exactly as the dev bundle reader over the bundle the release was cut
 * from — went with that reader (T61). `tools/release-parity.test.ts` still holds every release query to the pipeline
 * bundle's rows (Tier 2).
 */
import { mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import { answerFrom, classStandings, httpFeed, parseManifest, sectionStanding, sheetLead, speakerFor, speaks, verify, type ItemId, type Manifest, type Panel,
         type WaterSource, type SectionId } from "@app/data";
import { openSqlite } from "@app/data/sql/node";
import { makeReleaseSource, releaseLoader, type DbName, type ReleaseLoader } from "@app/data/release";
import { fixtureRelease } from "./fixtureRelease";
import { runConformance, type ConformanceSections } from "./suite";

const FEEDS = join(__dirname, "../../packages/data/dev/feeds/gauge");

let dir = "", m: Manifest;
const sections = {} as { -readonly [K in keyof ConformanceSections]: SectionId };

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
    return openSqlite(p);
  });
}

/** One column of a release file's rows (the test's own reads; the reader under test asks through its loader). */
let ask: (file: DbName, sql: string) => Promise<(string | number)[]>;

beforeAll(async () => {
  ({ dir } = fixtureRelease("conformance"));
  m = parseManifest(JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")));
  const L = loader();
  ask = async (file, sql) => (await (await L.db(file)).all(sql)).map((r) => Object.values(r)[0] as string | number);
  const chilliwack = new Set(await ask("core",
    "SELECT s.sid FROM item_section s JOIN item i ON i.ord = s.ord WHERE i.item_id = 'gnis:8634'"));
  const good = await ask("conditions", "SELECT sid FROM section_gauge WHERE trust = 'good' ORDER BY sid");
  const gauged = new Set(await ask("conditions", "SELECT sid FROM section_gauge"));
  sections.lowerChilliwack = Number(good.find((s) => chilliwack.has(s))) as SectionId;
  sections.jeperson = Number((await ask("core", "SELECT DISTINCT sid FROM item_section ORDER BY sid"))
    .find((s) => !gauged.has(s))) as SectionId;
}, 60_000);

/** The fixture feed, served from disk: the feed the fixture slice was cut with (feeds/gauge, `feed-contract.test`). */
const feedFetch = (async (url: string) => {
  const name = url.split("/").pop()!;
  if (!readdirSync(FEEDS).includes(name)) return { ok: false, status: 404 };
  const body = readFileSync(join(FEEDS, name), "utf8");
  return { ok: true, status: 200, json: async () => JSON.parse(body) };
}) as unknown as typeof fetch;

runConformance("release", async () => makeReleaseSource(loader(), { feed: httpFeed("fixture:/feeds/gauge", feedFetch) }),
               sections, { noProducer: ["lakes"] });

describe("the release reader over the fixture release (P11)", () => {
  let release: WaterSource, L: ReleaseLoader;
  const feed = () => httpFeed("fixture:/feeds/gauge", feedFetch);
  beforeAll(() => {
    L = loader();
    release = makeReleaseSource(L, { feed: feed() });
  });
  const allSections = async () =>
    (await ask("core", "SELECT DISTINCT sid FROM item_section ORDER BY sid")).map((s) => Number(s) as SectionId);

  it("geometry: the fixture release has no water file, so every water's box is null, never a guess", async () => {
    const ids = (await ask("core", "SELECT item_id FROM item ORDER BY ord")) as ItemId[];
    expect(ids.length).toBeGreaterThan(0);
    expect(m.files.water).toBeUndefined();
    const fromRelease = await Promise.all(ids.map((id) => release.waterBox(id)));
    expect(fromRelease.filter((b) => b !== null)).toEqual([]);
    expect(L.opened()).not.toContain("water");
  });

  it("the lake road takes the lowest of a lake's stations", async () => {
    const lakes = await release.lakeStationsFor(await allSections());
    expect(new Set(lakes.values())).toEqual(new Set(["08MH016", "08MH056"]));     // the lowest of a lake's two
  });

  /*
   * THE CONDITIONS MAP'S CLASSES (2026-10-21): a section's class value IS its viewport-path standing — for every section
   * of the fixture, at every horizon and in both quantities. The per-tile web map colours from the class table; the
   * native map from `panelsFor` + `lakeStationsFor` per visible section. One number, or the two maps disagree about a
   * river.
   */
  it("the standing classes answer every section exactly as its own panel or lake station does", async () => {
    const oracle = (p: Panel, i: typeof idx, q: "discharge" | "level", h: 0 | 1 | 3 | 5 | 10 | "change") => {
      const { answer } = answerFrom(p, i, q, h);
      return answer.ok ? answer.value.percentile : -0.01;
    };
    const all = await allSections();
    const idx = await feed().index();
    expect(idx).not.toBeNull();
    const [c, panels, lakes] = await Promise.all([release.standingClasses(), release.panelsFor(all),
                                                  release.lakeStationsFor(all)]);
    expect([lakes.size > 0, panels.size > 0]).toEqual([true, true]);
    let coloured = 0;
    for (const q of ["discharge", "level"] as const)
      // every horizon, the Change view (T52) included: one pathway, so one oracle
      for (const h of [0, 1, 3, 5, 10, "change"] as const) {
        const v = classStandings(c, idx, q, h);
        for (const s of all) {
          const k = c.classOf(s);
          // the oracle is the SHEET's answer (`answerFrom`, rows and all) for a panel, the station's reading for a lake
          const p = panels.get(s);
          const want = p ? oracle(p, idx, q, h) : sectionStanding(undefined, lakes.get(s), idx, q, h);
          const got = k === null ? null : Number.isNaN(v[k]!) ? null : v[k]!;
          if (got !== want) expect([s, q, h, got]).toEqual([s, q, h, want]);
          if (got !== null) coloured++;
        }
      }
    expect(coloured > 0).toBe(true);
  });

  /*
   * ONE SPEAKER (T105): the map's class for a section IS the speaker the sheet asks for it (`speakerFor` over the
   * section's own `panelsFor` + `lakeStationsFor`), and the sheet's lead is one of that speaker's stations — every
   * section.
   */
  it("the map's class and the sheet's speaker are one, and the sheet leads with the speaker's station", async () => {
    const all = await allSections();
    const [c, panels, lakes] = await Promise.all([release.standingClasses(), release.panelsFor(all),
                                                  release.lakeStationsFor(all)]);
    const kinds = new Set<string>();
    for (const s of all) {
      const k = c.classOf(s), map = k === null ? null : c.classes[k]!;
      const sheet = speakerFor(panels.get(s), lakes.get(s));
      kinds.add(sheet?.kind ?? "none");
      if (JSON.stringify(map) !== JSON.stringify(sheet)) expect([s, map]).toEqual([s, sheet]);
      const lead = sheetLead(sheet, [], null);
      expect([s, sheet === null ? lead === null : speaks(map, lead)]).toEqual([s, true]);
    }
    expect([...kinds].sort()).toEqual(["lake", "none", "panel"]);
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
