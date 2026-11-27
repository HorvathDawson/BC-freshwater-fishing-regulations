// @vitest-environment jsdom
/**
 * (T1167) the owner's pictures of the louder in-season banner (not a test: skipped unless `T1167_SHOTS=1`): the real
 * Regulations page (`buildRegsPage` over the trout fixture release's Vedder River, on each picture's day) with the real
 * Okanagan River notice of 2026 ("No fishing July 30 to September 15.", effective July 30, 2026) joined by core on that
 * day (`joinInSeason` → `inSeasonBanner`), drawn by the real RegsScreen and photographed by headless Chrome
 * (`wide-measure.mjs`) at 390 px, light and dark: BEFORE the window (Jul 20, upcoming), INSIDE it (Aug 15) and AFTER it
 * (Sep 20, nothing). Into `T1167_SHOTS_DIR` (default tickets/T1167-shots/) as
 * `<state>-<theme>.png`.
 *
 *   T1167_SHOTS=1 T1167_SHOTS_DIR=/abs/dir pnpm vitest run tools/inseason-shots.test.ts
 */
import "./render-setup.ts";
import { createElement as h, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { buildRegsPage, inSeasonBanner, joinInSeason, type InSeasonFeed } from "@app/core";
import { openTroutFixture, TROUT_FIXTURE_ROOT } from "@app/data/release/node";
import { loadRegsInputs, openRegsStore, targetFor } from "@app/data/regs";
import { RegsScreen } from "../packages/ui-native/src/regs/RegsScreen";
import { StripWidthHint } from "../packages/ui-native/src/regs/YearStrip";
import { WaterFrame } from "../packages/ui-native/src/WaterFrame";
import { TEST_SOURCES, TEST_TROUT_SOURCES } from "../packages/ui-native/src/regs/testing/sources";
import { DARK, LIGHT } from "../packages/ui-native/src/theme";
import { snapshot } from "./wide-snapshot";

const ON = process.env.T1167_SHOTS === "1";
const OUT = process.env.T1167_SHOTS_DIR ?? join(process.cwd(), "..", "tickets", "T1167-shots");
const [W, HEIGHT] = [390, 900];
const WEEKDAY = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"] as const;
/** the three states and their days */
const STATES: readonly [string, string][] = [["before", "2026-07-20"], ["inside", "2026-08-15"], ["after", "2026-09-20"]];
/** the production feed's Okanagan River row (2026-11-11 capture), cut to what the app reads */
const FEED: InSeasonFeed = { format: "inseason-feed/1", fetchedAt: "2026-11-11T12:00:00Z", source: "https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations/in-season-regulation-changes", removed: [],
  rows: [{ id: "is-2fa458fef3c3", region: "8", water: "Okanagan River", effective: "July 30, 2026", from: "2026-07-30", to: null,
           change: "No fishing July 30 to September 15.",
           parsed: { kinds: ["closure"], window: { from_month: 7, from_day: 30, to_month: 9, to_day: 15 } } }] };
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };

interface Vector { readonly name: string; readonly item: string; readonly anchor: { readonly stream: number; readonly m: number } | { readonly lake: string } }
const read = (f: string) => JSON.parse(readFileSync(join(TROUT_FIXTURE_ROOT, f), "utf8"));

/** the pages' working folder: made by the run (never at load: a skipped file leaves nothing behind) */
let dir: string | null = null;
afterAll(() => { if (dir) rmSync(dir, { recursive: true, force: true }); });

describe.skipIf(!ON)("T1167 in-season banner shots", () => {
  it("photographs the page before, inside and after the window at 390 px, light and dark", async () => {
    const work = dir = mkdtempSync(join(tmpdir(), "inseason-shots-"));
    const V = read("trout-vectors.generated.json") as { cases: Vector[] };
    const LINES = read("section-lines.json").lines as [number, number, number, number][];
    const st = await openRegsStore(openTroutFixture());
    const c = V.cases.find((x) => x.name === "vedder-jul15")!;
    const ord = (await st.itemOrd(c.item))!, mine = new Set<number>();
    for (const p of await st.parts(ord)) for (const sid of (await st.partLengths(ord, p.partIx)).keys()) mine.add(sid);
    const a = c.anchor as { stream: number; m: number };
    const sid = LINES.find(([s, b, lo]) => b === a.stream && lo === a.m && mine.has(s))![0];
    mkdirSync(OUT, { recursive: true });
    const jobs: { page: string; size: [number, number]; shot: string }[] = [];
    for (const [state, day] of STATES) {
      const [y, mm, dd] = day.split("-").map(Number);
      const weekday = WEEKDAY[new Date(Date.UTC(y!, mm! - 1, dd!)).getUTCDay()]!;
      const inp = await loadRegsInputs(st, await targetFor(st, c.item, sid), { month: mm!, day: dd! }, weekday, y!);
      const page = buildRegsPage(inp, { today: { ...inp.day, year: y! }, who: { age: "adult", residency: "resident" } });
      const j = joinInSeason(FEED, null, day);
      const banners = j.unmatched.map((ch) => inSeasonBanner(ch, { where: null }));
      expect([state, banners.map((b) => b.phase)]).toEqual([state, state === "before" ? ["upcoming"] : state === "inside" ? ["in force"] : []]);
      for (const dark of [false, true]) {
        for (const [k, v] of [["clientWidth", W], ["clientHeight", HEIGHT]] as const)
          Object.defineProperty(document.documentElement, k, { value: v, configurable: true });
        act(() => { window.dispatchEvent(new Event("resize")); });
        const palette = dark ? DARK : LIGHT;
        render(h(StripWidthHint.Provider, { value: W - 32 },
          h(WaterFrame, { palette, face: "regulations" },
            h(RegsScreen, { page, palette, actions: NOOP, onDate() {}, inSeason: banners, sources: TEST_SOURCES, troutSources: TEST_TROUT_SOURCES }))) as ReactNode);
        const theme = dark ? "dark" : "light", file = join(work, `${state}-${theme}.html`);
        writeFileSync(file, snapshot().replace("<body>", `<body style="background:${palette.card}">`));
        cleanup();
        jobs.push({ page: file, size: [W, HEIGHT], shot: join(OUT, `${state}-${theme}.png`) });
      }
    }
    const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                             { encoding: "utf8", timeout: 180_000 });
    expect((JSON.parse(out) as unknown[]).length).toBe(jobs.length);
  }, 240_000);
});
