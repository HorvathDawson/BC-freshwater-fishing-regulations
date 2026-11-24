// @vitest-environment jsdom
/**
 * (T1031, trout per-row pages) the owner's pictures of a trout row's species sheet, Sources tab (not a test: skipped unless
 * `T1031_SHOTS=1`). Each sheet is core's own (`tf:<row>`, built by `buildRegsPage` from the trout fixture release, the
 * vectors' case and day), drawn by the real SheetHost and photographed by headless Chrome (`wide-measure.mjs`) at 390 px,
 * light and dark, into handoff/PLAN/app-integration/T1031-shots/ as `rowpages-<tag>-<case>-<row>-<theme>.png`. Run it on
 * the base and on the change (`ROWPAGES_TAG=before|after`) for the before/after pair.
 *
 *   T1031_SHOTS=1 ROWPAGES_TAG=after pnpm vitest run tools/rowpages-shots.test.ts
 */
import "./render-setup.ts";
import { createElement as h, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { buildRegsPage, type SheetKey } from "@app/core";
import { openTroutFixture, TROUT_FIXTURE_ROOT } from "@app/data/release/node";
import { loadRegsInputs, openRegsStore, targetFor } from "@app/data/regs";
import { SheetHost } from "../packages/ui-native/src/regs/SheetHost";
import { DARK, LIGHT } from "../packages/ui-native/src/theme";
import { snapshot } from "./wide-snapshot";

const ON = process.env.T1031_SHOTS === "1";
const TAG = process.env.ROWPAGES_TAG ?? "after";
const OUT = join(process.cwd(), "..", "handoff", "PLAN", "app-integration", "T1031-shots");
const WEEKDAY = { mon: "Monday", tue: "Tuesday", wed: "Wednesday", thu: "Thursday", fri: "Friday", sat: "Saturday", sun: "Sunday" } as const;
/** Lois Lake's Dolly Varden (Region 2's table, p.21, and the lake's own row, p.24) and the Vedder's rainbow (p.21, p.22) */
const SHOTS: readonly [string, string][] = [["lois-jul15", "DV"], ["vedder-jul15", "RB"]];

interface Vector { readonly name: string; readonly item: string; readonly day: string; readonly weekday: string;
                   readonly anchor: { readonly stream: number; readonly m: number } | { readonly lake: string } }
const read = (f: string) => JSON.parse(readFileSync(join(TROUT_FIXTURE_ROOT, f), "utf8"));

const dir = mkdtempSync(join(tmpdir(), "rowpages-shots-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

describe.skipIf(!ON)("T1031 per-row pages shots", () => {
  it("photographs each row's Sources tab at 390 px, light and dark", async () => {
    const V = read("trout-vectors.generated.json") as { cases: Vector[] };
    const LINES = read("section-lines.json").lines as [number, number, number, number][];
    const st = await openRegsStore(openTroutFixture());
    mkdirSync(OUT, { recursive: true });
    const jobs: { page: string; size: [number, number]; shot: string }[] = [];
    for (const [name, row] of SHOTS) {
      const c = V.cases.find((x) => x.name === name)!;
      const ord = (await st.itemOrd(c.item))!, mine = new Set<number>();
      for (const p of await st.parts(ord)) for (const sid of (await st.partLengths(ord, p.partIx)).keys()) mine.add(sid);
      const a = c.anchor, sid = "lake" in a ? Math.min(...mine)
        : LINES.find(([s, b, lo]) => b === a.stream && lo === a.m && mine.has(s))![0];
      const [mm, dd] = c.day.split("-").map(Number);
      const inp = await loadRegsInputs(st, await targetFor(st, c.item, sid), { month: mm!, day: dd! },
        WEEKDAY[c.weekday as keyof typeof WEEKDAY], 2026);
      const page = buildRegsPage(inp, { today: inp.day, who: { age: "adult", residency: "resident" } });
      for (const dark of [false, true]) {
        const [w, hgt] = [390, 1800];
        for (const [k, v] of [["clientWidth", w], ["clientHeight", hgt]] as const)
          Object.defineProperty(document.documentElement, k, { value: v, configurable: true });
        act(() => { window.dispatchEvent(new Event("resize")); });
        render(h(SheetHost, { page, open: `tf:${row}` as SheetKey, onOpen() {}, onClose() {}, palette: dark ? DARK : LIGHT, layout: "phone" }) as ReactNode);
        const theme = dark ? "dark" : "light", file = join(dir, `${name}-${row}-${theme}.html`);
        writeFileSync(file, snapshot().replace("<body>", `<body style="background:${(dark ? DARK : LIGHT).card}">`));
        cleanup();
        jobs.push({ page: file, size: [w, hgt], shot: join(OUT, `rowpages-${TAG}-${name.replace(/-\w+$/, "")}-${row.toLowerCase()}-${theme}.png`) });
      }
    }
    const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                             { encoding: "utf8", timeout: 180_000 });
    expect((JSON.parse(out) as unknown[]).length).toBe(jobs.length);
  }, 240_000);
});
