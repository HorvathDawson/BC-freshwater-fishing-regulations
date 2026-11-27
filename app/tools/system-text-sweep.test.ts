// @vitest-environment jsdom
/**
 * T1201 — THE SHELL'S SCREENS AT A LARGER SYSTEM TEXT SIZE, walked: the Map tab (its search field, the Layers button and the
 * bottom bar; the map is blank, and the Trout / Salmon switch, legend and day bar draw only with live data, so they are NOT swept here: the switch is
 * held by `system-text-labels.test.ts`), Conditions, Runs, Spots and More with its pages, each laid out in headless Chrome at 320, 360 and
 * 412 wide. The same phone-text emulation as `regs-text-scale.test.ts` (size and line height x min(OS scale, the Text's own
 * cap); react-native-web drops both props), the same Chrome layout (`wide-measure.mjs`).
 *
 * Measured per screen: a word cut by a clamp or hidden overflow (`cut`), two lines of different words drawn over each
 * other (`over`), a text line past the window's edge (`wide`), sideways overflow, and a one-word label drawn on two lines
 * (`broken`: "SALMO" / "N"). The gate is 1.3x (what a phone's labels follow, `LABEL_SCALE`): none of those. 1.15, 1.5 and
 * 2.0 are laid out too and reported (T1201_REPORT=<file.json>; T1201_SHOTS=<dir> saves each as a PNG): at 2.0 wrapping is
 * fine, loss is not, so a screen at 1.5 or 2.0 is judged by the report and the pictures, not by this gate.
 */
import "./render-setup.ts";
import { createElement as h, forwardRef, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import { parseRunTimingIndex, RunTimingFacts, type PaintDoc, type RunsDoc, type SalmonFacts, type StatusIndex } from "@app/core";
import { parseManifest, type RunTimingSource } from "@app/data";
import { HOME, type PairingInput, type Route, type StatusSource } from "@app/ui";

const SCALE = vi.hoisted(() => ({ v: 1 }));
vi.mock("react-native", async (importOriginal) => {
  const rn = await importOriginal<typeof import("react-native")>();
  type P = React.ComponentProps<typeof rn.Text>;
  const Text = forwardRef<unknown, P>(function Text(p, ref) {
    const m = p.allowFontScaling === false ? 1 : Math.min(SCALE.v, p.maxFontSizeMultiplier ?? Infinity);
    const flat = (rn.StyleSheet.flatten(p.style) ?? {}) as { fontSize?: number; lineHeight?: number };
    const scaled = m === 1 ? p.style : [p.style, {
      ...(flat.fontSize != null ? { fontSize: flat.fontSize * m } : {}),
      ...(flat.lineHeight != null ? { lineHeight: flat.lineHeight * m } : {}),
    }];
    return h(rn.Text as never, { ...p, ref, style: scaled });
  });
  return { ...rn, Text };
});
// no WebGL in jsdom: the map itself is blank, the screen around it (rail, switch, legend, day bar) is the app's
vi.mock("@app/map", async (importOriginal) => {
  const { View, Pressable } = await import("react-native");
  const { useEffect } = await import("react");
  // a stand-in for the map canvas: a tinted card that fills its place and says its look is drawn (as the real map does), so the
  // screen around it (legend, day bar, switch) is laid out as on a phone
  const Map = (p: { lookKey?: string; onLookDrawn?: (k: string) => void; onPressFeature?: (l: string, id: number) => void }) => {
    useEffect(() => { if (p.lookKey !== undefined) p.onLookDrawn?.(p.lookKey); }, [p.lookKey, p.onLookDrawn]);
        // a one-pixel press target standing for "a tap on a stream" (no text, so it is no part of what is measured)
    return h(View, { testID: "map-canvas", style: { flex: 1, backgroundColor: "#dfe5df" } },
             h(Pressable, { accessibilityRole: "button", accessibilityLabel: "tap a stream", onPress: () => { void p.onPressFeature?.("streams", 15); }, style: { width: 1, height: 1 } }));
  };
  return { ...(await importOriginal<typeof import("@app/map")>()), Map };
});
vi.mock("../packages/ui-native/src/FishSpinner", () => ({ FishSpinner: () => null }));
const { PixelRatio } = await import("react-native");
vi.spyOn(PixelRatio, "getFontScale").mockImplementation(() => SCALE.v);
const { snapshot } = await import("./wide-snapshot");
const { LIGHT } = await import("../packages/ui-native/src/theme");
const { Shell } = await import("../packages/ui-native/src/Shell");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");
const { NotFishingSheet } = await import("../packages/ui-native/src/NotFishingSheet");
const { ObstacleSheet } = await import("../packages/ui-native/src/ObstacleSheet");
const { SyncErrorSheet } = await import("../packages/ui-native/src/SyncErrorSheet");
const { AppUpdatePopup } = await import("../packages/ui-native/src/AppUpdatePopup");
const { SpotDropSheet } = await import("../packages/ui-native/src/SpotDropSheet");
const { ErrorState } = await import("../packages/ui-native/src/ErrorState");
const { EmptyState } = await import("../packages/ui-native/src/EmptyState");
const { InSeasonNotice } = await import("../packages/ui-native/src/InSeasonNotice");
const { inboxVM } = await import("@app/core");

const WIN = { w: 0, h: 0 };
const docEl = document.documentElement;
vi.spyOn(docEl, "clientWidth", "get").mockImplementation(() => WIN.w);
vi.spyOn(docEl, "clientHeight", "get").mockImplementation(() => WIN.h);

const MEASURE = `(() => {
  for (const a of document.getAnimations()) { try { a.finish(); } catch {} }
  // a More page is drawn over the More tab, which stays mounted beneath it: that tab is not on screen
  const page = document.querySelector('[data-testid^=menu-page-]');
  const gone = (e) => { for (; e && e.nodeType === 1; e = e.parentElement) { const s = getComputedStyle(e);
    if (page && e.getAttribute('data-testid') === 'more-screen' && !e.contains(page)) return true;
    if (s.display === 'none' || s.opacity === '0' || s.visibility === 'hidden') return true; } return false; };
  const name = (e) => (e.closest('[data-testid]')?.getAttribute('data-testid') ?? '') + ' "' + (e.textContent ?? '').trim().replace(/\\s+/g, ' ').slice(0, 40) + '"';
  const cut = [], broken = [];
  for (const e of document.querySelectorAll('body *')) {
    if (gone(e) || !(e.textContent ?? '').trim()) continue;
    const s = getComputedStyle(e);
    const hides = /hidden|clip/.test(s.overflowY) || /hidden|clip/.test(s.overflowX) || s.webkitLineClamp !== 'none' && s.webkitLineClamp !== '';
    if (hides && !/auto|scroll/.test(s.overflowY) && (e.scrollHeight > e.clientHeight + 1 || e.scrollWidth > e.clientWidth + 1))
      cut.push(name(e) + ' ' + e.scrollWidth + 'x' + e.scrollHeight + ' in ' + e.clientWidth + 'x' + e.clientHeight);
    // a one-word leaf drawn on two lines is a word broken in the middle
    if (!e.children.length && !/[\\s-]/.test(e.textContent.trim())) {
      const lh = parseFloat(s.lineHeight) || e.getBoundingClientRect().height;
      if (e.getBoundingClientRect().height > lh * 1.5) broken.push(name(e));
    }
  }
  const boxes = [], range = document.createRange(), walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let n, i = 0; (n = walk.nextNode()); i++) {
    if (!n.textContent.trim() || gone(n.parentElement)) continue;
    range.selectNodeContents(n);
    for (const q of range.getClientRects()) if (q.width > 1 && q.height > 1) {
      const pad = q.height / 5;
      // under another layer (a page's sheet over the tab beneath, scrolled content under the bar) it is not on screen
      const cx = (q.left + q.right) / 2, cy = (q.top + q.bottom) / 2;
      if (cx >= 0 && cy >= 0 && cx < innerWidth && cy < innerHeight) {
        const top = document.elementFromPoint(cx, cy);
        if (top && !n.parentElement.contains(top) && !top.contains(n.parentElement)) continue;
      }
      boxes.push({ i, r: { left: q.left, right: q.right, top: q.top + pad, bottom: q.bottom - pad }, el: n.parentElement });
    }
  }
  const wide = [], over = [];
  for (const b of boxes) if (b.r.right > innerWidth + 1 || b.r.left < -1) wide.push(name(b.el));
  for (let a = 0; a < boxes.length; a++) for (let b = a + 1; b < boxes.length; b++) {
    const A = boxes[a], B = boxes[b];
    if (A.i === B.i) continue;
    const x = Math.min(A.r.right, B.r.right) - Math.max(A.r.left, B.r.left), y = Math.min(A.r.bottom, B.r.bottom) - Math.max(A.r.top, B.r.top);
    if (x > 2 && y > 2) over.push(name(A.el) + ' x ' + name(B.el));
  }
  return JSON.stringify({ cut, broken, over: [...new Set(over)], wide: [...new Set(wide)],
    overflow: Math.max(0, document.documentElement.scrollWidth - innerWidth), lines: boxes.length });
})()`;

const dir = mkdtempSync(join(tmpdir(), "t1201-sweep-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "measure.js"), MEASURE);

type M = { min?: number; fonts: string; cut: string[]; broken: string[]; over: string[]; wide: string[]; overflow: number; lines: number };

// ---- what the Shell is handed: the T856 recipe (a proven tile pair, a day's status, salmon facts, the fixture release) ----
const H = "d768d6e135e62b99";
const INDEX = { handles: H, reachDigest: "", sections: 0, waters: 0, profiles: 0, codeOn: () => "base",
                profileOf: () => null, profileCodeOn: () => "base", waterCodeOn: () => "base" } as unknown as StatusIndex;
const FACTS = { handles: H, chains: 1, chainOf: () => 0, payload: { chains: [{ m: [0, 3] }] } } as unknown as SalmonFacts;
const STATUS: StatusSource = { key: "t1203-status", load: async () => INDEX };
const PAIRING: PairingInput = {
  kind: "release",
  manifest: parseManifest({
    format: "app-manifest/2", data_release: "r-t1203",
    tiles: { compatible: [`${H}.*`], assets: null, mask: null,
             current: { id: `${H}.aaa`, url: `tiles/${H}.aaa/atlas.pmtiles`, bytes: 1, sha256: "x" } },
    basemap: null, files: {}, regulations: { valid_until: null, versions: null },
  }),
  held: { tiles: null, basemap: null, release: "r-1", files: {} },
};
// ---- the Runs tab's feed: a Steelhead and a Coho run bound to the tiles' atlas (the T261 fixture) ----
const SLOTS = ["Chinook", "Chum", "Coho", "Pink (even)", "Pink (odd)", "Sockeye", "Steelhead"];
const hex = (ps: number[]) => ps.reduce((h, p) => h | (1n << BigInt(p)), 0n).toString(16).padStart(19, "0");
const RUNS: RunsDoc = {
  format: "runtiming-runs/1", slots: SLOTS, default: 6, relations: ["spawning", "passage", "observed"],
  relation_words: { spawning: "Spawns in this watershed", passage: "Passes through here",
                    observed: "Seen in this stream (DFO spawner surveys)" },
  sources: {
    psf: { credit: "Pacific Salmon Foundation (Salmon Watersheds Program)", title: "t", dataset: "d", updated: null,
           fetched: null, terms: "Used with the written permission of the Pacific Salmon Foundation.", url: "u" },
    dfo: { credit: "Fisheries and Oceans Canada", title: "t", dataset: "d", updated: null, fetched: null,
           terms: "Contains information licensed under the Open Government Licence – Canada.", url: "u" },
  },
  runs: [
    { id: "psf:1", src: "psf", sp: "Steelhead", slot: 6, name: "Chilliwack Winter", label: "winter", months: 0,
      pentads: hex([56, 57, 58, 59]) },
    { id: "dfo:9:Coho:-", src: "dfo", sp: "Coho", slot: 2, name: "Chilliwack River", months: 0, pentads: hex([58, 60]),
      years: [12, 2001, 2024], n: [[2020, 2600], [2021, 3100], [2022, 2900]], na: [[2021, 1500], [2022, 800]] },
  ],
};
const RT_PAINT: PaintDoc = {
  format: "runtiming-paint/1", section_handles: H, tile_version: `${H}.aaa`, runs_sha256: "s",
  profiles: [{ r: [1, 5], s: 0, m: [] }], sid: [15], p: [0], km: [0],
  basins: { ids: ["CHWK"], m: [[[2, hex([58]), hex([])], [6, hex([]), hex([56])]]] },
};
const RT_INDEX = parseRunTimingIndex({ format: "runtiming-index/1", built_at: "2026-10-29T00:00:00Z", section_handles: H,
  tile_version: `${H}.aaa`, sources: {}, files: { runs: { url: "r", bytes: 1, sha256: "s" },
                                                  paint: { url: "p", bytes: 1, sha256: "q" } } })!;
const FEED: RunTimingSource = { latest: async () => ({ facts: new RunTimingFacts(RT_INDEX, RUNS, RT_PAINT), from: "live" }) };

const VISIT = new Date(2026, 7, 30, 23, 30).getTime();
const SPOTS = [{
  id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0, lat: 49.0974, lon: -121.9675, item: null,
  waterName: null, title: "Tamihi run", notes: "Two coho on a spinner, the water low and clear.", photos: [], reading: null, weather: null, trace: null, panel: null,
}] as never;
const WATER = (view: "regs" | "conditions" | "map", item = "gnis:8634"): Route =>
  ({ mode: "water", stack: [{ item: item as never, part: null, tapSid: null, at: null, view }], from: HOME });

const settle = (ms = 260) => act(async () => { await new Promise((r) => setTimeout(r, ms)); });
const press = (re: RegExp) => act(() => { fireEvent.click(screen.getAllByText(re)[0]!); });
const pressLabel = (re: RegExp | string) => act(() => { fireEvent.click(screen.getAllByLabelText(re)[0]!); });
const pressId = (id: string) => act(() => { fireEvent.click(screen.getByTestId(id)); });
const tab = (name: string) => act(() => { fireEvent.click(screen.getByRole("tab", { name })); });
const slidOut = () => act(() => {
  for (const el of document.querySelectorAll("div"))
    for (const type of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(type, { bubbles: true }));
});

type Screen = {
  id: string; start?: Route; spots?: boolean;
  /** more of what the Shell is handed (a store, a source) */
  props?: Record<string, unknown>;
  /** a screen the Shell reaches only by a native event or a failure: the component itself, as the Shell draws it */
  node?: () => ReactNode;
  /** what the screen must show (a testid or a text): a blank or near-empty render fails the walk */
  must: string | RegExp;
  /** the fewest text lines it may draw (default 8): a near-empty render fails the walk */
  min?: number;
  go?: () => void | Promise<void>;
};

const type = (label: string, value: string) => act(() => {
  const el = screen.getByLabelText(label);
  fireEvent.focus(el); fireEvent.change(el, { target: { value } });
});
const SCREENS: Screen[] = [
  { id: "map", must: "regs-paint" },
  { id: "map-day-picker", must: /Today/, go: () => pressLabel(/Choose a day/) },
  { id: "map-layers", must: /Layers/, go: () => pressLabel("Choose layers") },
  { id: "map-search-open", must: "search-field", go: () => act(() => { fireEvent.focus(screen.getByLabelText("Search waters and towns")); }) },
  { id: "map-search-results", must: /Chilliwack/, go: async () => { await type("Search waters and towns", "chilliwack"); await settle(500); } },
  { id: "conditions", must: "legend-ramp", go: () => tab("Conditions") },
  { id: "runs", must: "runs-legend", props: { runTiming: FEED }, go: async () => { tab("Runs"); await settle(600); } },
  { id: "runs-sheet", must: /Done|Close/, props: { runTiming: FEED }, go: async () => { tab("Runs"); await settle(600); pressLabel("tap a stream"); await settle(600); } },
  { id: "spots-empty", must: "spots-screen", go: () => tab("Spots") },
  { id: "spots", spots: true, must: /Tamihi/, go: () => tab("Spots") },
  { id: "spot-page", spots: true, must: /Tamihi/, start: { ...HOME, tab: "spots", spot: "s1" } },
  { id: "spot-add", must: /Pick the water|Save/, start: { ...HOME, adding: { at: { lat: 49.1, lon: -122 } } }, go: () => settle(700) },
  { id: "more", must: /Settings/, go: () => tab("More") },
  { id: "more-settings", must: "menu-page-settings", go: async () => { tab("More"); press(/^Settings$/); await settle(500); } },
  { id: "more-offline-maps", must: "menu-page-offline", go: async () => { tab("More"); press(/^Offline maps$/); await settle(600); } },
  { id: "more-help", must: "menu-page-help", go: async () => { tab("More"); press(/^Help$/); await settle(500); } },
  { id: "more-glossary", must: "menu-page-glossary", go: async () => { tab("More"); press(/^Glossary$/); await settle(500); } },
  { id: "more-sources-and-licences", must: "menu-page-sources", go: async () => { tab("More"); press(/^Sources and licences$/); await settle(500); } },
  { id: "more-about", must: "menu-page-about", go: async () => { tab("More"); press(/^About$/); await settle(500); } },
  { id: "more-whats-new", must: "menu-page-changelog", go: async () => { tab("More"); press(/^About$/); await settle(500); press(/What's new/); await settle(500); } },
  { id: "more-report-a-problem", must: "menu-page-report", go: async () => { tab("More"); press(/^Report a problem$/); await settle(500); } },
  { id: "walkthrough", must: /1 of 5/, props: { walkthrough: { read: () => null, write() {} } }, go: () => settle(600) },
  { id: "first-run-notice", must: /Before you fish/, props: { firstRun: { read: () => null, write() {} } }, go: () => settle(600) },
  { id: "water-regs-gear", start: WATER("regs"), must: "regs-scroll", go: async () => { await settle(900); press(/^Gear$/); await settle(300); } },
  { id: "water-regs-methods", start: WATER("regs"), must: "regs-scroll", go: async () => { await settle(900); press(/^Methods$/); await settle(300); } },
  { id: "water-regs-licence", start: WATER("regs"), must: "regs-scroll", go: async () => { await settle(900); press(/^Licence$/); await settle(300); } },
  { id: "water-regs-fish-sheet", start: WATER("regs"), must: /Done/, go: async () => { await settle(900); pressLabel(/Kokanee — what it looks like/); await settle(500); } },
  { id: "water-regs-sources-sheet", start: WATER("regs"), must: /Done/, go: async () => { await settle(900); pressId("otherfish-src:other:o0"); await settle(500); } },
  { id: "water-regs-day-picker", start: WATER("regs"), must: "regs-scroll", go: async () => { await settle(900); pressLabel(/change date/); await settle(500); } },
  { id: "sheet-not-fishing", must: /./, min: 2, node: () => h(NotFishingSheet, { kind: "sewage", palette: LIGHT, onClose() {} }) },
  { id: "sheet-obstacle", must: /Done|Close/, node: () => h(ObstacleSheet, { palette: LIGHT, onClose() {}, props: { type: "dam", name: "Chilliwack Lake Road dam", water: "Chilliwack River", height_m: 4, year: 2008, source: "fiss", obstacle_id: "o1" } }) },
  { id: "sheet-sync-error", must: /./, min: 2, node: () => h(SyncErrorSheet, { open: true, palette: LIGHT, checking: false, onRetry() {}, onClose() {},
      failed: [{ k: "gauges", checkedAt: null, updatedAt: null, error: "network", failedAt: Date.now() - 3_600_000, offline: true }, { k: "release", checkedAt: null, updatedAt: null, error: "checksum", failedAt: Date.now() - 7_200_000, offline: false }] }) },
  { id: "popup-app-update", must: /./, min: 2, node: () => h(AppUpdatePopup, { palette: LIGHT, onUpdate() {}, onDismiss() {},
      popup: { key: "0.10.0", strength: "gentle", title: "A newer version of the app is ready", sub: null,
               whatsNew: { title: "What's new in 0.10.0", notes: ["Larger text now holds on every screen.", "The Trout / Salmon switch is easier to read."], missing: null },
               button: { label: "Update now", disabled: false } as never, said: null, failed: false, notNow: "Not now", later: "The bell keeps it for later" } as never }) },
  { id: "sheet-spot-drop", must: /./, min: 2, node: () => h(SpotDropSheet, { at: { lat: 49.0974, lon: -121.9675 }, palette: LIGHT, onCancel() {}, onContinue() {} }) },
  { id: "state-error", must: /./, min: 2, node: () => h(ErrorState, { palette: LIGHT, size: "body", message: "The regulations for this water could not be read. Check your connection and try again.", onRetry() {}, retryLabel: "Try again" }) },
  { id: "state-empty", must: /./, min: 2, node: () => h(EmptyState, { palette: LIGHT, title: "No spots yet", body: "A spot is a place you fished. Press and hold the map, or use Add a spot." }) },
  { id: "sheet-bell-news", must: /./, min: 2, node: () => h(InSeasonNotice, { open: true, palette: LIGHT, onClose() {}, inbox: { vm: inboxVM(null, "2026-11-26"), detail: null, failure: null, pick() {}, back() {} } as never }) },
  { id: "water-regs", start: WATER("regs"), must: "regs-scroll", go: () => settle(900) },
  { id: "water-conditions", start: WATER("conditions"), must: /./ },
  { id: "water-map", start: WATER("map"), must: /./ },
];

const only = process.env.T1203_ONLY ? process.env.T1203_ONLY.split(",") : undefined;

async function arrive(sc: Screen) {
  if (sc.node) {
    render(h(GestureHandlerRootView, { style: { flex: 1 } }, h(View, { style: { flex: 1, backgroundColor: LIGHT.bg } }, sc.node())));
    await settle(300);
  } else render(h(GestureHandlerRootView, null,
    h(Shell, { source: { ...makeFixtureSource(), salmonFacts: async () => FACTS }, loader: openRegsFixtureWithLines(), palette: LIGHT, theme: "light", themeName: "light", onTheme() {},
               tiles: { atlas: "a", basemap: "b" } as never, initialRoute: sc.start ?? HOME, spots: sc.spots ? SPOTS : [], onSaveSpot() {},
               status: STATUS, pairing: PAIRING, locate: null, ...sc.props,
               feed: { index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never })));
  await settle();
  try { await sc.go?.(); } catch (e) { throw new Error(`${sc.id}: could not arrive (${(e as Error).message.split("\n")[0]}); the screen reads: ${document.body.textContent?.slice(0, 500)}`); }
  await settle(150);
  slidOut();
  const there = typeof sc.must === "string" ? document.querySelector(`[data-testid="${sc.must}"]`) !== null : sc.must.test(document.body.textContent ?? "");
  if (!there) throw new Error(`${sc.id}: not on screen (wanted ${sc.must}); it reads: ${document.body.textContent?.slice(0, 500)}`);
  if (process.env.T1203_DUMP) console.log(`${sc.id} ids:`, [...document.querySelectorAll("[data-testid]")].map((e) => e.getAttribute("data-testid")).join(" "),
    "\nlabels:", [...document.querySelectorAll("[aria-label]")].map((e) => e.getAttribute("aria-label")).join(" | "));
}

async function sweep(scales: readonly number[], widths: readonly number[]): Promise<Record<string, M>> {
  const jobs: { page: string; size: [number, number]; measure: string; shot?: string }[] = [];
  const keys: string[] = [], mins: number[] = [];
  for (const scale of scales) for (const w of widths) for (const sc of SCREENS.filter((x) => !only || only.includes(x.id))) {
    SCALE.v = scale; WIN.w = w; WIN.h = 800;
    window.dispatchEvent(new Event("resize"));
    await arrive(sc);
    const file = join(dir, `${sc.id}-${w}-${scale}.html`);
    writeFileSync(file, snapshot());
    cleanup();
    const shot = process.env.T1201_SHOTS ? { shot: join(process.env.T1201_SHOTS, `${sc.id}-${w}-${String(scale).replace(".", "_")}.png`) } : {};
    jobs.push({ page: file, size: [w, 800], measure: join(dir, "measure.js"), ...shot });
    keys.push(`${sc.id} ${w} ${scale}x`); mins.push(sc.min ?? 8);
  }
  SCALE.v = 1;
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                           { encoding: "utf8", timeout: 900_000, maxBuffer: 64 << 20 });
  const res = JSON.parse(out) as M[];
  return Object.fromEntries(keys.map((k, i) => [k, { ...res[i]!, min: mins[i]! }]));
}

const problems = (m: Record<string, M>): string[] => Object.entries(m).flatMap(([k, v]) => [
  ...(/Archivo_400Regular/.test(v.fonts) ? [] : [`${k}: the body face did not load`]),
  ...v.cut.map((c) => `${k}: cut ${c}`), ...v.broken.map((c) => `${k}: one word on two lines ${c}`),
  ...v.over.map((c) => `${k}: overlap ${c}`), ...v.wide.map((c) => `${k}: past the edge ${c}`),
  ...(v.overflow > 0 ? [`${k}: ${v.overflow} px sideways overflow`] : []),
]);

describe("T1201 / T1203 the app's screens at a larger system text size, laid out in Chrome", () => {
  it("at 1.3x, 320, 360 and 412 wide: no cut word, no overlap, nothing past the edge, no word broken in two (and the walk reached every screen)", async () => {
    const m = await sweep(process.env.T1201_REPORT ? [1, 1.15, 1.3, 1.5, 2] : [1.3], process.env.T1203_WIDTHS ? process.env.T1203_WIDTHS.split(",").map(Number) : [320, 360, 412]);
    if (process.env.T1201_REPORT) writeFileSync(process.env.T1201_REPORT, JSON.stringify(Object.fromEntries(Object.entries(m).map(([k, v]) => [k, problems({ [k]: v })])), null, 1));
    expect(problems(Object.fromEntries(Object.entries(m).filter(([k]) => / 1\.3x$/.test(k))))).toEqual([]);
    expect(Object.keys(m).length, "the walk ran").toBeGreaterThan(0);
    expect(Object.entries(m).filter(([, v]) => v.lines < (v.min ?? 8)).map(([k]) => k)).toEqual([]);
  }, 1_800_000);
});
