/**
 * T1201 / T1203 — THE APP'S SCREENS AT A LARGER SYSTEM TEXT SIZE, walked (shared by the `system-text-sweep.*.test.ts` shards).
 * Each shard file installs the phone-text emulation and the map stand-in (its `vi.mock`s must sit in the test file), then calls
 * `walk(group, ids)` here. See the shards' header for the method and the exclusions.
 */
import { createElement as h, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import { parseRunTimingIndex, RunTimingFacts, type PaintDoc, type RunsDoc, type SalmonFacts, type StatusIndex } from "@app/core";
import { parseManifest, type RunTimingSource } from "@app/data";
import { HOME, type PairingInput, type Route, type StatusSource } from "@app/ui";

const SCALE = (globalThis as unknown as { __T1203_SCALE: { v: number } }).__T1203_SCALE;
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
const { inboxVM, inboxDetail, inboxMerge, inboxRead, joinInSeason, joinTestAsk } = await import("@app/core");
const { JoinTestCard } = await import("../packages/ui-native/src/JoinTestCard");
const { DeveloperModal } = await import("../packages/ui-native/src/DeveloperModal");

// the bell's inbox: three provincial in-season notices, one already read (the T700 fixture)
const INBOX_TODAY = "2026-11-13";
const inRow = (id: string, water: string, o: Record<string, unknown> = {}) =>
  ({ id, region: "4", water, effective: "", from: null, to: null, change: `No fishing on ${water} from the bridge to the falls, all species, until further notice`, status: "unchanged",
     firstSeen: "2026-11-12T00:00:00Z", ...o });
const inFeed = (rows: unknown[]) => ({ format: "inseason-feed/1", fetchedAt: "x", source: "https://www2.gov.bc.ca/x", rows, removed: [] }) as never;
const inLinks = { format: "inseason-links/4" as const, notices: { "is-a": { waters: { "gnis:1": { all: true as const } } } } } as never;
const inS0 = inboxMerge(null, inFeed([]), { matched: [], unmatched: [], ended: [] } as never, INBOX_TODAY);
const inF = inFeed([inRow("is-a", "Kaslo River"), inRow("is-b", "Okanagan River", { region: "8", firstSeen: "2026-11-13T00:00:00Z" }), inRow("is-c", "Slocan River", { firstSeen: "2026-11-01T00:00:00Z" })]);
const INBOX = inboxRead(inboxMerge(inS0, inF, joinInSeason(inF, inLinks, INBOX_TODAY), INBOX_TODAY), ["is-c"]);

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
  const cut = [], broken = [], clamps = [];
  // DESIGNED CLAMPS (header, exclusion 3): these Texts are cut to two lines with an ellipsis at every size on purpose (T214 / T875:
  // "a longer name is cut; its full words are the accessible label"; the inbox row's first lines, the notice opens on a tap).
  // They are not skipped: the clamp must be that clamp (two lines at most), and every OTHER element is judged as any other.
  const DESIGNED = /water-map-(name|step-)|inbox-row/;
  // an id (a tile version, a release) breaks where it must; a hyphenated word may break at its hyphen
  const ID_LIKE = /^[0-9A-Za-z._-]{12,}$/, HYPHENATED = /^\\p{L}{3,}-\\p{L}{3,}$/u;
  for (const e of document.querySelectorAll('body *')) {
    if (gone(e) || !(e.textContent ?? '').trim()) continue;
    const s = getComputedStyle(e), tid = e.closest('[data-testid]')?.getAttribute('data-testid') ?? '';
    const clamp = s.webkitLineClamp !== 'none' && s.webkitLineClamp !== '';
    if (clamp && DESIGNED.test(tid)) {
      const lh = parseFloat(s.lineHeight) || 16, shown = Math.round(e.clientHeight / lh);
      if (shown > 2) clamps.push(name(e) + ' shows ' + shown + ' lines (designed: 2)');
      continue;
    }
    // NOT COVERED (header, exclusion 2): the glossary's rows are cells of a virtual list; this static render gives each cell a height
    // measured for one text size, so a cell's clamp there says nothing about the app
    if (/glossary-row/.test(tid)) continue;
    const hides = /hidden|clip/.test(s.overflowY) || /hidden|clip/.test(s.overflowX) || clamp;
    if (hides && !/auto|scroll/.test(s.overflowY) && !/auto|scroll/.test(s.overflowX) && (e.scrollHeight > e.clientHeight + 1 || e.scrollWidth > e.clientWidth + 1))
      cut.push(name(e) + ' ' + e.scrollWidth + 'x' + e.scrollHeight + ' in ' + e.clientWidth + 'x' + e.clientHeight);
    // a one-word leaf drawn on two lines is a word broken in the middle (counted from the text's own line rects: a leaf stretched
    // tall by its flex row is not two lines)
    const word = e.textContent.trim();
    if (!e.children.length && !/\\s/.test(word) && !ID_LIKE.test(word) && !HYPHENATED.test(word) && e.firstChild && e.firstChild.nodeType === 3) {
      const r = document.createRange(); r.selectNodeContents(e.firstChild);
      const tops = new Set([...r.getClientRects()].map((q) => Math.round(q.top / 4)));
      if (tops.size > 1) broken.push(name(e) + ' [' + word + ']');
    }
  }
  // a sheet over a page: the page's words under the sheet's panel are covered (its scrim does not take pointer events)
  const panel = document.querySelector('[data-testid=sheet-panel]'), panelRect = panel ? panel.getBoundingClientRect() : null;
  const boxes = [], range = document.createRange(), walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let n, i = 0; (n = walk.nextNode()); i++) {
    if (!n.textContent.trim() || gone(n.parentElement)) continue;
    range.selectNodeContents(n);
    // a text clipped by its own box (an ellipsis, a clamp) is drawn only inside it: its range rects run on past the box (reported as cut)
    const own = n.parentElement, os = getComputedStyle(own), ob = /hidden|clip/.test(os.overflowX) || /hidden|clip/.test(os.overflowY) ? own.getBoundingClientRect() : null;
    for (let q of range.getClientRects()) if (q.width > 1 && q.height > 1) {
      if (ob) q = { left: Math.max(q.left, ob.left), right: Math.min(q.right, ob.right), top: Math.max(q.top, ob.top), bottom: Math.min(q.bottom, ob.bottom), width: 0, height: Math.min(q.bottom, ob.bottom) - Math.max(q.top, ob.top) };
      if (!(q.right - q.left > 1 && q.height > 1)) continue;
      const pad = q.height / 5;
      // under another layer (a page's sheet over the tab beneath, scrolled content under the bar) it is not on screen
      const cx = (q.left + q.right) / 2, cy = (q.top + q.bottom) / 2;
      if (panelRect && !panel.contains(n.parentElement) && cx >= panelRect.left && cx <= panelRect.right && cy >= panelRect.top && cy <= panelRect.bottom) continue;
      if (cx >= 0 && cy >= 0 && cx < innerWidth && cy < innerHeight) {
        const top = document.elementFromPoint(cx, cy);
        if (top && !n.parentElement.contains(top) && !top.contains(n.parentElement)) continue;
      }
      boxes.push({ i, r: { left: q.left, right: q.right, top: q.top + pad, bottom: q.bottom - pad }, el: n.parentElement });
    }
  }
  const wide = [], over = [];
  // a row that scrolls sideways (the Runs species chips, T902) is reached by scrolling: its far chips are not "past the edge"
  const scrollsX = (e) => { for (; e && e.nodeType === 1; e = e.parentElement) if (/auto|scroll/.test(getComputedStyle(e).overflowX)) return true; return false; };
  const scrollsY = (e) => { for (; e && e.nodeType === 1; e = e.parentElement) if (/auto|scroll/.test(getComputedStyle(e).overflowY)) return true; return false; };
  // past a side edge, or above the window's top outside any scroller (a sheet taller than the window loses its header)
  for (const b of boxes) if (((b.r.right > innerWidth + 1 || b.r.left < -1) && !scrollsX(b.el)) || (b.r.top < -4 && !scrollsY(b.el))) wide.push(name(b.el));
  for (let a = 0; a < boxes.length; a++) for (let b = a + 1; b < boxes.length; b++) {
    const A = boxes[a], B = boxes[b];
    if (A.i === B.i) continue;
    if (/glossary-/.test(A.el.closest('[data-testid]')?.getAttribute('data-testid') ?? '') && /glossary-/.test(B.el.closest('[data-testid]')?.getAttribute('data-testid') ?? '')) continue;
    if (panel && panel.contains(A.el) !== panel.contains(B.el)) continue;   // a sheet's words and the page's beneath it
    const x = Math.min(A.r.right, B.r.right) - Math.max(A.r.left, B.r.left), y = Math.min(A.r.bottom, B.r.bottom) - Math.max(A.r.top, B.r.top);
    if (x > 2 && y > 2) over.push(name(A.el) + ' x ' + name(B.el));
  }
  return JSON.stringify({ cut, broken, clamps, over: [...new Set(over)], wide: [...new Set(wide)],
    overflow: Math.max(0, document.documentElement.scrollWidth - innerWidth), lines: boxes.length });
})()`;

const dir = mkdtempSync(join(tmpdir(), "t1203-walk-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "measure.js"), MEASURE);

type M = { min?: number; fonts: string; clamps: string[]; cut: string[]; broken: string[]; over: string[]; wide: string[]; overflow: number; lines: number };

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
/** a water's Regulations page has drawn (a loaded machine takes longer than a fixed wait) */
const waitReg = async () => { await waitFor(() => { if (!screen.queryByTestId("regs-scroll")) throw new Error("regs not drawn"); }, { timeout: 15_000 }); await settle(200); };
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

// ---- real gauge rows for the Conditions views (T1203 review 7): the fixture has no panel, so the water's Conditions card read
// "basin days failed". Here the Chilliwack's sections have a three-gauge panel (the fixture's own gauge as the lead), their
// route, a reading and a year of series, and the feed carries the lead's standing: the real card, chart header, legend ramp and
// Change panel are drawn. ----
const FIX = makeFixtureSource();
const SECS = (await FIX.water("gnis:8634" as never))!.sections;
const LEAD = "08MH001";
const PANEL = { areaKm2: 50, members: [
  { station: LEAD, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true },
  { station: "DAM", role: "down", areaKm2: 55, years: { discharge: 6, level: 6 }, regulated: true, sameRiver: true },
  { station: "QUIET", role: "up", areaKm2: 300, years: { discharge: 30, level: 30 }, regulated: false, sameRiver: true },
] };
const WITH_GAUGES = {
  ...FIX,
  salmonFacts: async () => FACTS,
  panelsFor: async (secs: readonly number[]) => new Map(secs.map((x) => [x, PANEL])),
  panelRoutes: async () => [
    { station: LEAD, name: "Chilliwack River at Vedder Crossing", lat: 49.1, lon: -122, role: "up", path: SECS },
    { station: "DAM", name: "Dam Creek below the Dam", lat: 49.15, lon: -122.1, role: "down", path: SECS },
    { station: "QUIET", name: "Quiet River", lat: 49.2, lon: -122.2, role: "up", path: SECS },
  ],
} as never;
const GAUGE_FEED = { index: async () => ({ fetchedAt: new Date().toISOString(), stations: {
  [LEAD]: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 },
  DAM: { percentile: 0.8, parameter: "discharge" as const, discharge: 0.8 },
} }) } as never;

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
  { id: "spot-add", must: /Pick the water|Save/, min: 3, start: { ...HOME, adding: { at: { lat: 49.1, lon: -122 } } }, go: () => settle(700) },
  { id: "more", must: /Settings/, go: () => tab("More") },
  { id: "more-settings", must: "menu-page-settings", go: async () => { tab("More"); press(/^Settings$/); await settle(500); } },
  { id: "more-offline-maps", must: "menu-page-offline", go: async () => { tab("More"); press(/^Offline maps$/); await settle(600); } },
  { id: "more-help", must: "menu-page-help", go: async () => { tab("More"); press(/^Help$/); await settle(500); } },
  { id: "more-glossary", must: "menu-page-glossary", go: async () => { tab("More"); press(/^Glossary$/); await settle(500); } },
  { id: "more-sources-and-licences", must: "menu-page-sources", go: async () => { tab("More"); press(/^Sources and licences$/); await settle(500); } },
  { id: "more-about", must: "menu-page-about", go: async () => { tab("More"); press(/^About$/); await settle(500); } },
  { id: "more-whats-new", must: "menu-page-changelog", go: async () => { tab("More"); press(/^About$/); await settle(500); press(/What's new/); await settle(500); } },
  { id: "more-report-a-problem", must: "menu-page-report", go: async () => { tab("More"); press(/^Report a problem$/); await settle(500); } },
  { id: "walkthrough", must: /1 of 5/, min: 0,   /* its card is placed from a measured target: in the page, not on this static render's screen (phone check) */ props: { walkthrough: { read: () => null, write() {} } }, go: () => settle(600) },
  { id: "first-run-notice", must: /Before you fish/, props: { firstRun: { read: () => null, write() {} } }, go: () => settle(600) },
  { id: "water-regs-gear", start: WATER("regs"), must: "regs-scroll", go: async () => { await waitReg(); press(/^Gear$/); await settle(300); } },
  { id: "water-regs-methods", start: WATER("regs"), must: "regs-scroll", go: async () => { await waitReg(); press(/^Methods$/); await settle(300); } },
  { id: "water-regs-licence", start: WATER("regs"), must: "regs-scroll", go: async () => { await waitReg(); press(/^Licence$/); await settle(300); } },
  { id: "water-regs-fish-sheet", start: WATER("regs"), must: /Done/, go: async () => { await waitReg(); pressLabel(/Kokanee — what it looks like/); await settle(500); } },
  { id: "water-regs-sources-sheet", start: WATER("regs"), must: /Done/, go: async () => { await waitReg(); pressId("otherfish-src:other:o0"); await settle(500); } },
  { id: "water-regs-day-picker", start: WATER("regs"), must: "regs-scroll", go: async () => { await waitReg(); pressLabel(/change date/); await settle(500); } },
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
  { id: "sheet-bell-news", must: "inbox-row", node: () => h(InSeasonNotice, { open: true, palette: LIGHT, onClose() {}, inbox: { vm: inboxVM(INBOX, INBOX_TODAY), detail: null, failure: null, pick() {}, back() {} } as never }) },
  { id: "sheet-bell-news-detail", must: /./, node: () => h(InSeasonNotice, { open: true, palette: LIGHT, onClose() {}, inbox: { vm: inboxVM(INBOX, INBOX_TODAY), detail: inboxDetail(INBOX, "is-a", INBOX_TODAY), failure: null, pick() {}, back() {} } as never }) },
  { id: "popup-join-test", must: /./, min: 3, node: () => h(JoinTestCard, { store: "android", ask: joinTestAsk("android", "contact@example.invalid"), palette: LIGHT, onClose() {}, copy: async () => true }) },
  { id: "card-developer", must: /./, node: () => h(DeveloperModal, { open: true, theme: "light", palette: LIGHT, info: { appBuild: null, contact: null } as never, release: "r-t1203", onClose() {} }) },
  { id: "water-regs", start: WATER("regs"), must: "regs-scroll", go: () => waitReg() },
  { id: "water-conditions", start: WATER("conditions"), must: "reading-figure", go: () => waitFor(() => { if (!screen.queryByTestId("reading-figure")) throw new Error("no reading yet"); }, { timeout: 15_000 }) },
  { id: "water-map", start: WATER("map"), must: /./ },
];

/** the window's height, px (T1203_HEIGHT: a taller page for a picture of something below the fold) */
const HEIGHT = Number(process.env.T1203_HEIGHT ?? 800);
const only = process.env.T1203_ONLY ? process.env.T1203_ONLY.split(",") : undefined;

async function arrive(sc: Screen) {
  if (sc.node) {
    render(h(GestureHandlerRootView, { style: { flex: 1 } }, h(View, { style: { flex: 1, backgroundColor: LIGHT.bg, padding: 12 } }, sc.node())));
    await settle(300);
  } else render(h(GestureHandlerRootView, null,
    h(Shell, { source: WITH_GAUGES, loader: openRegsFixtureWithLines(), palette: LIGHT, theme: "light", themeName: "light", onTheme() {},
               tiles: { atlas: "a", basemap: "b" } as never, initialRoute: sc.start ?? HOME, spots: sc.spots ? SPOTS : [], onSaveSpot() {},
               status: STATUS, pairing: PAIRING, locate: null, ...sc.props,
               feed: GAUGE_FEED })));
  await settle();
  try { await sc.go?.(); } catch (e) { throw new Error(`${sc.id}: could not arrive (${(e as Error).message.split("\n")[0]}); the screen reads: ${document.body.textContent?.slice(0, 500)}`); }
  await settle(150);
  slidOut();
  const there = typeof sc.must === "string" ? document.querySelector(`[data-testid="${sc.must}"]`) !== null : sc.must.test(document.body.textContent ?? "");
  if (!there) throw new Error(`${sc.id}: not on screen (wanted ${sc.must}); it reads: ${document.body.textContent?.slice(0, 500)}`);
  if (process.env.T1203_DUMP) console.log(`${sc.id} ids:`, [...document.querySelectorAll("[data-testid]")].map((e) => e.getAttribute("data-testid")).join(" "),
    "\nlabels:", [...document.querySelectorAll("[aria-label]")].map((e) => e.getAttribute("aria-label")).join(" | "));
}

/** the screens a shard walks, at each width and scale; the file for each is Chrome's laid-out page of it */
async function sweep(ids: readonly string[], scales: readonly number[], widths: readonly number[]): Promise<Record<string, M>> {
  const jobs: { page: string; size: [number, number]; measure: string; shot?: string }[] = [];
  const keys: string[] = [], mins: number[] = [];
  for (const scale of scales) for (const w of widths) for (const sc of SCREENS.filter((x) => ids.includes(x.id) && (!only || only.includes(x.id)))) {
    SCALE.v = scale; WIN.w = w; WIN.h = HEIGHT;
    window.dispatchEvent(new Event("resize"));
    await arrive(sc);
    const file = join(dir, `${sc.id}-${w}-${scale}.html`);
    writeFileSync(file, snapshot());
    cleanup();
    const shot = process.env.T1201_SHOTS ? { shot: join(process.env.T1201_SHOTS, `${sc.id}-${w}-${String(scale).replace(".", "_")}.png`) } : {};
    jobs.push({ page: file, size: [w, HEIGHT], measure: join(dir, "measure.js"), ...shot });
    keys.push(`${sc.id} ${w} ${scale}x`); mins.push(sc.min ?? 8);
  }
  SCALE.v = 1;
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                           { encoding: "utf8", timeout: 900_000, maxBuffer: 64 << 20 });
  const res = JSON.parse(out) as M[];
  return Object.fromEntries(keys.map((k, i) => [k, { ...res[i]!, min: mins[i]! }]));
}

type Kind = "cut" | "broken" | "over" | "wide" | "overflow" | "clamp";
const faultsOf = (v: M): { kind: Kind; text: string }[] => [
  ...v.cut.map((text) => ({ kind: "cut" as const, text })), ...v.broken.map((text) => ({ kind: "broken" as const, text })),
  ...v.over.map((text) => ({ kind: "over" as const, text })), ...v.wide.map((text) => ({ kind: "wide" as const, text })),
  ...v.clamps.map((text) => ({ kind: "clamp" as const, text })),
  ...(v.overflow > 0 ? [{ kind: "overflow" as const, text: `${v.overflow} px sideways overflow` }] : []),
];

/**
 * THE FAULTS A SCREEN MAY CARRY, each with its reason (header, exclusion 5). A fault matching one of these is not reported at any
 * scale; every other fault at 1x or 1.3x fails, and at 2.0x every LOSS (`over`, `wide`, `overflow`, `clamp`) fails.
 */
const ALLOWED: { screen: RegExp; kind: Kind; match: RegExp; why: string }[] = [
  { screen: /^walkthrough$/, kind: "overflow", match: /sideways overflow/, why: "exclusion 1: the card is placed from a measured target, which this static render does not give it: it sits off the page (the whole window's width past the edge) at every size, 1x included" },
  { screen: /^water-conditions$/, kind: "cut", match: /^chart-hint /, why: "the chart's zoom hint is one line with an ellipsis by design (Hydrograph.tsx PlotBand, numberOfLines 1): cut at 1x on a narrow phone in 0.9.0 too; a hint, nothing is lost" },
];

/** the faults of one walked page that the gate counts at its scale */
function counted(key: string, v: M): string[] {
  const at = /^(.*) (\d+) ([\d.]+)x$/.exec(key)!;
  const scale = Number(at[3]);
  const loss: Kind[] = ["over", "wide", "overflow", "clamp"];
  return faultsOf(v)
    .filter((f) => scale === 2 ? loss.includes(f.kind) : true)
    .filter((f) => !ALLOWED.some((a) => a.screen.test(at[1]!) && a.kind === f.kind && a.match.test(f.text)))
    .map((f) => `${key}: ${f.kind} ${f.text}`);
}

export function walk(group: string, ids: readonly string[]) {
  describe(`T1203 the app's screens at a larger system text size, laid out in Chrome: ${group}`, () => {
    it("1x holds only what its allowlist names, 1.3x is clean, 2.0x loses nothing (no press target off screen, no text over text)", async () => {
      const scales = process.env.T1203_SCALES ? process.env.T1203_SCALES.split(",").map(Number) : process.env.T1201_REPORT ? [1, 1.15, 1.3, 1.5, 2] : [1, 1.3, 2];
      const widths = process.env.T1203_WIDTHS ? process.env.T1203_WIDTHS.split(",").map(Number) : [320, 360, 412];
      const m = await sweep(ids, scales, widths);
      if (process.env.T1201_REPORT) writeFileSync(process.env.T1201_REPORT, JSON.stringify(Object.fromEntries(Object.entries(m).map(([k, v]) => [k, faultsOf(v).map((f) => `${f.kind} ${f.text}`)])), null, 1));
      expect(Object.keys(m).length, "the walk ran").toBe(ids.filter((i) => !only || only.includes(i)).length * scales.length * widths.length);
      expect(Object.entries(m).filter(([, v]) => !/Archivo_400Regular/.test(v.fonts)).map(([k]) => k), "the body face loaded").toEqual([]);
      expect(Object.entries(m).filter(([, v]) => v.lines < (v.min ?? 8)).map(([k]) => `${k} drew ${m[k]!.lines} lines`), "no near-empty render").toEqual([]);
      expect(Object.entries(m).flatMap(([k, v]) => counted(k, v))).toEqual([]);
    }, 900_000);
  });
}
