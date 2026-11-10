// (T430) THE CRASH AND PROBLEM-REPORT WIRE, app ↔ feeds worker. The two formats are typed twice: the app builds them
// (`@app/ui` crashPayload, `@app/core` reportPayload) and the worker (`workers/feeds/src/{crashes,reports}.ts`, outside
// the pnpm workspace) validates them, refusing any unknown key or enum value with a 400 — and the app's outbox marks a
// 400 "refused" and never sends it again (`data/src/report/outbox.ts`). So one new field or enum value on the app side
// would lose every report for good. This pins the two halves together: every enum list compared as a set, every value
// sent through the worker's own validator, and a payload with every field filled accepted and inside the byte limit.
import { describe, expect, it } from "vitest";
import { CRASH_FORMAT, CRASH_KINDS, CRASH_LIMITS, CRASH_SHEETS, ROUTE_KINDS, crashPayload, type CrashContext } from "@app/ui";
import { REPORT_FORMAT, REPORT_LIMITS, REPORT_REASONS, WATER_VIEWS, reportPayload, type Diagnostics, type ReportContext } from "@app/core";
import * as worker from "../workers/feeds/src/crashes";
import * as reports from "../workers/feeds/src/reports";

const set = (xs: readonly unknown[]) => [...new Set(xs)].map(String).sort();
const NOW = new Date("2026-11-05T12:00:00Z");
const INSTALL = "0123456789abcdef";

const ctx = (over: Partial<CrashContext> = {}): CrashContext => ({
  route: "water", water: "fwa:356308001", view: "regs", part: 2, whole: true, sheet: "spot",
  app: "0.6.0 (60)", env: "production", platform: "android 34", release: "r-5a5999c94588", install: INSTALL, ...over,
});
const crash = (over: Partial<CrashContext> = {}, kind: (typeof CRASH_KINDS)[number] = "js") =>
  crashPayload({ error: new TypeError("x is undefined"), kind, fatal: false, ctx: ctx(over), now: NOW });

describe("crash/2: the app's payload is one the worker takes", () => {
  it("the formats and enums are the same sets on both sides", () => {
    expect(CRASH_FORMAT).toBe(worker.CRASH_FORMAT);
    expect(set(CRASH_KINDS)).toEqual(set(worker.CRASH_KINDS));
    expect(set(ROUTE_KINDS)).toEqual(set(worker.ROUTE_KINDS));
    expect(set(WATER_VIEWS)).toEqual(set(worker.VIEWS));
    expect(set(CRASH_SHEETS)).toEqual(set(worker.SHEETS));
    for (const [k, v] of Object.entries(CRASH_LIMITS)) expect([k, worker.CRASH_LIMITS[k as keyof typeof worker.CRASH_LIMITS]]).toEqual([k, v]);
  });

  it("every enum value, sent through validateCrash, is accepted", () => {
    const sent = [
      ...CRASH_KINDS.map((k) => crash({}, k)),
      ...ROUTE_KINDS.map((route) => crash({ route })),
      ...WATER_VIEWS.map((view) => crash({ view })),
      ...CRASH_SHEETS.map((sheet) => crash({ sheet })),
      crash({ water: null, view: null, part: null, sheet: null, whole: false }),
    ];
    for (const p of sent) expect([p.kind, p.route, p.view, p.sheet, worker.validateCrash(JSON.parse(JSON.stringify(p)))]).toMatchObject([p.kind, p.route, p.view, p.sheet, { ok: true }]);
  });

  it("a crash with every field at its limit is accepted and fits the worker's byte limit", () => {
    const long = (n: number) => "a".repeat(n + 50);
    const e = new Error(long(CRASH_LIMITS.message));
    e.name = long(CRASH_LIMITS.name);
    e.stack = Array.from({ length: 200 }, (_, i) => `    at fn${i} (https://x.example/app.js:1:${i})`).join("\n");
    const p = crashPayload({ error: e, kind: "native", fatal: true, background: true, now: NOW,
      ctx: ctx({ water: long(CRASH_LIMITS.field), app: long(100), env: long(100), platform: long(100), release: long(100) }) });
    // the water id has its own shape on the worker; the app cuts it to the field limit
    const v = worker.validateCrash(JSON.parse(JSON.stringify({ ...p, water: "fwa:1" })));
    expect(v).toMatchObject({ ok: true });
    expect(new TextEncoder().encode(JSON.stringify(p)).length).toBeLessThanOrEqual(worker.CRASH_LIMITS.bytes);
  });
});

const DIAG: Diagnostics = {
  appBuild: "0.6.0 (60)", env: "production", platform: "android 34", release: "r-5a5999c94588", tiles: "t-2026-11-01",
  where: { screen: "water · regs", water: "fwa:356308001", place: "Harrison River", section: 4, part: 1,
           partLabel: "Mouth → Source", day: "Thu 5 Nov", tab: "rules", mode: "standard",
           station: "08MH001", quantity: "flow", span: "7 days", feed: "2026-11-10T18:05:00Z" },
  map: { layer: "regulations · satellite", horizon: 2, zoom: 9.43, lat: 49.12345, lon: -121.95432 },
  install: INSTALL,
  errors: ["boom", "bang"],
};
/** every key of the app's ReportContext — a new one here must be one the worker takes (`REPORT_CONTEXT_KEYS`) */
const APP_CONTEXT_KEYS: Record<keyof Required<ReportContext>, true> = {
  screen: true, tab: true, place: true, water: true, part: true, partLabel: true, section: true, day: true, mode: true,
  station: true, quantity: true, span: true, feed: true,
  layer: true, horizon: true, zoom: true, lat: true, lon: true, install: true, app: true, env: true, platform: true,
  release: true, tiles: true, errors: true,
};
const report = (over: Partial<Parameters<typeof reportPayload>[0]> = {}) =>
  reportPayload({ id: "fedcba9876543210", created: NOW.toISOString(), reason: "rule", what: "The limit is wrong.",
                  email: "a@b.ca", contact: true, diagnostics: DIAG, on: {}, ...over });

describe("report/2: the app's payload is one the worker takes", () => {
  it("the formats, reasons, limits and context keys are the same on both sides", () => {
    expect(REPORT_FORMAT).toBe(reports.REPORT_FORMAT);
    expect(set(REPORT_REASONS)).toEqual(set(reports.REASONS));
    for (const [k, v] of Object.entries(REPORT_LIMITS)) expect([k, reports.LIMITS[k as keyof typeof reports.LIMITS]]).toEqual([k, v]);
    expect(set(Object.keys(APP_CONTEXT_KEYS))).toEqual(set(reports.REPORT_CONTEXT_KEYS));
  });

  it("every reason, with every diagnostic row on, is accepted with its whole context kept", () => {
    for (const reason of REPORT_REASONS) {
      const p = report({ reason });
      expect(set(Object.keys(p.context))).toEqual(set(Object.keys(APP_CONTEXT_KEYS)));
      const v = reports.validateReport(JSON.parse(JSON.stringify(p)));
      expect([reason, v]).toMatchObject([reason, { ok: true }]);
      if (v.ok) expect(v.report.context).toEqual(p.context);
    }
  });

  it("every diagnostic row off, and no email, is accepted too", () => {
    const off = { app: false, platform: false, release: false, tiles: false, where: false, map: false, install: false, errors: false };
    expect(reports.validateReport(JSON.parse(JSON.stringify(report({ on: off, email: "" }))))).toMatchObject({ ok: true });
  });

  it("a report with every field at its limit is accepted and fits the worker's byte limit", () => {
    const long = (n: number) => "a".repeat(n + 50);
    const d: Diagnostics = { ...DIAG, appBuild: long(200), platform: long(200), release: long(200), tiles: long(200), env: long(200),
      where: { screen: long(200), water: long(200), place: long(200), partLabel: long(200), day: long(200), tab: long(200),
               mode: long(200), section: 99999, part: 99 },
      map: { ...DIAG.map, layer: long(200) },
      errors: Array.from({ length: 40 }, () => long(REPORT_LIMITS.line)) };
    const p = report({ what: long(REPORT_LIMITS.text), email: `${"e".repeat(240)}@b.ca`, diagnostics: d });
    expect(reports.validateReport(JSON.parse(JSON.stringify(p)))).toMatchObject({ ok: true });
    expect(new TextEncoder().encode(JSON.stringify(p)).length).toBeLessThanOrEqual(reports.LIMITS.bytes);
  });
});
