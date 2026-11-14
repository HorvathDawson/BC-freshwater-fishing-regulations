/**
 * (T242) CRASH AND ERROR REPORTS — the app's own, to our own server (the feeds worker's `POST /crash`; no third-party
 * crash service, owner rule). What leaves the device, and only while "Send crash reports" (More → Data, default on) is on,
 * the device is online and the error is new this run:
 *
 *   the app build, environment, platform, data release; the screen KIND (map / conditions / runs / spots / menu / water);
 *   (T352, owner 2026-11-03) the OPEN WATER'S durable id and its pane — the view in front, the part (never a section id:
 *   AGENTS rule 5, section ids never leave the bundle), whether the whole water is framed, which route-held sheet is up — from the route (`crashWhere`), never a
 *   water's name or a coordinate; a random INSTALL id made once on the device (`installId`, the problem report's too);
 *   the error's kind (js / promise / render / native), whether it ended the app, whether the system closed the app in the
 *   background (`background`), its name, its message with every quoted text, address, number run and coordinate taken
 *   out (`scrubMessage`; a short quoted module number kept) and its stack with the addresses cut to file names. Nothing
 *   else about the device or the person; the server stores neither the IP address nor the user agent.
 *
 * Offline → dropped (never queued). A fatal error is kept in one slot on the device and sent at the next start (the app is
 * dying as it is caught), then cleared. At most `maxPerRun` different errors a run; the same error is sent once.
 * Switching it off sends nothing and forgets a kept one.
 */
import { createContext, createElement, useContext, useEffect, useState, type ReactNode } from "react";
import { isEnvironment, reportFailure, setFailureSink, now as clockNow } from "@app/core";
import { waterFrame, type Route, type View } from "./route";

/** (T352) crash/2: the open water and its pane, the install id, `background`. The worker takes crash/1 too (0.4.0 phones). */
export const CRASH_FORMAT = "crash/2";
const CRASH_FORMATS: readonly string[] = ["crash/1", CRASH_FORMAT];
// (T430) The wire's enums as VALUES, not only types: the feeds worker (`workers/feeds/src/crashes.ts`, outside the
// workspace) types them again and refuses anything else with a 400, which the app never retries. `tools/wire.test.ts`
// compares these lists with the worker's and sends every value through `validateCrash`.
export const CRASH_KINDS = ["js", "promise", "render", "native"] as const;
export type CrashKind = (typeof CRASH_KINDS)[number];
export const ROUTE_KINDS = ["map", "conditions", "runs", "spots", "menu", "water", "other"] as const;
export type RouteKind = (typeof ROUTE_KINDS)[number];
/** (T352) the route-held sheet up when the error happened: search over a map, a spot open, a spot being added */
export const CRASH_SHEETS = ["search", "spot", "adding"] as const;
export type CrashSheet = (typeof CRASH_SHEETS)[number];
export const CRASH_LIMITS = { name: 60, message: 200, stack: 2000, field: 100 } as const;

/** (T352) Where the reader was, from the route: the screen kind and the open water's pane. Every field null when unknown. */
export interface CrashWhere {
  route: RouteKind;
  /** the open water's durable item id (null: none open, or an unnamed reach) */
  water: string | null;
  /** the view in front of it */
  view: View | null;
  /** the part's index in its water */
  part: number | null;
  /** the water's map framed on the whole water (the route's `whole`) */
  whole: boolean;
  sheet: CrashSheet | null;
}

export interface CrashPayload extends CrashWhere {
  format: typeof CRASH_FORMAT;
  created: string;
  kind: CrashKind;
  fatal: boolean;
  /** (T352) the system closed the app while it was not in front (Android: low memory, a signal): not a crash */
  background: boolean;
  /** (T352) the random id made once on this device */
  install: string;
  app: string; env: string; platform: string; release: string;
  name: string; message: string; stack: string;
}
/** What the app hands the reporter at each error: its build facts, the install id and where the reader is (`crashWhere`). */
export interface CrashContext extends CrashWhere { app: string; env: string; platform: string; release: string; install: string }

/** The screen as a KIND: the tab, a water, or the menu's pages (the More tab). */
export function routeKind(r: Route | null | undefined): RouteKind {
  if (!r) return "other";
  if (r.mode === "water") return "water";
  switch (r.tab) {
    case "map": return "map";
    case "conditions": return "conditions";
    case "runs": return "runs";
    case "spots": return "spots";
    case "more": return "menu";
    default: return "other";
  }
}

/** Nowhere known: a native exit read at the next start, or no route yet. */
export const NO_WHERE: CrashWhere = Object.freeze({ route: "other", water: null, view: null, part: null, whole: false, sheet: null });

/** (T352) The route's facts a crash carries: the screen kind, the open water's id and its pane (never a name or a place). */
export function crashWhere(r: Route | null | undefined): CrashWhere {
  if (!r) return NO_WHERE;
  const f = waterFrame(r);
  const sheet: CrashSheet | null = r.mode === "water" ? (r.spot ? "spot" : null)
    : r.adding ? "adding" : r.spot ? "spot" : r.search ? "search" : null;
  return {
    route: routeKind(r),
    water: f?.item ?? null,
    view: f?.view ?? null,
    part: f?.part ?? null,
    whole: !!f?.whole,
    sheet,
  };
}

const cut = (s: string, n: number) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);

/** (T352) A module number quoted after "module" (Metro's `Requiring unknown module "1234"`): kept, it is the code's own id. */
const MODULE_ID = /\bmodule\s+(['"`])(\d{1,6})\1/gi;

/**
 * An error's message with what could identify a person or a place taken out: e-mail addresses, web addresses, every quoted
 * text ('…', "…", `…` — a water's name, a spot's note), decimals with 3+ places (coordinates) and number runs of 5+ digits
 * (ids). What is left says what went wrong ("undefined is not an object (evaluating '…')"). (T352) One exception: a short
 * quoted NUMBER after "module" is the bundle's module id (`Requiring unknown module "1234"`), kept as it was.
 */
export function scrubMessage(m: string): string {
  const kept: string[] = [];
  const held = m.replace(MODULE_ID, (_all, _q: string, id: string) => `module \u0000${kept.push(id) - 1}\u0000`);
  return cut(held
    .replace(/[^\s@'"`]+@[^\s@'"`]+\.[^\s@'"`]+/g, "…")
    .replace(/\b[a-z][a-z0-9+.-]*:\/\/[^\s)'"`]*/gi, "…")
    .replace(/'[^']*'|"[^"]*"|`[^`]*`/g, "'…'")
    .replace(/-?\d+\.\d{3,}/g, "#")
    .replace(/\d{5,}/g, "#")
    .replace(/\s+/g, " ").trim()
    .replace(/\u0000(\d+)\u0000/g, (_all, i: string) => `"${kept[Number(i)]}"`), CRASH_LIMITS.message);
}

/** A stack with every address cut to its file name and no message line (the message is sent scrubbed, on its own). */
export function scrubStack(stack: string): string {
  const lines = stack.split("\n").filter((l) => /^\s*at\s|@/.test(l) || /^\s+\S/.test(l));
  return cut(lines.slice(0, 30).map((l) => l
    .replace(/[^\s()@]*[\\/]([^\\/\s()@]+)/g, "$1")      // any path or URL → its last segment
    .replace(/\s+/g, " ").trim()).join("\n"), CRASH_LIMITS.stack);
}

const nameOf = (e: unknown): string => {
  const n = e instanceof Error ? e.name : typeof e === "object" && e && typeof (e as { name?: unknown }).name === "string" ? (e as { name: string }).name : "Error";
  return cut(n.replace(/[^\w$. -]/g, "").trim() || "Error", CRASH_LIMITS.name);
};

/** THE PAYLOAD — every field, pinned by test. */
export function crashPayload(o: { error: unknown; kind: CrashKind; fatal: boolean; background?: boolean; ctx: CrashContext; now: Date }): CrashPayload {
  const e = o.error, c = o.ctx;
  const message = e instanceof Error ? e.message : typeof e === "string" ? e : (() => { try { return JSON.stringify(e) ?? String(e); } catch { return String(e); } })();
  const stack = e instanceof Error && typeof e.stack === "string" ? e.stack : "";
  const int = (n: number | null) => (typeof n === "number" && Number.isInteger(n) && n >= 0 ? n : null);
  return {
    format: CRASH_FORMAT, created: o.now.toISOString(), kind: o.kind, fatal: o.fatal, background: !!o.background,
    route: c.route, water: c.water === null ? null : cut(c.water, CRASH_LIMITS.field), view: c.view, part: int(c.part),
    whole: !!c.whole, sheet: c.sheet,
    install: cut(c.install, CRASH_LIMITS.field),
    app: cut(c.app, CRASH_LIMITS.field), env: cut(c.env, CRASH_LIMITS.field),
    platform: cut(c.platform, CRASH_LIMITS.field), release: cut(c.release, CRASH_LIMITS.field),
    name: nameOf(e), message: scrubMessage(message), stack: scrubStack(stack),
  };
}

// ── the install id ───────────────────────────────────────────────────────────────────────────────────────────────────

const INSTALL_RE = /^[0-9a-f]{16}$/;

/**
 * (T352, owner 2026-11-03) THE INSTALL ID: 16 random hex characters made once on this device and kept in its own small
 * text store — no personal data, nothing derived from the device. Crash reports and problem reports carry the same one, so
 * reports from one install can be told apart from another's (the owner's from testers'; distinct installs counted). A
 * store that cannot be read or written gives a new id for this run only (said by `kept: false`).
 */
export function installId(store: TextStore | null | undefined, rand: () => number = Math.random): { id: string; kept: boolean } {
  try {
    const was = (store?.read() ?? "").trim();
    if (INSTALL_RE.test(was)) return { id: was, kept: true };
  } catch { /* unreadable: a new one below */ }
  let id = "";
  for (let i = 0; i < 16; i++) id += Math.floor(rand() * 16).toString(16);
  try { if (store) { store.write(id); return { id, kept: true }; } } catch { /* this run only */ }
  return { id, kept: false };
}

// ── the switch ───────────────────────────────────────────────────────────────────────────────────────────────────────

export interface CrashSetting {
  /** on unless the reader switched it off */
  get(): boolean;
  set(on: boolean): void;
  subscribe(on: () => void): () => void;
}
interface TextStore { read(): string | null; write(text: string): void }

/** Default ON (anonymous, owner 2026-10-28); only the word "off" turns it off. */
export function createCrashSetting(store?: TextStore | null): CrashSetting {
  let on = true;
  try { on = (store?.read() ?? "").trim() !== "off"; } catch { /* unreadable: the default */ }
  const subs = new Set<() => void>();
  return {
    get: () => on,
    set(v) {
      if (v === on) return;
      on = v;
      try { store?.write(v ? "on" : "off"); } catch { /* kept for this run */ }
      for (const s of [...subs]) s();
    },
    subscribe(f) { subs.add(f); return () => { subs.delete(f); }; },
  };
}

const CrashSettingContext = createContext<CrashSetting | null>(null);
export function CrashSettingRoot({ setting, children }: { setting: CrashSetting | null; children?: ReactNode }) {
  return createElement(CrashSettingContext.Provider, { value: setting }, children);
}
/** The switch for the Data block: `on` and `set`, or null where the app has no reporter (a screen mounted alone). */
export function useCrashSetting(): { on: boolean; set: (on: boolean) => void } | null {
  const s = useContext(CrashSettingContext);
  const [, bump] = useState(0);
  useEffect(() => (s ? s.subscribe(() => bump((n) => n + 1)) : undefined), [s]);
  return s ? { on: s.get(), set: (v) => s.set(v) } : null;
}

// ── the reporter ─────────────────────────────────────────────────────────────────────────────────────────────────────

export interface CrashReporter {
  /** Report one error. Never throws, never waits. */
  report(error: unknown, o: { kind: CrashKind; fatal: boolean }): void;
  /** Send a fatal error kept from the last run (at start). Resolves true when the last run's fatal error was told: now, or
   *  (T242) already, before the app died. */
  flush(): Promise<boolean>;
  /** (Android, T362 iOS) Send why the last run ended, as read from the system (iOS: the app's own record). Resolves true when sent; false = not sent (off, offline, repeat, refused). */
  sendExit(r: ExitRecord): Promise<boolean>;
}

/** (T242) the fatal slot's word for "the last run's fatal error was sent before the app died" */
const FATAL_SENT = "sent";

export function createCrashReporter(o: {
  url: string;
  setting: CrashSetting;
  online: () => boolean;
  /** (T352) asked at each error: the build facts, the install id and where the reader is (`crashWhere` of the route) */
  context: () => CrashContext;
  /** one slot for a fatal error, sent at the next start */
  store?: TextStore | null;
  fetch?: typeof fetch;
  now?: () => Date;
  maxPerRun?: number;
  /** (T291) a tool's build: payloads are tagged `test` (the staging worker stores those apart and ignores them) */
  test?: boolean;
}): CrashReporter {
  const f = o.fetch ?? ((u: RequestInfo | URL, i?: RequestInit) => fetch(u, i));
  const now = o.now ?? (() => new Date(clockNow()));
  const seen = new Set<string>();
  const max = o.maxPerRun ?? 5;
  const clearKept = () => { try { o.store?.write(""); } catch { /* nothing kept */ } };

  async function post(p0: { format: string }): Promise<boolean> {
    const p = o.test ? { ...p0, test: true } : p0;
    try {
      const r = await f(o.url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p), keepalive: true });
      return r.ok;
    } catch { return false; }
  }

  return {
    report(error, opt) {
      try {
        if (!o.setting.get()) return;
        const p = crashPayload({ error, kind: opt.kind, fatal: opt.fatal, ctx: o.context(), now: now() });
        const id = `${p.name}|${p.message}|${p.stack.split("\n").slice(0, 3).join("|")}`.replace(/\d+/g, "#");
        if (seen.has(id) || seen.size >= max) return;
        seen.add(id);
        // a fatal error is kept for the next start: the app is dying while it is caught
        if (opt.fatal && o.store) { try { o.store.write(JSON.stringify(p)); } catch { /* sent now, if it can be */ } }
        if (!o.online()) return;                        // offline: dropped (a kept fatal one goes at the next start)
        // (T242 test bed) sent before the app died: the slot now says so, so the next start knows the system's record of
        // this death (a plain CRASH) is the same event and does not send it again
        void post(p).then((ok) => { if (ok && opt.fatal) { try { o.store?.write(FATAL_SENT); } catch { /* it may go twice */ } } });
      } catch { /* reporting never breaks the app */ }
    },
    async sendExit(r) {
      try {
        if (!o.setting.get() || !o.online()) return false;
        const e = new Error(`${r.reason} while ${r.importance.toLowerCase()}${r.rssKb > 0 ? `, ${Math.round(r.rssKb / 1024)} MB in use` : ""}${r.status ? `, status ${r.status}` : ""}${r.description ? `: ${r.description}` : ""}`);
        e.name = "NativeExit"; e.stack = "";
        // (T352) the screen at the time is not known (this is the next run); a background kill is sent non-fatal, marked
        const bg = backgroundKill(r);
        return await post(crashPayload({ error: e, kind: "native", fatal: !bg, background: bg, ctx: { ...o.context(), ...NO_WHERE },
                                         now: new Date(r.timestamp) }));
      } catch { return false; }
    },
    async flush() {
      try {
        const text = o.store?.read();
        if (!text) return false;
        if (text === FATAL_SENT) { clearKept(); return true; }    // told before it died
        if (!o.setting.get()) { clearKept(); return false; }      // switched off: forgotten, nothing sent
        if (!o.online()) return false;
        let p: { format: string };
        try { p = JSON.parse(text) as { format: string }; } catch { clearKept(); return false; }
        // a crash/1 kept by the version before this one is sent as it was (the worker takes both)
        if (!CRASH_FORMATS.includes(p?.format)) { clearKept(); return false; }
        const ok = await post(p);
        if (ok) clearKept();
        return ok;
      } catch { return false; }
    },
  };
}

// ── a failure the app shows ──────────────────────────────────────────────────────────────────────────────────────────

/**
 * (owner 2026-11-03: "make sure we get reports on failures like this ... true universally in app") A failure the app
 * CATCHES and shows the reader — a card's "Couldn't load", a viewer that cannot draw — is reported too, not only an
 * uncaught one: through the same reporter (the Data switch, the scrubbing, the per-run cap), non-fatal. (T368) The one
 * function is core's (`@app/core` failure.ts), so data and map report the same way; the ENVIRONMENT (offline, a refused
 * permission) is never reported. Before the handlers are installed (tests, a tool) nothing is sent.
 */
export { reportFailure } from "@app/core";

/**
 * (T368, owner Q2 2026-11-04) THE MAP'S `onError`, every host: a map-engine failure (a feature-state write, a source's
 * data, a style step, a tap's handler) is logged and REPORTED. Most need no face — the map redraws on the next frame —
 * so nothing is said to the reader here; what the reader can act on is said where it is decided.
 */
export function mapError(e: Error): void {
  console.error("map:", e.message);
  reportFailure(e);
}

// ── the catchers ─────────────────────────────────────────────────────────────────────────────────────────────────────

interface HermesLike { enablePromiseRejectionTracker?(o: { allRejections: boolean; onUnhandled: (id: number, e: unknown) => void; onHandled: (id: number) => void }): void }
interface ErrorUtilsLike { getGlobalHandler?(): (e: unknown, fatal?: boolean) => void; setGlobalHandler?(h: (e: unknown, fatal?: boolean) => void): void }

/**
 * Hand the reporter every uncaught error: React Native's global handler (kept: the previous handler still runs, so the
 * red box / the crash is exactly what it was) and, on the web, the window's `error` and `unhandledrejection`.
 * Returns the undo (tests). Installing twice on one global does nothing the second time.
 */
export function installCrashHandlers(reporter: CrashReporter, g: Record<string, any> = globalThis as never): () => void { // eslint-disable-line @typescript-eslint/no-explicit-any
  if (g.__crashHandlers) return () => {};
  g.__crashHandlers = true;
  const unsink = setFailureSink((e) => {
    // (T716, test bed only) every reported failure said in the log, so a scenario can prove a gesture reported none
    if ((globalThis as { __openProbe?: boolean }).__openProbe)
      console.log(`[failure] ${String(e)} ${((e as { stack?: string } | null)?.stack ?? "").split("\n").slice(1, 5).join(" | ")}`);
    reporter.report(e, { kind: "js", fatal: false });
  });
  const undo: Array<() => void> = [() => { delete g.__crashHandlers; unsink(); }];
  const eu = g.ErrorUtils as ErrorUtilsLike | undefined;
  if (eu?.setGlobalHandler) {
    const prev = eu.getGlobalHandler?.();
    const mine = (e: unknown, fatal?: boolean) => {
      reporter.report(e, { kind: "js", fatal: !!fatal });
      prev?.(e, fatal);
    };
    eu.setGlobalHandler(mine);
    undo.push(() => { if (prev) eu.setGlobalHandler!(prev); });
  }
  // (T368 X1) A PHONE'S UNHANDLED REJECTIONS: Hermes keeps its own promise tracker, and React Native installs one only
  // in a dev build (a LogBox warning). In a release build a `void p` that rejects was silent on a phone while the same
  // code reported on the web. Installed here in every build, reported like the web's `unhandledrejection`.
  const hermes = g.HermesInternal as HermesLike | undefined;
  if (typeof hermes?.enablePromiseRejectionTracker === "function") {
    hermes.enablePromiseRejectionTracker({
      allRejections: true,
      onUnhandled: (_id: number, e: unknown) => {
        if (isEnvironment(e)) return;
        console.warn("Unhandled promise rejection", e);
        reporter.report(e ?? new Error("Unhandled rejection"), { kind: "promise", fatal: false });
      },
      onHandled: () => {},
    });
  }
  if (typeof g.addEventListener === "function") {
    const onError = (ev: { error?: unknown; message?: string }) => reporter.report(ev.error ?? new Error(ev.message ?? "Error"), { kind: "js", fatal: false });
    const onRej = (ev: { reason?: unknown }) => isEnvironment(ev.reason) ? undefined : reporter.report(ev.reason ?? new Error("Unhandled rejection"), { kind: "promise", fatal: false });
    g.addEventListener("error", onError);
    g.addEventListener("unhandledrejection", onRej);
    undo.push(() => { g.removeEventListener?.("error", onError); g.removeEventListener?.("unhandledrejection", onRej); });
  }
  return () => { for (const u of undo.reverse()) u(); };
}

// ── why the last run ended (Android's ApplicationExitInfo; iOS: modules/exit-info's own record, T362) ──────────────

export interface ExitRecord {
  readonly timestamp: number;
  /** CRASH, CRASH_NATIVE, ANR, LOW_MEMORY, EXCESSIVE_RESOURCE_USAGE, INITIALIZATION_FAILURE, SIGNALED, … or one that is not an error */
  readonly reason: string;
  readonly importance: string;
  readonly status: number;
  readonly rssKb: number;
  readonly description: string;
}
export interface ExitSource { recent(since: number): Promise<ExitRecord[]> }

/**
 * (T352) THE SYSTEM CLOSING THE APP BEHIND THE READER'S BACK — a low-memory kill or a signal (SIGNALED 9: the OS reclaiming
 * a cached process) while the app was not in front (importance neither FOREGROUND nor VISIBLE). Not a crash: sent
 * non-fatal and marked `background`, and the worker keeps it out of the crash counts and the digest.
 */
export function backgroundKill(r: Pick<ExitRecord, "reason" | "importance">): boolean {
  return (r.reason === "LOW_MEMORY" || r.reason === "SIGNALED") && r.importance !== "FOREGROUND" && r.importance !== "VISIBLE";
}

/** exits that end the app badly; EXIT_SELF, USER_REQUESTED, USER_STOPPED, PERMISSION_CHANGE and the like are not errors */
const BAD_EXITS: readonly string[] = ["CRASH", "CRASH_NATIVE", "ANR", "LOW_MEMORY", "EXCESSIVE_RESOURCE_USAGE", "INITIALIZATION_FAILURE", "SIGNALED", "DEPENDENCY_DIED"];

/**
 * At start: send why earlier runs ended badly, from the system's own record (so a native crash, an ANR, a low-memory kill or a
 * Java out-of-memory the JS handler never saw is reported). The same switch, scrubbing and server as every other error; nothing
 * for a normal exit. Every exit is looked at once: the newest timestamp seen is kept (`seen`), but only when sending was
 * possible (switch on and online) - offline, the system still holds them for the next start. A plain "CRASH" is skipped when the
 * last run's JS error was kept and sent (`jsFatalSent`): that is the same event. At most `max` a start, newest first.
 */
export async function reportPastExits(o: { reporter: CrashReporter; source: ExitSource | null; seen: TextStore | null; setting: CrashSetting;
                                           online: () => boolean; jsFatalSent: boolean; max?: number }): Promise<number> {
  try {
    if (!o.source || !o.setting.get() || !o.online()) return 0;
    const since = Number(o.seen?.read() ?? 0) || 0;
    const list = (await o.source.recent(since)).filter((r) => r.timestamp > since && BAD_EXITS.includes(r.reason))
      .sort((a, b) => b.timestamp - a.timestamp);
    let sent = 0, failedAt = Infinity;
    for (const r of list.slice(0, o.max ?? 3)) {
      if (r.reason === "CRASH" && o.jsFatalSent) continue;
      if (await o.reporter.sendExit(r)) sent++;
      else failedAt = Math.min(failedAt, r.timestamp);
    }
    const all = await o.source.recent(since);
    // (T242 test bed) a record whose send FAILED (server unreachable or refusing) is looked at again at the next start: the
    // mark stops just before the oldest one that failed
    const newest = Math.min(all.reduce((m, r) => Math.max(m, r.timestamp), since), failedAt - 1);
    try { o.seen?.write(String(newest)); } catch { /* looked at again next start */ }
    console.info(`[crash-reports] sent ${sent} exit record(s); ${all.length} new in the system's record (${all.map((r) => r.reason).join(",")}), ${list.length} bad`);   // the test bed reads this line (logcat)
    return sent;
  } catch { return 0; }
}
