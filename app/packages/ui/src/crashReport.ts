/**
 * (T242) CRASH AND ERROR REPORTS — the app's own, to our own server (the feeds worker's `POST /crash`; no third-party
 * crash service, owner rule). What leaves the device, and only while "Send crash reports" (More → Data, default on) is on,
 * the device is online and the error is new this run:
 *
 *   the app build, environment, platform, data release; the screen KIND (map / conditions / runs / spots / menu / water —
 *   never a water's name, a coordinate or an id); the error's kind (js / promise / render), whether it ended the app, its
 *   name, its message with every quoted text, address, number run and coordinate taken out (`scrubMessage`) and its stack
 *   with the addresses cut to file names. No identifier of the device or the person; the server stores neither the IP
 *   address nor the user agent.
 *
 * Offline → dropped (never queued). A fatal error is kept in one slot on the device and sent at the next start (the app is
 * dying as it is caught), then cleared. At most `maxPerRun` different errors a run; the same error is sent once.
 * Switching it off sends nothing and forgets a kept one.
 */
import { createContext, createElement, useContext, useEffect, useState, type ReactNode } from "react";
import type { Route } from "./route";

export const CRASH_FORMAT = "crash/1";
export type CrashKind = "js" | "promise" | "render" | "native";
export type RouteKind = "map" | "conditions" | "runs" | "spots" | "menu" | "water" | "other";
export const CRASH_LIMITS = { name: 60, message: 200, stack: 2000, field: 100 } as const;

export interface CrashPayload {
  format: typeof CRASH_FORMAT;
  created: string;
  kind: CrashKind;
  fatal: boolean;
  route: RouteKind;
  app: string; env: string; platform: string; release: string;
  name: string; message: string; stack: string;
}
export interface CrashContext { app: string; env: string; platform: string; release: string }

/** The screen as a KIND, never as a place: the tab, a water, or the menu's pages (the More tab). */
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

const cut = (s: string, n: number) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);

/**
 * An error's message with what could identify a person or a place taken out: e-mail addresses, web addresses, every quoted
 * text ('…', "…", `…` — a water's name, a spot's note), decimals with 3+ places (coordinates) and number runs of 5+ digits
 * (ids). What is left says what went wrong ("undefined is not an object (evaluating '…')").
 */
export function scrubMessage(m: string): string {
  return cut(m
    .replace(/[^\s@'"`]+@[^\s@'"`]+\.[^\s@'"`]+/g, "…")
    .replace(/\b[a-z][a-z0-9+.-]*:\/\/[^\s)'"`]*/gi, "…")
    .replace(/'[^']*'|"[^"]*"|`[^`]*`/g, "'…'")
    .replace(/-?\d+\.\d{3,}/g, "#")
    .replace(/\d{5,}/g, "#")
    .replace(/\s+/g, " ").trim(), CRASH_LIMITS.message);
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
export function crashPayload(o: { error: unknown; kind: CrashKind; fatal: boolean; route: RouteKind; ctx: CrashContext; now: Date }): CrashPayload {
  const e = o.error;
  const message = e instanceof Error ? e.message : typeof e === "string" ? e : (() => { try { return JSON.stringify(e) ?? String(e); } catch { return String(e); } })();
  const stack = e instanceof Error && typeof e.stack === "string" ? e.stack : "";
  return {
    format: CRASH_FORMAT, created: o.now.toISOString(), kind: o.kind, fatal: o.fatal, route: o.route,
    app: cut(o.ctx.app, CRASH_LIMITS.field), env: cut(o.ctx.env, CRASH_LIMITS.field),
    platform: cut(o.ctx.platform, CRASH_LIMITS.field), release: cut(o.ctx.release, CRASH_LIMITS.field),
    name: nameOf(e), message: scrubMessage(message), stack: scrubStack(stack),
  };
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
  /** Send a fatal error kept from the last run (at start). Resolves true when one went. */
  flush(): Promise<boolean>;
  /** (Android) Send why the last run ended, as read from the system. Resolves true when sent; false = not sent (off, offline, repeat, refused). */
  sendExit(r: ExitRecord): Promise<boolean>;
}

export function createCrashReporter(o: {
  url: string;
  setting: CrashSetting;
  online: () => boolean;
  context: () => CrashContext;
  route: () => RouteKind;
  /** one slot for a fatal error, sent at the next start */
  store?: TextStore | null;
  fetch?: typeof fetch;
  now?: () => Date;
  maxPerRun?: number;
  /** (T291) a tool's build: payloads are tagged `test` (the staging worker stores those apart and ignores them) */
  test?: boolean;
}): CrashReporter {
  const f = o.fetch ?? ((u: RequestInfo | URL, i?: RequestInit) => fetch(u, i));
  const now = o.now ?? (() => new Date());
  const seen = new Set<string>();
  const max = o.maxPerRun ?? 5;
  const clearKept = () => { try { o.store?.write(""); } catch { /* nothing kept */ } };

  async function post(p0: CrashPayload): Promise<boolean> {
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
        const p = crashPayload({ error, kind: opt.kind, fatal: opt.fatal, route: o.route(), ctx: o.context(), now: now() });
        const id = `${p.name}|${p.message}|${p.stack.split("\n").slice(0, 3).join("|")}`.replace(/\d+/g, "#");
        if (seen.has(id) || seen.size >= max) return;
        seen.add(id);
        // a fatal error is kept for the next start: the app is dying while it is caught
        if (opt.fatal && o.store) { try { o.store.write(JSON.stringify(p)); } catch { /* sent now, if it can be */ } }
        if (!o.online()) return;                        // offline: dropped (a kept fatal one goes at the next start)
        void post(p).then((ok) => { if (ok && opt.fatal) clearKept(); });
      } catch { /* reporting never breaks the app */ }
    },
    async sendExit(r) {
      try {
        if (!o.setting.get() || !o.online()) return false;
        const e = new Error(`${r.reason} while ${r.importance.toLowerCase()}${r.rssKb > 0 ? `, ${Math.round(r.rssKb / 1024)} MB in use` : ""}${r.status ? `, status ${r.status}` : ""}${r.description ? `: ${r.description}` : ""}`);
        e.name = "NativeExit"; e.stack = "";
        return await post(crashPayload({ error: e, kind: "native", fatal: true, route: "other", ctx: o.context(), now: new Date(r.timestamp) }));
      } catch { return false; }
    },
    async flush() {
      try {
        const text = o.store?.read();
        if (!text) return false;
        if (!o.setting.get()) { clearKept(); return false; }      // switched off: forgotten, nothing sent
        if (!o.online()) return false;
        let p: CrashPayload;
        try { p = JSON.parse(text) as CrashPayload; } catch { clearKept(); return false; }
        if (p?.format !== CRASH_FORMAT) { clearKept(); return false; }
        const ok = await post(p);
        if (ok) clearKept();
        return ok;
      } catch { return false; }
    },
  };
}

// ── the catchers ─────────────────────────────────────────────────────────────────────────────────────────────────────

interface ErrorUtilsLike { getGlobalHandler?(): (e: unknown, fatal?: boolean) => void; setGlobalHandler?(h: (e: unknown, fatal?: boolean) => void): void }

/**
 * Hand the reporter every uncaught error: React Native's global handler (kept: the previous handler still runs, so the
 * red box / the crash is exactly what it was) and, on the web, the window's `error` and `unhandledrejection`.
 * Returns the undo (tests). Installing twice on one global does nothing the second time.
 */
export function installCrashHandlers(reporter: CrashReporter, g: Record<string, any> = globalThis as never): () => void { // eslint-disable-line @typescript-eslint/no-explicit-any
  if (g.__crashHandlers) return () => {};
  g.__crashHandlers = true;
  const undo: Array<() => void> = [() => { delete g.__crashHandlers; }];
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
  if (typeof g.addEventListener === "function") {
    const onError = (ev: { error?: unknown; message?: string }) => reporter.report(ev.error ?? new Error(ev.message ?? "Error"), { kind: "js", fatal: false });
    const onRej = (ev: { reason?: unknown }) => reporter.report(ev.reason ?? new Error("Unhandled rejection"), { kind: "promise", fatal: false });
    g.addEventListener("error", onError);
    g.addEventListener("unhandledrejection", onRej);
    undo.push(() => { g.removeEventListener?.("error", onError); g.removeEventListener?.("unhandledrejection", onRej); });
  }
  return () => { for (const u of undo.reverse()) u(); };
}

// ── why the last run ended (Android's ApplicationExitInfo) ────────────────────────────────────────────────────────────

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
    let sent = 0;
    for (const r of list.slice(0, o.max ?? 3)) {
      if (r.reason === "CRASH" && o.jsFatalSent) continue;
      if (await o.reporter.sendExit(r)) sent++;
    }
    const all = await o.source.recent(since);
    const newest = all.reduce((m, r) => Math.max(m, r.timestamp), since);
    try { o.seen?.write(String(newest)); } catch { /* looked at again next start */ }
    console.info(`[crash-reports] sent ${sent} exit record(s); ${all.length} new in the system's record (${all.map((r) => r.reason).join(",")}), ${list.length} bad`);   // the test bed reads this line (logcat)
    return sent;
  } catch { return 0; }
}
