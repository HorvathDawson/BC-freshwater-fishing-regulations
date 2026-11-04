// T242: the crash payload is pinned whole (every field), nothing identifying survives the scrub, the switch off sends
// nothing, offline drops, a repeat is sent once, a fatal error is kept and sent at the next start, handlers chain.
import { describe, expect, it, vi } from "vitest";
import { backgroundKill, createCrashReporter, createCrashSetting, crashPayload, crashWhere, installCrashHandlers, installId, NO_WHERE, reportFailure, routeKind,
         scrubMessage, scrubStack, type CrashWhere } from "./crashReport";
import { HOME, type Route } from "./route";

const WATER: CrashWhere = { route: "water", water: "356308001", view: "map", part: 2, whole: true, sheet: null };
const ctx = { app: "0.1.0 (build 12)", env: "staging", platform: "android 35", release: "r-0fa7927b16e5", install: "0123456789abcdef", ...WATER };
const at = (w: Partial<CrashWhere>) => ({ ...ctx, ...NO_WHERE, ...w });
const NOW = new Date("2026-10-29T20:00:00.000Z");
const boom = () => {
  const e = new TypeError("undefined is not an object (evaluating 'vedder.items') at 49.1234567,-121.9876543 for angler@example.org");
  e.stack = "TypeError: undefined is not an object\n    at render (/Users/dawson/dev/app/packages/ui/src/Shell.tsx:12:34)\n" +
            "    at Shell (https://staging.canifishthis.ca/_expo/static/js/web/entry-abc123.js:1:99999)\n    at anonymous (address at index.android.bundle:1:123456)";
  return e;
};

describe("crashPayload (T242)", () => {
  it("is exactly these fields, scrubbed", () => {
    const p = crashPayload({ error: boom(), kind: "js", fatal: true, ctx, now: NOW });
    expect(p).toEqual({
      format: "crash/2", created: "2026-10-29T20:00:00.000Z", kind: "js", fatal: true, background: false, route: "water",
      water: "356308001", view: "map", part: 2, whole: true, sheet: null, install: "0123456789abcdef",
      app: "0.1.0 (build 12)", env: "staging", platform: "android 35", release: "r-0fa7927b16e5",
      name: "TypeError",
      message: "undefined is not an object (evaluating '…') at #,# for …",
      stack: "at render (Shell.tsx:12:34)\nat Shell (entry-abc123.js:1:99999)\nat anonymous (address at index.android.bundle:1:123456)",
    });
    expect(Object.keys(p).sort()).toEqual(["app", "background", "created", "env", "fatal", "format", "install", "kind", "message", "name",
                                           "part", "platform", "release", "route", "sheet", "stack", "view", "water", "whole"]);
  });
  it("(T352) keeps a short quoted module number, and scrubs everything else as before", () => {
    expect(scrubMessage('Requiring unknown module "1234". If you are sure the module exists, try restarting Metro.'))
      .toBe('Requiring unknown module "1234". If you are sure the module exists, try restarting Metro.');
    expect(scrubMessage("Requiring unknown module '98765'")).toBe('Requiring unknown module "98765"');
    // not a number, or too long to be a module id, or not after "module": scrubbed
    expect(scrubMessage('Requiring unknown module "Vedder River"')).toBe("Requiring unknown module '…'");
    expect(scrubMessage('Requiring unknown module "356308001"')).toBe("Requiring unknown module '…'");
    expect(scrubMessage('no section "1234"')).toBe("no section '…'");
  });
  it("nothing that names a place or a person survives", () => {
    const s = scrubMessage("no section for 'Vedder River' (item 356308001) at 49.123,-121.9 mail a@b.ca http://x.y/z?q=1 \"Chilliwack\"");
    expect(s).not.toMatch(/Vedder|356308001|49\.123|a@b|http|Chilliwack/);
    expect(scrubStack("Error: x\n    at f (/home/me/secret/dir/file.js:1:2)")).toBe("at f (file.js:1:2)");
  });
  it("a non-Error throw is still a report", () => {
    const p = crashPayload({ error: "plain string", kind: "promise", fatal: false, ctx: at({ route: "map" }), now: NOW });
    expect(p).toMatchObject({ name: "Error", message: "plain string", stack: "", kind: "promise" });
  });
  it("limits hold", () => {
    const p = crashPayload({ error: Object.assign(new Error("m".repeat(900)), { stack: "x\n" + "    at f (a.js:1:1)\n".repeat(400) }), kind: "js", fatal: false, ctx, now: NOW });
    expect(p.message.length).toBeLessThanOrEqual(200);
    expect(p.stack.length).toBeLessThanOrEqual(2000);
  });
});

describe("routeKind (T242)", () => {
  it("a kind, never a place", () => {
    expect(routeKind(null)).toBe("other");
    expect(routeKind(HOME)).toBe("map");
    expect(routeKind({ ...HOME, tab: "more" })).toBe("menu");
    expect(routeKind({ ...HOME, tab: "runs" })).toBe("runs");
    expect(routeKind({ mode: "water", stack: [], from: HOME } as unknown as Route)).toBe("water");
  });
});

describe("crashWhere (T352): the open water and its pane, from the route", () => {
  const frame = { item: "356308001", part: 2, tapSid: 812345, at: { lat: 49.1, lon: -121.9 }, view: "conditions" };
  it("names the water by its id, its view, part, whole and the route-held sheet — never its name or the tap point", () => {
    expect(crashWhere(null)).toEqual(NO_WHERE);
    expect(crashWhere(HOME)).toEqual({ ...NO_WHERE, route: "map" });
    expect(crashWhere({ ...HOME, search: true })).toEqual({ ...NO_WHERE, route: "map", sheet: "search" });
    expect(crashWhere({ ...HOME, tab: "spots", spot: "s1" })).toEqual({ ...NO_WHERE, route: "spots", sheet: "spot" });
    expect(crashWhere({ ...HOME, adding: { at: null } })).toEqual({ ...NO_WHERE, route: "map", sheet: "adding" });
    const w = { mode: "water", stack: [{ ...frame, item: "x" }, { ...frame, whole: true }], from: HOME } as unknown as Route;
    expect(crashWhere(w)).toEqual({ route: "water", water: "356308001", view: "conditions", part: 2, whole: true, sheet: null });
    expect(crashWhere({ ...(w as object), spot: "s1" } as Route).sheet).toBe("spot");
    // an unnamed reach: no water id, and never a section id (AGENTS rule 5)
    const reach = { mode: "water", stack: [{ ...frame, item: null, part: null }], from: HOME } as unknown as Route;
    expect(crashWhere(reach)).toEqual({ route: "water", water: null, view: "conditions", part: null, whole: false, sheet: null });
    expect(JSON.stringify(crashWhere(w))).not.toMatch(/49\.1|121\.9/);
  });
});

describe("the install id (T352)", () => {
  it("is made once (16 random hex), kept, and read back the same; nothing else is in it", () => {
    const mem = { v: null as string | null };
    const st = { read: () => mem.v, write: (t: string) => { mem.v = t; } };
    const a = installId(st, () => 0.5);
    expect(a).toEqual({ id: "8888888888888888", kept: true });
    expect(mem.v).toBe("8888888888888888");
    expect(installId(st, () => 0.1)).toEqual({ id: "8888888888888888", kept: true });
    expect(installId({ read: () => "garbage", write: () => {} }, () => 0).id).toBe("0000000000000000");
    expect(installId({ read: () => { throw new Error("x"); }, write: () => { throw new Error("y"); } }, () => 0))
      .toEqual({ id: "0000000000000000", kept: false });
    expect(installId(null).id).toMatch(/^[0-9a-f]{16}$/);
  });
  it("rides every report from the context", () => {
    expect(crashPayload({ error: new Error("x"), kind: "js", fatal: false, ctx, now: NOW }).install).toBe("0123456789abcdef");
  });
});

function rig(over: Partial<Parameters<typeof createCrashReporter>[0]> = {}) {
  const sent: unknown[] = [];
  const mem = { v: "" as string | null };
  const store = { read: () => mem.v, write: (t: string) => { mem.v = t; } };
  const setting = createCrashSetting(null);
  let online = true;
  const fetch = vi.fn(async (_u: unknown, i?: RequestInit) => { sent.push(JSON.parse(String(i?.body))); return new Response("{}", { status: 201 }); });
  const r = createCrashReporter({ url: "https://w.example/crash", setting, online: () => online, context: () => at({ route: "menu" }),
                                  store, fetch: fetch as never, now: () => NOW, ...over });
  return { r, sent, fetch, store, mem, setting, setOnline: (b: boolean) => { online = b; } };
}
const tick = () => new Promise((r) => setTimeout(r, 0));

describe("the reporter (T242)", () => {
  it("sends a new error once, to the crash URL, as the payload", async () => {
    const t = rig();
    t.r.report(boom(), { kind: "js", fatal: false });
    t.r.report(boom(), { kind: "js", fatal: false });
    await tick();
    expect(t.sent).toHaveLength(1);
    expect(t.sent[0]).toMatchObject({ format: "crash/2", route: "menu", fatal: false, background: false, install: "0123456789abcdef" });
    expect(t.fetch.mock.calls[0]![0]).toBe("https://w.example/crash");
  });
  it("switched off sends nothing, and forgets a kept error", async () => {
    const t = rig();
    t.r.report(boom(), { kind: "js", fatal: true });   // kept (fatal), sent
    await tick();
    t.mem.v = JSON.stringify(crashPayload({ error: boom(), kind: "js", fatal: true, ctx, now: NOW }));
    t.setting.set(false);
    t.r.report(new Error("another"), { kind: "js", fatal: false });
    expect(await t.r.flush()).toBe(false);
    await tick();
    expect(t.sent).toHaveLength(1);
    expect(t.mem.v).toBe("");
  });
  it("offline drops a plain error (never queued)", async () => {
    const t = rig();
    t.setOnline(false);
    t.r.report(boom(), { kind: "js", fatal: false });
    await tick();
    t.setOnline(true);
    expect(await t.r.flush()).toBe(false);
    expect(t.sent).toEqual([]);
  });
  it("a fatal error is kept and goes at the next start, then is cleared", async () => {
    const t = rig();
    t.setOnline(false);
    t.r.report(boom(), { kind: "js", fatal: true });
    expect(t.sent).toEqual([]);
    expect(JSON.parse(t.mem.v!)).toMatchObject({ fatal: true });
    t.setOnline(true);
    const next = createCrashReporter({ url: "https://w.example/crash", setting: t.setting, online: () => true, context: () => ctx,
                                       store: t.store, fetch: t.fetch as never, now: () => NOW });
    expect(await next.flush()).toBe(true);
    expect(t.sent).toHaveLength(1);
    expect(t.mem.v).toBe("");
    expect(await next.flush()).toBe(false);
    // (T352) a crash/1 kept by the version before is still sent as it was; an unknown format is forgotten
    t.mem.v = JSON.stringify({ format: "crash/1", name: "Error" });
    expect(await next.flush()).toBe(true);
    expect(t.sent[1]).toEqual({ format: "crash/1", name: "Error" });
    t.mem.v = JSON.stringify({ format: "crash/9" });
    expect(await next.flush()).toBe(false);
    expect(t.mem.v).toBe("");
  });
  it("at most maxPerRun different errors a run; a server failure never throws", async () => {
    const t = rig({ maxPerRun: 2, fetch: (async () => { throw new Error("net"); }) as never });
    for (const m of ["a", "b", "c"]) t.r.report(new Error(m), { kind: "js", fatal: false });
    await tick();
    expect(t.setting.get()).toBe(true);
  });
  it("the setting defaults on, and only 'off' turns it off; set persists", () => {
    const mem = { v: null as string | null };
    const st = { read: () => mem.v, write: (t: string) => { mem.v = t; } };
    expect(createCrashSetting(st).get()).toBe(true);
    createCrashSetting(st).set(false);
    expect(mem.v).toBe("off");
    expect(createCrashSetting(st).get()).toBe(false);
    expect(createCrashSetting({ read: () => "garbage", write: () => {} }).get()).toBe(true);
  });
});

describe("the catchers (T242)", () => {
  it("native: the global handler reports and still calls the previous one; undo restores it", () => {
    const calls: string[] = [];
    const prev = vi.fn();
    let handler: ((e: unknown, f?: boolean) => void) | undefined;
    const g: Record<string, unknown> = { ErrorUtils: { getGlobalHandler: () => prev, setGlobalHandler: (h: typeof handler) => { handler = h; } } };
    const undo = installCrashHandlers({ report: (e, o) => calls.push(`${(e as Error).message}:${o.kind}:${o.fatal}`), flush: async () => false, sendExit: async () => false }, g);
    handler!(new Error("x"), true);
    expect(calls).toEqual(["x:js:true"]);
    expect(prev).toHaveBeenCalledOnce();
    installCrashHandlers({ report: () => calls.push("twice"), flush: async () => false, sendExit: async () => false }, g);   // second install: nothing
    handler!(new Error("y"), false);
    expect(calls).toEqual(["x:js:true", "y:js:false"]);
    undo();
    expect(handler).toBe(prev);
  });
  it("web: error and unhandledrejection events", () => {
    const l: Record<string, (ev: never) => void> = {};
    const g = { addEventListener: (k: string, f: (ev: never) => void) => { l[k] = f; }, removeEventListener: () => {} };
    const calls: string[] = [];
    installCrashHandlers({ report: (e, o) => calls.push(`${(e as Error).message}:${o.kind}`), flush: async () => false, sendExit: async () => false }, g as never);
    l.error!({ error: new Error("e1") } as never);
    l.unhandledrejection!({ reason: new Error("r1") } as never);
    expect(calls).toEqual(["e1:js", "r1:promise"]);
  });
  it("(T368 X1) a phone's unhandled rejections: Hermes's tracker reports them, the environment's excepted", () => {
    let opts: { allRejections: boolean; onUnhandled: (id: number, e: unknown) => void } | null = null;
    const g = { HermesInternal: { enablePromiseRejectionTracker: (o: typeof opts) => { opts = o; } } };
    const calls: string[] = [];
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const undo = installCrashHandlers({ report: (e, o) => calls.push(`${(e as Error).message}:${o.kind}:${o.fatal}`), flush: async () => false, sendExit: async () => false }, g as never);
    expect(opts!.allRejections).toBe(true);
    opts!.onUnhandled(1, new Error("p1"));
    opts!.onUnhandled(2, Object.assign(new Error("offline"), { environment: true }));
    expect(calls).toEqual(["p1:promise:false"]);
    undo();
    warn.mockRestore();
  });
  it("(T368) reportFailure reaches the installed reporter (non-fatal), never the environment's, nothing after undo", () => {
    const calls: string[] = [];
    const undo = installCrashHandlers({ report: (e, o) => calls.push(`${(e as Error).message}:${o.kind}:${o.fatal}`), flush: async () => false, sendExit: async () => false }, {} as never);
    reportFailure(new Error("caught"));
    reportFailure(Object.assign(new TypeError("Network request failed")));
    undo();
    reportFailure(new Error("after"));
    expect(calls).toEqual(["caught:js:false"]);
  });
});

describe("background OS kills (T352)", () => {
  it("are LOW_MEMORY or SIGNALED while not in front; everything else is an exit like before", () => {
    expect(backgroundKill({ reason: "LOW_MEMORY", importance: "BACKGROUND" })).toBe(true);
    expect(backgroundKill({ reason: "SIGNALED", importance: "SERVICE" })).toBe(true);
    expect(backgroundKill({ reason: "SIGNALED", importance: "FOREGROUND" })).toBe(false);
    expect(backgroundKill({ reason: "LOW_MEMORY", importance: "VISIBLE" })).toBe(false);
    expect(backgroundKill({ reason: "CRASH_NATIVE", importance: "BACKGROUND" })).toBe(false);
    expect(backgroundKill({ reason: "ANR", importance: "BACKGROUND" })).toBe(false);
  });
});

describe("why the last run ended (Android exit records, T242)", () => {
  const rec = (reason: string, t: number, over = {}) => ({ timestamp: t, reason, importance: "FOREGROUND", status: 0, rssKb: 524288, description: "", ...over });
  const setup = async (recs: ReturnType<typeof rec>[], over: { online?: boolean; on?: boolean; jsFatalSent?: boolean; seen?: string | null } = {}) => {
    const sent: any[] = [];
    const setting = createCrashSetting(null); if (over.on === false) setting.set(false);
    const mem = { v: over.seen ?? null as string | null };
    const fetch = vi.fn(async (_u: unknown, i?: RequestInit) => { sent.push(JSON.parse(String(i?.body))); return new Response("{}", { status: 201 }); });
    const reporter = createCrashReporter({ url: "u", setting, online: () => over.online !== false, context: () => ctx, fetch: fetch as never, now: () => NOW });
    const { reportPastExits } = await import("./crashReport");
    const n = await reportPastExits({ reporter, source: { recent: async (s) => recs.filter((r) => r.timestamp > s) }, seen: { read: () => mem.v, write: (t) => { mem.v = t; } },
                                      setting, online: () => over.online !== false, jsFatalSent: over.jsFatalSent ?? false });
    return { n, sent, mem };
  };
  it("sends a native crash, an ANR and an out-of-memory kill as kind native with no stack, once", async () => {
    const t = await setup([rec("CRASH_NATIVE", 3000, { status: 11 }), rec("ANR", 2000), rec("EXIT_SELF", 1500), rec("LOW_MEMORY", 1000, { importance: "BACKGROUND" })]);
    expect(t.n).toBe(3);
    expect(t.sent.map((p) => p.message)).toEqual([
      "CRASH_NATIVE while foreground, 512 MB in use, status 11", "ANR while foreground, 512 MB in use", "LOW_MEMORY while background, 512 MB in use"]);
    expect(t.sent[0]).toMatchObject({ kind: "native", fatal: true, background: false, route: "other", name: "NativeExit", stack: "" });
    // (T352) the screen at the time is not known: no water is named (the context's is this run's, not the dead one's)
    expect(t.sent[0]).toMatchObject({ ...NO_WHERE, install: "0123456789abcdef" });
    // a low-memory kill in the background is not a crash: sent non-fatal, marked
    expect(t.sent[2]).toMatchObject({ fatal: false, background: true });
    expect(t.mem.v).toBe("3000");
    const again = await setup([rec("CRASH_NATIVE", 3000)], { seen: t.mem.v });
    expect(again.n).toBe(0);
  });
  it("a plain CRASH is the kept JS error already sent; switched off or offline sends nothing and marks nothing", async () => {
    expect((await setup([rec("CRASH", 5)], { jsFatalSent: true })).n).toBe(0);
    expect((await setup([rec("CRASH", 5)])).n).toBe(1);
    const off = await setup([rec("ANR", 5)], { on: false });
    expect(off.n).toBe(0); expect(off.mem.v).toBeNull();
    const net = await setup([rec("ANR", 5)], { online: false });
    expect(net.n).toBe(0); expect(net.mem.v).toBeNull();
  });
});
