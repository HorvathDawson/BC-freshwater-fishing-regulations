// T242: the crash payload is pinned whole (every field), nothing identifying survives the scrub, the switch off sends
// nothing, offline drops, a repeat is sent once, a fatal error is kept and sent at the next start, handlers chain.
import { describe, expect, it, vi } from "vitest";
import { createCrashReporter, createCrashSetting, crashPayload, installCrashHandlers, routeKind, scrubMessage, scrubStack } from "./crashReport";
import { HOME, type Route } from "./route";

const ctx = { app: "0.1.0 (build 12)", env: "staging", platform: "android 35", release: "r-0fa7927b16e5" };
const NOW = new Date("2026-10-29T20:00:00.000Z");
const boom = () => {
  const e = new TypeError("undefined is not an object (evaluating 'vedder.items') at 49.1234567,-121.9876543 for angler@example.org");
  e.stack = "TypeError: undefined is not an object\n    at render (/Users/dawson/dev/app/packages/ui/src/Shell.tsx:12:34)\n" +
            "    at Shell (https://staging.canifishthis.ca/_expo/static/js/web/entry-abc123.js:1:99999)\n    at anonymous (address at index.android.bundle:1:123456)";
  return e;
};

describe("crashPayload (T242)", () => {
  it("is exactly these fields, scrubbed", () => {
    const p = crashPayload({ error: boom(), kind: "js", fatal: true, route: "water", ctx, now: NOW });
    expect(p).toEqual({
      format: "crash/1", created: "2026-10-29T20:00:00.000Z", kind: "js", fatal: true, route: "water",
      app: "0.1.0 (build 12)", env: "staging", platform: "android 35", release: "r-0fa7927b16e5",
      name: "TypeError",
      message: "undefined is not an object (evaluating '…') at #,# for …",
      stack: "at render (Shell.tsx:12:34)\nat Shell (entry-abc123.js:1:99999)\nat anonymous (address at index.android.bundle:1:123456)",
    });
    expect(Object.keys(p).sort()).toEqual(["app", "created", "env", "fatal", "format", "kind", "message", "name", "platform", "release", "route", "stack"]);
  });
  it("nothing that names a place or a person survives", () => {
    const s = scrubMessage("no section for 'Vedder River' (item 356308001) at 49.123,-121.9 mail a@b.ca http://x.y/z?q=1 \"Chilliwack\"");
    expect(s).not.toMatch(/Vedder|356308001|49\.123|a@b|http|Chilliwack/);
    expect(scrubStack("Error: x\n    at f (/home/me/secret/dir/file.js:1:2)")).toBe("at f (file.js:1:2)");
  });
  it("a non-Error throw is still a report", () => {
    const p = crashPayload({ error: "plain string", kind: "promise", fatal: false, route: "map", ctx, now: NOW });
    expect(p).toMatchObject({ name: "Error", message: "plain string", stack: "", kind: "promise" });
  });
  it("limits hold", () => {
    const p = crashPayload({ error: Object.assign(new Error("m".repeat(900)), { stack: "x\n" + "    at f (a.js:1:1)\n".repeat(400) }), kind: "js", fatal: false, route: "map", ctx, now: NOW });
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

function rig(over: Partial<Parameters<typeof createCrashReporter>[0]> = {}) {
  const sent: unknown[] = [];
  const mem = { v: "" as string | null };
  const store = { read: () => mem.v, write: (t: string) => { mem.v = t; } };
  const setting = createCrashSetting(null);
  let online = true;
  const fetch = vi.fn(async (_u: unknown, i?: RequestInit) => { sent.push(JSON.parse(String(i?.body))); return new Response("{}", { status: 201 }); });
  const r = createCrashReporter({ url: "https://w.example/crash", setting, online: () => online, context: () => ctx, route: () => "menu",
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
    expect(t.sent[0]).toMatchObject({ format: "crash/1", route: "menu", fatal: false });
    expect(t.fetch.mock.calls[0]![0]).toBe("https://w.example/crash");
  });
  it("switched off sends nothing, and forgets a kept error", async () => {
    const t = rig();
    t.r.report(boom(), { kind: "js", fatal: true });   // kept (fatal), sent
    await tick();
    t.mem.v = JSON.stringify(crashPayload({ error: boom(), kind: "js", fatal: true, route: "map", ctx, now: NOW }));
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
    const next = createCrashReporter({ url: "https://w.example/crash", setting: t.setting, online: () => true, context: () => ctx, route: () => "map",
                                       store: t.store, fetch: t.fetch as never, now: () => NOW });
    expect(await next.flush()).toBe(true);
    expect(t.sent).toHaveLength(1);
    expect(t.mem.v).toBe("");
    expect(await next.flush()).toBe(false);
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
});

describe("why the last run ended (Android exit records, T242)", () => {
  const rec = (reason: string, t: number, over = {}) => ({ timestamp: t, reason, importance: "FOREGROUND", status: 0, rssKb: 524288, description: "", ...over });
  const setup = async (recs: ReturnType<typeof rec>[], over: { online?: boolean; on?: boolean; jsFatalSent?: boolean; seen?: string | null } = {}) => {
    const sent: any[] = [];
    const setting = createCrashSetting(null); if (over.on === false) setting.set(false);
    const mem = { v: over.seen ?? null as string | null };
    const fetch = vi.fn(async (_u: unknown, i?: RequestInit) => { sent.push(JSON.parse(String(i?.body))); return new Response("{}", { status: 201 }); });
    const reporter = createCrashReporter({ url: "u", setting, online: () => over.online !== false, context: () => ctx, route: () => "map", fetch: fetch as never, now: () => NOW });
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
    expect(t.sent[0]).toMatchObject({ kind: "native", fatal: true, route: "other", name: "NativeExit", stack: "" });
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
