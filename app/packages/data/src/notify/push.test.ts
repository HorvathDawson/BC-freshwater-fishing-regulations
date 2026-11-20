import { describe, expect, it, vi } from "vitest";
import { isEnvironment, UPDATE_DOWNLOAD_NOTE } from "@app/core";
import type { Manifest } from "../manifest";
import { createPushRegistrar, PUSH_REFRESH_MS, PUSH_RETRY_MS, PUSH_TOKEN_WAIT_MS, type PushSeam } from "./push";
import { runWake } from "./wake";
import { createNotifySettings, NOTIFY_DEFAULT, type NotifyState } from "./settings";
import type { Notice } from "./check";

function memory(init: string | null = null) {
  let text: string | null = init;
  return { read: () => text, write: (t: string) => { text = t; } };
}
const IOS = "a".repeat(64);
function seam(first = IOS, env: "sandbox" | "production" = "sandbox") {
  let on: ((t: string) => void) | null = null;
  let token = first;
  const s: PushSeam & { give(t: string): void; listening(): boolean } = {
    platform: "ios", env, token: async () => token, onToken: (f) => { on = f; return () => { on = null; }; },
    give: (t) => { token = t; on?.(t); }, listening: () => on !== null,
  };
  return s;
}
function server(status = 201) {
  const calls: { url: string; body: unknown }[] = [];
  const fetch = async (url: string, i?: RequestInit) => { calls.push({ url, body: JSON.parse(String(i!.body)) }); return new Response("{}", { status }); };
  return { calls, fetch };
}
const URL_ = "https://w.example/push";

describe("the push token follows the reader's notifications (T739)", () => {
  it("on: registers {platform, token, env} once; again only when the token changes (the old one unregistered first)", async () => {
    const s = seam(), srv = server(), store = memory();
    const r = createPushRegistrar({ seam: s, url: URL_, fetch: srv.fetch, store });
    await r.sync(true);
    await r.sync(true);
    expect(srv.calls).toEqual([{ url: "https://w.example/push/register", body: { platform: "ios", token: IOS, env: "sandbox" } }]);
    expect(JSON.parse(store.read()!)).toEqual({ platform: "ios", token: IOS, env: "sandbox", url: URL_, at: expect.any(Number) });
    expect(s.listening()).toBe(true);
    s.give("b".repeat(64));
    await r.sync(true);   // waits for the token's send in the queue
    expect(srv.calls.slice(1)).toEqual([
      { url: "https://w.example/push/unregister", body: { platform: "ios", token: IOS, env: "sandbox" } },
      { url: "https://w.example/push/register", body: { platform: "ios", token: "b".repeat(64), env: "sandbox" } },
    ]);
  });

  it("(L1) an unchanged token is registered again only once its last register is 7 days old: a token the worker lost comes back", async () => {
    const srv = server(), store = memory();
    let t = 1_000;
    const first = createPushRegistrar({ seam: seam(), url: URL_, fetch: srv.fetch, store, now: () => t });
    await first.sync(true);
    await first.sync(true);
    expect(srv.calls.map((c) => c.url)).toEqual(["https://w.example/push/register"]);
    expect(JSON.parse(store.read()!)).toEqual({ platform: "ios", token: IOS, env: "sandbox", url: URL_, at: 1_000 });
    // the next app start a day later (many phones relaunching on one wake): nothing sent, the line shows the stored time
    t = 1_000 + 86_400_000;
    const next = createPushRegistrar({ seam: seam(), url: URL_, fetch: srv.fetch, store, now: () => t });
    await next.sync(true);
    expect(srv.calls).toHaveLength(1);
    expect(next.status()).toEqual({ state: "registered", at: 1_000 });
    // a start past PUSH_REFRESH_MS registers again (and keeps the new time)
    t = 1_000 + PUSH_REFRESH_MS;
    const late = createPushRegistrar({ seam: seam(), url: URL_, fetch: srv.fetch, store, now: () => t });
    await late.sync(true);
    expect(srv.calls.map((c) => c.url)).toEqual(["https://w.example/push/register", "https://w.example/push/register"]);
    expect(srv.calls[1]!.body).toEqual({ platform: "ios", token: IOS, env: "sandbox" });
    expect(JSON.parse(store.read()!).at).toBe(t);
    // a changed environment (a TestFlight build over a development one) or a record from before `at` registers at once
    const prod = createPushRegistrar({ seam: seam(IOS, "production"), url: URL_, fetch: srv.fetch, store, now: () => t });
    await prod.sync(true);
    expect(srv.calls).toHaveLength(3);
    const old = memory(JSON.stringify({ platform: "ios", token: IOS, env: "sandbox", url: URL_ }));
    await createPushRegistrar({ seam: seam(), url: URL_, fetch: srv.fetch, store: old, now: () => t }).sync(true);
    expect(srv.calls).toHaveLength(4);
  });

  it("(T822) the worker's per-address gap (429) is waited out once; a second 429 is a reported defect, said in the status", async () => {
    const waits: number[] = [];
    const wait = async (ms: number) => { waits.push(ms); };
    // the first answer 429 (a quick off → on inside the worker's 2 s gap), the retry 201: registered
    let n = 0;
    const calls: string[] = [];
    const gap = async (u: string) => { calls.push(u); return new Response("{}", { status: n++ === 0 ? 429 : 201 }); };
    const store = memory();
    const r = createPushRegistrar({ seam: seam(), url: URL_, fetch: gap, store, wait, now: () => 7 });
    await r.sync(true);
    expect(waits).toEqual([PUSH_RETRY_MS]);
    expect(calls).toEqual(["https://w.example/push/register", "https://w.example/push/register"]);
    expect(r.status()).toEqual({ state: "registered", at: 7 });
    expect(JSON.parse(store.read()!).token).toBe(IOS);
    // (L1) 429 twice: the shared address's limit, not a defect: the environment's (never reported), nothing kept, and
    // the status says PENDING (the page tells the reader; the next start tries again)
    const kept = memory();
    const capped = createPushRegistrar({ seam: seam(), url: URL_, fetch: server(429).fetch, store: kept, wait, now: () => 8 });
    const e = await capped.sync(true).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(Error);
    expect(isEnvironment(e)).toBe(true);
    expect((e as Error).message).toBe("push register: HTTP 429 twice (the worker's per-address limit)");
    expect(capped.status()).toEqual({ state: "pending", at: 8, what: "register" });
    expect(kept.read()).toBe(null);
    // (re-review) the same limit on the way OFF: pending as an unregister, the record kept so the next start withdraws it
    let limited = false;
    const sent = memory(JSON.stringify({ platform: "ios", token: IOS, env: "sandbox", url: URL_, at: 1 }));
    const off = createPushRegistrar({ seam: seam(), url: URL_, wait, now: () => 9, store: sent,
                                      fetch: async () => { limited = true; return new Response("{}", { status: 429 }); } });
    const e2 = await off.sync(false).catch((x: unknown) => x);
    expect(limited).toBe(true);
    expect(isEnvironment(e2)).toBe(true);
    expect(off.status()).toEqual({ state: "pending", at: 9, what: "unregister" });
    expect(JSON.parse(sent.read()!).token).toBe(IOS);
  });

  it("(T822) a token the system never hands over fails the sync after the deadline, REPORTED, and frees the queue", async () => {
    vi.useFakeTimers();
    try {
      const srv = server();
      let on: ((t: string) => void) | null = null;
      // iOS never calls didRegisterForRemoteNotifications…: expo-notifications' promise never settles
      const hung: PushSeam = { platform: "ios", env: "production", token: () => new Promise<string>(() => {}),
                               onToken: (f) => { on = f; return () => { on = null; }; } };
      const r = createPushRegistrar({ seam: hung, url: URL_, fetch: srv.fetch, store: memory(), now: () => 9 });
      const seen: string[] = [];
      r.subscribe(() => seen.push(r.status().state));
      const first = r.sync(true).catch((x: unknown) => x);
      await vi.advanceTimersByTimeAsync(0);   // the queue starts the sync
      expect(r.status()).toEqual({ state: "sending", since: 9 });
      await vi.advanceTimersByTimeAsync(PUSH_TOKEN_WAIT_MS);
      const e = await first;
      expect((e as Error).message).toBe("push register: the system gave no push token in 30 s (ios: the request for one was never answered)");
      expect(isEnvironment(e)).toBe(false);   // reported: NotifyRoot hands it to reportFailure
      expect(r.status()).toMatchObject({ state: "failed", environment: false });
      expect(srv.calls).toEqual([]);
      // the queue is free: turning off now runs (nothing was sent, so nothing to withdraw)
      await r.sync(false);
      expect(r.status()).toEqual({ state: "withdrawn", at: 9 });
      // a token iOS hands over late still registers while notifications are on
      const again = r.sync(true).catch(() => undefined);
      on!(IOS);
      await vi.advanceTimersByTimeAsync(PUSH_TOKEN_WAIT_MS);
      await again;
      expect(srv.calls.map((c) => c.url)).toContain("https://w.example/push/register");
      expect(seen.slice(0, 3)).toEqual(["sending", "failed", "sending"]);
    } finally { vi.useRealTimers(); }
  });

  it("(T822) offline is the environment's: not reported, but SHOWN in the status", async () => {
    const r = createPushRegistrar({ seam: seam(), url: URL_, fetch: async () => { throw new TypeError("Network request failed"); },
                                    store: memory(), now: () => 5 });
    expect(r.status()).toEqual({ state: "idle" });
    await expect(r.sync(true)).rejects.toMatchObject({ environment: true });
    expect(r.status()).toEqual({ state: "failed", at: 5, why: "Network request failed", environment: true });
  });

  it("off: unregisters the token last sent, forgets it, and stops listening; off again sends nothing", async () => {
    const s = seam(), srv = server(), store = memory();
    const r = createPushRegistrar({ seam: s, url: URL_, fetch: srv.fetch, store });
    await r.sync(true);
    await r.sync(false);
    await r.sync(false);
    expect(srv.calls.map((c) => c.url)).toEqual(["https://w.example/push/register", "https://w.example/push/unregister"]);
    expect(store.read()).toBe("");
    expect(s.listening()).toBe(false);
  });

  it("a refused send throws and keeps nothing (the next sync tries again); offline is the environment's", async () => {
    const store = memory();
    const r = createPushRegistrar({ seam: seam(), url: URL_, fetch: server(503).fetch, store });
    await expect(r.sync(true)).rejects.toThrow("push register: HTTP 503");
    expect(store.read()).toBe(null);
    const off = createPushRegistrar({ seam: seam(), url: URL_, fetch: async () => { throw new TypeError("Network request failed"); }, store });
    await expect(off.sync(true)).rejects.toMatchObject({ environment: true });
  });

  it("nothing but the platform, the token and the environment is sent", async () => {
    const srv = server();
    await createPushRegistrar({ seam: seam(IOS, "production"), url: URL_, fetch: srv.fetch, store: memory() }).sync(true);
    expect(Object.keys(srv.calls[0]!.body as object).sort()).toEqual(["env", "platform", "token"]);
  });
});

describe("a push wake-up (T739)", () => {
  const manifest = (release: string) => ({ data_release: release, minAppVersion: null }) as unknown as Manifest;
  function harness(state: Partial<NotifyState>, phase: string | null) {
    const settings = createNotifySettings(memory(JSON.stringify({ ...NOTIFY_DEFAULT, ...state })));
    const told: Notice[] = [];
    const checks: unknown[] = [];
    const updates = phase === null ? null : {
      check: async (o?: unknown) => { checks.push(o); },
      snapshot: () => ({ release: { phase, target: "r-2" } }) as never,
    };
    return { settings, told, checks, deps: { settings, updates, obs: async () => ({ fetchedAt: "", stations: {} }), manifest: async () => manifest("r-2"),
      notify: async (n: Notice) => { told.push(n); }, latestApp: null, appVersion: null, now: () => 0 } };
  }

  it("a newer release whose download the update service started: taken, not announced", async () => {
    const h = harness({ on: true, updateReady: true, held: "r-1" }, "downloading");
    const r = await runWake(h.deps);
    expect(h.checks).toEqual([{ feeds: true, release: true }]);
    expect(r.downloading).toBe("r-2");
    expect(h.told).toEqual([]);
    expect(r.line).toBe("woken by our server, new data downloading");
    expect(h.settings.read().runs.map((x) => x.result)).toEqual(["woken by our server, new data downloading"]);
  });

  it("held (Wi-Fi only on mobile data, or no runtime to download in): ONE notice saying new data is ready to download", async () => {
    for (const phase of ["waiting-wifi", null]) {
      const h = harness({ on: true, updateReady: true, held: "r-1" }, phase);
      const r = await runWake(h.deps);
      await runWake(h.deps);
      expect(r.downloading).toBe(null);
      expect(h.settings.read().runs.map((x) => x.result)).toEqual(["woken by our server, new data ready", "woken by our server, nothing to tell"]);
      expect(h.told.map((n) => `${n.title}. ${n.body}`)).toEqual(["New regulations data is ready to download. Open the app to download it."]);
      expect(h.told[0]!.title).toBe(UPDATE_DOWNLOAD_NOTE.title);
    }
  });

  it("the reader's \"New regulations data\" switch off: nothing told, the download still starts when allowed", async () => {
    const h = harness({ on: true, updateReady: false, held: "r-1" }, "waiting-wifi");
    await runWake(h.deps);
    expect(h.told).toEqual([]);
  });
});
