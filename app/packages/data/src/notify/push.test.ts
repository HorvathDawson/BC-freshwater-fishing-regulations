import { describe, expect, it } from "vitest";
import { UPDATE_DOWNLOAD_NOTE } from "@app/core";
import type { Manifest } from "../manifest";
import { createPushRegistrar, type PushSeam } from "./push";
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
    expect(JSON.parse(store.read()!)).toEqual({ platform: "ios", token: IOS, env: "sandbox", url: URL_ });
    expect(s.listening()).toBe(true);
    s.give("b".repeat(64));
    await r.sync(true);   // waits for the token's send in the queue
    expect(srv.calls.slice(1)).toEqual([
      { url: "https://w.example/push/unregister", body: { platform: "ios", token: IOS, env: "sandbox" } },
      { url: "https://w.example/push/register", body: { platform: "ios", token: "b".repeat(64), env: "sandbox" } },
    ]);
  });

  it("an unchanged token is registered once per app start, not once per sync: a token the worker lost comes back", async () => {
    const srv = server(), store = memory();
    const first = createPushRegistrar({ seam: seam(), url: URL_, fetch: srv.fetch, store });
    await first.sync(true);
    await first.sync(true);
    await first.sync(true);
    expect(srv.calls.map((c) => c.url)).toEqual(["https://w.example/push/register"]);
    // the next app start: a new registrar over the same stored record (token, env and address unchanged) registers again
    const next = createPushRegistrar({ seam: seam(), url: URL_, fetch: srv.fetch, store });
    await next.sync(true);
    await next.sync(true);
    expect(srv.calls.map((c) => c.url)).toEqual(["https://w.example/push/register", "https://w.example/push/register"]);
    expect(srv.calls[1]!.body).toEqual({ platform: "ios", token: IOS, env: "sandbox" });
  });

  it("the worker's per-address limit (429) is the environment's: thrown (tried again next start), never reported as a defect", async () => {
    const r = createPushRegistrar({ seam: seam(), url: URL_, fetch: server(429).fetch, store: memory() });
    await expect(r.sync(true)).rejects.toMatchObject({ environment: true, message: "push register: HTTP 429 (the worker's per-address limit; tried again at the next start)" });
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
