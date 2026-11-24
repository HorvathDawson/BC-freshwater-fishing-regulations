/**
 * (T1016) A WEB TAB OLDER THAN WHAT THE SERVER SERVES, against a FAKE ORIGIN: releases published under the release root,
 * the live pointer moved, the older ones pruned (production keeps the live release and the previous one), a site deploy
 * that replaces the code chunks. The reads go through the web's own release source (`@app/data/release/platform`, the
 * `.web` driver: the verified fetch over the app's `Net`, as the app runs it), every failure through core's
 * `reportFailure` with the triage the web installs (`replacedTriage` over `createReplacedProbe` and `siteProbe`).
 *
 *   known state   a file of a release the pointer no longer names (a slice not yet opened, the tile folder's mask and
 *                 sprites, the in-season lines' derived failure), a chunk gone after a deploy: the notice is raised and
 *                 the report is renamed (`ReleaseReplaced` / `SiteReplaced`: class `expected`, tools/wire.test.ts)
 *   defect        a file missing from the release the pointer DOES name, a chunk missing from the live deploy: reported
 *                 as it is, no notice (W46)
 *   the line      a failed fetch (no answer) is the environment's: never reported, never a notice — told apart from a 404
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { failuresSent, REFRESH_NOT_MADE, REPLACED_NAMES, REPLACED_WORDS, reportFailure, setFailureSink, setFailureTriage } from "@app/core";
import { createNet, sha256Hex, type Manifest } from "@app/data";
import { createReplacedProbe } from "@app/data/release";
import { releaseCache, releaseSource } from "@app/data/release/platform";
import { createReplacedState, loadLiveRelease, LOOK_MS, RELOADED_FROM_KEY, reloadOnce, replacedTriage, siteProbe, type ReplacedRaised,
         type ReplacedState, type SessionStore } from "./replaced";

const ORIGIN = "https://data.test";
const BASE = `${ORIGIN}/release/`;
const POINTER = `${BASE}manifest.json`;
const SITE = "https://site.test/";

const sha = sha256Hex;
const enc = (s: string) => new TextEncoder().encode(s);

/** One release: its data files under `r-<id>/`, its tile folder `tiles/<tiles>/` (atlas, mask, map assets). */
function release(id: string, tiles: string, names: readonly string[]): { m: Manifest; bodies: Map<string, Uint8Array> } {
  const bodies = new Map<string, Uint8Array>();
  const files: Record<string, unknown> = {};
  for (const n of names) {
    const b = enc(`${id}:${n}`);
    const url = `${id}/${n}-${sha(b).slice(0, 12)}.bin`;
    bodies.set(url, b);
    files[n] = { url, bytes: b.length, sha256: sha(b), format: `${n}/1` };
  }
  const mask = `tiles/${tiles}/bc_outside.geojson`;
  bodies.set(mask, enc("{}"));
  bodies.set(`tiles/${tiles}/map-assets-1/sprites/light@2x.png`, enc("png"));
  const m = { format: "app-manifest/2", data_release: id, files,
              tiles: { compatible: [`${tiles.split(".")[0]}.*`],
                       current: { id: tiles, url: `tiles/${tiles}/atlas.pmtiles`, bytes: 1, sha256: "0" },
                       assets: { id: "map-assets-1", url: `tiles/${tiles}/map-assets-1/`, bytes: 1, sha256: "0" },
                       mask: { url: mask, bytes: 2, sha256: sha(enc("{}")) } },
              basemap: null } as unknown as Manifest;
  return { m, bodies };
}

/** THE FAKE ORIGIN: what is published, what the pointer names, what was pruned, and a switch for the line. */
function fakeOrigin() {
  const bodies = new Map<string, Uint8Array>();
  let live: Manifest | null = null;
  const pruned: string[] = [];
  const asked: string[] = [];
  const o = {
    down: false,
    /** the pointer read hangs until it is aborted (a stalled line on the one request) */
    stall: false,
    asked,
    publish(r: { m: Manifest; bodies: Map<string, Uint8Array> }) { for (const [k, v] of r.bodies) bodies.set(k, v); live = r.m; },
    /** production's prune: everything under these folders answers 404 */
    prune(...prefixes: string[]) { pruned.push(...prefixes); },
    /** a file the release names that the server lost (a broken upload) */
    lose(path: string) { bodies.delete(path); },
    fetch: (async (u: RequestInfo | URL, init?: RequestInit) => {
      const url = String(u);
      asked.push(url);
      if (o.down) throw new TypeError("Failed to fetch");
      if (url === POINTER && o.stall) return new Promise<Response>((_ok, no) => {
        init?.signal?.addEventListener("abort", () => no(new DOMException("The operation was aborted.", "AbortError")));
      });
      if (url === POINTER) return new Response(JSON.stringify(live), { status: 200 });
      const path = url.slice(BASE.length);
      if (!url.startsWith(BASE) || pruned.some((p) => path.startsWith(p)) || !bodies.has(path))
        return new Response("Not Found", { status: 404 });
      return new Response(bodies.get(path)! as BodyInit, { status: 200 });
    }) as typeof fetch,
  };
  return o;
}

const A = release("r-aaaaaaaaaaaa", "tA.000000000001", ["core", "water", "regs-1", "regs-3", "lines"]);
const B = release("r-bbbbbbbbbbbb", "tA.000000000002", ["core", "water", "regs-1", "regs-3", "lines"]);
const C = release("r-cccccccccccc", "tA.000000000003", ["core", "water", "regs-1", "regs-3", "lines"]);

let reported: unknown[] = [];
let state: ReplacedState;
/** every automatic refresh the state asked for, and what the next one answers (null: made) */
let refreshes: ReplacedRaised[] = [];
let refreshWith: (r: ReplacedRaised) => Promise<string | null>;
/** the state as App.tsx makes it: the refresh, core's `failuresSent`, the release in use (the set opened last) */
const newState = () => createReplacedState({ refresh: (r) => { refreshes.push(r); return refreshWith(r); }, sent: failuresSent,
                                             inUse: () => opened[opened.length - 1]?.data_release ?? null });
let undo: Array<() => void> = [];
/** every set this page opened, the one in use last (App.tsx `openedSets`) */
let opened: Manifest[] = [];
let origin: ReturnType<typeof fakeOrigin>;

/** the web's triage as `apps/mobile` replaced.web.ts installs it, over the fake origin */
function install(site?: Parameters<typeof replacedTriage>[0]["site"], data?: Parameters<typeof replacedTriage>[0]["data"]) {
  undo.push(setFailureTriage(replacedTriage({
    state, site: site ?? null,
    data: data ?? createReplacedProbe({ manifestUrl: POINTER, releaseBase: BASE, opened: () => opened, fetch: origin.fetch }) })));
}
/** a release opened as the web opens it: its files fetched verified over the app's Net, from the cache or the origin */
async function boot(m: Manifest) {
  opened.push(m);
  return releaseSource(m, { base: BASE, cache: await releaseCache(`t1016-${m.data_release}-${Math.random()}`), held: null,
                            fetch: origin.fetch, net: createNet() });
}
/** every report core sends, once the triage's look has settled */
const settle = () => new Promise((r) => setTimeout(r, 0));
const failureOf = async (p: Promise<unknown>) => { try { await p; } catch (e) { return e; } throw new Error("the read did not fail"); };
const names = () => reported.map((e) => (e as Error).name);

beforeEach(() => {
  reported = [];
  refreshes = [];
  refreshWith = async () => null;
  state = newState();
  origin = fakeOrigin();
  undo = [setFailureSink((e) => reported.push(e))];
});
afterEach(() => { for (const u of undo.reverse()) u(); undo = []; opened = []; vi.useRealTimers(); });

describe("a tab booted on release A, then B and C published and A pruned (T1016 item 1)", () => {
  it("a slice not yet opened: the 404 is the known state — the notice is raised, the report is ReleaseReplaced", async () => {
    install();
    origin.publish(A);
    const src = await boot(A.m);
    expect(new TextDecoder().decode(await src.bytes("core"))).toBe("r-aaaaaaaaaaaa:core");   // opened while A was live
    origin.publish(B);
    origin.publish(C);
    origin.prune("r-aaaaaaaaaaaa/", "tiles/tA.000000000001/");
    const e = await failureOf(src.bytes("regs-3"));
    expect(String((e as Error).message)).toMatch(/r-aaaaaaaaaaaa\/regs-3-[0-9a-f]{12}\.bin: HTTP 404$/);
    reportFailure(e);
    expect(reported).toEqual([]);                    // nothing sent before the look has asked the pointer
    await settle();
    expect(names()).toEqual([REPLACED_NAMES.data]);
    expect((reported[0] as Error).message).toBe(
      `a file of release r-aaaaaaaaaaaa is no longer served (HTTP 404); the server's release is r-cccccccccccc: ${(e as Error).message}`);
    expect((reported[0] as Error).cause).toBe(e);
    // (owner 2026-11-24 "Always auto refresh") refreshed by itself, no notice
    expect([refreshes, state.get()]).toEqual([[{ kind: "data", release: "r-aaaaaaaaaaaa" }], null]);
    expect(origin.asked.filter((u) => u === POINTER)).toHaveLength(1);
  });

  it("the tile folder's mask and sprites (the map's own reads: a status and a url), and the in-season lines' derived failure", async () => {
    install();
    origin.publish(A);
    await boot(A.m);
    origin.publish(C);
    origin.prune("r-aaaaaaaaaaaa/", "tiles/tA.000000000001/");
    // the outside mask (map/web/outsideMask.ts) and MapLibre's AJAXError for a sprite sheet: both carry status + url
    reportFailure(Object.assign(new Error("outside mask: HTTP 404"), { status: 404, url: `${BASE}tiles/tA.000000000001/bc_outside.geojson` }));
    const ajax = Object.assign(new Error("Not Found"), { name: "AJAXError", status: 404,
                                                         url: `${BASE}tiles/tA.000000000001/map-assets-1/sprites/light@2x.png` });
    reportFailure(ajax);
    // the in-season hook's "no lines" failure carries its read's 404 (ui inseason.ts)
    const lines404 = new Error(`${BASE}${(A.m.files as Record<string, { url: string }>).lines!.url}: HTTP 404`);
    reportFailure(new Error("in-season: this release's section lines could not be read", { cause: lines404 }));
    await settle();
    expect(names()).toEqual([REPLACED_NAMES.data, REPLACED_NAMES.data, REPLACED_NAMES.data]);
  });

  it("the notice's one action is the app's in-session apply: the update service stages the live release, then apply", async () => {
    const svc = fakeUpdates("current");
    let applied = 0;
    loadLiveRelease(svc, () => { applied++; });
    expect(svc.checks).toEqual([{ manual: true, feeds: false }]);
    expect(applied).toBe(0);
    svc.set("downloading");
    svc.set("ready");
    await settle();
    expect(applied).toBe(1);                          // once: the check's end sees "ready" too
    expect(svc.listeners()).toBe(0);
    // staged already: applied at once, nothing asked
    const ready = fakeUpdates("ready");
    loadLiveRelease(ready, () => { applied++; });
    expect([applied, ready.checks.length]).toEqual([2, 0]);
    // a check that stages nothing (the line down: the sync icon says it): the wait ends, nothing applied
    const none = fakeUpdates("current");
    loadLiveRelease(none, () => { applied++; });
    await settle();
    expect([applied, none.listeners()]).toEqual([2, 0]);
  });
});

describe("(T1016, owner 2026-11-24 \"Always auto refresh\") a replaced set is refreshed BY ITSELF, once", () => {
  const dec = (b: Uint8Array) => new TextDecoder().decode(b);
  /** the reports in the order the sink took them, and the app's apply, on one line */
  let order: string[] = [];
  const watch = () => { order = []; undo.push(setFailureSink((e) => { reported.push(e); order.push(`report:${(e as Error).name}`); })); };
  const staging = fakeUpdates;

  it("its report is in the sink first, then the app's one apply, once; the read that failed is read again on the new release", async () => {
    install();
    watch();
    origin.publish(A);
    const srcA = await boot(A.m);
    origin.publish(B);
    origin.publish(C);
    origin.prune("r-aaaaaaaaaaaa/", "tiles/tA.000000000001/");
    const svc = staging("ready");                     // the update service staged C (its own check)
    let again: Promise<string> | null = null;
    const apply = () => { order.push("apply"); again = (async () => dec(await (await boot(C.m)).bytes("regs-3")))(); };
    refreshWith = () => new Promise((ended) => loadLiveRelease(svc, apply, ended));
    // two reads of A fail at once (the slice, the water file): one verdict, one refresh
    const e1 = await failureOf(srcA.bytes("regs-3"));
    const e2 = await failureOf(srcA.bytes("water"));
    reportFailure(e1);
    reportFailure(e2);
    await settle(); await settle();
    expect(order).toEqual([`report:${REPLACED_NAMES.data}`, `report:${REPLACED_NAMES.data}`, "apply"]);
    expect(await again).toBe("r-cccccccccccc:regs-3");          // the read that failed, made again on C
    expect([refreshes, state.get()]).toEqual([[{ kind: "data", release: "r-aaaaaaaaaaaa" }], null]);
    // a late 404 of a read made on A, after the apply: reported as replaced, never refreshed again
    reportFailure(await failureOf(srcA.bytes("lines")));
    await settle(); await settle();
    expect(order).toEqual([`report:${REPLACED_NAMES.data}`, `report:${REPLACED_NAMES.data}`, "apply", `report:${REPLACED_NAMES.data}`]);
    expect(refreshes).toHaveLength(1);
  });

  it("no loop: the same verdict again after its refresh (the apply went back to A) is never refreshed again", async () => {
    install();
    watch();
    origin.publish(A);
    const srcA = await boot(A.m);
    origin.publish(C);
    origin.prune("r-aaaaaaaaaaaa/");
    let applies = 0;
    // the apply starts, and the boot goes back to A (T555: said and reported by the apply itself): the page is on A still
    refreshWith = () => new Promise((ended) => loadLiveRelease(staging("ready"), () => { applies++; }, ended));
    reportFailure(await failureOf(srcA.bytes("regs-3")));
    await settle(); await settle();
    reportFailure(await failureOf(srcA.bytes("regs-1")));
    await settle(); await settle();
    expect([applies, refreshes.length, state.get(), names()]).toEqual([1, 1, null, [REPLACED_NAMES.data, REPLACED_NAMES.data]]);
  });

  it("a refresh that could not be made (nothing staged): the notice shows the failure, reported as a defect once; never again", async () => {
    install();
    watch();
    origin.publish(A);
    const srcA = await boot(A.m);
    origin.publish(C);
    origin.prune("r-aaaaaaaaaaaa/");
    const none = staging("current");                  // its check stages nothing
    let applies = 0;
    refreshWith = () => new Promise((ended) => loadLiveRelease(none, () => { applies++; }, ended));
    reportFailure(await failureOf(srcA.bytes("regs-3")));
    await settle(); await settle();
    expect([applies, state.get(), names()]).toEqual([0, { kind: "data", release: "r-aaaaaaaaaaaa" }, [REPLACED_NAMES.data, REFRESH_NOT_MADE]]);
    expect((reported[1] as Error).message).toBe(
      "the automatic refresh of release r-aaaaaaaaaaaa's replaced data was not made: the update service staged no newer release (its release phase: current)");
    reportFailure(await failureOf(srcA.bytes("regs-1")));
    await settle(); await settle();
    expect([none.checks.length, refreshes.length, names()]).toEqual([1, 1, [REPLACED_NAMES.data, REFRESH_NOT_MADE, REPLACED_NAMES.data]]);
    // the notice's words (core, whole): the failure said, its action the same apply by hand
    expect(REPLACED_WORDS.data).toEqual({ line: "This page’s data was replaced by a newer release.", action: "Load the new data ›", a11y: "Load the new data" });
  });
});

describe("a 404 that is a defect stays a defect (T1016 item 2, W46)", () => {
  it("a file missing from the release the pointer DOES name: reported as it is, no notice", async () => {
    install();
    origin.publish(A);
    origin.publish(C);
    origin.prune("r-aaaaaaaaaaaa/");
    const lost = (C.m.files as Record<string, { url: string }>)["regs-3"]!.url;
    origin.lose(lost);
    const src = await boot(C.m);
    const e = await failureOf(src.bytes("regs-3"));
    reportFailure(e);
    await settle();
    expect(reported).toEqual([e]);
    expect([refreshes, state.get()]).toEqual([[], null]);
  });

  it("the page's own release still live, its file gone: a defect; a 404 off the release root: reported at once, nothing asked", async () => {
    install();
    origin.publish(A);
    const src = await boot(A.m);
    origin.lose((A.m.files as Record<string, { url: string }>).water!.url);
    const e = await failureOf(src.bytes("water"));
    reportFailure(e);
    const feed = new Error(`${ORIGIN}/feeds/live/cards/08GA043.json: HTTP 404`);
    reportFailure(feed);
    expect(reported).toEqual([feed]);              // not a release file: no look, sent at once
    await settle();
    expect(reported).toEqual([feed, e]);
    expect([refreshes, state.get()]).toEqual([[], null]);
  });

  it("a 404 the pointer cannot explain (the pointer does not answer): the 404 is reported as it is", async () => {
    install();
    origin.publish(A);
    await boot(A.m);
    origin.publish(C);
    origin.prune("r-aaaaaaaaaaaa/");
    const e = new Error(`${BASE}${(A.m.files as Record<string, { url: string }>)["regs-1"]!.url}: HTTP 404`);
    origin.down = true;
    reportFailure(e);
    await settle();
    expect(reported).toEqual([e]);
    expect([refreshes, state.get()]).toEqual([[], null]);
  });
});

describe("(T1016 review) no look holds a report for good", () => {
  const regs1 = () => new Error(`${BASE}${(A.m.files as Record<string, { url: string }>)["regs-1"]!.url}: HTTP 404`);
  it("a pointer read that stalls is aborted after the wait: the 404 reported as it is, and the next 404 reads afresh", async () => {
    install();
    origin.publish(A);
    await boot(A.m);
    origin.publish(C);
    origin.prune("r-aaaaaaaaaaaa/");
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    origin.stall = true;
    const e = regs1();
    reportFailure(e);
    await vi.advanceTimersByTimeAsync(LOOK_MS - 1);
    expect(reported).toEqual([]);                    // held while the look may still answer
    await vi.advanceTimersByTimeAsync(1);
    expect(reported).toEqual([e]);                   // as it is: the pointer did not answer
    // the stalled read was aborted, not left to hold every later 404: the next one reads the pointer again, and answers
    origin.stall = false;
    reportFailure(regs1());
    await vi.advanceTimersByTimeAsync(0);
    expect(names()).toEqual(["Error", REPLACED_NAMES.data]);
    expect(origin.asked.filter((u) => u === POINTER)).toHaveLength(2);
  });

  it("a probe that never settles (whatever it does) cannot hold the report past LOOK_MS", async () => {
    const never = Object.assign(() => new Promise<never>(() => {}), { covers: () => true });
    install(undefined, never);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const e = regs1();
    reportFailure(e);
    await vi.advanceTimersByTimeAsync(LOOK_MS);
    expect(reported).toEqual([e]);
  });

  it("the site probe's asks are aborted after the wait too (the line's: AbortError)", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const hang = ((_u: RequestInfo | URL, i?: RequestInit) => new Promise<Response>((_ok, no) => {
      i?.signal?.addEventListener("abort", () => no(new DOMException("The operation was aborted.", "AbortError"))); })) as typeof fetch;
    const asked = siteProbe(hang, "/_expo/static/js/web/index-1.js", SITE)(`${SITE}_expo/static/js/web/later-2.js`).catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(LOOK_MS);
    expect((await asked as Error).name).toBe("AbortError");
  });

  it("a late 404 of a read made on A, landing after the apply to C, is judged by A: renamed, no notice on C", async () => {
    install();
    origin.publish(A);
    const srcA = await boot(A.m);
    origin.publish(B);
    origin.publish(C);
    await boot(C.m);                                 // the apply: C is in use now
    origin.prune("r-aaaaaaaaaaaa/");
    const e = await failureOf(srcA.bytes("regs-3"));
    reportFailure(e);
    await settle();
    expect(names()).toEqual([REPLACED_NAMES.data]);
    expect([refreshes, state.get()]).toEqual([[], null]);   // the page is on C: nothing to refresh, no notice
  });
});

describe("a 404 told apart from a failed line", () => {
  it("no answer at all (offline, a dropped line) is the environment's: never reported, no notice, the pointer never asked", async () => {
    install();
    origin.publish(A);
    const src = await boot(A.m);
    origin.publish(C);
    origin.prune("r-aaaaaaaaaaaa/");
    origin.down = true;
    const e = await failureOf(src.bytes("regs-3"));
    origin.down = false;
    reportFailure(e);
    await settle();
    expect(reported).toEqual([]);
    expect([refreshes, state.get()]).toEqual([[], null]);
    expect(origin.asked.filter((u) => u === POINTER)).toEqual([]);
  });
});

describe("a chunk that is gone after a site deploy (AsyncRequireError)", () => {
  const ENTRY = "/_expo/static/js/web/index-1111aaaa2222.js";
  const CHUNK = `${SITE}_expo/static/js/web/later-3333bbbb4444.js`;
  /** Expo's own error for a script that would not load (expo/src/async-require/fetchThenEval.web.ts) */
  const chunkError = () => Object.assign(new Error(`Loading module ${CHUNK} failed.\n(error: ${CHUNK})`),
                                         { name: "AsyncRequireError", type: "error", request: CHUNK });
  const site = (o: { chunk: number | "down"; page: string }) => siteProbe((async (u: RequestInfo | URL) => {
    if (String(u) === CHUNK) { if (o.chunk === "down") throw new TypeError("Failed to fetch"); return new Response("", { status: o.chunk }); }
    if (String(u) === SITE) return new Response(o.page, { status: 200 });
    return new Response("", { status: 404 });
  }) as typeof fetch, ENTRY, SITE);

  it("the site's page names another entry now: the notice (reload) is raised, the report is SiteReplaced", async () => {
    install(site({ chunk: 404, page: `<script src="/_expo/static/js/web/index-5555cccc6666.js" defer></script>` }));
    const e = chunkError();
    reportFailure(e);
    await settle();
    expect(names()).toEqual([REPLACED_NAMES.code]);
    expect((reported[0] as Error).cause).toBe(e);
    expect([refreshes, state.get()]).toEqual([[{ kind: "code", release: null }], null]);
  });

  it("(owner 2026-11-24 \"Always auto refresh\") the reports are in the sink, then the page reloads ONCE; back on the same code, it stops", async () => {
    const store = memoryStore();
    const order: string[] = [];
    undo.push(setFailureSink((e) => { reported.push(e); order.push(`report:${(e as Error).name}`); }));
    refreshWith = async () => reloadOnce({ store: () => store, entry: ENTRY, reload: () => order.push("reload") });
    const page = `<script src="/_expo/static/js/web/index-5555cccc6666.js" defer></script>`;
    install(site({ chunk: 404, page }));
    // two chunks gone at once: both reports first, one reload
    reportFailure(chunkError());
    reportFailure(chunkError());
    await settle(); await settle();
    expect(order).toEqual([`report:${REPLACED_NAMES.code}`, `report:${REPLACED_NAMES.code}`, "reload"]);
    expect([store.getItem(RELOADED_FROM_KEY), state.get()]).toEqual([ENTRY, null]);
    // THE NEXT PAGE LIFE (the reload came back on the same code: a cached page): its own state, the same session store
    for (const u of undo.splice(1).reverse()) u();
    undo.push(setFailureSink((e) => { reported.push(e); order.push(`report:${(e as Error).name}`); }));
    state = newState();
    install(site({ chunk: 404, page }));
    reportFailure(chunkError());
    await settle(); await settle();
    expect(order.slice(3)).toEqual([`report:${REPLACED_NAMES.code}`, `report:${REFRESH_NOT_MADE}`]);   // no second reload
    expect((reported[reported.length - 1] as Error).message).toBe(
      `the automatic refresh of the replaced site code was not made: this page reloaded once already from this code (${ENTRY}) and the chunk is still missing`);
    expect(state.get()).toEqual({ kind: "code", release: null });   // the notice: "Reload ›" by hand
  });

  it("reloadOnce: once per page code; a store that throws or does not keep never reloads (it could loop)", () => {
    let reloads = 0;
    const reload = () => { reloads++; };
    const store = memoryStore();
    expect(reloadOnce({ store: () => store, entry: ENTRY, reload })).toBeNull();
    expect(reloadOnce({ store: () => store, entry: ENTRY, reload })).toBe(
      `this page reloaded once already from this code (${ENTRY}) and the chunk is still missing`);
    // a page on new code may reload once for a later deploy
    expect(reloadOnce({ store: () => store, entry: "/_expo/static/js/web/index-2.js", reload })).toBeNull();
    expect(reloads).toBe(2);
    const blocked = () => { throw new DOMException("The operation is insecure.", "SecurityError"); };
    expect(reloadOnce({ store: blocked, entry: ENTRY, reload })).toBe(
      "the marker that stops a reload loop could not be kept (The operation is insecure.)");
    expect(reloadOnce({ store: () => ({ getItem: () => null, setItem: () => {} }), entry: ENTRY, reload }))
      .toBe("the marker that stops a reload loop did not keep");
    expect(reloadOnce({ store: () => store, entry: null, reload }))
      .toBe("this page's code is not known (no entry script), so a reload could not be told from a loop");
    expect(reloads).toBe(2);
  });

  it("the site's page still names this entry: the chunk is missing from the live deploy — a defect, as it is", async () => {
    install(site({ chunk: 404, page: `<script src="${ENTRY}" defer></script>` }));
    const e = chunkError();
    reportFailure(e);
    await settle();
    expect(reported).toEqual([e]);
    expect([refreshes, state.get()]).toEqual([[], null]);
  });

  it("the chunk loads now (a passing fault): reported as it is; no answer: reported as the line's, the part's error untouched", async () => {
    install(site({ chunk: 200, page: "" }));
    const e = chunkError();
    reportFailure(e);
    await settle();
    expect(reported).toEqual([e]);
    for (const u of undo.splice(1)) u();
    install(site({ chunk: "down", page: "" }));
    const down = chunkError();
    const before = { ...down, message: down.message, name: down.name };
    reportFailure(down);
    await settle();
    expect(reported).toHaveLength(2);
    const sent = reported[1] as Error;
    expect([sent.name, sent.message, sent.cause]).toEqual(["AsyncRequireError",
      `no answer when asked again (Failed to fetch): ${down.message}`, down]);
    expect({ ...down, message: down.message, name: down.name }).toEqual(before);   // derived, never written
    expect((down as { environment?: unknown }).environment).toBeUndefined();
    expect([refreshes, state.get()]).toEqual([[], null]);
  });
});

/** the browser's session store, in memory (it survives a page life as `sessionStorage` survives a reload) */
function memoryStore(): SessionStore {
  const m = new Map<string, string>();
  return { getItem: (k) => m.get(k) ?? null, setItem: (k, v) => { m.set(k, v); } };
}

/** the update service as `loadLiveRelease` reads it: its release phase, its listeners, the checks asked */
function fakeUpdates(start: "current" | "downloading" | "ready") {
  let phase: string = start;
  const subs = new Set<() => void>();
  const checks: unknown[] = [];
  return {
    checks,
    listeners: () => subs.size,
    set(p: string) { phase = p; for (const s of [...subs]) s(); },
    snapshot: () => ({ release: { phase } }) as never,
    subscribe(on: () => void) { subs.add(on); return () => { subs.delete(on); }; },
    check: async (o?: unknown) => { checks.push(o); },
  };
}
