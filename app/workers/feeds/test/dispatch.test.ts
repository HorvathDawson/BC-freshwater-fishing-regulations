// T413: a new HYDAT starts the HYDAT workflow once (with the owner's token); without it, today's button email.
import { describe, expect, it } from "vitest";
import { DEFAULT_REF, dispatchOnce, dispatchSteps, MAX_STARTS, OVERDUE_MS, RETRY_MS, unfinished } from "../src/dispatch";
import { hydatSection } from "../src/email";
import { gaugeEvents, hydatUnfinished } from "../src/index";

const mem = () => {
  const m = new Map<string, string>();
  return { m, get: async (k: string) => m.get(k) ?? null, put: async (k: string, b: string) => void m.set(k, b) };
};
const gh = (status: number) => {
  const calls: { url: string; init?: RequestInit }[] = [];
  const f = async (url: string, init?: RequestInit) => {
    calls.push({ url, init });
    return new Response(status === 204 ? null : "nope", { status });
  };
  return { calls, f };
};
const T0 = new Date("2026-11-05T12:00:00Z");

describe("dispatchOnce", () => {
  it("(T995) starts the workflow on the trunk, main", () => {
    expect(DEFAULT_REF).toBe("main");
  });

  it("does nothing without the token", async () => {
    const g = gh(204);
    expect(await dispatchOnce({}, g.f, mem(), "feeds-hydat.yml", "20261019", { upload: "true" }, T0)).toEqual({ state: "no-token" });
    expect(g.calls).toHaveLength(0);
  });

  it("starts one run per version", async () => {
    const g = gh(204), s = mem(), env = { GITHUB_DISPATCH_TOKEN: "t" };
    const a = await dispatchOnce(env, g.f, s, "feeds-hydat.yml", "20261019", { upload: "true" }, T0);
    expect(a.state).toBe("started");
    expect(g.calls[0]!.url).toContain("/actions/workflows/feeds-hydat.yml/dispatches");
    expect(JSON.parse(String(g.calls[0]!.init!.body))).toEqual({ ref: DEFAULT_REF, inputs: { upload: "true" } });
    await dispatchOnce(env, g.f, s, "feeds-hydat.yml", "20261019", { upload: "true" }, new Date(T0.getTime() + 60_000));
    expect(g.calls).toHaveLength(1);
    await dispatchOnce(env, g.f, s, "feeds-hydat.yml", "20270115", { upload: "true" }, T0);
    expect(g.calls).toHaveLength(2);
  });

  it("(T404) the production worker asks for the production bucket; staging's runs keep the workflow's default", async () => {
    const g = gh(204);
    await dispatchOnce({ GITHUB_DISPATCH_TOKEN: "t", FEEDS_ENV: "production" }, g.f, mem(), "feeds-hydat.yml", "20261019",
                       { upload: "true" }, T0);
    await dispatchOnce({ GITHUB_DISPATCH_TOKEN: "t", FEEDS_ENV: "staging" }, g.f, mem(), "feeds-hydat.yml", "20261019",
                       { upload: "true" }, T0);
    expect(g.calls.map((c) => JSON.parse(String(c.init!.body)).inputs))
      .toEqual([{ upload: "true", target: "production" }, { upload: "true" }]);
  });

  it("reports a failed start and retries it only after RETRY_MS", async () => {
    const g = gh(403), s = mem(), env = { GITHUB_DISPATCH_TOKEN: "t" };
    const a = await dispatchOnce(env, g.f, s, "feeds-hydat.yml", "20261019", {}, T0);
    expect(a).toMatchObject({ state: "failed", error: expect.stringContaining("403") });
    await dispatchOnce(env, g.f, s, "feeds-hydat.yml", "20261019", {}, new Date(T0.getTime() + 60_000));
    expect(g.calls).toHaveLength(1);
    await dispatchOnce(env, g.f, s, "feeds-hydat.yml", "20261019", {}, new Date(T0.getTime() + RETRY_MS + 1));
    expect(g.calls).toHaveLength(2);
  });
});

// T554 item 5: GitHub's 204 is "accepted", not "the run succeeded". The record said `started` for that HYDAT forever, so a
// run that failed was never started again and the owner (told once, key hydat:<latest>:<current>) never heard more.
describe("(T554 item 5) a started run that did not do its work", () => {
  const env = { GITHUB_DISPATCH_TOKEN: "t" };
  const H = 3600_000;
  const at = (h: number) => new Date(T0.getTime() + h * H);
  const go = (g: ReturnType<typeof gh>, s: ReturnType<typeof mem>, h: number) =>
    dispatchOnce(env, g.f, s, "feeds-hydat.yml", "20261019", { upload: "true" }, at(h), { redoAfterMs: OVERDUE_MS });

  it("still asked for after OVERDUE_MS: started ONE more time, then never again (overdue)", async () => {
    const g = gh(204), s = mem();
    expect(await go(g, s, 0)).toEqual({ state: "started", at: at(0).toISOString(), workflow: "feeds-hydat.yml", ref: DEFAULT_REF });
    expect(unfinished(await go(g, s, 5.9))).toBe(false);                 // inside the window: the run may still be going
    expect(g.calls).toHaveLength(1);
    const again = await go(g, s, 6);
    expect(again).toEqual({ state: "started", at: at(6).toISOString(), workflow: "feeds-hydat.yml", ref: DEFAULT_REF, starts: 2, first: at(0).toISOString() });
    expect(g.calls).toHaveLength(2);
    expect(unfinished(again)).toBe(true);
    expect(await go(g, s, 8)).toEqual({ ...again, version: "20261019" });   // the second run's own window: the record
    const stuck = await go(g, s, 12);
    expect(stuck).toEqual({ ...again, version: "20261019", overdue: true });
    for (const h of [13, 30, 100]) expect(await go(g, s, h)).toMatchObject({ overdue: true, starts: MAX_STARTS });
    expect(g.calls).toHaveLength(2);                                     // never a third start
    // a newer version is a new matter
    expect(await dispatchOnce(env, g.f, s, "feeds-hydat.yml", "20270115", {}, at(101), { redoAfterMs: OVERDUE_MS })).toMatchObject({ state: "started" });
    expect(g.calls).toHaveLength(3);
  });

  it("a caller that does not ask for it (run timing) keeps one start per version", async () => {
    const g = gh(204), s = mem();
    await dispatchOnce(env, g.f, s, "feeds-runtiming.yml", "20270101", {}, at(0));
    expect(await dispatchOnce(env, g.f, s, "feeds-runtiming.yml", "20270101", {}, at(500))).toMatchObject({ state: "started", at: at(0).toISOString() });
    expect(g.calls).toHaveLength(1);
  });

  it("the second start refused: said (unfinished), retried after RETRY_MS, and it still counts as the second", async () => {
    const s = mem();
    await go(gh(204), s, 0);
    const no = gh(403);
    const failed = await go(no, s, 6);
    expect(failed).toMatchObject({ state: "failed", starts: 1, first: at(0).toISOString(), error: expect.stringContaining("403") });
    expect(unfinished(failed)).toBe(true);
    await go(no, s, 7);
    expect(no.calls).toHaveLength(1);                                    // inside RETRY_MS
    const ok = gh(204);
    expect(await go(ok, s, 12.1)).toMatchObject({ state: "started", starts: 2, first: at(0).toISOString() });
    expect(await go(ok, s, 19)).toMatchObject({ overdue: true });
    expect(ok.calls).toHaveLength(1);
  });

  it("the owner is told again, once a day, with what happened (the key carries the day)", async () => {
    const hy = (iso: string) => gaugeEvents([{ kind: "hydat", since: "2026-11-05T11:00:00Z", current: "20260717", latest: "20261019", action: "x" }], iso)[0]!;
    const first = hy("2026-11-05T12:00:00.000Z");
    hydatUnfinished(first, { state: "started", at: "2026-11-05T12:00:00.000Z", workflow: "feeds-hydat.yml", ref: "r" }, first.at);
    expect(first.key).toBe("hydat:20261019:20260717");                   // the first start: the one notice, as before
    const redo = hy("2026-11-05T18:05:00.000Z");
    hydatUnfinished(redo, { state: "started", at: "2026-11-05T18:05:00.000Z", workflow: "feeds-hydat.yml", ref: "r", starts: 2, first: "2026-11-05T12:00:00.000Z" }, redo.at);
    expect(redo.key).toBe("hydat:20261019:20260717:unfinished:2026-11-05");
    expect(redo.title).toBe("HYDAT 20261019: the first workflow run did not update clim.json — started again");
    const stuck = hy("2026-11-07T09:05:00.000Z");
    const dis = { state: "started" as const, at: "2026-11-05T18:05:00.000Z", workflow: "feeds-hydat.yml", ref: "r", starts: 2, first: "2026-11-05T12:00:00.000Z", overdue: true };
    stuck.data = { ...(stuck.data as object), dispatch: dis };
    hydatUnfinished(stuck, dis, stuck.at);
    expect(stuck.key).toBe("hydat:20261019:20260717:unfinished:2026-11-07");
    expect(stuck.text).toBe(
      "HYDAT 20261019: the workflow was started 2 times and clim.json is still on 20260717.\n\n" +
      "HYDAT 20261019 is out; the gauge envelope (clim.json) is still on 20260717. The worker started the HYDAT workflow at " +
      "2026-11-05T12:00:00.000Z and again at 2026-11-05T18:05:00.000Z; it does not start it a third time. Look at the failed " +
      "runs in GitHub → Actions, then start it by hand.\n\nNoticed 2026-11-05T11:00:00Z.");
    const sec = hydatSection([stuck]);
    expect(sec.subject).toBe("HYDAT 20261019 — the workflow did not finish (clim.json still on 20260717)");
    expect(sec.steps.map((x) => x.text)).toEqual([
      'The worker started "feeds-hydat.yml" on r 2 times (first 2026-11-05T12:00:00.000Z, last 2026-11-05T18:05:00.000Z) and its work is still not done. It does not start it again: look at the failed runs in GitHub → Actions, fix the cause, then start it by hand:',
      'GitHub → Actions → "feeds — HYDAT → climatology → R2", with upload: true (the feeds worker\'s clim.json).',
      "Then, on the Mac, the bundle (scripts/update-hydat.sh prints the release command at the end):",
    ]);
  });
});

describe("the HYDAT email", () => {
  const ev = (dispatch?: unknown) => [{ key: "hydat:x", kind: "hydat" as const, title: "", text: "", at: "", data: { latest: "20261019", current: "20260717", dispatch } }];
  it("keeps the button without a token", () => {
    expect(hydatSection(ev({ state: "no-token" })).steps[0]!.text).toContain("GitHub → Actions");
  });
  it("says the worker started it", () => {
    expect(hydatSection(ev({ state: "started", at: "now", workflow: "feeds-hydat.yml", ref: "r" })).steps[0]!.text).toContain("Nothing to press");
  });
  it("says why a start failed, then the button", () => {
    const st = hydatSection(ev({ state: "failed", at: "now", workflow: "feeds-hydat.yml", ref: "r", error: "GitHub answered 403" })).steps;
    expect(st[0]!.text).toContain("403");
    expect(st[1]!.text).toContain("GitHub → Actions");
  });
  it("matches the old steps when nothing was dispatched", () => {
    expect(dispatchSteps(undefined, "B")).toEqual([{ text: "B" }]);
  });
});

describe("(T412) the weekly run-timing source check", async () => {
  const { checkRunTiming, nusedsLatest } = await import("../src/runtiming");
  const record = (names: string[]) => async (url: string) =>
    url.includes("open.canada.ca") ? Response.json({ result: { resources: names.map((name) => ({ name })) } }) : new Response(null, { status: 204 });
  const bucket = (updated: string | null) => ({
    get: async () => updated === null ? null : { text: async () => JSON.stringify({ sources: { dfo: { updated } } }) },
  });
  it("reads the newest All Areas date", async () => {
    expect(await nusedsLatest(record(["All Areas NuSEDS_20250101.zip", "All Areas NuSEDS_20260601.zip", "Other_20990101.zip"]))).toBe("20260601");
    await expect(nusedsLatest(record(["something else"]))).rejects.toThrow("lists no");
  });
  it("nothing newer: no event, no dispatch", async () => {
    expect(await checkRunTiming({ GITHUB_DISPATCH_TOKEN: "t" }, record(["All Areas NuSEDS_20260601.zip"]), bucket("20260601"), mem(), T0)).toBeNull();
  });
  it("newer: starts the workflow once and says so", async () => {
    const s = mem();
    const e = await checkRunTiming({ GITHUB_DISPATCH_TOKEN: "t" }, record(["All Areas NuSEDS_20270101.zip"]), bucket("20260601"), s, T0);
    expect(e).toMatchObject({ key: "runtiming:20270101", kind: "runtiming", data: { latest: "20270101", current: "20260601", dispatch: { state: "started" } } });
    expect(JSON.parse(s.m.get("feeds-runtiming.yml.json")!)).toMatchObject({ version: "20270101", state: "started" });
  });
  it("without a token: the button", async () => {
    const e = await checkRunTiming({}, record(["All Areas NuSEDS_20270101.zip"]), bucket("20260601"), mem(), T0);
    expect(e!.text).toContain("GitHub → Actions");
    expect(e!.text).toContain("with upload: true and refresh: true.");
  });
  it("(T552 item 2) the run refreshes its sources and names the NuSEDS it must reach", async () => {
    const bodies: unknown[] = [];
    const f = async (url: string, init?: RequestInit) => {
      if (init?.method === "POST") bodies.push(JSON.parse(String(init.body)));
      return record(["All Areas NuSEDS_20270101.zip"])(url);
    };
    await checkRunTiming({ GITHUB_DISPATCH_TOKEN: "t" }, f, bucket("20260601"), mem(), T0);
    expect(bodies).toEqual([{ ref: DEFAULT_REF, inputs: { upload: "true", refresh: "true", expect_dfo: "20270101" } }]);
  });
});

describe("(T413) the token check", async () => {
  const { checkToken } = await import("../src/dispatch");
  it("reads the workflow with the token and starts nothing", async () => {
    const calls: string[] = [];
    const r = await checkToken({ GITHUB_DISPATCH_TOKEN: "t" }, async (u, i) => {
      calls.push(`${i?.method ?? "GET"} ${u}`);
      return Response.json({ name: "feeds — HYDAT → climatology → R2", state: "active" });
    });
    expect(r).toEqual({ ok: true, status: 200, says: "feeds — HYDAT → climatology → R2 (active)" });
    expect(calls).toEqual(["GET https://api.github.com/repos/canifishthis/canifishthis/actions/workflows/feeds-hydat.yml"]);
  });
  it("says why not", async () => {
    expect(await checkToken({}, async () => new Response())).toMatchObject({ ok: false, says: "GITHUB_DISPATCH_TOKEN is not set" });
    expect(await checkToken({ GITHUB_DISPATCH_TOKEN: "bad" }, async () => Response.json({ message: "Bad credentials" }, { status: 401 })))
      .toEqual({ ok: false, status: 401, says: "Bad credentials" });
  });
  // T994 #30: a network error threw out of the route, a bare 500 instead of the {ok: false} its siblings answer
  it("a network error is an answer that says so, never a throw", async () => {
    expect(await checkToken({ GITHUB_DISPATCH_TOKEN: "t" }, async () => { throw new TypeError("fetch failed"); }))
      .toEqual({ ok: false, status: null, says: "GitHub not reached: fetch failed" });
  });
});

// T994 #30: the JSON answer and the capped body read were copied into each public route; one copy now (io.ts)
describe("the public routes share one body reader and one JSON answer (T994 #30)", () => {
  it("reports.ts, crashes.ts and push.ts build no Response of their own and read no body themselves", async () => {
    const { readFileSync } = await import("node:fs");
    for (const f of ["reports", "crashes", "push"]) {
      const src = readFileSync(new URL(`../src/${f}.ts`, import.meta.url), "utf-8");
      expect(src, f).not.toMatch(/new Response\(JSON\.stringify/);
      expect(src, f).not.toMatch(/readCapped\(|JSON\.parse\(raw\)/);
      expect(src, f).toMatch(/readJsonBody\(req, /);
    }
  });
  it("readJsonBody: a declared or a real body over the cap is 413, junk is 400, JSON is the value", async () => {
    const { readJsonBody } = await import("../src/io");
    const post = (body: string, h: Record<string, string> = {}) => new Request("https://w/x", { method: "POST", body, headers: h });
    expect(await readJsonBody(post("{}", { "Content-Length": "99" }), 10, "too large")).toEqual({ ok: false, error: "too large", status: 413 });
    expect(await readJsonBody(post('{"a":"' + "x".repeat(20) + '"}'), 10, "too large")).toEqual({ ok: false, error: "too large", status: 413 });
    expect(await readJsonBody(post("{nope"), 100, "too large")).toEqual({ ok: false, error: "not JSON", status: 400 });
    expect(await readJsonBody(post('{"a":1}'), 100, "too large")).toEqual({ ok: true, body: { a: 1 } });
  });
});

describe("(T485 step 3) the monthly run-timing sources refresh, worker-started", async () => {
  const { checkSourceRefresh, REFRESH_DAYS } = await import("../src/runtiming");
  const bucket = (fetched: string | null) => ({ get: async () => fetched === null ? null : { text: async () => fetched + "\n" } });
  const now = new Date("2026-12-07T10:40:00Z");
  const posts = () => { const bodies: any[] = []; return { bodies, f: async (_u: string, init?: RequestInit) => { bodies.push(JSON.parse(String(init!.body))); return new Response(null, { status: 204 }); } }; };

  it("fresh copy: nothing asked, nothing started", async () => {
    const g = posts();
    expect(await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, g.f, bucket("2026-11-20"), mem(), now)).toBeNull();
    expect(g.bodies).toHaveLength(0);
    expect(REFRESH_DAYS).toBe(30);
  });
  it("a copy older than the month: starts the workflow with refresh, once for the month, and says so", async () => {
    const g = posts(), s = mem();
    const e = await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, g.f, bucket("2026-11-01"), s, now);
    expect(e).toMatchObject({ key: "runtiming-refresh:2026-12", kind: "runtiming", data: { age: 36, dispatch: { state: "started" } } });
    expect(g.bodies).toEqual([{ ref: DEFAULT_REF, inputs: { upload: "true", refresh: "true" } }]);
    await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, g.f, bucket("2026-11-01"), s, new Date(now.getTime() + 3600_000));
    expect(g.bodies).toHaveLength(1);
  });
  it("a start that did not finish: redone once, then the owner is told it did not finish (a new key each day)", async () => {
    const g = posts(), s = mem();
    const d = (h: number) => new Date(now.getTime() + h * 3600_000);
    await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, g.f, bucket("2026-11-01"), s, d(0));
    const again = await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, g.f, bucket("2026-11-01"), s, d(7));
    expect(again!.key).toBe("runtiming-refresh:2026-12:unfinished:2026-12-07");
    const over = await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, g.f, bucket("2026-11-01"), s, d(24 * 7));
    expect(over!.title).toContain("did not finish");
    expect(g.bodies).toHaveLength(2);
  });
  it("no date kept (the hand-seeded copy) is due and said; no token: the button; an unreadable date throws", async () => {
    const e = await checkSourceRefresh({}, async () => new Response(), bucket(null), mem(), now);
    expect(e!.text).toContain("no fetch date is kept");
    expect(e!.text).toContain("refresh: true");
    await expect(checkSourceRefresh({}, async () => new Response(), bucket("soon"), mem(), now)).rejects.toThrow("not a date");
  });
  it("a refused start is told (failed), never silent", async () => {
    const e = await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, async () => new Response("no", { status: 403 }), bucket("2026-01-01"), mem(), now);
    expect(e!.data).toMatchObject({ dispatch: { state: "failed" } });
    expect(e!.text).toContain("403");
    // (review) a refusal every week is told every week, and the start that finally goes through is told too
    const s = mem(), week = new Date(now.getTime() + 7 * 86_400_000);
    const no = async () => new Response("no", { status: 403 });
    const a = await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, no, bucket("2026-11-01"), s, now);
    const b = await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, no, bucket("2026-11-01"), s, week);
    expect([a!.key, b!.key]).toEqual(["runtiming-refresh:2026-12:refused:2026-12-07", "runtiming-refresh:2026-12:refused:2026-12-14"]);
    expect(a!.title).toContain("could not be started");
    const ok = await checkSourceRefresh({ GITHUB_DISPATCH_TOKEN: "t" }, async () => new Response(null, { status: 204 }), bucket("2026-11-01"), s,
                                        new Date(week.getTime() + 7 * 86_400_000));
    expect(ok!.key).toBe("runtiming-refresh:2026-12");
    await expect(checkSourceRefresh({}, async () => new Response(), bucket("2026-13-01"), mem(), now)).rejects.toThrow("not a real date");
  });
});
