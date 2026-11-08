// T413: a new HYDAT starts the HYDAT workflow once (with the owner's token); without it, today's button email.
import { describe, expect, it } from "vitest";
import { dispatchOnce, dispatchSteps, RETRY_MS } from "../src/dispatch";
import { hydatSection } from "../src/email";

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
    expect(JSON.parse(String(g.calls[0]!.init!.body))).toEqual({ ref: "redesign/stream-sections", inputs: { upload: "true" } });
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
    expect(calls).toEqual(["GET https://api.github.com/repos/HorvathDawson/BC-freshwater-fishing-regulations/actions/workflows/feeds-hydat.yml"]);
  });
  it("says why not", async () => {
    expect(await checkToken({}, async () => new Response())).toMatchObject({ ok: false, says: "GITHUB_DISPATCH_TOKEN is not set" });
    expect(await checkToken({ GITHUB_DISPATCH_TOKEN: "bad" }, async () => Response.json({ message: "Bad credentials" }, { status: 401 })))
      .toEqual({ ok: false, status: 401, says: "Bad credentials" });
  });
});
