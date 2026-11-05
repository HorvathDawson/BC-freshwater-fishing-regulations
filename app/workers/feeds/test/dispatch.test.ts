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
