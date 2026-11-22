/** (T994 finding 8) `timedFetch`: a request that never answers is cut, as the environment's failure (shown, not reported). */
import { describe, expect, it, vi } from "vitest";
import { isEnvironment } from "@app/core";
import { timedFetch } from "./timeout";

describe("timedFetch", () => {
  it("rejects a half-open request after the limit, as an environment error", async () => {
    vi.useFakeTimers();
    try {
      let seen: AbortSignal | undefined;
      const hang = (async (_u: unknown, i?: RequestInit) => { seen = i?.signal ?? undefined; return new Promise(() => {}); }) as typeof fetch;
      const p = timedFetch(hang, 1000)("https://x/y");
      const out = p.then(() => "ok", (e: unknown) => e);
      await vi.advanceTimersByTimeAsync(1001);
      const e = await out;
      expect(e).toBeInstanceOf(Error);
      expect(isEnvironment(e)).toBe(true);
      expect((e as Error).message).toBe("timed out after 1 s");
      expect(seen?.aborted).toBe(true);        // the request itself is cut, not only awaited
    } finally { vi.useRealTimers(); }
  });

  it("passes an answer through, and a caller's own abort stays an AbortError", async () => {
    const ok = (async () => new Response("{}")) as unknown as typeof fetch;
    expect((await timedFetch(ok, 1000)("https://x/y")).ok).toBe(true);
    const hang = (async () => new Promise(() => {})) as unknown as typeof fetch;
    const ctl = new AbortController();
    const p = timedFetch(hang, 60_000)("https://x/y", { signal: ctl.signal });
    ctl.abort();
    await expect(p).rejects.toMatchObject({ name: "AbortError" });
  });
});
