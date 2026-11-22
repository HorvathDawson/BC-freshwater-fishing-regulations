/**
 * (T994 finding 8) A FETCH THAT CANNOT HANG: React Native's `fetch` has no timeout, so a half-open socket leaves the
 * held promise pending for ever (and the TTL re-ask never fires for a promise that never settles). `timedFetch` wraps
 * a fetch once: the request is aborted `ms` after it starts, headers AND body (the timer is left running once the
 * headers arrive, so a body that stalls is cut too; aborting a finished request is a no-op). The rejection is the
 * environment's (the line: shown, never reported, core `isEnvironment`). One helper for every small JSON read (salmon,
 * in-season, run timing, HYDAT day, weather, the manifest recovery); the release files have `fetchResilient`.
 */
import { environmentError } from "@app/core";

export const FEED_READ_MS = 30_000;

export function timedFetch(f: typeof fetch, ms = FEED_READ_MS): typeof fetch {
  return (async (u: Parameters<typeof fetch>[0], i?: Parameters<typeof fetch>[1]) => {
    const ctl = new AbortController();
    let cut = false;
    const timer = setTimeout(() => { cut = true; ctl.abort(); }, ms);
    (timer as { unref?: () => void }).unref?.();
    const outer = i?.signal;
    if (outer?.aborted) ctl.abort(); else outer?.addEventListener("abort", () => ctl.abort(), { once: true });
    // the transport may not honour the signal (the whole point): the abort rejects on its own, racing the request
    const cutWith = new Promise<never>((_, fail) => {
      const stop = () => fail(cut
        ? environmentError(new Error(`timed out after ${Math.round(ms / 1000)} s`))
        : Object.assign(new Error("aborted"), { name: "AbortError" }));
      if (ctl.signal.aborted) stop(); else ctl.signal.addEventListener("abort", stop, { once: true });
    });
    return Promise.race([f(u, { ...i, signal: ctl.signal }), cutWith]);
  }) as typeof fetch;
}
