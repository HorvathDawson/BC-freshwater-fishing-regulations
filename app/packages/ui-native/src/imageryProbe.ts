/**
 * ONE IMAGERY TILE: does it answer? The probe `useImageryReach` runs (satellite is online-only, owner 2026-10-21).
 * `fetch` and `AbortController` are the same on both targets. `no-store`, so a browser cache cannot answer for a
 * network that is gone. Never throws: a timeout, a network error or a non-2xx is a no.
 */
import { imageryProbeUrl } from "@app/core";

export function imageryProbe(template: string, timeoutMs = 8000): () => Promise<boolean> {
  const url = imageryProbeUrl(template);
  return async () => {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), timeoutMs);
    try {
      const r = await fetch(url, { cache: "no-store", signal: ctl.signal });
      return r.ok;
    } catch {
      return false;
    } finally {
      clearTimeout(t);
    }
  };
}
