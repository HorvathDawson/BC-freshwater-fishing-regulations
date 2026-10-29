/**
 * ONE IMAGERY TILE: does it answer? The probe `useImageryReach` runs (satellite is online-only, owner 2026-10-21).
 * `fetch` and `AbortController` are the same on both targets. `no-store`, so a browser cache cannot answer for a
 * network that is gone. Never throws: a timeout, a network error or a non-2xx is a no.
 *
 * (T277) A no is asked once more (after `againMs`) before it is said: on a phone the probe is the ONLY signal that the
 * imagery is down, and one slow or dropped request (a Wi-Fi handover, a cold DNS) turned satellite off for the 30 s
 * until the next probe. Each no names itself in the log (logcat / the Updates log), so a phone that still says "needs a
 * connection" while online tells why.
 */
import { imageryProbeUrl } from "@app/core";

export function imageryProbe(template: string, timeoutMs = 8000, againMs = 1500): () => Promise<boolean> {
  const url = imageryProbeUrl(template);
  const once = async (): Promise<string | null> => {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), timeoutMs);
    try {
      const r = await fetch(url, { cache: "no-store", signal: ctl.signal });
      return r.ok ? null : `HTTP ${r.status}`;
    } catch (e) {
      return ctl.signal.aborted ? `no answer in ${timeoutMs} ms` : e instanceof Error ? e.message : String(e);
    } finally {
      clearTimeout(t);
    }
  };
  return async () => {
    let why = await once();
    if (why === null) return true;
    await new Promise<void>((r) => setTimeout(r, againMs));
    why = await once();
    if (why === null) return true;
    console.error(`[imagery] satellite tile probe failed twice: ${why}`);
    return false;
  };
}
