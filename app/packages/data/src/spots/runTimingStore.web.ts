/** (T199) The last captured run timing on the web: one Cache API entry (browser storage, not the release's). */
import type { RunTimingStore } from "../feed/runtiming";

const CACHE = "canifishthis.runtiming.v1";
const KEY = "/runtiming/capture.json";

export function openRunTimingStore(): RunTimingStore {
  const open = () => (typeof caches === "undefined" ? null : caches.open(CACHE));
  return {
    async read() {
      try { const c = await open(); const r = await c?.match(KEY); return r ? await r.text() : null; } catch { return null; }
    },
    async write(text) {
      const c = await open();
      if (!c) return;
      await c.put(KEY, new Response(text, { headers: { "Content-Type": "application/json" } }));
    },
  };
}
