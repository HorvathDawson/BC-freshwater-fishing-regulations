/**
 * (T254) BAD CONNECTIONS: the policy (mode, order, timeouts), the meter, the gate, the resilient download, and what is
 * downloading now — one `Net` per app, shared by the release source (web), the synopsis download and the screens.
 */
import { createFetchGate, type FetchGate } from "./gate";
import { createNetActivity, type NetActivity } from "./activity";
import { createNetMeter, type NetMeter } from "./meter";
import { netMode, NO_HINTS, type NetMode } from "./policy";

export * from "./policy";
export { createFetchGate, type FetchGate } from "./gate";
export { pendingOf, type ActivitySnapshot, type NetActivity } from "./activity";
export { createNetMeter, type NetMeter } from "./meter";
export { fetchResilient, NetError } from "./fetch";
export { fetchVerifiedVia } from "./verified";

/** The device's word on being online (`@app/ui` `Connectivity`'s first half): the web's events, a phone's NetInfo. */
export interface OnlineSource {
  online(): boolean;
  subscribe(on: (online: boolean) => void): () => void;
}

export interface Net {
  readonly meter: NetMeter;
  readonly activity: NetActivity;
  readonly gate: FetchGate;
  /** the mode now (set by the screens' `useNetMode`, which owns the setting and the platform's hints) */
  mode(): NetMode;
  setMode(m: NetMode): void;
  online(): boolean;
  /** resolves when the device is online again (at once when it is) */
  waitOnline(signal?: AbortSignal): Promise<void>;
}

const ALWAYS_ONLINE: OnlineSource = { online: () => true, subscribe: () => () => {} };

export function createNet(o: { online?: OnlineSource; meter?: NetMeter } = {}): Net {
  const src = o.online ?? ALWAYS_ONLINE;
  let mode: NetMode = netMode("auto", NO_HINTS);
  const gate = createFetchGate({ low: () => mode.low });
  return {
    meter: o.meter ?? createNetMeter(),
    activity: createNetActivity(),
    gate,
    mode: () => mode,
    setMode(m) {
      const was = mode.low;
      mode = m;
      if (was !== m.low) gate.changed();
    },
    online: () => src.online(),
    waitOnline(signal) {
      if (src.online()) return Promise.resolve();
      return new Promise((ok, fail) => {
        let done = false, stop: (() => void) | null = null;
        const finish = () => { done = true; stop?.(); signal?.removeEventListener("abort", abort); };
        const abort = () => { finish(); const e = new Error("aborted"); e.name = "AbortError"; fail(e); };
        signal?.addEventListener("abort", abort, { once: true });
        stop = src.subscribe((up) => { if (up && !done) { finish(); ok(); } });
        if (done) stop();
      });
    },
  };
}
