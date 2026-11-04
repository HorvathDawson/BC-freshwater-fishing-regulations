import { now as clockNow } from "@app/core";
/**
 * (T273) THE PAN PROBE — test-bed measurement only, beside T74's grey-frame probe (`probe.ts`). On while
 * `globalThis.__panProbe` is set (apps/mobile sets it from `EXPO_PUBLIC_PAN_PROBE=1`, the test bed's build); otherwise
 * every call here is a no-op and no timer runs.
 *
 * What it counts, per second with anything in it, one line to the log (`[pan] {…}`, `adb logcat -s ReactNativeJS`):
 *   lag      the JS thread's worst delay that second (a 50 ms timer's overshoot, ms) and how many ticks were >100 ms late
 *   tiles    atlas tile events (`MLRNTileIds`) the colouring took, and the section labels they produced
 *   sends    `setFeatureStates` bridge calls, and their worst / mean round trip (ms) — label → acknowledged on the map
 *   renders  the native Map component's renders
 */
interface Win { lagMax: number; late: number; tiles: number; labels: number; sends: number; ackMax: number; ackSum: number;
  acks: number; renders: number }
const fresh = (): Win => ({ lagMax: 0, late: 0, tiles: 0, labels: 0, sends: 0, ackMax: 0, ackSum: 0, acks: 0, renders: 0 });

const on = () => !!(globalThis as { __panProbe?: boolean }).__panProbe;
let w = fresh();
let started = false;

function start(): void {
  if (started || !on()) return;
  started = true;
  let due = clockNow() + 50;
  setInterval(() => {
    const now = clockNow(), lag = Math.max(0, now - due);
    due = now + 50;
    if (lag > w.lagMax) w.lagMax = lag;
    if (lag > 100) w.late += 1;
  }, 50);
  setInterval(() => {
    const busy = w.tiles || w.sends || w.renders || w.late;
    if (busy) {
      const ackMean = w.acks ? Math.round(w.ackSum / w.acks) : 0;
      console.log(`[pan] ${JSON.stringify({ t: clockNow(), lagMax: w.lagMax, late: w.late, tiles: w.tiles, labels: w.labels,
                                            sends: w.sends, ackMax: w.ackMax, ackMean, renders: w.renders })}`);
    }
    w = fresh();
  }, 1000);
}

export const panProbe = {
  tile(labels: number): void { if (!on()) return; start(); w.tiles += 1; w.labels += labels; },
  /** A `setFeatureStates` call sent: call the returned function when it is acknowledged. */
  send(): () => void {
    if (!on()) return () => {};
    start();
    w.sends += 1;
    const t0 = clockNow();
    return () => { const ms = clockNow() - t0; w.acks += 1; w.ackSum += ms; if (ms > w.ackMax) w.ackMax = ms; };
  },
  render(): void { if (!on()) return; start(); w.renders += 1; },
};
