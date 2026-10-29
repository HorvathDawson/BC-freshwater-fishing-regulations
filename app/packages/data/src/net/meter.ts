/**
 * (T254) THE APP'S OWN NETWORK METER: bytes per second over the app's real downloads (release files, the synopsis), so
 * low-bandwidth mode follows the link the reader actually has — not what the platform claims (Chrome says "4g" on a
 * throttled line; a phone's "LTE" can be one bar).
 *
 * WIRE BYTES, NOT FILE BYTES: a release file is gzip on the wire and the browser may hand over the decoded body, three
 * times the size; the caller scales each chunk to the wire (`fetch.ts` `wireScale`) before it reaches the meter.
 *
 * THE RATE is the bytes that arrived in the last `WINDOW_MS`, over the time in that window when at least one download
 * was in flight (so idle time is not counted as slowness, and two downloads at once are one link, not two). A window
 * needs `MIN_BUSY_MS` of downloading to say anything; until then the last rate stands (null: never measured). A stall —
 * downloads in flight and nothing arriving — reads as a rate near nothing, which is what the reader is getting.
 */
export const WINDOW_MS = 8000;
export const MIN_BUSY_MS = 2000;

export interface NetMeter {
  /** A download began; returns its handle. */
  start(): number;
  /** `wireBytes` more arrived on download `h`. */
  chunk(h: number, wireBytes: number): void;
  /** Download `h` ended (done, failed or cancelled). */
  end(h: number): void;
  /** The measured rate, bytes per second; null: never measured. */
  bps(): number | null;
  /** Every change of the rate (at most once a second); returns the unsubscribe. */
  subscribe(on: () => void): () => void;
}

export function createNetMeter(o: { now?: () => number; notifyEveryMs?: number } = {}): NetMeter {
  const now = o.now ?? (() => Date.now());
  const every = o.notifyEveryMs ?? 1000;
  const open = new Set<number>();
  let next = 1;
  /** arrivals in the window: [time, bytes] */
  let arrivals: [number, number][] = [];
  /** busy intervals in the window: [from, to | null (still busy)] */
  let busy: [number, number | null][] = [];
  let last: number | null = null;
  let told = 0;
  const subs = new Set<() => void>();

  const trim = (t: number) => {
    const from = t - WINDOW_MS;
    arrivals = arrivals.filter(([at]) => at >= from);
    busy = busy.filter(([, to]) => to === null || to >= from);
  };
  const busyMs = (t: number) => {
    const from = t - WINDOW_MS;
    return busy.reduce((a, [s, e]) => a + Math.max(0, (e ?? t) - Math.max(s, from)), 0);
  };
  const measure = () => {
    const t = now();
    trim(t);
    const b = busyMs(t);
    if (b >= MIN_BUSY_MS) last = arrivals.reduce((a, [, n]) => a + n, 0) / (b / 1000);
    return last;
  };
  const tell = () => {
    const t = now();
    if (t - told < every) return;
    told = t;
    measure();
    subs.forEach((f) => f());
  };

  return {
    start() {
      const h = next++;
      if (!open.size) busy.push([now(), null]);
      open.add(h);
      return h;
    },
    chunk(h, wireBytes) {
      if (!open.has(h) || !(wireBytes > 0)) return;
      arrivals.push([now(), wireBytes]);
      tell();
    },
    end(h) {
      if (!open.delete(h)) return;
      if (!open.size) { const b = busy[busy.length - 1]; if (b && b[1] === null) b[1] = now(); }
      tell();
    },
    bps: measure,
    subscribe(on) { subs.add(on); return () => { subs.delete(on); }; },
  };
}
