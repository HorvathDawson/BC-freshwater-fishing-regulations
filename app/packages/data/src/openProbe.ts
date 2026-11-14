import { now as clockNow } from "@app/core";
/**
 * (T286) THE OPEN PROBE — test-bed measurement only, beside T273's pan probe. On while `globalThis.__openProbe` is set
 * (apps/mobile sets it from `EXPO_PUBLIC_PAN_PROBE=1`, the test bed's build); otherwise every call is a no-op.
 *
 * One trace per map tap: `start` (the tapped section handed to the opener; the map's press time, when its handler left
 * one, is the trace's zero, so the hit test is counted), then the first time each stage is reached (`mark`), and every
 * native SQLite call made meanwhile (`sql`: how many, time waiting in T264's queue, time running). `end` writes one line
 * to the log (`[open] {…}`, `adb logcat -s ReactNativeJS`): `marks` is stage → [ms since the press, SQLite calls by then].
 */
interface Trace {
  what: string; t0: number; marks: Record<string, [number, number]>; spans: Record<string, number>;
  sql: number; sqlWait: number; sqlRun: number; sqlMax: number; sqlMaxWhat: string; lagMax: number; lagAt: number; done: boolean;
  /** (T716) the press as an instant (epoch ms, the map's handler), for the test bed's tap → JS delay; null: none left */
  wall: number | null;
}
type Probe = { __openProbe?: boolean; __openPressAt?: number; __openPressWall?: number };
const g = globalThis as Probe;
const now = (): number => globalThis.performance?.now?.() ?? clockNow();
const r1 = (x: number) => Math.round(x * 10) / 10;
let cur: Trace | null = null;

let tick: ReturnType<typeof setInterval> | null = null;
function write(t: Trace): void {
  t.done = true;
  if (tick) { clearInterval(tick); tick = null; }
  console.log(`[open] ${JSON.stringify({ what: t.what, marks: t.marks, spans: t.spans, sql: t.sql, sqlWait: r1(t.sqlWait),
                                          sqlRun: r1(t.sqlRun), sqlMax: r1(t.sqlMax), sqlMaxWhat: t.sqlMaxWhat,
                                          lagMax: r1(t.lagMax), lagAt: r1(t.lagAt), wall: t.wall })}`);
}

export const openProbe = {
  on: (): boolean => !!g.__openProbe,
  /** The map's press handler, before its hit test: the trace's zero. */
  press(): void { if (g.__openProbe) g.__openPressAt = now(); },
  /** A tap resolved to a section, handed to the opener. */
  start(what: string): void {
    if (!g.__openProbe) return;
    if (cur && !cur.done) write(cur);
    const at = g.__openPressAt, wall = g.__openPressWall ?? null, t = now();
    g.__openPressAt = undefined; g.__openPressWall = undefined;
    cur = { what, t0: at ?? t, marks: {}, spans: {}, sql: 0, sqlWait: 0, sqlRun: 0, sqlMax: 0, sqlMaxWhat: "", lagMax: 0, lagAt: 0, done: false, wall };
    cur.marks.hit = [r1(t - cur.t0), 0];
    const mine = cur;
    // the JS thread's worst stall during the open (a 16 ms timer's overshoot) and when it ended
    let due = now() + 16;
    tick = setInterval(() => {
      const n = now(), lag = n - due;
      due = n + 16;
      if (lag > mine.lagMax) { mine.lagMax = lag; mine.lagAt = n - mine.t0; }
    }, 16);
    setTimeout(() => { if (!mine.done) { mine.marks.timeout = [r1(now() - mine.t0), mine.sql]; write(mine); } }, 15_000);
  },
  /** The first time the open reaches `stage`. */
  mark(stage: string): void {
    if (!cur || cur.done || cur.marks[stage]) return;
    cur.marks[stage] = [r1(now() - cur.t0), cur.sql];
  },
  /** Time spent in one named piece of work (summed). */
  span(name: string, ms: number): void {
    if (!cur || cur.done) return;
    cur.spans[name] = r1((cur.spans[name] ?? 0) + ms);
  },
  /** A native SQLite call queued: call `run()` when it starts, `done()` when it answers. */
  sql(what = ""): { run(): void; done(): void } {
    const t = cur;
    if (!t || t.done) return NOOP;
    const q = now();
    let s = q;
    t.sql += 1;
    return {
      run: () => { s = now(); t.sqlWait += s - q; },
      done: () => { const ms = now() - s; t.sqlRun += ms; if (ms > t.sqlMax) { t.sqlMax = ms; t.sqlMaxWhat = what; } },
    };
  },
  /** (T287) Time a piece of synchronous work on the JS thread: one `[work] <label> <ms>` line when it held it 20 ms or
   *  more (test bed only; otherwise just runs it). */
  work<T>(label: string, f: () => T): T {
    if (!g.__openProbe) return f();
    const t = now(), out = f(), ms = now() - t;
    if (ms >= 20) console.log(`[work] ${label} ${r1(ms)} ms`);
    return out;
  },
  /** (T287) Time a piece of work done in slices: one `[work] <label> sliced <ms>` line (wall time, first slice to last;
   *  test bed only). */
  async workAsync<T>(label: string, f: () => Promise<T>): Promise<T> {
    if (!g.__openProbe) return f();
    const t = now(), out = await f();
    console.log(`[work] ${label} sliced ${r1(now() - t)} ms`);
    return out;
  },
  /** The open is drawn: the trace is written, 1.5 s later (what still lands then — the Salmon card, the access
   *  notice, the held pane shown (T716: `shown`, up to `PANE_HOLD_MS` after), the JS thread's stalls — is in it too). */
  end(stage: string): void {
    if (!cur || cur.done || cur.marks[stage]) return;
    openProbe.mark(stage);
    const t = cur;
    setTimeout(() => { if (!t.done) write(t); }, 1500);
  },
  /** (T716) `[<tag>] {wall}` on the next drawn frame: an instant (epoch ms) the test bed sets beside the device clock. */
  instantOnFrame(tag: string): void {
    if (!g.__openProbe) return;
    const raf = (globalThis as { requestAnimationFrame?: (f: () => void) => unknown }).requestAnimationFrame;
    const say = () => console.log(`[${tag}] ${JSON.stringify({ wall: clockNow() })}`);
    if (raf) raf(say); else setTimeout(say, 0);
  },
  /** `end(stage)` on the next drawn frame (the screen's commit has reached the screen). Here, not in the component:
   *  the phone components ask the JS thread for no frames (tools/no-per-frame-animation.test.ts). */
  endOnFrame(stage: string): void {
    if (!cur || cur.done) return;
    const raf = (globalThis as { requestAnimationFrame?: (f: () => void) => unknown }).requestAnimationFrame;
    if (raf) raf(() => openProbe.end(stage)); else setTimeout(() => openProbe.end(stage), 0);
  },
};
const NOOP = { run() {}, done() {} };
