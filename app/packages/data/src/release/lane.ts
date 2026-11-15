/**
 * (T737, 2026-11-15) THE AHEAD LANE (the web source's): every release file fetched AHEAD of need — the idle tier's region
 * file and `core`, the background tier's files, the address bar's `lines` — goes down ONE lane, and the reader always
 * goes first:
 *
 *   ONE AT A TIME   at most one file fetched ahead is on the network at once (the idle tier, the background tier and the
 *                   address bar asked theirs independently: up to three shared the line).
 *   READER FIRST    nothing ahead starts while a reader download is under way, nor for `graceMs` after the last one (a
 *                   tap's reads come in steps with a gap between them). A file ahead already on the network when the
 *                   reader starts downloading is ABORTED after `joinMs` unless the reader asked for it meanwhile (then it
 *                   is the reader's: it goes on, never aborted). An aborted file is not a failure: it waits in the lane
 *                   again, first, and starts from scratch when the line is the lane's again (a release file is served
 *                   with `Content-Encoding: gzip`, so a Range resume has no offsets to name). Its promise is the same
 *                   promise throughout: a caller never sees the abort.
 *   JOINED          a reader asking for a file still WAITING in the lane takes it out and starts it at once, as the
 *                   reader's (`promote`): the reader never waits behind the lane.
 *
 * Pure (timers injectable): the web source runs the downloads; this decides when.
 */

/** How long after the reader's first download starts a file ahead still on the network is aborted, unless the reader
 *  asked for it meanwhile: a tap asks its files within a few ms of each other (measured on staging: 4-16 ms). */
export const LANE_JOIN_MS = 150;
/** How long the line must stay free of the reader's downloads before the lane starts anything (the background tier's
 *  `READER_GRACE_MS`, measured there: a 1.2 MB file started in the 6 ms gap between a tap's two steps). */
export const LANE_GRACE_MS = 1500;

export interface LaneWork<T> {
  (o: {
    /** aborted when a reader download needs the line (the work must stop: reject) */
    readonly signal: AbortSignal;
    /** true: the reader asked for this file (its priority, its fetch priority); false: fetched ahead */
    readonly reader: boolean;
  }): Promise<T>;
}

export interface AheadLane {
  /** A file fetched ahead of need: `work` runs when the lane is free; aborted for the reader, it runs again later. */
  ahead<T>(key: string, work: LaneWork<T>): Promise<T>;
  /**
   * The reader asks for `key`. "started": it was waiting in the lane and now runs as the reader's; "running": it was
   * on the network ahead and is the reader's now (never aborted); null: not in the lane.
   */
  promote(key: string): "started" | "running" | null;
  /** A reader download starts on the network; call the returned function when it ends (either way). */
  reader(key: string): () => void;
  /** What is in the lane (tests, the Updates log). */
  state(): { readonly running: string | null; readonly waiting: readonly string[]; readonly readers: number;
             readonly aborted: number };
}

interface Item {
  readonly key: string;
  readonly work: LaneWork<unknown>;
  readonly ok: (v: unknown) => void;
  readonly fail: (e: unknown) => void;
  /** the reader asked for it: never aborted, counted as a reader download while it runs */
  reader: boolean;
  ctl: AbortController | null;
}

export interface LaneOptions {
  joinMs?: number;
  graceMs?: number;
  setTimer?: (f: () => void, ms: number) => unknown;
  clearTimer?: (h: unknown) => void;
}

export function createAheadLane(o: LaneOptions = {}): AheadLane {
  const joinMs = Math.max(0, o.joinMs ?? LANE_JOIN_MS);
  const graceMs = Math.max(0, o.graceMs ?? LANE_GRACE_MS);
  const setTimer = o.setTimer ?? ((f, ms) => setTimeout(f, ms));
  const clearTimer = o.clearTimer ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
  const items = new Map<string, Item>();
  const waiting: Item[] = [];
  /** the one file ahead on the network (not the reader's) */
  let run: Item | null = null;
  let readers = 0, aborted = 0;
  let grace: unknown = null, join: unknown = null;

  const lineIsReaders = () => readers > 0 || grace !== null;
  const pump = () => {
    if (run || lineIsReaders()) return;
    const next = waiting.shift();
    if (next) start(next);
  };
  const readerEnd = () => {
    readers -= 1;
    if (readers > 0) return;
    if (join !== null) { clearTimer(join); join = null; }
    if (graceMs === 0) { pump(); return; }
    grace = setTimer(() => { grace = null; pump(); }, graceMs);
  };
  const readerStart = () => {
    readers += 1;
    if (grace !== null) { clearTimer(grace); grace = null; }
  };
  /** the reader is downloading: the file ahead on the network goes after `joinMs`, unless the reader asked for it */
  const yieldSoon = () => {
    if (!run || join !== null) return;
    join = setTimer(() => {
      join = null;
      if (run && !run.reader && readers > 0) { aborted += 1; run.ctl?.abort(); }
    }, joinMs);
  };
  const start = (it: Item) => {
    const ctl = new AbortController();
    it.ctl = ctl;
    if (it.reader) readerStart(); else run = it;
    const asReader = it.reader;
    it.work({ signal: ctl.signal, reader: asReader }).then((v) => {
      done(it, asReader);
      it.ok(v);
    }, (e: unknown) => {
      if (ctl.signal.aborted && !asReader) {
        it.ctl = null;
        if (run === it) run = null;
        // the reader asked for it after its abort: it goes again at once, as the reader's
        if (it.reader) { start(it); return; }
        // aborted for the reader: back to the head of the lane, the same promise still waiting
        waiting.unshift(it);
        pump();
        return;
      }
      done(it, asReader);
      it.fail(e);
    });
  };
  const done = (it: Item, startedAsReader: boolean) => {
    items.delete(it.key);
    if (run === it) run = null;
    // a file the reader took over counts as the reader's until it ends (started so, or promoted while running)
    if (startedAsReader || it.reader) readerEnd(); else pump();
  };

  return {
    ahead<T>(key: string, work: LaneWork<T>): Promise<T> {
      let ok!: (v: unknown) => void, fail!: (e: unknown) => void;
      const p = new Promise<unknown>((a, b) => { ok = a; fail = b; });
      const it: Item = { key, work: work as LaneWork<unknown>, ok, fail, reader: false, ctl: null };
      items.set(key, it);
      waiting.push(it);
      pump();
      return p as Promise<T>;
    },
    promote(key) {
      const it = items.get(key);
      if (!it || it.reader) return it ? "running" : null;
      it.reader = true;
      const at = waiting.indexOf(it);
      if (at >= 0) {
        waiting.splice(at, 1);
        start(it);
        return "started";
      }
      // aborted already, its end not yet in: it starts again then, as the reader's (`start` counts it)
      if (it.ctl?.signal.aborted) return "started";
      // on the network ahead: the reader's now — it no longer holds the lane's one place, but the reader's line holds
      // the lane until it is in (`lineIsReaders`)
      if (run === it) run = null;
      readerStart();
      return "running";
    },
    reader() {
      readerStart();
      yieldSoon();
      let ended = false;
      return () => { if (!ended) { ended = true; readerEnd(); } };
    },
    state: () => ({ running: run?.key ?? null, waiting: waiting.map((i) => i.key), readers, aborted }),
  };
}
