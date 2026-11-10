/**
 * ANSWERS ALREADY ASKED FOR, per source — what lets the Conditions chart switch gauges without a loading frame
 * (owner 2026-10-21: "when clicking between the gauges ... it flickers a millisecond before showing the chart").
 *
 * The flicker was not the network. `useAsync` sets LOADING on every new key and only learns the answer a microtask
 * later, so even a series the feed had cached drew the spinner for one frame. Here a question is asked ONCE (a second
 * asker shares the in-flight promise, so the chart and the model chips are one request, not two) and its settled
 * answer can be PEEKED synchronously — `useAsync(..., peek)` returns it in the very render the key changes.
 *
 * Bounded twice: entries live as long as the feed reuses its own files (`TTL_MS`, data/src/feed/http.ts), and the
 * map keeps the most recently used `CAP` of them. A failure is never kept — the next asker tries again.
 */
import { FEED_TTL_MS, type Parameter, type StationId } from "@app/data";
import { now as clockNow } from "@app/core";

/** How long an answer is reused: the feed's own file reuse (`TTL_MS` in data/src/feed/http.ts). */
export const ANSWER_TTL_MS = FEED_TTL_MS;
/** At most this many answers per source — a panel has a handful of donors, two spans, a parameter list each. */
export const ANSWER_CAP = 96;

interface Entry { at: number; promise: Promise<unknown>; done: { value: unknown } | null }

let stores = new WeakMap<object, Map<string, Entry>>();

function storeOf(source: object): Map<string, Entry> {
  let m = stores.get(source);
  if (!m) stores.set(source, (m = new Map()));
  return m;
}

/**
 * The answer to `key`, asked of `run` only when no live entry has it. `maxAge` narrows the reuse window (a polled
 * reading asks again once its entry is older than that).
 */
export function askOnce<T>(source: object, key: string, run: () => Promise<T>,
                           maxAge = ANSWER_TTL_MS, now = clockNow()): Promise<T> {
  const m = storeOf(source);
  const hit = m.get(key);
  if (hit && now - hit.at < maxAge) {
    m.delete(key);
    m.set(key, hit);
    return hit.promise as Promise<T>;
  }
  const e: Entry = { at: now, promise: Promise.resolve(), done: null };
  e.promise = (async () => run())().then(
    (value) => { e.done = { value }; return value; },
    (err: unknown) => { if (m.get(key) === e) m.delete(key); throw err; },
  );
  m.delete(key);
  m.set(key, e);
  while (m.size > ANSWER_CAP) m.delete(m.keys().next().value!);
  return e.promise as Promise<T>;
}

/** The settled answer to `key`, synchronously; null while it is unasked, in flight, failed or older than `maxAge`. */
export function peekAnswer<T>(source: object, key: string, maxAge = ANSWER_TTL_MS,
                              now = clockNow()): { value: T } | null {
  const hit = stores.get(source)?.get(key);
  return hit?.done && now - hit.at < maxAge ? (hit.done as { value: T }) : null;
}

/** Test seam: the answers are module state and a test must be able to start from empty. */
export function clearAnswers(): void {
  stores = new WeakMap();
}

/** One series' identity — the chart, the model chips and the prefetch all ask under this one key. */
export function seriesKey(station: StationId | null, span: "72h" | "year", parameter: Parameter | undefined,
                          model: string | undefined): string {
  return `series:${station}:${span}:${parameter ?? "auto"}:${model ?? "auto"}`;
}

/**
 * Which quantity a station is charted in: the picked one where the station measures it, else the station's own
 * (undefined). One rule, so the prefetch asks for exactly the series the chart will.
 */
export function chartParameter(picked: Parameter | null | undefined,
                               available: readonly Parameter[]): Parameter | undefined {
  return picked && available.includes(picked) ? picked : undefined;
}

/**
 * (T581, T580 F1) WHICH QUANTITY THE GAUGE CARD IS ABOUT. When the caller owns the quantity (the map's Show) the card is
 * about exactly that, always: a station that does not measure it is `silent`, and the card says so in the words its row
 * and pin use, offering the other quantity as the one Show choice, never quietly drawing it. Left to the panel (nothing
 * asked) the card is the station's own quantity; before the station's quantities are known it is Flow, the default
 * everywhere, never Level.
 */
export interface CardQuantity {
  shown: Parameter;
  /** the station is known not to measure `shown` */
  silent: boolean;
  /** the toggle's choices: what the station measures, plus the asked quantity when it is silent in it */
  options: readonly Parameter[];
}

export function cardQuantity(asked: Parameter | null | undefined, available: readonly Parameter[], known: boolean,
                             readingQuantity: Parameter | null | undefined): CardQuantity {
  if (asked) {
    const silent = known && !available.includes(asked);
    return { shown: asked, silent,
             options: silent ? (["discharge", "level"] as const).filter((q) => q === asked || available.includes(q)) : available };
  }
  return { shown: readingQuantity ?? available[0] ?? "discharge", silent: false, options: available };
}
