/**
 * The one async primitive every hook here uses.
 *
 * Three things it has to get right, and all three are bugs you only see on a slow phone:
 *   - a resolved promise from a query you have moved on from must not overwrite the current
 *     answer (tap two rivers quickly and the first one wins the race);
 *   - unmounting mid-flight must not set state;
 *   - a failure is a STATE, not a silent empty result — "we could not check" and "nothing
 *     here" look identical if you model an error as `null`.
 */
import { useEffect, useRef, useState } from "react";

export type Async<T> =
  | { state: "loading"; value: null; error: null }
  | { state: "ready"; value: T; error: null }
  | { state: "failed"; value: null; error: Error };

// Module-private: the sentinel `useAsync` returns; callers match on `state`.
const LOADING = { state: "loading", value: null, error: null } as const;

/**
 * `key` is a STRING, not a dependency array, and that is deliberate.
 *
 * The array version needed `// eslint-disable-next-line react-hooks/exhaustive-deps`,
 * because the rule cannot verify a spread. Worse: this workspace has no eslint, so the
 * comment suppressed nothing while implying a check had been considered and waived —
 * a lie in a comment is harder to spot than a missing test.
 *
 * A caller now names its own identity ("water-item:gnis:8634"), the
 * dependency array is a literal, and no suppression is possible.
 *
 * WHICH IS THE WHOLE POINT OF THE REF BELOW. `run` must NOT be a dependency. Every caller
 * passes an inline arrow, so its identity changes on every render; with `run` in the array
 * the effect re-ran every render, called `setOut(LOADING)`, and re-rendered — an infinite
 * loop, in every screen that used any hook in this file. It survived a full green test run
 * and only appeared the first time a browser loaded the app, because nothing mounted a
 * component that used it. `key` is the identity; `run` is just the newest closure to call.
 */
export function useAsync<T>(
  run: () => Promise<T>,
  key: string,
  enabled = true,
  /** A REFRESH, not a new question (P02's re-poll): when the previous run had this same `sameAs`, the last ready
   *  value stays on screen while the new one loads, and a failed refresh keeps it (its age says how old it is). */
  sameAs?: string,
  /** AN ANSWER ALREADY IN HAND (`peekAnswer`, answers.ts): returned in the very render the key changes — no loading
   *  frame — and the question is not asked again. Consulted for a new question only, never for a refresh. */
  peek?: () => { value: T } | null,
): Async<T> {
  // THE ANSWER IS TAGGED WITH THE QUESTION IT ANSWERS, so the render right after a key change knows the answer it holds
  // is the last key's — and draws a peeked answer for the new key in its place.
  const [out, setOut] = useState<Tagged<T>>({ key: null, v: LOADING as Async<T> });
  const seq = useRef(0);
  const lastSame = useRef<string | undefined>(undefined);
  // The peeked answer, kept by identity so a memo downstream does not rebuild when the effect stores it.
  const peeked = useRef<{ key: string; v: Async<T> } | null>(null);

  // Declared FIRST, so it has already run by the time the effect below fires: the effect
  // always calls the closure from the most recent render without depending on it.
  const latest = useRef(run);
  const latestPeek = useRef(peek);
  useEffect(() => { latest.current = run; latestPeek.current = peek; });

  useEffect(() => {
    if (!enabled) return;
    const mine = ++seq.current;
    let alive = true;
    const refresh = sameAs !== undefined && lastSame.current === sameAs;
    lastSame.current = sameAs;
    const known = refresh ? null : latestPeek.current?.() ?? null;
    if (known) {
      const same = peeked.current?.key === key && peeked.current.v.value === known.value;
      setOut({ key, sameAs, v: same ? peeked.current!.v : { state: "ready", value: known.value, error: null } });
      return;
    }
    // Already loading stays the SAME state object, so React bails out: a caller whose key changes every render (a
    // loader built inline) must not turn that into a render loop.
    if (!refresh) setOut((prev) => (prev.v.state === "loading" ? prev : { key, sameAs, v: LOADING as Async<T> }));
    latest.current().then(
      (value) => { if (alive && mine === seq.current) setOut({ key, sameAs, v: { state: "ready", value, error: null } }); },
      (e: unknown) => {
        if (alive && mine === seq.current) {
          const failed = { state: "failed", value: null, error: e instanceof Error ? e : new Error(String(e)) } as const;
          setOut((prev) => (refresh && prev.v.state === "ready" ? { ...prev, key } : { key, sameAs, v: failed }));
        }
      },
    );
    return () => { alive = false; };
  }, [key, enabled]);

  if (out.key === key) return out.v;
  const hit = enabled && !(sameAs !== undefined && out.sameAs === sameAs) ? peek?.() ?? null : null;
  if (hit) {
    if (peeked.current?.key !== key || peeked.current.v.value !== hit.value)
      peeked.current = { key, v: { state: "ready", value: hit.value, error: null } };
    return peeked.current.v;
  }
  // Otherwise the last answer until the effect has run, as it always was: a caller whose key changes every render (a
  // loader built inline, regs.render.test 6) only ever sees an answer this way. A chart keeps its last one on purpose
  // (`useChartView`'s `pending`).
  return out.v;
}

/** What `useAsync` holds: an answer, and the question (`key`) it answers — so a peeked answer can stand in for it. */
interface Tagged<T> { key: string | null; sameAs?: string; v: Async<T> }

/** Debounce a fast-changing value — a search box, mostly. */
export function useDebounced<T>(value: T, ms: number): T {
  const [held, setHeld] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setHeld(value), ms);
    return () => clearTimeout(t);
  }, [value, ms]);
  return held;
}

/** A counter that ticks every `intervalMs` while `enabled`: put it in a `useAsync` key to re-ask on a schedule. */
export function usePoll(intervalMs: number, enabled = true): number {
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (!enabled) return;
    const id = setInterval(() => setTick((t) => t + 1), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs, enabled]);
  return tick;
}
