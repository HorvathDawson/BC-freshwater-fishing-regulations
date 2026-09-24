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
): Async<T> {
  const [out, setOut] = useState<Async<T>>(LOADING as Async<T>);
  const seq = useRef(0);

  // Declared FIRST, so it has already run by the time the effect below fires: the effect
  // always calls the closure from the most recent render without depending on it.
  const latest = useRef(run);
  useEffect(() => { latest.current = run; });

  useEffect(() => {
    if (!enabled) return;
    const mine = ++seq.current;
    let alive = true;
    setOut(LOADING as Async<T>);
    latest.current().then(
      (value) => { if (alive && mine === seq.current) setOut({ state: "ready", value, error: null }); },
      (e: unknown) => {
        if (alive && mine === seq.current) {
          setOut({ state: "failed", value: null, error: e instanceof Error ? e : new Error(String(e)) });
        }
      },
    );
    return () => { alive = false; };
  }, [key, enabled]);

  return out;
}

/** Debounce a fast-changing value — a search box, mostly. */
export function useDebounced<T>(value: T, ms: number): T {
  const [held, setHeld] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setHeld(value), ms);
    return () => clearTimeout(t);
  }, [value, ms]);
  return held;
}
