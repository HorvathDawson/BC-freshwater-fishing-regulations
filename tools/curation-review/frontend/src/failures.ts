/* ONE FAILURE REPORTER (T434, audit 2026-11-05). A read that fails is SHOWN, never turned into an empty answer: a
   failed list used to become [] ("nothing here"), a failed verdict an empty one, a failed policy list a copy kept in
   the frontend. Every `.catch` that does not set its own on-screen error calls `reportFailure(where, err)` (or
   `.catch(failed(where))`), and `FailuresBanner` in the notice stack says "could not load <where>" with the server's
   reason. `pipeline/tests/test_review_frontend_catches.py` fails on a new `.catch` that does neither. */

export interface Failure { where: string; message: string; at: number; n: number }

let failures: Failure[] = [];
const listeners = new Set<(f: Failure[]) => void>();

/** an error's words: the server's `detail` when getJSON put it there, else the message */
export function errorText(err: unknown): string {
  if (err instanceof Error) return err.message;
  return typeof err === "string" ? err : JSON.stringify(err) ?? String(err);
}

/** record a failed read (one line per `where`; a repeat moves its count and time) */
export function reportFailure(where: string, err: unknown): void {
  const message = errorText(err);
  console.error(`could not load ${where}: ${message}`);
  const was = failures.find((f) => f.where === where && f.message === message);
  failures = was
    ? failures.map((f) => (f === was ? { ...f, n: f.n + 1, at: Date.now() } : f))
    : [...failures.filter((f) => f.where !== where), { where, message, at: Date.now(), n: 1 }];
  listeners.forEach((l) => l(failures));
}

/** `.catch(failed("the DFO progress"))`: report, and resolve to undefined (the state keeps what it had) */
export const failed = (where: string) => (err: unknown): undefined => { reportFailure(where, err); return undefined; };

/** a read of `where` that succeeded again: its line goes */
export function cleared(where: string): void {
  if (!failures.some((f) => f.where === where)) return;
  failures = failures.filter((f) => f.where !== where);
  listeners.forEach((l) => l(failures));
}

export function currentFailures(): Failure[] { return failures; }
export function subscribeFailures(l: (f: Failure[]) => void): () => void {
  listeners.add(l);
  return () => { listeners.delete(l); };
}
