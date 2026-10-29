/**
 * (T251) THE UPDATES LOG'S RING BUFFER: the last `cap` update events (`@app/core` `UpdateEvent`), kept on the device
 * (`openUpdateLogStore`) so a tester can reopen the app and still read what happened. Development and staging builds
 * only — App.tsx makes none in a production build. A write that fails costs the history, never a check.
 */
import { UPDATE_LOG_CAP, type UpdateEvent } from "@app/core";

export interface UpdateLog {
  append(e: UpdateEvent): void;
  /** oldest first */
  events(): readonly UpdateEvent[];
  clear(): void;
  subscribe(on: () => void): () => void;
}

interface Kept { v: 1; events: readonly UpdateEvent[] }

export function createUpdateLog(o: { store?: { read(): string | null; write(text: string): void }; cap?: number } = {}): UpdateLog {
  const cap = o.cap ?? UPDATE_LOG_CAP;
  let events: readonly UpdateEvent[] = [];
  try {
    const t = o.store?.read();
    const k = t ? (JSON.parse(t) as Kept) : null;
    if (k?.v === 1 && Array.isArray(k.events)) events = k.events.filter((e) => e && typeof e.at === "number").slice(-cap);
  } catch { /* unreadable: start empty */ }
  const listeners = new Set<() => void>();
  const changed = () => {
    try { o.store?.write(JSON.stringify({ v: 1, events } satisfies Kept)); } catch { /* history only */ }
    for (const l of [...listeners]) l();
  };
  return {
    append(e) { events = [...events, e].slice(-cap); changed(); },
    events: () => events,
    clear() { events = []; changed(); },
    subscribe(on) { listeners.add(on); return () => { listeners.delete(on); }; },
  };
}

/**
 * (T251) A fetch that says which app asked: `headers` (`@app/core` `versionHeaders`) added to every request it makes,
 * the request's own headers kept (an If-None-Match, a Range).
 */
export function versionedFetch(inner: typeof fetch, headers: Record<string, string>): typeof fetch {
  return ((input: RequestInfo | URL, init?: RequestInit) => {
    const h = new Headers(init?.headers);
    for (const [k, v] of Object.entries(headers)) if (!h.has(k)) h.set(k, v);
    return inner(input, { ...init, headers: h });
  }) as typeof fetch;
}
