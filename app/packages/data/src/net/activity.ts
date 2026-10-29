/**
 * (T254) WHAT IS DOWNLOADING NOW, for the screens' slow states: each release file on its way (waiting its turn, or
 * arriving: so many bytes of so many), and the last one that failed. A page that is waiting on a slow line says what
 * it is waiting for ("Regulations: 1.2 of 3.4 MB") rather than spinning in silence — and a part that is not there says
 * whether it is still coming or cannot come offline. A plain store: the web source writes it, `@app/ui` reads it.
 */
import type { FetchPriority } from "./policy";

export interface Download {
  readonly key: string;
  readonly priority: FetchPriority;
  /** waiting its turn in low-bandwidth mode (`gate.ts`), or arriving */
  readonly waiting: boolean;
  readonly loaded: number;
  readonly total: number | null;
  /** the try under way (1 = the first) */
  readonly attempt: number;
}

export interface NetActivity {
  queued(key: string, priority: FetchPriority, total: number | null): void;
  started(key: string): void;
  /** a download queued ahead that the reader now needs: it is the reader's now (the waiting lines count it) */
  raise(key: string, priority: FetchPriority): void;
  progress(key: string, loaded: number, total: number | null): void;
  retry(key: string, attempt: number): void;
  done(key: string): void;
  failed(key: string, why: string): void;
  snapshot(): ActivitySnapshot;
  subscribe(on: () => void): () => void;
}

export interface ActivitySnapshot {
  readonly downloads: readonly Download[];
  /** the files that failed since they were last asked for, and why */
  readonly failed: Readonly<Record<string, string>>;
}

const EMPTY: ActivitySnapshot = { downloads: [], failed: {} };

export function createNetActivity(): NetActivity {
  const map = new Map<string, Download>();
  let failed: Record<string, string> = {};
  let snap = EMPTY;
  const subs = new Set<() => void>();
  const set = () => { snap = { downloads: [...map.values()], failed }; subs.forEach((f) => f()); };
  const patch = (key: string, p: Partial<Download>) => { const d = map.get(key); if (d) { map.set(key, { ...d, ...p }); set(); } };
  return {
    queued(key, priority, total) {
      if (key in failed) { const { [key]: _gone, ...rest } = failed; failed = rest; }
      map.set(key, { key, priority, waiting: true, loaded: 0, total, attempt: 1 });
      set();
    },
    started: (key) => patch(key, { waiting: false }),
    raise(key, priority) { const d = map.get(key); if (d && priority < d.priority) patch(key, { priority }); },
    progress: (key, loaded, total) => patch(key, { loaded, ...(total !== null ? { total } : {}) }),
    retry: (key, attempt) => patch(key, { attempt }),
    done(key) { if (map.delete(key)) set(); },
    failed(key, why) { map.delete(key); failed = { ...failed, [key]: why }; set(); },
    snapshot: () => snap,
    subscribe(on) { subs.add(on); return () => { subs.delete(on); }; },
  };
}

/** The bytes still to come for the downloads a screen waits on (`keys`), and their total (null: a size unknown). */
export function pendingOf(s: ActivitySnapshot, keys: (k: string) => boolean): { loaded: number; total: number | null; files: number; waiting: number } {
  const ds = s.downloads.filter((d) => keys(d.key));
  const total = ds.every((d) => d.total !== null) ? ds.reduce((a, d) => a + (d.total ?? 0), 0) : null;
  return { loaded: ds.reduce((a, d) => a + d.loaded, 0), total, files: ds.length, waiting: ds.filter((d) => d.waiting).length };
}
