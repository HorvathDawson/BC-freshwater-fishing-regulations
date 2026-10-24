/**
 * THE RECENT ERROR LOG: a small in-memory ring of the last errors the app logged, for a problem report's diagnostics
 * (core/menu/report.ts `Diagnostics.errors`). It lives in memory only — never written to disk, never sent anywhere. The
 * reader sees every line in the report form and can switch the whole log off before sending; NOTHING here reports
 * on its own (owner 2026-10-21: no background telemetry).
 *
 * The app shell tees `console.error` into it once (`teeConsoleErrors`), so every error the app already logs — the map's
 * `onError`, a failed fetch, a refused release — is in the ring without each screen knowing about it.
 */
import { pad2 } from "@app/core";

export interface ErrorLog {
  /** Add one line (an Error's message, or the logged values joined). */
  record(...values: unknown[]): void;
  /** The ring, oldest first, as the report shows it: "HH:MM message". */
  recent(): string[];
  clear(): void;
}

/** One logged value as words: an Error's message (its name when it has none), a string as is, anything else as JSON. */
function word(v: unknown): string {
  if (v instanceof Error) return v.message || v.name;
  if (typeof v === "string") return v;
  try { return JSON.stringify(v) ?? String(v); } catch { return String(v); }
}

/** A ring of the last `size` lines, each cut to `width` characters. `now` is for tests. */
export function createErrorLog(size = 20, width = 240, now: () => number = Date.now): ErrorLog {
  const ring: string[] = [];
  return {
    record(...values) {
      const d = new Date(now());
      const text = values.map(word).join(" ").replace(/\s+/g, " ").trim();
      if (!text) return;
      const line = `${pad2(d.getHours())}:${pad2(d.getMinutes())} ${text}`;
      ring.push(line.length > width ? `${line.slice(0, width - 1)}…` : line);
      if (ring.length > size) ring.splice(0, ring.length - size);
    },
    recent: () => [...ring],
    clear() { ring.length = 0; },
  };
}

/** The app's one log. */
export const ERROR_LOG: ErrorLog = createErrorLog();

/**
 * Tee `console.error` into `log` (the app shell calls this once, at start). The console still gets every call, as
 * before. Returns the undo (tests). Calling it twice on the same console tees once.
 */
export function teeConsoleErrors(c: Pick<Console, "error">, log: ErrorLog = ERROR_LOG): () => void {
  const was = c.error as Console["error"] & { __teed?: boolean };
  if (was.__teed) return () => {};
  const teed = Object.assign((...args: unknown[]) => {
    try { log.record(...args); } catch { /* the log never breaks logging */ }
    was.apply(c, args);
  }, { __teed: true });
  c.error = teed;
  return () => { if (c.error === teed) c.error = was; };
}
