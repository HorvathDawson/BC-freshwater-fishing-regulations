// The review screen's small pure helpers (T134): no React, no fetch — so each is one function a
// reader can check. What a verdict IS, what is stale, what tier an entry is in: the server's
// (`pipeline.regs.review`), never decided here.

import type { Check, CheckAnswer, QueueRow } from "./types";

/** The four checks, in the order PLAN §2 works them, with the question each one asks. */
export const QUESTIONS: Record<Check, string> = {
  T: "Text — the book says what we stored (the row / passage against the raw name and the regs)",
  R: "Rules — every clause is a rule, nothing invented (species, quota, size, dates, gear, tributaries)",
  P: "Place — the rules land on the right water and extent: matched, every split the map labels (a wrong cut is Place: wrong here), ambiguous cut, tributary walk",
  A: "Answer — the app's page for a sample day says the same thing, open or closed",
};

/** A key press is the reviewer's only while no form control has the focus. */
export function isTyping(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null;
  if (!el || !el.tagName) return false;
  return el.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(el.tagName);
}

/** The key's answer, or null: y / n / ? (shift-/ on most layouts). */
export function answerKey(key: string): CheckAnswer | null {
  return key === "y" ? "y" : key === "n" ? "n" : key === "?" ? "?" : null;
}

/** The check a key selects (t r p a), or null. */
export function checkKey(key: string): Check | null {
  const k = key.toUpperCase();
  return k === "T" || k === "R" || k === "P" || k === "A" ? (k as Check) : null;
}

/** After answering `at`, the next check still unanswered (wrapping), or `at` when all are. */
export function nextCheck(checks: Check[], answers: Partial<Record<Check, CheckAnswer>>,
                          at: Check, enabled: (c: Check) => boolean): Check {
  const i = checks.indexOf(at);
  for (let k = 1; k <= checks.length; k++) {
    const c = checks[(i + k) % checks.length];
    if (enabled(c) && !answers[c]) return c;
  }
  return at;
}

/** THE CURRENT ROW STAYS IN THE LIST after marking (PLAN §4: `pos` read "–" when a mark took the
 *  entry out of a "to do" filter). A reload that no longer lists the open entry keeps its old row
 *  at its old place, so j / k carry on from where the reviewer is. */
export function keepSelected(fresh: QueueRow[], old: QueueRow[], selected: string | null): QueueRow[] {
  if (!selected || fresh.some((r) => r.entry_id === selected)) return fresh;
  const at = old.findIndex((r) => r.entry_id === selected);
  if (at < 0) return fresh;
  // its place: after the last row that preceded it before and is still listed
  const before = new Set(old.slice(0, at).map((r) => r.entry_id));
  let pos = 0;
  fresh.forEach((r, i) => { if (before.has(r.entry_id)) pos = i + 1; });
  return [...fresh.slice(0, pos), old[at], ...fresh.slice(pos)];
}

/** A ZONE ENTRY'S NEXT RULE after `keys[at]` is marked: the next one still open after it, then
 *  WRAPPING to an open one before it; -1 when none is open — only then is the entry done and the
 *  screen moves on (a reviewer who clicked rule 3 first still gets rules 1 and 2). */
export function nextOpenRule(keys: string[], at: number, isOpen: (k: string) => boolean): number {
  for (let d = 1; d < keys.length; d++) {
    const i = (at + d) % keys.length;
    if (isOpen(keys[i])) return i;
  }
  return -1;
}

/** The index of the next row whose mark is `unsure` after `from` (wrapping), or -1. */
export function nextUnsure<T extends { verify?: string; status?: string }>(rows: T[], from: number): number {
  for (let k = 1; k <= rows.length; k++) {
    const i = (from + k + rows.length) % rows.length;
    const st = rows[i].verify ?? rows[i].status;
    if (st === "unsure") return i;
  }
  return -1;
}

// ---- the reviewer's pace, for the ETA ------------------------------------------------------------

const PACE_KEY = "curation.review.pace";
const REVIEWER_KEY = "curation.review.reviewer";
/** the pace is the median of the reviewer's last this-many marked entries */
export const PACE_WINDOW = 20;
/** no ETA before this many timed entries — one fast entry is not a pace */
export const PACE_MIN = 3;

function readList(): number[] {
  try {
    const v = JSON.parse(localStorage.getItem(PACE_KEY) ?? "[]");
    return Array.isArray(v) ? v.filter((x) => typeof x === "number" && x > 0) : [];
  } catch { return []; }
}

/** Record how long one entry took (seconds, from opening it to its last mark). */
export function recordPace(seconds: number): number[] {
  const list = [...readList(), Math.round(seconds * 10) / 10].slice(-PACE_WINDOW);
  try { localStorage.setItem(PACE_KEY, JSON.stringify(list)); } catch { /* private window */ }
  return list;
}

export function paceSamples(): number[] { return readList(); }

export function median(xs: number[]): number | null {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/** "1 h 05 m", "4 m 10 s", "38 s" */
export function duration(seconds: number): string {
  const s = Math.round(seconds);
  if (s < 60) return `${s} s`;
  if (s < 3600) return `${Math.floor(s / 60)} m ${String(s % 60).padStart(2, "0")} s`;
  return `${Math.floor(s / 3600)} h ${String(Math.floor((s % 3600) / 60)).padStart(2, "0")} m`;
}

/** The ETA for `todo` entries at the reviewer's pace, or why there is none yet. */
export function eta(todo: number, samples: number[]): string {
  if (todo === 0) return "done";
  if (samples.length < PACE_MIN) return `ETA after ${PACE_MIN} timed entries`;
  return `ETA ${duration(todo * (median(samples) ?? 0))}`;
}

export function getReviewer(): string {
  try { return localStorage.getItem(REVIEWER_KEY) ?? ""; } catch { return ""; }
}
export function setReviewer(name: string): void {
  try { localStorage.setItem(REVIEWER_KEY, name); } catch { /* private window */ }
}
