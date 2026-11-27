/* REFUSED SAVES (owner 2026-10-28): a save the server refuses — or that never reaches it — must be
   impossible to miss. Every failed `api.postMark` lands here (with its payload, so it can be
   retried) and stays, across reloads (localStorage, per viewer), until a save of the same item
   succeeds or the reviewer drops it. The server also logs each refusal to
   data/generated/review/refused-saves.jsonl. */
import type { MarkPost, MarkView } from "./types";

export interface Refusal {
  key: string;          // scope|target|rule_id
  at: string;
  scope: string;
  target: string;
  reasons: string[];
  mark: MarkPost;
}

const KEY = "review-refused-saves";
const listeners = new Set<(r: Refusal[]) => void>();

export function keyOf(m: Pick<MarkPost, "scope" | "target" | "rule_id">): string {
  return `${m.scope}|${m.target}|${m.rule_id ?? ""}`;
}

export function refusals(): Refusal[] {
  try { return JSON.parse(localStorage.getItem(KEY) ?? "[]") as Refusal[]; } catch { return []; }
}

function save(list: Refusal[]) {
  try { localStorage.setItem(KEY, JSON.stringify(list)); } catch { /* private window: memory only */ }
  memory = list;
  listeners.forEach((f) => f(list));
}
let memory: Refusal[] = refusals();

export function current(): Refusal[] { return memory; }

export function addRefusal(mark: MarkPost, reasons: string[]) {
  const key = keyOf(mark);
  save([...memory.filter((r) => r.key !== key),
    { key, at: new Date().toISOString(), scope: mark.scope, target: mark.target, reasons, mark }]);
}

export function resolve(mark: Pick<MarkPost, "scope" | "target" | "rule_id">) {
  const key = keyOf(mark);
  if (memory.some((r) => r.key === key)) save(memory.filter((r) => r.key !== key));
}

/** THE SAVE THAT TOOK A REFUSAL'S PLACE (owner 2026-11-09: a refusal kept in one browser outlived the owner's later
 *  saves from another): the mark in force on the refused item (`marks` from GET /api/marks on its target), when it
 *  was saved on or after the refusal's day — `sameDay` when on that very day (a mark holds only its date, so which
 *  came first cannot be told). Only SAID: the refusal stays listed until the reviewer drops it. */
export function supersededBy(r: Refusal, marks: Pick<MarkView, "scope" | "rule_id" | "date" | "verdict">[]):
    { date: string; verdict: string; sameDay: boolean } | null {
  const day = new Date(r.at).toLocaleDateString("en-CA");          // the refusal's own day, YYYY-MM-DD
  const m = marks.find((x) => x.scope === r.scope && (x.rule_id ?? null) === (r.mark.rule_id ?? null));
  return m && m.date >= day ? { date: m.date, verdict: m.verdict, sameDay: m.date === day } : null;
}

export function drop(key: string) { save(memory.filter((r) => r.key !== key)); }

export function subscribe(f: (r: Refusal[]) => void): () => void {
  listeners.add(f);
  return () => { listeners.delete(f); };
}
