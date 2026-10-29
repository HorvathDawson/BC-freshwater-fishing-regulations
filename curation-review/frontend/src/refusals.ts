/* REFUSED SAVES (owner 2026-10-28): a save the server refuses — or that never reaches it — must be
   impossible to miss. Every failed `api.postMark` lands here (with its payload, so it can be
   retried) and stays, across reloads (localStorage, per viewer), until a save of the same item
   succeeds or the reviewer drops it. The server also logs each refusal to
   data/generated/review/refused-saves.jsonl. */
import type { MarkPost } from "./types";

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

export function drop(key: string) { save(memory.filter((r) => r.key !== key)); }

export function subscribe(f: (r: Refusal[]) => void): () => void {
  listeners.add(f);
  return () => { listeners.delete(f); };
}
