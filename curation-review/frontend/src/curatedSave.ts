/* WRITE-THROUGH (T325, owner 2026-10-30): every saved mark lands in its domain's final curated file by itself — the
   server runs the domain's apply after the save (in the request for sloughs / names; debounced in the background for
   DFO / entries / tidal, whose binding check takes seconds). This store holds what the server said: "saving to
   curated…" while a background apply runs, and, per item, a refusal (the mark is KEPT; the reason says why it is
   not in the curated file yet) or a proposal waiting on its side rebuild. Read from POST /api/marks
   (`write_through`) and GET /api/review/write-through. */

export interface CuratedOutcome { state: "saved" | "saving" | "refused" | "waiting"; why?: string | null; domain?: string | null; at?: string; sha?: string }
export interface DomainState { state: string; at?: string; why?: string | null; files?: string[]; waiting?: string[]; took_s?: number }
export interface WriteThroughStatus {
  enabled: boolean; saving: boolean;
  domains: Record<string, DomainState>;
  outcomes: Record<string, CuratedOutcome>;
  attention: Record<string, CuratedOutcome>;
}
export interface WriteThroughReply { state: string; domain?: string | null; why?: string | null; files?: string[]; outcomes?: Record<string, CuratedOutcome> }

let status: WriteThroughStatus = { enabled: false, saving: false, domains: {}, outcomes: {}, attention: {} };
const listeners = new Set<(s: WriteThroughStatus) => void>();
let timer: number | null = null;

function publish(s: WriteThroughStatus) { status = s; listeners.forEach((l) => l(s)); }

export function current(): WriteThroughStatus { return status; }
export function subscribe(l: (s: WriteThroughStatus) => void): () => void {
  listeners.add(l);
  return () => { listeners.delete(l); };
}

/** read the server's state; poll while an apply is pending or running */
export async function refresh(): Promise<void> {
  try {
    const r = await fetch("/api/review/write-through");
    if (!r.ok) return;
    publish((await r.json()) as WriteThroughStatus);
  } catch { /* the server is away: the refused-saves banner says so on the next save */ }
  if (status.saving && timer == null) {
    timer = window.setTimeout(() => { timer = null; void refresh(); }, 1500);
  }
}

/** called by `api.postMark` with the reply's `write_through` */
export function noteReply(wt: WriteThroughReply | undefined) {
  if (!wt || wt.state === "off" || wt.state === "none") return;
  const outcomes = { ...status.outcomes, ...(wt.outcomes ?? {}) };
  publish({ ...status, saving: status.saving || wt.state === "saving", outcomes });
  void refresh();
}

export function keyOf(scope: string, target: string, ruleId?: string | null) {
  return `${scope}|${target}|${ruleId ?? ""}`;
}
