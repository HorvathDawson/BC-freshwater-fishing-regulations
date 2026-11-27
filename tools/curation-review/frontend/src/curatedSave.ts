/* WRITE-THROUGH (T325, owner 2026-10-30): every saved mark lands in its domain's final curated file by itself — the
   server runs the domain's apply after the save (in the request for sloughs / names; debounced in the background for
   DFO / entries / tidal, whose binding check takes seconds). This store holds what the server said: "saving to
   curated…" while a background apply runs, and, per item, a refusal (the mark is KEPT; the reason says why it is
   not in the curated file yet) or a proposal waiting on its side rebuild. Read from POST /api/marks
   (`write_through`) and GET /api/review/write-through. */

export interface CuratedOutcome { state: "saved" | "saving" | "refused" | "waiting"; why?: string | null; domain?: string | null; at?: string; sha?: string }
export interface DomainState { state: string; at?: string; why?: string | null; files?: string[]; waiting?: string[]; took_s?: number;
  /** T545 / T562: every row the last apply took OUT of the domain's curated file, by name; and its commit */
  dropped?: string[]; sha?: string | null }
/** T562 item 4: GET /api/review/not-landed — the landed check (`apply --check`), run once per server process */
export interface NotLanded { state: "idle" | "checking" | "done" | "failed"; ok?: boolean; why?: string; allowed?: number;
  not_landed?: { domain: string; status: string; why: string; marks: string[] }[] }
/** what the banner says of it: nothing while clean, the rows to act on (a mark that only WAITS is listed apart) */
export function notLandedWords(n: NotLanded): { tone: "warn" | "info" | "error"; head: string; rows: string[] } | null {
  if (n.state === "failed") return { tone: "error", head: `The check that every mark has landed could not run: ${n.why ?? "no reason given"}`, rows: [] };
  if (n.state !== "done" || !n.not_landed?.length) return null;
  const act = n.not_landed.filter((r) => r.status !== "waiting");
  const rows = (act.length ? act : n.not_landed).map((r) => `${r.status} · ${r.domain}${r.marks.length ? ` · ${r.marks.join(", ")}` : ""} — ${r.why}`);
  return act.length
    ? { tone: "warn", head: `${act.length} saved mark${act.length === 1 ? " is" : "s are"} NOT in the curated files${n.not_landed.length > act.length ? ` (and ${n.not_landed.length - act.length} wait for a cut to be built)` : ""}`, rows }
    : { tone: "info", head: `${n.not_landed.length} saved mark${n.not_landed.length === 1 ? " waits" : "s wait"} for a cut to be built before landing`, rows };
}
/** a file domain's last save, in words: the rows it took out of its file, and its commit */
export function domainSaveWords(domains: Record<string, DomainState>): { dropped: string[]; commits: string[] } {
  const dropped = Object.entries(domains).flatMap(([d, s]) => (s.dropped ?? []).map((x) => `${d}: ${x}`));
  const commits = Object.entries(domains).flatMap(([d, s]) => (s.sha ? [`${d}: ${(s.files ?? []).join(", ") || "its file"} · commit ${s.sha.slice(0, 9)}`] : []));
  return { dropped, commits };
}
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
