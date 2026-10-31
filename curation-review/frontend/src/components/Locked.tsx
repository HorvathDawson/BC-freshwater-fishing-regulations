import { useEffect, useState } from "react";
import { TopNav, type TabId } from "./TopNav";

/* T325: CLOSED (LOCKED) REVIEW DOMAINS. A finished review is closed with `python -m pipeline.regs.review.lock
   <domain>`: its marks stay in marks.jsonl, untouched, but no longer move anything — the decisions live in the
   domain's final curated file. The server lists them (GET /api/review/closed); a locked tab shows this notice in
   place of its queue, and the server refuses a mark on it (423). Reopen = remove the entry (`lock --reopen`). */

export interface ClosedEntry { domain: string; scopes: string[]; date: string; by: string; why: string; final: string | null }
export type Closed = Record<string, ClosedEntry>;

let cache: Closed | null = null;
let pending: Promise<Closed> | null = null;
const listeners = new Set<(c: Closed) => void>();

function load(): Promise<Closed> {
  if (!pending) {
    pending = fetch("/api/review/closed")
      .then((r) => (r.ok ? r.json() : { closed: {} }))
      .then((j) => (j.closed ?? {}) as Closed)
      .catch(() => ({}) as Closed)
      .then((c) => { cache = c; listeners.forEach((l) => l(c)); return c; });
  }
  return pending;
}

/** the closed domains (read once per page load) */
export function useClosed(): Closed {
  const [c, setC] = useState<Closed>(cache ?? {});
  useEffect(() => {
    listeners.add(setC);
    load();
    return () => { listeners.delete(setC); };
  }, []);
  return c;
}

/** the notice a locked tab shows in place of its queue */
export function LockedView({ domain, entry, active, sub }: { domain: string; entry: ClosedEntry; active: TabId; sub?: string }) {
  return (
    <div className="app">
      <TopNav active={active} sub={sub} />
      <div className="main">
        <div className="detail">
          <div className="empty-state" data-testid="locked">
            <h3>Locked — the {domain} review is closed</h3>
            <p>Closed {entry.date} by {entry.by}: {entry.why}.</p>
            <p>{entry.final ? <>Its decisions live in <code>{entry.final}</code>.</> : <>Nothing reads these calls; there is no final file.</>}
              {" "}The marks stay in marks.jsonl as history and can no longer be changed here.</p>
            <p className="dim">Reopen: <code className="cmd">python -m pipeline.regs.review.lock --reopen {domain}</code></p>
          </div>
        </div>
      </div>
    </div>
  );
}
