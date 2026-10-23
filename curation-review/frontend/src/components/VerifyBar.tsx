import type { MarkView, VerifyStatus } from "../types";

/** Where the open entry sits in the list being worked through. */
export interface QueueNav {
  pos: number;          // 1-based; 0 when the entry is not in the current list
  total: number;
  onPrev?: () => void;
  onNext?: () => void;
}

const WORDS: Record<VerifyStatus, string> = {
  unmarked: "not marked",
  verified: "verified against the book",
  wrong: "marked wrong",
  unsure: "marked unsure",
  deferred: "deferred",
  stale: "changed since marked",
};

interface Props {
  entryId: string;
  marks?: MarkView[];
  nav?: QueueNav;
}

/* The marks in force on this entry (T97) — READ ONLY. A mark lives in the append-only store
   `marks.jsonl`, held against the entry's fingerprints; when something it was held against
   changes it reads "changed since marked" with the typed reasons. Marking is the review screen's
   (phase 3, T134: `POST /api/marks`), not this bar's. */
export function VerifyBar({ entryId, marks, nav }: Props) {
  const shown = marks ?? [];
  return (
    <div className="verify-bar" data-testid="verify-bar" data-entry={entryId}>
      <div className="verify-row">
        {shown.length === 0 && (
          <span className="verify-chip unmarked" data-testid="verify-status">{WORDS.unmarked}</span>
        )}
        {shown.map((m) => (
          <span key={`${m.scope}:${m.rule_id ?? ""}`} className={`verify-chip ${m.status}`}
            data-testid="verify-status"
            title={`${m.reviewer}, ${m.date}${m.note ? ` — ${m.note}` : ""}`}>
            {m.rule_id ? `${m.rule_id}: ` : ""}{WORDS[m.status]}
            {m.status === "stale" && m.reasons.length > 0 && ` (${m.reasons.join(", ")})`}
          </span>
        ))}
        {nav && (
          <span className="verify-nav">
            <button className="btn" disabled={!nav.onPrev} onClick={nav.onPrev} aria-label="previous entry">←</button>
            <span className="dim">{nav.pos ? `${nav.pos} / ${nav.total}` : `– / ${nav.total}`}</span>
            <button className="btn" disabled={!nav.onNext} onClick={nav.onNext} aria-label="next entry">→</button>
          </span>
        )}
      </div>
    </div>
  );
}
