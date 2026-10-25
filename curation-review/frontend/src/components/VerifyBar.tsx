import type { MarkView, Reviewed, Vintage, VerifyStatus } from "../types";

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
  /** T133: whether the reach run, bundle and release on disk are this entry's */
  vintage?: Vintage;
  /** T184: the human-reviewed state — who verified it on every check, when, and whether it holds */
  reviewed?: Reviewed | null;
}

/* The marks in force on this entry (T97) — READ ONLY. A mark lives in the append-only store
   `marks.jsonl`, held against the entry's fingerprints; when something it was held against
   changes it reads "changed since marked" with the typed reasons. Marking is the review screen's
   (phase 3, T134: `POST /api/marks`), not this bar's.
   ONE VINTAGE (T133): while the entry AWAITS ITS BUILD the bar says so, why, and what the owner
   runs; the binding and the answer on screen are then another vintage's, and only T and R can be
   marked. It clears by itself once the rebuild lands (the server re-reads the digests). */
export function VerifyBar({ entryId, marks, nav, vintage, reviewed }: Props) {
  const shown = marks ?? [];
  return (
    <div className="verify-bar" data-testid="verify-bar" data-entry={entryId}>
      {/* HUMAN-REVIEWED (T184): verified on every check (T R P A) — and it says so until the entry's
          text, rules or extent change, when it is back for a re-check (T97: the mark goes stale). */}
      {reviewed && (
        <div className={`verify-row reviewed${reviewed.holds ? "" : " recheck"}`} data-testid="reviewed">
          {reviewed.holds ? (
            <span className="verify-chip verified">
              ✓ Human-reviewed by {reviewed.by.join(", ")} on {reviewed.on}
            </span>
          ) : (
            <span className="verify-chip stale">
              Was human-reviewed by {reviewed.by.join(", ")} on {reviewed.on} — changed since: re-check it
            </span>
          )}
        </div>
      )}
      {vintage && vintage.state !== "current" && (
        <div className="verify-row awaiting-build" data-testid="awaiting-build">
          <span className="verify-chip stale" title={vintage.reasons.join("; ")}>
            awaiting build — only T and R can be marked
          </span>
          <span className="dim">{vintage.reasons.join("; ")}
            {vintage.splits.length > 0 && ` (${vintage.splits.join(", ")})`}</span>
          {vintage.commands.map((c) => <code key={c} className="cmd">{c}</code>)}
        </div>
      )}
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
