import type { Check, CheckAnswer } from "../types";
import { QUESTIONS } from "../review";

/** One target's T/R/P/A answers being made (an entry, one zone rule, or one split point). */
export interface ChecklistState {
  answers: Partial<Record<Check, CheckAnswer>>;
  active: Check;
  problem: string;
  note: string;
}

export function blankChecklist(first: Check): ChecklistState {
  return { answers: {}, active: first, problem: "", note: "" };
}

interface Props {
  /** what is being marked, in words ("this entry", "rule adam_river.r1", "split point …") */
  target: string;
  checks: Check[];
  state: ChecklistState;
  onChange: (s: ChecklistState) => void;
  /** a check that cannot be answered now, and why (awaiting build: P and A wait) */
  held: Partial<Record<Check, string>>;
  problems: Record<Check, string[]>;
  /** the mark in force on the target, in words, if any */
  current?: string;
}

const WORD: Record<Check, string> = { T: "Text", R: "Rules", P: "Place", A: "Answer" };

/* THE CHECKLIST (PLAN §2), COMPACT: one row of four segmented controls (a one-word label, then
   y / n / ?). Keys: t r p a pick a check, y n ? answer it and move on; Enter (the verdict bar)
   records the mark. The long questions are the tooltip on each label and the "what do these mean"
   fold below. What the answers amount to — verified, wrong, unsure — is decided by the server
   (`marks.verdict_from_answers`); this only asks for what a "wrong" needs (a problem and a note)
   when a check is answered n. */
export function Checklist({ target, checks, state, onChange, held, problems, current }: Props) {
  const set = (patch: Partial<ChecklistState>) => onChange({ ...state, ...patch });
  return (
    <div className="checklist" data-testid="checklist">
      <div className="checklist-head">
        <strong>Check {target}</strong>
        {current && <span className="dim"> · now: {current}</span>}
      </div>
      <div className="check-seg-row">
        {checks.map((c) => {
          const a = state.answers[c];
          const why = held[c];
          return (
            <div key={c} data-check={c} title={why ? `${QUESTIONS[c]} — ${why}` : QUESTIONS[c]}
              className={`check-seg${state.active === c ? " active" : ""}${why ? " held" : ""}`}
              onClick={() => !why && set({ active: c })}>
              <span className="check-seg-label"><kbd>{c.toLowerCase()}</kbd> {WORD[c]}</span>
              {why ? <span className="dim check-seg-held">waits for the build</span> : (
                <span className="seg">
                  {(["y", "n", "?"] as CheckAnswer[]).map((x) => (
                    <button key={x} type="button" className={`ans ans-${x === "?" ? "q" : x}${a === x ? " on" : ""}`}
                      aria-pressed={a === x} aria-label={`${c} ${x}`}
                      onClick={(ev) => {
                        ev.stopPropagation();
                        // GIVE THE FOCUS BACK (T142): a focused button takes Enter as its own click,
                        // so after a click the next Enter re-answered this check instead of
                        // recording the mark. Keys-only use never focused it.
                        ev.currentTarget.blur();
                        set({ active: c, answers: { ...state.answers, [c]: x } });
                      }}>
                      {x}
                    </button>
                  ))}
                </span>
              )}
            </div>
          );
        })}
      </div>
      <details className="check-help">
        <summary className="dim">what do these mean</summary>
        <ul>
          {checks.map((c) => <li key={c}><kbd>{c.toLowerCase()}</kbd> {QUESTIONS[c]}</li>)}
          <li className="dim"><kbd>y</kbd> yes · <kbd>n</kbd> wrong (needs a problem and a note) · <kbd>?</kbd> unsure</li>
        </ul>
      </details>
      {checks.some((c) => state.answers[c] === "n") && (
        <div className="check-wrong">
          <select value={state.problem} aria-label="problem"
            onChange={(e) => set({ problem: e.target.value })}>
            <option value="">problem…</option>
            {checks.filter((c) => state.answers[c] === "n").map((c) => (
              <optgroup key={c} label={c}>
                {(problems[c] ?? []).map((p) => <option key={p} value={p}>{p}</option>)}
              </optgroup>
            ))}
          </select>
        </div>
      )}
      <textarea className="check-note" rows={2} value={state.note} aria-label="note"
        placeholder={checks.some((c) => state.answers[c] === "n")
          ? "what is wrong (needed for a wrong mark)" : "note (optional; needed to defer)"}
        onChange={(e) => set({ note: e.target.value })} />
    </div>
  );
}
