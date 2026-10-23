import { useEffect, useRef, useState } from "react";
import { api, type EntryRefused } from "../api";
import type { Check, MarkPost, MarkView } from "../types";
import { answerKey, checkKey, getReviewer, isTyping, nextCheck, setReviewer } from "../review";
import { blankChecklist, type ChecklistState } from "./Checklist";

export interface MarkTarget {
  scope: "entry" | "rule" | "split";
  target: string;
  rule_id?: string | null;
  checks: Check[];
  /** checks that cannot be answered now, and why */
  held: Partial<Record<Check, string>>;
}

interface Opts {
  /** what `key` names — the checklist state is kept per key (a zone entry: per rule) */
  key: string;
  mark: MarkTarget;
  /** after a mark is recorded */
  onRecorded: (got: MarkView) => void;
  /** `o`: the app page to open, if any */
  link?: string | null;
  /** `m` */
  onToggleMap?: () => void;
  /** reset every checklist when this changes (another entry / split opened) */
  resetOn: string;
}

/* THE MARKING, ONE PLACE (T134): the checklist state per target, the keys, and the one write —
   POST /api/marks. Used by the entry review and the split card alike. The keys: t r p a pick a
   check; y n ? answer it and move to the next unanswered one; Enter records the mark; o opens the
   app's page; m the map. (j k u move through the list — the screen's.) */
export function useMarking({ key, mark, onRecorded, link, onToggleMap, resetOn }: Opts) {
  const enabled = (c: Check) => !mark.held[c];
  const first = mark.checks.find(enabled) ?? mark.checks[0];
  const [states, setStates] = useState<Record<string, ChecklistState>>({});
  const [reviewer, setName] = useState(getReviewer);
  const [busy, setBusy] = useState(false);
  const [errors, setErrors] = useState<string[]>([]);
  const [done, setDone] = useState("");
  useEffect(() => { setStates({}); setErrors([]); setDone(""); }, [resetOn]);

  const state = states[key] ?? blankChecklist(first);
  const setState = (s: ChecklistState) => setStates((m) => ({ ...m, [key]: s }));

  // one mark in flight at a time: Enter held down, or Enter on the focused button, posts once
  const inFlight = useRef(false);
  async function submit(defer = false) {
    if (inFlight.current) return;
    setErrors([]);
    if (!reviewer.trim()) { setErrors(["your name first — every mark names its reviewer"]); return; }
    inFlight.current = true;
    const answers = Object.fromEntries(mark.checks.filter((c) => state.answers[c])
      .map((c) => [c, state.answers[c]!]));
    const body: MarkPost = {
      scope: mark.scope, target: mark.target, rule_id: mark.rule_id ?? null, answers,
      problem: state.problem || null, note: state.note, reviewer: reviewer.trim(),
      ...(defer ? { verdict: "deferred" as const } : {}),
    };
    setBusy(true);
    try {
      const got = await api.postMark(body);
      setDone(`${mark.rule_id ? mark.rule_id + ": " : ""}${got.status}`);
      onRecorded(got);
    } catch (e) {
      const fe = (e as EntryRefused).fieldErrors;
      setErrors(fe ? fe.map((x) => (x.path ? `${x.path}: ` : "") + x.msg) : [String(e)]);
    } finally {
      setBusy(false);
      inFlight.current = false;
    }
  }

  const latest = useRef({ state, submit, link, onToggleMap, mark });
  latest.current = { state, submit, link, onToggleMap, mark };
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      const L = latest.current;
      const ok = (c: Check) => !L.mark.held[c];
      const c = checkKey(ev.key);
      if (c && L.mark.checks.includes(c) && ok(c)) {
        setState({ ...L.state, active: c });
        ev.preventDefault();
        return;
      }
      const a = answerKey(ev.key);
      if (a && ok(L.state.active)) {
        const answers = { ...L.state.answers, [L.state.active]: a };
        setState({ ...L.state, answers, active: nextCheck(L.mark.checks, answers, L.state.active, ok) });
        ev.preventDefault();
        return;
      }
      if (ev.key === "Enter") {
        // Enter on a focused button or link is that control's own click
        if ((ev.target as HTMLElement | null)?.closest?.("button, a")) return;
        if (ev.repeat) return;
        void L.submit();
        ev.preventDefault();
        return;
      }
      if (ev.key === "o" && L.link) { window.open(L.link, "_blank", "noopener"); ev.preventDefault(); return; }
      if (ev.key === "m" && L.onToggleMap) { L.onToggleMap(); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, resetOn]);

  return {
    state, setState, submit, busy, errors, done, reviewer,
    setReviewer: (n: string) => { setName(n); setReviewer(n); },
  };
}

interface BarProps {
  checks: Check[];
  m: ReturnType<typeof useMarking>;
  held: Partial<Record<Check, string>>;
}

/** THE VERDICT BAR — always visible (sticky): the answers so far, the reviewer, record (Enter), defer. */
export function VerdictBar({ checks, m, held }: BarProps) {
  const s = m.state;
  return (
    <div className="verdict-bar" data-testid="verdict-bar">
      <span className="verdict-answers">
        {checks.map((c) => (
          <span key={c} className={`va va-${s.answers[c] === "?" ? "q" : s.answers[c] ?? "none"}`}>
            {c} {s.answers[c] ?? (held[c] ? "–" : "·")}
          </span>
        ))}
      </span>
      <input className="reviewer" value={m.reviewer} placeholder="your name" aria-label="reviewer"
        onChange={(e) => m.setReviewer(e.target.value)} />
      <button className="btn primary" disabled={m.busy} onClick={() => void m.submit()}
        data-testid="record-mark" title="Enter">record mark ↵</button>
      <button className="btn" disabled={m.busy || !s.note.trim()} onClick={() => void m.submit(true)}
        title="defer: the reviewer's own call, with a note saying why">defer</button>
      {m.done && <span className="toast" data-testid="mark-done">{m.done}</span>}
      {m.errors.length > 0 && <span className="field-error" data-testid="mark-errors">{m.errors.join(" · ")}</span>}
      <span className="dim keys">t r p a · y n ? · ↵ · j k · u · o · m</span>
    </div>
  );
}
