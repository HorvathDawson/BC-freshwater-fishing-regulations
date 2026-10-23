import { useEffect, useState } from "react";
import type { AnswerLink, Check, EntryDetail, MarkView, Mode, ReviewView } from "../types";
import { Checklist } from "./Checklist";
import { useMarking, VerdictBar } from "./useMarking";
import { nextOpenRule } from "../review";

interface Props {
  detail: EntryDetail;
  view: ReviewView;
  mode: Mode;
  /** a mark was recorded; `final` when the entry is done (a water row's mark, a zone entry's
   *  last open rule) — the screen then records the pace and moves on */
  onMarked: (final: boolean) => void;
  /** `m`: show the map large / back */
  onToggleMap: () => void;
  /** the day the app link opens on */
  onDate: (day: string) => void;
}

const WORD: Record<string, string> = {
  unmarked: "not marked", verified: "verified", wrong: "wrong", unsure: "unsure",
  deferred: "deferred", stale: "changed since marked",
};

export function markWords(m?: MarkView): string | undefined {
  if (!m) return undefined;
  return WORD[m.status] + (m.status === "stale" && m.reasons.length ? ` (${m.reasons.join(", ")})` : "");
}

const open = (m?: MarkView) => !m || m.status === "stale" || m.status === "unmarked";

/** P and A wait while the entry awaits its build (T133: only T and R can be marked). */
export function heldWhileAwaiting(awaiting: boolean): Partial<Record<Check, string>> {
  return awaiting
    ? { P: "awaiting build — where it binds is another vintage's",
        A: "awaiting build — the app's answer is another vintage's" }
    : {};
}

/* THE REVIEW OF ONE ENTRY (T134): the checklist, the answer link, the verdict bar. A water row is
   marked once (scope entry); a ZONE entry rule by rule (scope rule, PLAN §6 Q5), each rule with
   its impact ("binds N sections in region R"). The one write is POST /api/marks (`useMarking`). */
export function EntryReview({ detail, view, mode, onMarked, onToggleMap, onDate }: Props) {
  const zone = view.kind === "zone";
  const rules = detail.entry.rules ?? [];
  const keys = zone ? rules.map((r) => r.rule_id) : [""];
  const marksBy: Record<string, MarkView | undefined> = {};
  for (const m of detail.marks ?? []) marksBy[m.scope === "rule" ? m.rule_id ?? "" : ""] = m;
  const firstOpen = () => Math.max(0, keys.findIndex((k) => open(marksBy[k])));
  const [at, setAt] = useState(firstOpen);
  useEffect(() => { setAt(firstOpen()); },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [detail.entry.entry_id]);
  const key = keys[at] ?? "";
  const held = heldWhileAwaiting(view.vintage.state !== "current");
  const checks = mode.checks[zone ? "rule" : "entry"];
  const link: AnswerLink = (zone ? view.rules.find((r) => r.rule_id === key)?.answer : view.answer)
    ?? { url: null };
  const m = useMarking({
    key, resetOn: detail.entry.entry_id, link: link.url, onToggleMap,
    mark: { scope: zone ? "rule" : "entry", target: detail.entry.entry_id,
            rule_id: zone ? key : null, checks, held },
    onRecorded: () => {
      const next = nextOpenRule(keys, at, (k) => open(marksBy[k]));
      const final = !zone || next < 0;
      if (!final) setAt(next);
      onMarked(final);
    },
  });

  return (
    <div className="entry-review" data-testid="entry-review">
      {zone && (
        <div className="zone-rules" data-testid="zone-rules">
          {rules.map((r, i) => (
            <div key={r.rule_id} data-rule-pick={r.rule_id}
              className={`zone-rule${i === at ? " active" : ""}`} onClick={() => setAt(i)}>
              <span className="rule-id">{r.rule_id}</span>
              <span className="zone-rule-label">{r.label}</span>
              <span className="zone-impact" data-testid={`impact-${r.rule_id}`}>
                {view.rules.find((x) => x.rule_id === r.rule_id)?.impact}
              </span>
              {marksBy[r.rule_id] && (
                <span className={`verify-chip ${marksBy[r.rule_id]!.status}`}>{markWords(marksBy[r.rule_id])}</span>
              )}
            </div>
          ))}
        </div>
      )}
      <Checklist target={zone ? `rule ${key}` : "this entry"} checks={checks} state={m.state}
        onChange={m.setState} held={held} problems={mode.problems} current={markWords(marksBy[key])} />
      <div className="answer-link" data-testid="answer-link">
        <strong>A:</strong>{" "}
        {link.url ? (
          <>
            <a href={link.url} target="_blank" rel="noreferrer">open the app's page</a>
            <span className="dim"> ({link.item}{link.part != null ? `, part ${link.part}` : ""}
              {link.via && link.via !== "reach" ? `, via ${link.via}` : ""}) · key <kbd>o</kbd></span>
          </>
        ) : <span className="dim">{link.why}</span>}
        <label className="dim"> on{" "}
          <input type="date" value={view.date} onChange={(e) => e.target.value && onDate(e.target.value)} />
        </label>
      </div>
      <VerdictBar checks={checks} m={m} held={held} />
    </div>
  );
}
