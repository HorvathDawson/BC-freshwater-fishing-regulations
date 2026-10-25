import { useEffect, useState } from "react";
import type { AnswerLink, Check, EntryDetail, MarkView, Mode, ReviewView, TribCall } from "../types";
import { Checklist } from "./Checklist";
import { useMarking } from "./useMarking";
import { nextOpenRule } from "../review";

/** T189: what the mark carries besides the checks — the reviewer's tributary calls ("entry" or a
 *  rule id -> include | exclude) and "needs a lake split later". Kept per entry by the screen. */
export interface MarkExtras {
  trib: Record<string, TribCall>;
  setTrib: (key: string, call: TribCall | null) => void;
  lakeSplit: boolean;
  setLakeSplit: (on: boolean) => void;
}

interface Opts {
  detail: EntryDetail;
  view: ReviewView | null;
  mode: Mode;
  /** a mark was recorded; `final` when the entry is done (a water row's mark, a zone entry's
   *  last open rule) — the screen then records the pace and moves on */
  onMarked: (final: boolean) => void;
  /** `m`: show the map large / back */
  onToggleMap: () => void;
  extras: MarkExtras;
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
function heldWhileAwaiting(awaiting: boolean): Partial<Record<Check, string>> {
  return awaiting
    ? { P: "awaiting build — where it binds is another vintage's",
        A: "awaiting build — the app's answer is another vintage's" }
    : {};
}

/* THE REVIEW OF ONE ENTRY (T134): which target is being marked, its checks, the answer link and
   the marking. A water row is marked once (scope entry); a ZONE entry rule by rule (scope rule,
   PLAN §6 Q5). The one write is POST /api/marks (`useMarking`). A hook, so the screen can put the
   checklist below the rules and the verdict bar at the foot of the pane (T189), both on one state. */
export function useEntryReview({ detail, view, mode, onMarked, onToggleMap, extras }: Opts) {
  const zone = (view?.kind ?? detail.kind) === "zone";
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
  const vintage = detail.vintage ?? view?.vintage;
  const held = heldWhileAwaiting(!!vintage && vintage.state !== "current");
  const checks = mode.checks[zone ? "rule" : "entry"];
  const link: AnswerLink = (zone ? view?.rules.find((r) => r.rule_id === key)?.answer : view?.answer)
    ?? { url: null, why: "loading…" };
  // a rule mark calls only its own rule; an entry mark carries every call made on the entry
  const trib = zone
    ? Object.fromEntries(Object.entries(extras.trib).filter(([k]) => k === key))
    : extras.trib;
  const m = useMarking({
    key, resetOn: detail.entry.entry_id, link: link.url, onToggleMap,
    mark: { scope: zone ? "rule" : "entry", target: detail.entry.entry_id,
            rule_id: zone ? key : null, checks, held },
    extra: { tributaries: trib, lake_split_later: extras.lakeSplit },
    onRecorded: () => {
      const next = nextOpenRule(keys, at, (k) => open(marksBy[k]));
      const final = !zone || next < 0;
      if (!final) setAt(next);
      onMarked(final);
    },
  });
  return { zone, rules, key, at, setAt, held, checks, link, m, marksBy };
}

export type EntryReviewState = ReturnType<typeof useEntryReview>;

interface Props {
  r: EntryReviewState;
  view: ReviewView;
  mode: Mode;
  extras: MarkExtras;
  /** the day the app link opens on */
  onDate: (day: string) => void;
}

/** The checklist panel: a zone entry's rule picker, the T/R/P/A checklist, "needs a lake split
 *  later", and the answer link. The verdict bar is the screen's (`VerdictBar`, same state). */
export function EntryReview({ r, view, mode, extras, onDate }: Props) {
  const { zone, rules, at, setAt, key, held, checks, link, m, marksBy } = r;
  return (
    <div className="entry-review section" data-testid="entry-review">
      {zone && (
        <div className="zone-rules" data-testid="zone-rules">
          {rules.map((x, i) => (
            <div key={x.rule_id} data-rule-pick={x.rule_id}
              className={`zone-rule${i === at ? " active" : ""}`} onClick={() => setAt(i)}>
              <span className="rule-id">{x.rule_id}</span>
              <span className="zone-rule-label">{x.label}</span>
              <span className="zone-impact" data-testid={`impact-${x.rule_id}`}>
                {view.rules.find((y) => y.rule_id === x.rule_id)?.impact}
              </span>
              {marksBy[x.rule_id] && (
                <span className={`verify-chip ${marksBy[x.rule_id]!.status}`}>{markWords(marksBy[x.rule_id])}</span>
              )}
            </div>
          ))}
        </div>
      )}
      <Checklist target={zone ? `rule ${key}` : "this entry"} checks={checks} state={m.state}
        onChange={m.setState} held={held} problems={mode.problems} current={markWords(marksBy[key])} />
      <label className="lake-split-later" data-testid="lake-split-later"
        title="a lake divided into areas the book draws on a map — deferred (RULINGS K5); the mark puts it on the lake-split list">
        <input type="checkbox" checked={extras.lakeSplit}
          onChange={(e) => extras.setLakeSplit(e.target.checked)} />
        needs a lake split later
      </label>
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
    </div>
  );
}
