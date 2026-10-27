import { useCallback, useEffect, useState } from "react";

/* T185: THE PROPOSALS VIEW — a read-only "waiting to apply" list. Every proposed edit the review
   marks make (`python -m pipeline.regs.review.proposals build`): what it changes, the confidence of
   a proposed split, the cheap in-memory binding check, the side-rebuild commands. THE MARKS ARE THE
   DECISIONS (owner 2026-10-27): there is nothing to accept here — to change a proposal, change the
   mark. Claude's `python -m pipeline.regs.review.apply_proposals` lands each, one commit. */

export const PROPOSALS_HASH = "proposals";

interface Edit {
  file: string;
  kind: "catalogue" | "dfo" | "split";
  entry_id?: string;
  location_id?: string;
  scope?: string;
  field?: string;
  before?: unknown;
  after?: unknown;
  split?: Record<string, unknown>;
  waterbody?: { name: string; applies_to: Record<string, string> };
}

interface RuleCheck {
  n_before: number; n_after: number; gained: number; lost: number;
  km_before: number; km_after: number; outcome_before: string | null; outcome_after: string | null;
}

type Confidence = "high" | "medium" | "low";

interface Proposal {
  id: string;
  kind: "new_split" | "extent" | "lake_split_later";
  target: string | null;
  scope: string;
  title: string;
  marks: { target: string; reviewer: string; date: string; note?: string; verdict?: string }[];
  edits: Edit[];
  depends_on: string[];
  problems: string[];
  notes: string[];
  applicable: boolean;
  digest: string;
  patch: string | null;
  side_commands: string[];
  severity?: { level: string | null; reasons: string[] } | null;
  check?: { cheap: boolean; why: string | null; rules?: Record<string, RuleCheck>; changed?: string[] } | null;
  /** a proposed split's confidence (owner 2026-10-27); null for an extent / lake proposal */
  confidence?: Confidence | null;
}

interface View {
  made: string | null;
  proposals: Proposal[];
  commands: { build: string; apply: string };
}

const ABSENT = "__absent__";
const SEV_RANK: Record<string, number> = { high: 0, medium: 1, low: 2, none: 3 };
const KIND_WORDS: Record<Proposal["kind"], string> = {
  new_split: "new split", extent: "extent / tributaries", lake_split_later: "lake split later",
};

const show = (v: unknown) => (v === ABSENT ? "(not set)" : JSON.stringify(v));

function Change({ e }: { e: Edit }) {
  if (e.kind === "split") {
    return (
      <div className="prop-edit" data-testid="prop-edit">
        <code>{e.file}</code> · add to <b>{e.waterbody?.name}</b>
        <pre>{JSON.stringify(e.split, null, 1)}</pre>
      </div>
    );
  }
  const where = e.kind === "dfo" ? e.location_id : `${e.entry_id}${e.scope === "entry" ? "" : ` · ${e.scope}`}`;
  return (
    <div className="prop-edit" data-testid="prop-edit">
      <code>{e.file}</code> · {where} · <b>{e.field}</b>
      <div><span className="muted">before</span> <code>{show(e.before)}</code></div>
      <div><span className="muted">after</span> <code>{show(e.after)}</code></div>
    </div>
  );
}

function Check({ c }: { c: NonNullable<Proposal["check"]> }) {
  if (!c.cheap) return <div className="muted" data-testid="prop-check">check: {c.why}</div>;
  return (
    <table className="prop-check" data-testid="prop-check">
      <thead><tr><th>rule</th><th>sections</th><th>+ / −</th><th>km</th><th>outcome</th></tr></thead>
      <tbody>
        {Object.entries(c.rules ?? {}).map(([rid, r]) => (
          <tr key={rid} className={(c.changed ?? []).includes(rid) ? "changed" : ""}>
            <td>{rid}</td>
            <td>{r.n_before} → {r.n_after}</td>
            <td>+{r.gained} −{r.lost}</td>
            <td>{r.km_before} → {r.km_after}</td>
            <td>{r.outcome_before === r.outcome_after ? r.outcome_after : `${r.outcome_before} → ${r.outcome_after}`}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** where it stands: waiting to apply, waiting on a new split first, or not applicable */
function waiting(p: Proposal): string {
  if (p.kind === "lake_split_later") return "listed — no edit until lake splits are taken up";
  if (!p.applicable) return "not applicable — the mark needs changing";
  if (p.depends_on.length) return `waiting to apply — after ${p.depends_on.join(", ")}`;
  return "waiting to apply";
}

function Card({ p }: { p: Proposal }) {
  return (
    <div className={`prop-card prop-${p.kind}`} data-testid="proposal" data-id={p.id}>
      <div className="prop-head">
        <span className="chip-tag">{KIND_WORDS[p.kind]}</span>
        {p.severity?.level && <span className={`sev-chip sev-${p.severity.level}`}
          title={(p.severity.reasons ?? []).join("\n")}>{p.severity.level}</span>}
        {p.confidence && <span className={`chip-tag conf-${p.confidence}`} data-testid="prop-confidence"
          title="how sure the proposer is of where this cut stands">{p.confidence} confidence</span>}
        <b>{p.title}</b>
      </div>
      <div className="muted small">
        {p.id} · digest {p.digest}
        {p.depends_on.length > 0 && <> · needs {p.depends_on.join(", ")} first</>}
      </div>
      <ul className="prop-marks">
        {p.marks.map((m, i) => (
          <li key={i}>{m.target} — {m.reviewer}, {m.date}{m.verdict ? ` (${m.verdict})` : ""}
            {m.note ? <>: <i>{m.note}</i></> : null}</li>
        ))}
      </ul>
      {p.problems.map((x, i) => <div key={i} className="prop-problem" role="alert">problem: {x}</div>)}
      {p.notes.map((x, i) => <div key={i} className="muted">{x}</div>)}
      {p.edits.map((e, i) => <Change key={i} e={e} />)}
      {p.check && <Check c={p.check} />}
      {p.side_commands.length > 0 && (
        <details>
          <summary>side rebuild + binding diff (Claude runs it before a cut change lands)</summary>
          <pre className="prop-cmds">{p.side_commands.join("\n")}</pre>
        </details>
      )}
      {p.patch && <div className="muted small">patch: handoff/PLAN/curation-review/proposals/{p.patch}</div>}
      <div className="prop-decision" data-testid="prop-waiting">
        <span className={p.applicable ? "muted" : "prop-problem"}>{waiting(p)}</span>
      </div>
    </div>
  );
}

export function ProposalsView({ onLeave }: { onLeave: () => void }) {
  const [view, setView] = useState<View | null>(null);
  const [error, setError] = useState("");
  const load = useCallback(() => {
    fetch("/api/proposals")
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`${r.status} ${r.statusText}`))))
      .then(setView)
      .catch((e) => setError(String(e)));
  }, []);
  useEffect(() => { load(); }, [load]);
  const rows = [...(view?.proposals ?? [])]
    .sort((a, b) => (SEV_RANK[a.severity?.level ?? "none"] ?? 3) - (SEV_RANK[b.severity?.level ?? "none"] ?? 3));
  return (
    <div className="proposals" data-testid="proposals-view">
      <div className="prop-bar">
        <button className="btn small" onClick={onLeave}>← review</button>
        <b>Proposals — waiting to apply</b>
        <span className="muted">{view ? `${view.proposals.length} proposed · made ${view.made ?? "never"}` : "loading…"}</span>
        <button className="btn small" onClick={load}>refresh</button>
      </div>
      {error && <div className="prop-problem" role="alert">{error}</div>}
      {view && (
        <div className="muted small prop-cmds-top">
          your marks are the decisions — to change a proposal, change its mark. Rebuild the list:{" "}
          <code>{view.commands.build}</code> · Claude applies them, one commit each:{" "}
          <code>{view.commands.apply}</code>
        </div>
      )}
      {view && rows.length === 0 && <div className="muted">nothing waiting to apply</div>}
      {rows.map((p) => <Card key={p.id} p={p} />)}
    </div>
  );
}
