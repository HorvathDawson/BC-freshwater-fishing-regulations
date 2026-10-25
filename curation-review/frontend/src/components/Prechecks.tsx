import type { ReviewView } from "../types";

interface Props {
  view: ReviewView;
}

const m = (x: number) => `${Math.round(x).toLocaleString()} m`;

/* WHAT A MACHINE SAYS BEFORE THE REVIEWER READS (T135, PLAN §4 phase 4): the pre-checks (severity A
   facts first, then B heuristics — `precheck.prechecks`, the gate included), every cut the
   reach run chose between (ruling E16 — the P check asks whether the pick is right; fix mode's
   extent editor holds the `cut_pick` that overrides it), and the second-pass agent's findings on
   the parse when any exist. Read only. */
export function Prechecks({ view }: Props) {
  const cuts = Object.entries(view.ambiguous_cuts ?? {});
  const found = view.prechecks ?? [];
  const parse = view.parse_findings ?? [];
  if (!found.length && !cuts.length && !parse.length && view.run?.run) return null;
  return (
    <div className="section prechecks" data-testid="prechecks">
      <strong>Pre-checks</strong>{" "}
      <span className="dim">
        {found.length ? `${found.filter((f) => f.severity === "A").length} fact(s), `
          + `${found.filter((f) => f.severity === "B").length} hint(s)` : "nothing found"}
        {view.run && !view.run.run && " · no reach run: the ambiguous-cut and unresolved checks are not known"}
      </span>
      {found.length > 0 && (
        <ul>
          {found.map((f, i) => (
            <li key={`${f.code}-${f.rule_id}-${i}`} data-precheck={f.code}>
              <span className={`sev sev-${f.severity}`} title={f.severity === "A" ? "a fact" : "a hint"}>
                {f.severity}</span>
              {f.rule_id && <code>{f.rule_id}</code>} {f.what}
            </li>
          ))}
        </ul>
      )}
      {cuts.length > 0 && (
        <div data-testid="ambiguous-cuts">
          {cuts.map(([rid, list]) => list.map((c) => (
            <div key={`${rid}-${c.extent}-${c.split_id}`} className="amb-cut">
              <code>{rid}</code> cut <code>{c.split_id}</code> (extent {c.extent}): candidates{" "}
              {c.candidates.map(m).join(", ")} — took <strong>{m(c.used)}</strong>
              {c.toward === "curated" ? " (a hand-curated cut_pick)" : ` (ruling E16, toward ${c.toward})`}
            </div>
          )))}
        </div>
      )}
      {parse.length > 0 && (
        <div data-testid="parse-findings">
          <em>The second-pass agent said:</em>
          <ul>{parse.map((p, i) => <li key={i}>{p}</li>)}</ul>
        </div>
      )}
    </div>
  );
}
