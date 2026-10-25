import type { Precheck, ReviewView } from "../types";

interface Props {
  view: ReviewView;
}

/** The findings ON ONE RULE (or licensing record) — shown in that rule's block, once (T189). */
export function findingsOf(view: ReviewView, id: string): Precheck[] {
  return (view.prechecks ?? []).filter((f) => f.rule_id === id);
}

/** One finding, as a line. */
export function Finding({ f }: { f: Precheck }) {
  return (
    <li data-precheck={f.code}>
      <span className={`sev sev-${f.severity}`} title={f.severity === "A" ? "a fact" : "a hint"}>
        {f.severity}</span> {f.what}
    </li>
  );
}

/* WHAT A MACHINE SAYS BEFORE THE REVIEWER READS (T135, PLAN §4 phase 4): the pre-checks (severity A
   facts first, then B heuristics — `precheck.prechecks`, the gate included) and the second-pass
   agent's findings on the parse when any exist. Read only.
   ONE COPY OF EACH (T189): the findings on the entry are listed here; a finding on a rule is in
   that rule's block, beside its extent, and only counted here. An ambiguous cut is its
   `ambiguous_cut` finding — the screen used to list the run's cuts a second time. */
export function Prechecks({ view }: Props) {
  const found = view.prechecks ?? [];
  const own = found.filter((f) => !f.rule_id);
  const onRules = found.length - own.length;
  const parse = view.parse_findings ?? [];
  if (!found.length && !parse.length && view.run?.run) return null;
  return (
    <div className="section prechecks" data-testid="prechecks">
      <strong>Pre-checks</strong>{" "}
      <span className="dim">
        {found.length ? `${found.filter((f) => f.severity === "A").length} fact(s), `
          + `${found.filter((f) => f.severity === "B").length} hint(s)` : "nothing found"}
        {onRules > 0 && ` · ${onRules} on the rules, each shown in its rule below`}
        {view.run && !view.run.run && " · no reach run: the ambiguous-cut and unresolved checks are not known"}
      </span>
      {own.length > 0 && <ul>{own.map((f, i) => <Finding key={`${f.code}-${i}`} f={f} />)}</ul>}
      {parse.length > 0 && (
        <div data-testid="parse-findings">
          <em>The second-pass agent said:</em>
          <ul>{parse.map((p, i) => <li key={i}>{p}</li>)}</ul>
        </div>
      )}
    </div>
  );
}
