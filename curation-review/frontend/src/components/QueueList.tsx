import { useEffect } from "react";
import type { QueueFacts, QueueRow, SplitCard } from "../types";

interface Props {
  rows: QueueRow[];
  selected: string | null;
  loading: boolean;
  onSelect: (entryId: string) => void;
  /** T135: tier S0 — the split points still to verify — at the head of the one queue */
  splits?: SplitCard[];
  selectedSplit?: string | null;
  onSelectSplit?: (splitId: string) => void;
  /** which reach run the risk factors were read from (or why none) */
  facts?: QueueFacts;
}

/** One split card's row (tier S0) — in the one queue and in the split list alike. */
export function SplitRow({ c, selected, onSelect }: { c: SplitCard; selected: boolean; onSelect: () => void }) {
  return (
    <div data-entry={`split:${c.split_id}`}
      className={`queue-row${selected ? " selected" : ""}`} onClick={onSelect}>
      <div className="name">{c.label || c.split_id}</div>
      <div className="meta">
        {c.s0 && <span className="badge tier" title="review tier (PLAN §3)">S0</span>}
        {c.grade && <span className="badge warn">grade {c.grade}</span>}
        {c.status !== "unmarked" && <span className={`verify-chip ${c.status}`}>{c.status}</span>}
        {c.vintage && c.vintage.state !== "current" && <span className="verify-chip stale">awaiting build</span>}
        <span className="dim">{c.waterbody}</span>
      </div>
    </div>
  );
}

export function QueueList({ rows, selected, loading, onSelect, splits = [], selectedSplit = null,
                            onSelectSplit, facts }: Props) {
  // keep the open entry in view as the reviewer steps through with prev / next
  useEffect(() => {
    const at = selectedSplit ? `split:${selectedSplit}` : selected;
    if (!at) return;
    document.querySelector(`[data-entry="${CSS.escape(at)}"]`)
      ?.scrollIntoView({ block: "nearest" });
  }, [selected, selectedSplit, rows, splits]);
  if (loading) return <div className="queue"><div className="empty-state">Loading…</div></div>;
  if (!rows.length && !splits.length)
    return (
      <div className="queue">
        <div className="empty-state">No entries for this filter.</div>
      </div>
    );
  /* The two jobs, kept apart. A regional rule is checked against a REGION CHAPTER and binds
     to every stream in its region; a water row is checked against a table row and binds to one
     water. In the review order (T135) the tiers already keep them apart (Z is its own tier); in the
     book's order the heading marks where one kind gives way to the other. */
  let lastGroup: string | undefined;
  const byTier = isTierOrdered(rows) && new Set(rows.map((r) => r.tier)).size > 1;
  return (
    <div className="queue" data-testid="queue">
      {facts && !facts.run && (
        <div className="queue-group warn" title={facts.why ?? ""}>
          no reach run — the ambiguous-cut and unresolved factors are not known
        </div>
      )}
      {splits.length > 0 && (
        <>
          <div className="queue-group">S0 — split points to verify (a wrong cut is wrong everywhere)</div>
          {splits.map((c) => (
            <SplitRow key={`s-${c.split_id}`} c={c} selected={selectedSplit === c.split_id}
              onSelect={() => onSelectSplit?.(c.split_id)} />
          ))}
        </>
      )}
      {rows.map((r) => {
        const group = byTier && r.tier ? `tier:${r.tier}` : `kind:${r.kind}`;
        const first = group !== lastGroup;
        lastGroup = group;
        return (
      <div key={`g-${r.entry_id}`}>
      {first && (group.startsWith("tier:") ? (
        <div className="queue-group">{TIER_HEAD[r.tier ?? ""] ?? r.tier}</div>
      ) : r.kind && (
        <div className="queue-group">
          {r.kind === "zone"
            ? "Regional & provincial — from the region chapters, never parsed"
            : "Water tables — one row, one water"}
        </div>
      ))}
        <div
          key={r.entry_id}
          data-entry={r.entry_id}
          className={`queue-row${selected === r.entry_id ? " selected" : ""}`}
          onClick={() => onSelect(r.entry_id)}
        >
          <div className="name">
            {r.name}
            {r.also_item_ids?.length > 0 && (
              <span className="badge" title={`combined entry — also covers ${r.also_item_ids.join(", ")}`}>
                +{r.also_item_ids.length} water{r.also_item_ids.length === 1 ? "" : "s"}
              </span>
            )}
          </div>
          <div className="meta">
            {r.verify && r.verify !== "unmarked" && (
              <span className={`verify-chip ${r.verify}`}
                title={r.verify === "stale" ? (r.mark_reasons ?? []).join(", ") : (r.verify_note || r.verify)}>
                {r.verify === "verified" ? "✓ verified" : r.verify === "stale" ? "changed since marked" : r.verify}
              </span>
            )}
            {r.vintage && r.vintage !== "current" && (
              <span className="verify-chip stale" title={(r.vintage_reasons ?? []).join("; ")}>
                awaiting build
              </span>
            )}
            {r.tier && <span className="badge tier" title="review tier (PLAN §3)">{r.tier}</span>}
            {(r.risks ?? []).map((k) => (
              <span key={k} className="badge risk" title="risk factor (tiers.RISKS)">{k}</span>
            ))}
            {r.prechecks && r.prechecks.A > 0 && (
              <span className="badge warn" title="pre-check findings of severity A (facts)">
                {r.prechecks.A} pre-check{r.prechecks.A === 1 ? "" : "s"}
              </span>
            )}
            <span className="dim">r{r.region}</span>
            {r.page != null && <span className="dim">p.{r.page}</span>}
            {r.pointer && <span className="badge">pointer</span>}
            {r.status !== "plain" && <span className={`badge ${r.status}`}>{r.status}</span>}
            {r.mus.length > 0 && <span>MU {r.mus.join(", ")}</span>}
            <span className="dim">{r.n_rules} rule{r.n_rules === 1 ? "" : "s"}</span>
            {r.unused_curated_splits > 0 && (
              <span className="badge unused-marker" title="unused curated splits">
                ⚠ {r.unused_curated_splits} unused
              </span>
            )}
          </div>
        </div>
      </div>
        );
      })}
    </div>
  );
}

/** the tiers' headings in the review order (PLAN §3) */
const TIER_HEAD: Record<string, string> = {
  P1: "P1 — closures with a risk factor",
  Z: "Z — zone entries, rule by rule (each binds a whole region)",
  P2: "P2 — plain closures",
  P3: "P3 — non-closure entries with a risk factor",
  P4: "P4 — plain entries",
  sweep: "Sweep — pointer and empty rows",
};
const TIER_ORDER = ["S0", "P1", "Z", "P2", "P3", "P4", "sweep"];

/** whether the rows run in tier order (the review order), so the headings are the tiers' */
function isTierOrdered(rows: QueueRow[]): boolean {
  let last = -1;
  for (const r of rows) {
    const i = TIER_ORDER.indexOf(r.tier ?? "");
    if (i < last) return false;
    last = i;
  }
  return true;
}
