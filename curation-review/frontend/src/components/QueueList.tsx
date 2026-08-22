import type { QueueRow } from "../types";

interface Props {
  rows: QueueRow[];
  selected: string | null;
  loading: boolean;
  onSelect: (entryId: string) => void;
}

export function QueueList({ rows, selected, loading, onSelect }: Props) {
  if (loading) return <div className="queue"><div className="empty-state">Loading…</div></div>;
  if (!rows.length)
    return (
      <div className="queue">
        <div className="empty-state">No entries for this filter.</div>
      </div>
    );
  return (
    <div className="queue">
      {rows.map((r) => (
        <div
          key={r.entry_id}
          className={`queue-row${selected === r.entry_id ? " selected" : ""}`}
          onClick={() => onSelect(r.entry_id)}
        >
          <div className="name">
            {r.name}
            {r.locked && <span className="badge lock">🔒 locked</span>}
            {r.revisit && <span className="badge revisit" title="conditionally accepted — revisit later">↻ revisit</span>}
          </div>
          <div className="meta">
            <span className={`badge ${r.status}`}>{r.status}</span>
            {r.mus.length > 0 && <span>MU {r.mus.join(", ")}</span>}
            <span className="dim">{r.n_rules} rule{r.n_rules === 1 ? "" : "s"}</span>
            {r.unused_curated_splits > 0 && (
              <span className="badge unused-marker" title="unused curated splits">
                ⚠ {r.unused_curated_splits} unused
              </span>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}
