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
  /* The two jobs, kept apart. A regional rule is checked against a REGION CHAPTER and binds
     to every stream in its region; a water row is checked against a table row and binds to one
     water. Interleaved by name they read as one undifferentiated list, and the 117 regional
     entries — the ones the parser has never seen — disappear into 1,032 rows. */
  let lastKind: string | undefined;
  return (
    <div className="queue">
      {rows.map((r) => {
        const first = r.kind !== lastKind;
        lastKind = r.kind;
        return (
      <div key={`g-${r.entry_id}`}>
      {first && r.kind && (
        <div className="queue-group">
          {r.kind === "zone"
            ? "Regional & provincial — from the region chapters, never parsed"
            : "Water tables — one row, one water"}
        </div>
      )}
        <div
          key={r.entry_id}
          className={`queue-row${selected === r.entry_id ? " selected" : ""}` +
            (r.reference_only ? " is-reference" : "")}
          onClick={() => onSelect(r.entry_id)}
        >
          <div className="name">
            {r.name}
            {r.also_item_ids?.length > 0 && (
              <span className="badge" title={`combined entry — also covers ${r.also_item_ids.join(", ")}`}>
                +{r.also_item_ids.length} water{r.also_item_ids.length === 1 ? "" : "s"}
              </span>
            )}
            {r.reference_only && (
              <span className="badge reference" title="pointer row — carries no regulations of its own; it sends you to another entry">
                ↪ reference only
              </span>
            )}
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
      </div>
        );
      })}
    </div>
  );
}
