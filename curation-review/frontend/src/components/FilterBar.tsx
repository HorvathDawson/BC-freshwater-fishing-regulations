import type { ReactNode } from "react";
import type { RegionSummary, Status } from "../types";

const STATUSES: Status[] = [
  "no_registry",
  "needs_review",
  "unused_splits",
  "unreviewed",
  "zone",
];

interface Props {
  regions: RegionSummary[];
  region: string;
  status: Status | "";
  onRegion: (r: string) => void;
  onStatus: (s: Status | "") => void;
  onRefresh: () => void;
  slot?: ReactNode;
}

export function FilterBar({
  regions,
  region,
  status,
  onRegion,
  onStatus,
  onRefresh,
  slot,
}: Props) {
  const current = regions.find((r) => r.id === region);
  return (
    <div className="filterbar">
      <div>
        <label>region</label>
        <select value={region} onChange={(e) => onRegion(e.target.value)}>
          {regions.map((r) => (
            <option key={r.id} value={r.id}>
              region {r.id} ({r.total})
            </option>
          ))}
        </select>
      </div>
      <div>
        <label>status</label>
        <select
          value={status}
          onChange={(e) => onStatus(e.target.value as Status | "")}
        >
          <option value="">all</option>
          {STATUSES.map((s) => {
            const n = current?.by_status?.[s];
            return (
              <option key={s} value={s}>
                {s}
                {n != null ? ` (${n})` : ""}
              </option>
            );
          })}
        </select>
      </div>
      <button className="btn" style={{ padding: "2px 10px" }} onClick={onRefresh}
        title="reload the queue — picks up entries parsed since you opened the app">
        ↻ refresh
      </button>
      {slot}
      <div className="spacer" />
    </div>
  );
}
