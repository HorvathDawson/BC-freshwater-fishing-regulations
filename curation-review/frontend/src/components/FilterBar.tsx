import type { ReactNode } from "react";
import type { QueueOrder, RegionSummary, Status, VerifyStatus } from "../types";

const STATUSES: Status[] = [
  "no_registry",
  "flagged",
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
  verify: VerifyStatus | "todo" | "";
  onVerify: (v: VerifyStatus | "todo" | "") => void;
  order: QueueOrder;
  onOrder: (o: QueueOrder) => void;
  /** verified / total over the whole corpus */
  progress: { total: number; verified: number } | null;
}

const REVIEW: [VerifyStatus | "todo" | "", string][] = [
  ["", "all"], ["todo", "to do (not verified)"], ["unverified", "never verified"],
  ["stale", "changed since verified"], ["flagged", "flagged"], ["verified", "verified"],
];

export function FilterBar({
  regions,
  region,
  status,
  onRegion,
  onStatus,
  onRefresh,
  slot,
  verify,
  onVerify,
  order,
  onOrder,
  progress,
}: Props) {
  const current = regions.find((r) => r.id === region);
  const done = current?.by_verify?.verified ?? 0;
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
      <div>
        <label>review</label>
        <select value={verify} aria-label="review filter"
          onChange={(e) => onVerify(e.target.value as VerifyStatus | "todo" | "")}>
          {REVIEW.map(([v, w]) => {
            const n = v === "" ? current?.total
              : v === "todo" ? (current ? current.total - done : undefined)
              : current?.by_verify?.[v as VerifyStatus] ?? 0;
            return <option key={v} value={v}>{w}{n != null ? ` (${n})` : ""}</option>;
          })}
        </select>
      </div>
      <div>
        <label>order</label>
        <select value={order} aria-label="order" onChange={(e) => onOrder(e.target.value as QueueOrder)}>
          <option value="book">book (page)</option>
          <option value="attention">attention first</option>
        </select>
      </div>
      {current && (
        <span className="progress" data-testid="progress"
          title="verified against the book (a mark on an entry changed since does not count)">
          <span className="bar"><i style={{ width: `${current.total ? (100 * done) / current.total : 0}%` }} /></span>
          region {done} / {current.total}
          {progress && <span className="dim"> · all {progress.verified} / {progress.total}</span>}
        </span>
      )}
      <button className="btn" style={{ padding: "2px 10px" }} onClick={onRefresh}
        title="reload the queue — picks up entries parsed since you opened the app">
        ↻ refresh
      </button>
      {slot}
      <div className="spacer" />
    </div>
  );
}
