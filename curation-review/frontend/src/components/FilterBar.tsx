import type { ReactNode } from "react";
import type { QueueOrder, RegionSummary, Status, VerifyStatus } from "../types";

/** What the parse and the build left (never review progress — that is the "review" filter). */
const STATUSES: Status[] = [
  "no_registry",
  "parser-uncertain",
  "unused_splits",
  "plain",
  "zone",
];
/** the region value meaning every region: THE ONE QUEUE (T135) */
export const ALL_REGIONS = "all";

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
}

const REVIEW: [VerifyStatus | "todo" | "", string][] = [
  ["", "all"], ["todo", "to do (not verified)"], ["unmarked", "never marked"],
  ["stale", "changed since marked"], ["wrong", "wrong"], ["unsure", "unsure"],
  ["deferred", "deferred"], ["verified", "verified"],
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
}: Props) {
  // every region: the counts summed (so the filters' numbers stay right in the one queue)
  const current = region === ALL_REGIONS && regions.length ? {
    id: ALL_REGIONS,
    total: regions.reduce((n, r) => n + r.total, 0),
    by_status: sum(regions.map((r) => r.by_status ?? {})),
    by_verify: sum(regions.map((r) => r.by_verify ?? {})),
  } as RegionSummary : regions.find((r) => r.id === region);
  // over the whole corpus, from the regions' own counts (no second endpoint)
  const progress = regions.length ? {
    total: regions.reduce((n, r) => n + r.total, 0),
    verified: regions.reduce((n, r) => n + (r.by_verify?.verified ?? 0), 0),
  } : null;
  const done = current?.by_verify?.verified ?? 0;
  return (
    <div className="filterbar">
      <div>
        <label>region</label>
        <select value={region} aria-label="region" onChange={(e) => onRegion(e.target.value)}>
          <option value={ALL_REGIONS}>all regions{regions.length
            ? ` (${regions.reduce((n, r) => n + r.total, 0)})` : ""}</option>
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
          <option value="risk">review order (tier, risk)</option>
          <option value="book">book (page)</option>
        </select>
      </div>
      {current && (
        <span className="progress" data-testid="progress"
          title="verified against the book (a mark on an entry changed since does not count)">
          <span className="bar"><i style={{ width: `${current.total ? (100 * done) / current.total : 0}%` }} /></span>
          {region === ALL_REGIONS ? "all" : `region ${region}`} {done} / {current.total}
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

function sum<K extends string>(xs: Partial<Record<K, number>>[]): Partial<Record<K, number>> {
  const out: Partial<Record<K, number>> = {};
  for (const x of xs)
    for (const [k, n] of Object.entries(x) as [K, number][]) out[k] = (out[k] ?? 0) + n;
  return out;
}
