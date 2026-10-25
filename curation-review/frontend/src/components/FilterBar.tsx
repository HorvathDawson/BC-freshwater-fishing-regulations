import { useState, type ReactNode } from "react";
import type { QueueOrder, RegionSummary, ReviewFilter, Status, VerifyStatus } from "../types";

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
  /** kept on the one row (the pilot toggle) */
  slot?: ReactNode;
  /** in the collapsible area (tier, the per-tier progress and the output commands) */
  more?: ReactNode;
  /** filters set inside `more` (the tier), counted on the fold button */
  moreSet?: number;
  verify: ReviewFilter;
  onVerify: (v: ReviewFilter) => void;
  order: QueueOrder;
  onOrder: (o: QueueOrder) => void;
}

/** The review filter. "human-reviewed" is a verified mark — every check y — that still holds (T184);
 *  "back for a re-check" is one that was, and whose text, rules or extent changed since. */
const REVIEW: [ReviewFilter, string][] = [
  ["", "all"], ["todo", "to do (not human-reviewed)"], ["unmarked", "never marked"],
  ["recheck", "back for a re-check (reviewed, changed since)"],
  ["stale", "changed since marked"], ["wrong", "wrong"], ["unsure", "unsure"],
  ["deferred", "deferred"], ["verified", "human-reviewed"],
];

const MORE_KEY = "curation-review.filters-open";
function readOpen(): boolean {
  try { return window.localStorage.getItem(MORE_KEY) === "1"; } catch { return false; }
}

/* THE TOP BAR, ONE ROW (T191): region, review, the pilot toggle and a slim progress line stay in
   view; status, order, tier, refresh and the output commands fold into "filters", so the detail
   pane gets the height. A filter set in the fold is counted on its button, so it is never hidden. */
export function FilterBar({
  regions,
  region,
  status,
  onRegion,
  onStatus,
  onRefresh,
  slot,
  more,
  moreSet = 0,
  verify,
  onVerify,
  order,
  onOrder,
}: Props) {
  const [open, setOpen] = useState(readOpen);
  const toggle = () => setOpen((o) => {
    try { window.localStorage.setItem(MORE_KEY, o ? "0" : "1"); } catch { /* per-viewer only */ }
    return !o;
  });
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
  const pct = current?.total ? (100 * done) / current.total : 0;
  const folded = (status ? 1 : 0) + (order !== "risk" ? 1 : 0) + moreSet;
  return (
    <div className="filterbar-wrap" data-testid="filterbar">
      <div className="filterbar">
        <select value={region} aria-label="region" onChange={(e) => onRegion(e.target.value)}>
          <option value={ALL_REGIONS}>all regions{regions.length
            ? ` (${regions.reduce((n, r) => n + r.total, 0)})` : ""}</option>
          {regions.map((r) => (
            <option key={r.id} value={r.id}>
              region {r.id} ({r.total})
            </option>
          ))}
        </select>
        <select value={verify} aria-label="review filter"
          onChange={(e) => onVerify(e.target.value as ReviewFilter)}>
          {REVIEW.map(([v, w]) => {
            const n = v === "" ? current?.total
              : v === "todo" ? (current ? current.total - done : undefined)
              : v === "recheck" ? undefined
              : current?.by_verify?.[v as VerifyStatus] ?? 0;
            return <option key={v} value={v}>review: {w}{n != null ? ` (${n})` : ""}</option>;
          })}
        </select>
        {slot}
        {current && (
          <span className="progress slim" data-testid="progress"
            title="human-reviewed: verified on every check against the book (a mark on an entry changed since does not count)">
            {region === ALL_REGIONS ? "all" : `r${region}`} {done}/{current.total}
            {progress && region !== ALL_REGIONS && <span className="dim"> · all {progress.verified}/{progress.total}</span>}
          </span>
        )}
        <div className="spacer" />
        <button type="button" className={`btn small${open ? " primary" : ""}`} data-testid="filters-toggle"
          aria-expanded={open} onClick={toggle}
          title="status, order, tier, refresh, the per-tier progress and the output commands">
          filters{folded ? ` (${folded})` : ""} {open ? "▴" : "▾"}
        </button>
      </div>
      <div className="filterbar-line" aria-hidden><i style={{ width: `${pct}%` }} /></div>
      {open && (
        <div className="filterbar filterbar-more" data-testid="filters-more">
          <div>
            <label>status</label>
            <select value={status} aria-label="status"
              onChange={(e) => onStatus(e.target.value as Status | "")}>
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
            <label>order</label>
            <select value={order} aria-label="order" onChange={(e) => onOrder(e.target.value as QueueOrder)}>
              <option value="risk">review order (tier, risk)</option>
              <option value="book">book (page)</option>
            </select>
          </div>
          {more}
          <button className="btn small" onClick={onRefresh}
            title="reload the queue — picks up entries parsed since you opened the app">
            ↻ refresh
          </button>
        </div>
      )}
    </div>
  );
}

function sum<K extends string>(xs: Partial<Record<K, number>>[]): Partial<Record<K, number>> {
  const out: Partial<Record<K, number>> = {};
  for (const x of xs)
    for (const [k, n] of Object.entries(x) as [K, number][]) out[k] = (out[k] ?? 0) + n;
  return out;
}
