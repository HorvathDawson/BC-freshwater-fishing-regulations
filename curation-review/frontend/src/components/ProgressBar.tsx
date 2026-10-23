import type { Progress } from "../types";
import { duration, eta, median } from "../review";

interface Props {
  progress: Progress | null;
  /** the reviewer's last timed entries, in seconds */
  samples: number[];
}

/* PROGRESS AND AN ETA, PER TIER AND OVERALL (PLAN §4 phase 3). Done is a mark in force; a mark
   that went stale counts as to do again. The ETA is the reviewer's own pace — the median of their
   last timed entries in this browser — times what is left; none before three timed entries. */
export function ProgressBar({ progress, samples }: Props) {
  if (!progress) return null;
  const all = progress.overall;
  const pace = median(samples);
  return (
    <div className="review-progress" data-testid="review-progress">
      <span className="progress" title="marked (verified, wrong, unsure or deferred) / all">
        <span className="bar"><i style={{ width: `${all.total ? (100 * all.done) / all.total : 0}%` }} /></span>
        all {all.done} / {all.total} · {eta(all.todo, samples)}
        {pace != null && <span className="dim"> · {duration(pace)} per entry</span>}
      </span>
      {progress.tiers.map((t) => (
        <span key={t.tier} className={`tier-chip tier-${t.tier}`}
          title={`${t.tier}: ${t.done} done, ${t.todo} to do${t.stale ? ` (${t.stale} changed since marked)` : ""} — ${eta(t.todo, samples)}`}>
          {t.tier} {t.done}/{t.total}
        </span>
      ))}
    </div>
  );
}
