import type { Mode, Progress } from "../types";
import { duration, eta, median } from "../review";

interface Props {
  progress: Progress | null;
  /** the reviewer's last timed entries, in seconds */
  samples: number[];
  /** T135: the commands the screen prints for the human (it runs none) */
  commands?: Mode["commands"];
}

/* PROGRESS AND AN ETA, PER TIER AND OVERALL (PLAN §4 phase 3). Done is a mark in force; a mark
   that went stale counts as to do again. The ETA is the reviewer's own pace — the median of their
   last timed entries in this browser — times what is left; none before three timed entries.
   THE OUTPUT (T135, phase 4): the wrong and unsure marks as ticket text (read only), and the one
   command that puts the marks into git — the marks store and nothing else. */
export function ProgressBar({ progress, samples, commands }: Props) {
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
      <details className="tickets-link" data-testid="review-output">
        <summary>output</summary>
        <div>
          <a href="/api/review/tickets.md" target="_blank" rel="noreferrer">wrong / unsure marks as tickets</a>
        </div>
        {commands && (
          <div className="dim">
            commit the marks (the marks file only): <code className="cmd">{commands.commit_marks}</code>
            <br />tickets as files: <code className="cmd">{commands.tickets}</code>
          </div>
        )}
      </details>
    </div>
  );
}
