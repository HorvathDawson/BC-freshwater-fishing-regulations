import { useEffect, useRef, useState } from "react";
import { api } from "../api";
import type { RebuildStatus } from "../types";

interface Props {
  /** Called once when a rebuild finishes successfully — parent reloads the graph + all items. */
  onRebuilt: () => void;
}

function fmtElapsed(s: number): string {
  const m = Math.floor(s / 60);
  const sec = Math.round(s % 60);
  return m > 0 ? `${m}m ${sec}s` : `${sec}s`;
}

// "Rebuild graph" trigger + live progress. Runs pipeline.build --full on the backend (CPU-only, no
// credits); polls status while running and, on success, tells the parent to refresh every item so the
// newly-baked splits/boundaries show up without a restart.
export function RebuildButton({ onRebuilt }: Props) {
  const [st, setSt] = useState<RebuildStatus | null>(null);
  const [open, setOpen] = useState(false);
  const [starting, setStarting] = useState(false);
  const wasRunning = useRef(false);
  const poll = useRef<ReturnType<typeof setInterval> | null>(null);

  const running = st?.status === "running";

  function apply(next: RebuildStatus) {
    setSt(next);
    if (next.status === "running") wasRunning.current = true;
    // fire onRebuilt exactly once, on the running -> done edge
    if (next.status === "done" && wasRunning.current) {
      wasRunning.current = false;
      onRebuilt();
    }
  }

  // On mount, learn whether a rebuild is already in flight (e.g. page reloaded mid-build).
  useEffect(() => {
    api.rebuildStatus().then((s) => {
      setSt(s);
      if (s.status === "running") {
        wasRunning.current = true;
        setOpen(true);
      }
    }).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Poll every 2s while running; stop when it settles.
  useEffect(() => {
    if (!running) {
      if (poll.current) { clearInterval(poll.current); poll.current = null; }
      return;
    }
    if (poll.current) return;
    poll.current = setInterval(() => {
      api.rebuildStatus().then(apply).catch(() => {});
    }, 2000);
    return () => { if (poll.current) { clearInterval(poll.current); poll.current = null; } };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [running]);

  async function start() {
    if (running || starting) return;
    if (!window.confirm(
      "Rebuild the full graph now?\n\nRuns pipeline.build --full (~15–20 min, CPU-only, no credits). " +
      "It overwrites output/v2/full/* and bakes your splits.json edits into the section boundaries. " +
      "When it finishes, every item here refreshes automatically.",
    )) return;
    setStarting(true);
    setOpen(true);
    try {
      const s = await api.startRebuild();
      apply(s);
    } catch (e) {
      setSt({
        status: "error", elapsed_s: 0, current: "failed to start", stages: [],
        n_done: 0, n_total: 0, returncode: null, error: String(e), log_tail: [],
      });
    } finally {
      setStarting(false);
    }
  }

  const pct = st && st.n_total ? Math.round((st.n_done / st.n_total) * 100) : 0;
  const label = running
    ? `⟳ rebuilding… ${pct}%`
    : starting
      ? "⟳ starting…"
      : "⟳ rebuild graph";

  return (
    <div className="rebuild">
      <button
        className={"btn" + (running ? " rebuild-running" : "")}
        style={{ padding: "2px 10px" }}
        onClick={running || st?.status === "done" || st?.status === "error" ? () => setOpen((o) => !o) : start}
        disabled={starting}
        title="Run pipeline.build --full to bake splits.json edits into the graph (CPU-only, no credits)"
      >
        {label}
      </button>

      {open && st && (
        <div className="rebuild-panel">
          <div className="rebuild-panel-head">
            <strong>
              {st.status === "running" && "Rebuilding graph"}
              {st.status === "done" && "✓ Graph rebuilt — items refreshed"}
              {st.status === "error" && "✗ Rebuild failed"}
              {st.status === "idle" && "Rebuild graph"}
            </strong>
            <span className="dim">{fmtElapsed(st.elapsed_s)}</span>
            <button className="x" title="hide" onClick={() => setOpen(false)}>×</button>
          </div>

          {st.n_total > 0 && (
            <div className="rebuild-bar">
              <div
                className={"rebuild-bar-fill" + (st.status === "error" ? " err" : st.status === "done" ? " ok" : "")}
                style={{ width: `${st.status === "done" ? 100 : pct}%` }}
              />
            </div>
          )}

          {st.status === "running" && <div className="dim rebuild-current">{st.current}</div>}

          <ol className="rebuild-stages">
            {st.stages.map((s) => (
              <li key={s.label} className={s.done ? "done" : running && st.current === s.label ? "active" : ""}>
                <span className="tick">{s.done ? "✓" : running && st.current === s.label ? "▸" : "·"}</span>
                {s.label}
                {s.seconds != null && <span className="dim"> {s.seconds.toFixed(0)}s</span>}
              </li>
            ))}
          </ol>

          {st.status === "error" && st.error && <div className="errors">{st.error}</div>}

          {(st.status === "error" || (st.status === "running" && st.log_tail.length > 0)) && (
            <pre className="rebuild-log">{st.log_tail.join("\n")}</pre>
          )}

          {(st.status === "done" || st.status === "error") && (
            <button className="btn" style={{ padding: "2px 10px", marginTop: 6 }} onClick={start}>
              rebuild again
            </button>
          )}
        </div>
      )}
    </div>
  );
}
