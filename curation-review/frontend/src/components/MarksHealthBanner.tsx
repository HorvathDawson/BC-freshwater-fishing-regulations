import { useEffect, useState } from "react";
import { api } from "../api";
import type { MarkHealthRow, MarksHealth } from "../types";

/* OWNER 2026-10-28: "I don't want that to happen again." Every mark in force is accounted for at
   load (`GET /api/marks/health`): a mark whose target is GONE (orphaned — a regenerated / renamed
   id) or whose target moved (stale) is listed here, loudly, instead of silently reading undone. */
function Row({ r }: { r: MarkHealthRow }) {
  return (
    <li>
      <code>{r.scope}</code> <code>{r.target}</code>{r.rule_id ? ` / ${r.rule_id}` : ""} — {r.verdict},{" "}
      {r.reviewer} {r.date}
      {r.reasons?.length ? ` (${r.reasons.join(", ")})` : ""}
      {r.error ? ` (${r.error})` : ""}
      {r.new_splits.length > 0 && ` · added splits: ${r.new_splits.join(", ")}`}
      {r.note && <span className="dim"> · “{r.note}”</span>}
    </li>
  );
}

export function MarksHealthBanner() {
  const [h, setH] = useState<MarksHealth | null>(null);
  const [hidden, setHidden] = useState(false);
  useEffect(() => { api.marksHealth().then(setH).catch(() => {}); }, []);
  if (!h || hidden || (!h.n_orphans && !h.n_stale)) return null;
  const bad = h.n_orphans > 0;
  return (
    <div data-testid="marks-health" role="alert"
      style={{ position: "fixed", bottom: 8, left: 8, zIndex: 1000, maxWidth: 560, maxHeight: "40vh",
               overflow: "auto", padding: "6px 10px", borderRadius: 6, fontSize: 12,
               background: bad ? "#b3261e" : "#7a5a00", color: "#fff", boxShadow: "0 2px 8px #0006" }}>
      <strong>Marks:</strong>{" "}
      {bad && `${h.n_orphans} orphaned (target gone — counted nowhere)`}
      {bad && h.n_stale > 0 && ", "}
      {h.n_stale > 0 && `${h.n_stale} stale (target moved — needs a re-check)`}
      {" "}<button className="btn" style={{ padding: "0 6px" }} onClick={() => setHidden(true)}>hide</button>
      <details>
        <summary>which</summary>
        <ul style={{ margin: "4px 0", paddingLeft: 16 }}>
          {h.orphans.map((r) => <Row key={`o:${r.scope}:${r.target}:${r.rule_id}`} r={r} />)}
          {h.stale_rows.map((r) => <Row key={`s:${r.scope}:${r.target}:${r.rule_id}`} r={r} />)}
        </ul>
      </details>
    </div>
  );
}
