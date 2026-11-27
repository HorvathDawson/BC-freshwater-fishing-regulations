import { useEffect, useState } from "react";
import { api } from "../api";
import type { MarkHealthRow, MarksHealth } from "../types";
import { Notice } from "./NoticeStack";
import { failed } from "../failures";

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
  useEffect(() => { api.marksHealth().then(setH).catch(failed("the marks health")); }, []);
  // T532: the checklist screens say the stale count in their own filter bar; an orphan stays loud everywhere
  const [hash, setHash] = useState(() => window.location.hash);
  useEffect(() => {
    const on = () => setHash(window.location.hash);
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  if (!h || (!h.n_orphans && !h.n_stale)) return null;
  if (!h.n_orphans && /^#(dfo|checks)\//.test(hash)) return null;
  const bad = h.n_orphans > 0;
  const words = [bad ? `${h.n_orphans} orphaned (target gone — counted nowhere)` : "",
                 h.n_stale > 0 ? `${h.n_stale} stale (target moved — needs a re-check)` : ""].filter(Boolean).join(", ");
  // orphaned marks are critical (× shrinks to a pill); stale ones can be dismissed until the count changes
  return (
    <Notice id="marks-health" contentKey={`${h.n_orphans}:${h.n_stale}`} tone={bad ? "error" : "warn"} critical={bad}
      testid="marks-health" pill={`${h.n_orphans} orphaned mark${h.n_orphans === 1 ? "" : "s"}`}
      summary={<><strong>Marks:</strong> {words}</>}>
      <details>
        <summary>which</summary>
        <ul>
          {h.orphans.map((r) => <Row key={`o:${r.scope}:${r.target}:${r.rule_id}`} r={r} />)}
          {h.stale_rows.map((r) => <Row key={`s:${r.scope}:${r.target}:${r.rule_id}`} r={r} />)}
        </ul>
      </details>
    </Notice>
  );
}
