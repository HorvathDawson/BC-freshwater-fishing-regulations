import { useEffect, useState } from "react";
import { current, keyOf, refresh, subscribe, type WriteThroughStatus } from "../curatedSave";

/* T325 WRITE-THROUGH, ON SCREEN (owner 2026-10-30). Bottom-left: "saving to curated…" while a background apply
   runs. Amber: each item whose mark is saved but did NOT reach its curated file — the reason, from the apply's own
   check (the mark stays in the store; fix the cause and mark again, or it lands on the next save of that domain).
   A cut waiting on its side rebuild is listed apart (the rebuild is a heavy job, run by hand). */
export function CuratedSaveBanner() {
  const [s, setS] = useState<WriteThroughStatus>(current);
  const [open, setOpen] = useState(false);
  useEffect(() => { const off = subscribe(setS); void refresh(); return off; }, []);
  if (!s.enabled) return null;
  const items = Object.entries(s.attention);
  const refused = items.filter(([, o]) => o.state === "refused");
  const waiting = items.filter(([, o]) => o.state === "waiting");
  const label = (k: string) => { const [scope, target, rule] = k.split("|"); return `${scope} ${target}${rule ? " / " + rule : ""}`; };
  return (
    <div data-testid="curated-save" style={{ position: "fixed", left: 12, bottom: 12, zIndex: 1000, maxWidth: 560,
      fontSize: 12, display: "flex", flexDirection: "column", gap: 6 }}>
      {refused.length > 0 && (
        <div role="alert" data-testid="curated-refused" style={{ background: "#7a4a00", color: "#fff", padding: "6px 10px",
          borderRadius: 6, maxHeight: "35vh", overflow: "auto", boxShadow: "0 2px 8px #0006" }}>
          <strong>Mark saved, NOT in the curated file — {refused.length} item{refused.length === 1 ? "" : "s"}:</strong>
          <ul style={{ margin: "4px 0 0", paddingLeft: 16 }}>
            {refused.map(([k, o]) => <li key={k} data-testid="curated-refused-item"><code>{label(k)}</code> — {o.why}</li>)}
          </ul>
        </div>
      )}
      {waiting.length > 0 && (
        <div data-testid="curated-waiting" style={{ background: "#2d3a4a", color: "#fff", padding: "4px 10px", borderRadius: 6 }}>
          <button className="btn small" onClick={() => setOpen(!open)}>{open ? "▾" : "▸"}</button>{" "}
          {waiting.length} mark{waiting.length === 1 ? "" : "s"} wait for a side rebuild (a cut changes) before landing
          {open && <ul style={{ margin: "4px 0 0", paddingLeft: 16, maxHeight: "25vh", overflow: "auto" }}>
            {waiting.map(([k, o]) => <li key={k}><code>{label(k)}</code> — <span style={{ whiteSpace: "pre-wrap" }}>{o.why}</span></li>)}
          </ul>}
        </div>
      )}
      {s.saving && (
        <div data-testid="curated-saving" style={{ background: "#1f4f7a", color: "#fff", padding: "4px 10px", borderRadius: 6, alignSelf: "flex-start" }}>
          saving to curated…
        </div>
      )}
    </div>
  );
}

/** on an item: what the write-through did with its mark (nothing shown once it is saved) */
export function CuratedTag({ scope, target, ruleId }: { scope: string; target: string; ruleId?: string | null }) {
  const [s, setS] = useState<WriteThroughStatus>(current);
  useEffect(() => subscribe(setS), []);
  const o = s.outcomes[keyOf(scope, target, ruleId)];
  if (!o || o.state === "saved") return null;
  const text = o.state === "saving" ? "saving to curated…" : o.state === "waiting" ? "waits for a side rebuild"
    : `not in curated: ${o.why}`;
  return <span className={o.state === "refused" ? "field-error" : "dim"} data-testid="curated-tag" title={o.why ?? ""}>{text}</span>;
}
