import { useEffect, useState } from "react";
import { api } from "../api";
import { current, drop, subscribe, type Refusal } from "../refusals";

/* OWNER 2026-10-28: A REFUSED SAVE IS IMPOSSIBLE TO MISS. Every save the server refused (or never
   got) stays on this red banner — the item, the reason, "retry" — until a save of that item
   succeeds or the reviewer drops it. Persistent across reloads; the server keeps its own log
   (data/generated/review/refused-saves.jsonl). */
export function RefusedSavesBanner() {
  const [list, setList] = useState<Refusal[]>(current);
  const [busy, setBusy] = useState<string | null>(null);
  useEffect(() => subscribe(setList), []);
  if (!list.length) return null;
  const retry = async (r: Refusal) => {
    setBusy(r.key);
    try { await api.postMark(r.mark); } catch { /* stays listed, with the new reason */ }
    setBusy(null);
  };
  return (
    <div data-testid="refused-saves" role="alert"
      style={{ position: "fixed", top: 8, left: "50%", transform: "translateX(-50%)", zIndex: 1001,
               maxWidth: 720, width: "calc(100% - 32px)", maxHeight: "45vh", overflow: "auto",
               padding: "8px 12px", borderRadius: 6, fontSize: 13, background: "#b3261e", color: "#fff",
               boxShadow: "0 2px 10px #0008" }}>
      <strong>NOT SAVED — {list.length} save{list.length === 1 ? "" : "s"} refused.</strong>{" "}
      These marks are not in the store until a retry succeeds.
      <ul style={{ margin: "6px 0 0", paddingLeft: 16 }}>
        {list.map((r) => (
          <li key={r.key} data-testid="refused-save" style={{ marginBottom: 4 }}>
            <code>{r.scope}</code> <code>{r.target}</code>{r.mark.rule_id ? ` / ${r.mark.rule_id}` : ""}
            <span style={{ opacity: 0.8 }}> · {new Date(r.at).toLocaleString()}</span>
            <div>{r.reasons.join(" · ")}</div>
            <button className="btn" style={{ padding: "0 8px", marginRight: 6 }} disabled={busy === r.key}
              onClick={() => void retry(r)}>{busy === r.key ? "retrying…" : "retry"}</button>
            <button className="btn" style={{ padding: "0 8px" }}
              title="drop it from this list (you will redo it yourself)"
              onClick={() => { if (window.confirm(`Drop the refused save of ${r.target}? It is not saved.`)) drop(r.key); }}>
              drop</button>
          </li>
        ))}
      </ul>
    </div>
  );
}
