import { useEffect, useState } from "react";
import { api } from "../api";
import { current, drop, subscribe, type Refusal } from "../refusals";
import { Notice } from "./NoticeStack";

/* OWNER 2026-10-28: A REFUSED SAVE IS IMPOSSIBLE TO MISS. Every save the server refused (or never
   got) stays on this red banner — the item, the reason, "retry" — until a save of that item
   succeeds or the reviewer drops it. Persistent across reloads; the server keeps its own log
   (data/generated/review/refused-saves.jsonl). */
/** a save refused because it proposed a fix with P y (`marks` "an extent fix is proposed on a wrong or
 *  unsure mark"): the reviewer may RE-ANSWER P here — their choice, never an automatic change */
export const fixWithPy = (r: Refusal) => (r.mark.answers as Record<string, string> | undefined)?.P === "y"
  && r.reasons.some((x) => /proposed (extent fix|new split) says .*answer P n/.test(x));

export function RefusedSavesBanner() {
  const [list, setList] = useState<Refusal[]>(current);
  const [busy, setBusy] = useState<string | null>(null);
  // the re-answer form open on one refusal: P n needs a problem and a note (the model's rule)
  const [asN, setAsN] = useState<{ key: string; problem: string; note: string } | null>(null);
  const [pProblems, setPProblems] = useState<string[]>([]);
  useEffect(() => subscribe(setList), []);
  useEffect(() => { if (asN && !pProblems.length) api.mode().then((m) => setPProblems(m.problems.P ?? [])).catch(() => {}); },
    [asN, pProblems.length]);
  if (!list.length) return null;
  const retry = async (r: Refusal, patch?: Partial<Refusal["mark"]>) => {
    setBusy(r.key);
    try { await api.postMark(patch ? { ...r.mark, ...patch } : r.mark); setAsN(null); }
    catch { /* stays listed, with the new reason */ }
    setBusy(null);
  };
  const reanswer = (r: Refusal, p: "n" | "?") => ({ answers: { ...r.mark.answers, P: p } } as Partial<Refusal["mark"]>);
  // CRITICAL: a save NOT made — × only shrinks it to a red pill (owner 2026-11-02 notice stack)
  return (
    <Notice id="refused-saves" contentKey={list.map((r) => r.key + r.at).join(",")} tone="error" critical
      testid="refused-saves" pill={`${list.length} NOT saved`}
      summary={<><strong>NOT SAVED — {list.length} save{list.length === 1 ? "" : "s"} refused.</strong>{" "}
        Not in the store until a retry succeeds.</>}>
      <ul>
        {list.map((r) => (
          <li key={r.key} data-testid="refused-save" style={{ marginBottom: 4 }}>
            <code>{r.scope}</code> <code>{r.target}</code>{r.mark.rule_id ? ` / ${r.mark.rule_id}` : ""}
            <span style={{ opacity: 0.8 }}> · {new Date(r.at).toLocaleString()}</span>
            <div>{r.reasons.join(" · ")}</div>
            <button className="btn" style={{ padding: "0 8px", marginRight: 6 }} disabled={busy === r.key}
              onClick={() => void retry(r)}>{busy === r.key ? "retrying…" : "retry"}</button>
            {fixWithPy(r) && <>
              <button className="btn" style={{ padding: "0 8px", marginRight: 6 }} disabled={busy === r.key}
                data-testid="refused-retry-q" title="re-answer Place ? and retry (your choice; the rest as saved)"
                onClick={() => void retry(r, reanswer(r, "?"))}>retry with P ?</button>
              <button className="btn" style={{ padding: "0 8px", marginRight: 6 }} disabled={busy === r.key}
                data-testid="refused-retry-n" title="re-answer Place n (placement wrong — fix proposed) and retry"
                onClick={() => setAsN({ key: r.key, problem: "", note: r.mark.note ?? "" })}>retry with P n…</button>
            </>}
            <button className="btn" style={{ padding: "0 8px" }}
              title="drop it from this list (you will redo it yourself)"
              onClick={() => { if (window.confirm(`Drop the refused save of ${r.target}? It is not saved.`)) drop(r.key); }}>
              drop</button>
            {asN?.key === r.key && (
              <div style={{ marginTop: 4 }} data-testid="refused-as-n">
                <select aria-label="problem" value={asN.problem} onChange={(e) => setAsN({ ...asN, problem: e.target.value })}>
                  <option value="">— the problem —</option>
                  {pProblems.map((p) => <option key={p} value={p}>{p}</option>)}
                </select>{" "}
                <input aria-label="note" placeholder="note: what is wrong" value={asN.note} style={{ width: 260 }}
                  onChange={(e) => setAsN({ ...asN, note: e.target.value })} />{" "}
                <button className="btn" style={{ padding: "0 8px" }} disabled={busy === r.key || !asN.problem || !asN.note.trim()}
                  onClick={() => void retry(r, { ...reanswer(r, "n"), problem: asN.problem, note: asN.note.trim() })}>
                  retry with P n</button>
              </div>
            )}
          </li>
        ))}
      </ul>
    </Notice>
  );
}
