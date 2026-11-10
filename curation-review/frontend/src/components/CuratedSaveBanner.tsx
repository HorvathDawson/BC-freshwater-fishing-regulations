import { useEffect, useState } from "react";
import { current, domainSaveWords, keyOf, notLandedWords, refresh, subscribe, type NotLanded, type WriteThroughStatus } from "../curatedSave";
import { failed } from "../failures";
import { Notice } from "./NoticeStack";

/* T325 WRITE-THROUGH, ON SCREEN (owner 2026-10-30). Bottom-left: "saving to curated…" while a background apply
   runs. Amber: each item whose mark is saved but did NOT reach its curated file — the reason, from the apply's own
   check (the mark stays in the store; fix the cause and mark again, or it lands on the next save of that domain).
   A cut waiting on its side rebuild is listed apart (the rebuild is a heavy job, run by hand). */
export function CuratedSaveBanner() {
  const [s, setS] = useState<WriteThroughStatus>(current);
  useEffect(() => { const off = subscribe(setS); void refresh(); return off; }, []);
  // T562 item 4: the landed check (`apply --check`), asked once the screen is up and again while it is computing
  const [nl, setNl] = useState<NotLanded>({ state: "idle" });
  useEffect(() => {
    let live = true, timer: number | undefined;
    const ask = () => fetch("/api/review/not-landed").then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() as Promise<NotLanded>; })
      .then((got) => { if (!live) return; setNl(got); if (got.state === "checking") timer = window.setTimeout(ask, 5000); },
        failed("the check that every mark has landed"));
    void ask();
    return () => { live = false; if (timer) window.clearTimeout(timer); };
  }, []);
  const landed = notLandedWords(nl);
  const landedNotice = landed && (
    <Notice id="not-landed" contentKey={landed.rows.join("|") + landed.head} tone={landed.tone} testid="not-landed" summary={<strong>{landed.head}</strong>}>
      {landed.rows.length > 0 && <details open={landed.tone === "warn"}><summary>which</summary>
        <ul>{landed.rows.map((r, i) => <li key={i} data-testid="not-landed-row" style={{ whiteSpace: "pre-wrap" }}>{r}</li>)}</ul></details>}
    </Notice>);
  if (!s.enabled) return landedNotice || null;
  const saves = domainSaveWords(s.domains);
  const items = Object.entries(s.attention);
  const refused = items.filter(([, o]) => o.state === "refused");
  const waiting = items.filter(([, o]) => o.state === "waiting");
  const label = (k: string) => { const [scope, target, rule] = k.split("|"); return `${scope} ${target}${rule ? " / " + rule : ""}`; };
  return (
    <>
      {landedNotice}
      {saves.dropped.length > 0 && (
        <Notice id="curated-dropped" contentKey={saves.dropped.join("|")} tone="warn" testid="curated-dropped"
          summary={<strong>The last save took {saves.dropped.length} row{saves.dropped.length === 1 ? "" : "s"} OUT of a curated file</strong>}>
          <ul>{saves.dropped.map((x, i) => <li key={i}>{x}</li>)}</ul>
        </Notice>)}
      {saves.commits.length > 0 && (
        <Notice id="curated-commits" contentKey={saves.commits.join("|")} tone="info" testid="curated-commits"
          summary={<>Saved to the curated file{saves.commits.length === 1 ? "" : "s"}: {saves.commits.join("; ")}</>} />)}
      {refused.length > 0 && (
        <Notice id="curated-refused" contentKey={refused.map(([k, o]) => k + (o.why ?? "")).join(",")} tone="warn"
          testid="curated-refused"
          summary={<strong>Mark saved, NOT in the curated file — {refused.length} item{refused.length === 1 ? "" : "s"}</strong>}>
          <ul>
            {refused.map(([k, o]) => <li key={k} data-testid="curated-refused-item"><code>{label(k)}</code> — {o.why}</li>)}
          </ul>
        </Notice>
      )}
      {waiting.length > 0 && (
        <Notice id="curated-waiting" contentKey={waiting.map(([k]) => k).join(",")} tone="info" testid="curated-waiting"
          summary={<>{waiting.length} mark{waiting.length === 1 ? "" : "s"} wait for a side rebuild (a cut changes) before landing</>}>
          <details>
            <summary>which</summary>
            <ul>{waiting.map(([k, o]) => <li key={k}><code>{label(k)}</code> — <span style={{ whiteSpace: "pre-wrap" }}>{o.why}</span></li>)}</ul>
          </details>
        </Notice>
      )}
      {s.saving && (
        <Notice id="curated-saving" contentKey={String(Date.now() / 60000 | 0)} tone="info" testid="curated-saving"
          summary="saving to curated…" />
      )}
    </>
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
