/**
 * (T520, owner 2026-11-08: publish from the Mac only) WHERE A SAVE WENT: a saved input (an in-season match, a DFO
 * places / wording write-through) runs deploy/staging/sync-inputs.sh in the backend — each input staged on its own.
 * This line, on the tab whose save started the run (`reason`), says its last run; an input that did NOT go up is
 * named in red with why, never silently (the backend also logs it, and a failed read of the status is reported).
 */
import { useEffect, useState } from "react";
import { api } from "../api";
import type { InputsSync } from "../types";
import { failed } from "../failures";

export function InputsSyncLine({ reason }: { reason: "inseason-match" | "write-through" }) {
  const [s, setS] = useState<InputsSync | null>(null);
  useEffect(() => {
    let live = true;
    const tick = () => api.inputsSync().then((x) => { if (live) setS(x); }, (e) => { if (live) failed("inputs sync")(e); });
    tick();
    const t = setInterval(tick, 3000);
    return () => { live = false; clearInterval(t); };
  }, []);
  const mine = s && s.enabled && [...(s.reasons ?? []), ...(s.queued ?? [])].includes(reason);
  if (!s || !mine || s.state === "idle") return null;
  const bad = Object.entries(s.failed ?? {});
  const words = { queued: "staging: queued", running: "staging: publishing (sync-inputs.sh) …",
                  done: `staging: published ${s.at ?? ""}`, failed: `staging: NOT all published (rc ${s.rc}) ${s.at ?? ""}`,
                  off: "", idle: "" }[s.state];
  return (
    <div className={s.state === "failed" ? "errors" : "notice"} style={{ margin: 8 }} data-testid="inputs-sync" role="status">
      {words}
      {bad.map(([name, why]) => <div key={name} data-testid={`inputs-sync-failed-${name}`}><strong>{name}</strong>: {why}</div>)}
      {s.state === "failed" && !bad.length && s.output && <pre style={{ whiteSpace: "pre-wrap", maxHeight: 160, overflow: "auto" }}>{s.output}</pre>}
    </div>
  );
}
