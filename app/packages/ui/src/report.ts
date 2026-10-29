/**
 * (T237) SENDING A PROBLEM REPORT — the form's one action, headless. The page (ui-native `ReportPage`) hands what the
 * reader chose; this stamps the report's id and time, builds the payload (`reportPayload`, core: exactly the rows left
 * on) and hands it to the sender (the app's report outbox, `@app/data` `createReportOutbox`): sent, kept for later
 * (offline / the server busy), or refused. No sender (a build with no report address): the page offers email / share only.
 */
import { useCallback, useRef, useState } from "react";
import { reportPayload, type DiagKey, type Diagnostics, type ReportPayload, type ReportReason } from "@app/core";
import { reportId, type ReportSendResult } from "@app/data";

export interface ReportSender { send(p: ReportPayload): Promise<ReportSendResult> }

export type ReportSendState =
  | { readonly kind: "idle" } | { readonly kind: "sending" } | { readonly kind: "sent" }
  | { readonly kind: "queued"; readonly offline: boolean } | { readonly kind: "refused"; readonly error: string };

export interface ReportDraft {
  reason: ReportReason; what: string; email: string; diagnostics: Diagnostics; on: Partial<Record<DiagKey, boolean>>;
}

export function useReportSend(sender: ReportSender | null, now: () => Date = () => new Date()):
  { state: ReportSendState; send: ((d: ReportDraft) => void) | undefined; reset(): void } {
  const [state, setState] = useState<ReportSendState>({ kind: "idle" });
  const busy = useRef(false);
  const send = useCallback((d: ReportDraft) => {
    if (!sender || busy.current) return;
    busy.current = true;
    setState({ kind: "sending" });
    const p = reportPayload({ ...d, id: reportId(), created: now().toISOString() });
    sender.send(p).then(
      (r) => setState(r.kind === "sent" ? { kind: "sent" } : r.kind === "queued" ? { kind: "queued", offline: r.offline }
        : { kind: "refused", error: r.error }),
      (e: unknown) => setState({ kind: "refused", error: e instanceof Error ? e.message : String(e) }),
    ).finally(() => { busy.current = false; });
  }, [sender, now]);
  const reset = useCallback(() => setState({ kind: "idle" }), []);
  return { state, send: sender ? send : undefined, reset };
}
