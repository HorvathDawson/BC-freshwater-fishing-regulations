import { useEffect, useState } from "react";
import { api, type EntryRefused } from "../api";
import type { Verification, VerifyStatus } from "../types";

/** Where the open entry sits in the list being worked through. */
export interface QueueNav {
  pos: number;          // 1-based; 0 when the entry is not in the current list
  total: number;
  onPrev?: () => void;
  onNext?: () => void;
}

const WORDS: Record<VerifyStatus, string> = {
  unverified: "not verified",
  verified: "verified against the book",
  stale: "changed since verified",
  flagged: "⚑ flagged by the reviewer",
};

interface Props {
  entryId: string;
  verification?: Verification;
  /** unsaved edits: a mark is taken against the SAVED entry, so it waits for the save */
  dirty: boolean;
  nav?: QueueNav;
  onMarked?: () => void;
}

/* The reviewer's call on this entry, and the way to the next one. The mark is stored in a sidecar
   beside the catalogue against the entry's content hash — never in the entry — so an entry that
   changes after it was verified comes back as "changed since verified". */
export function VerifyBar({ entryId, verification, dirty, nav, onMarked }: Props) {
  const [status, setStatus] = useState<VerifyStatus>(verification?.status ?? "unverified");
  const [note, setNote] = useState(verification?.note ?? "");
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    setStatus(verification?.status ?? "unverified");
    setNote(verification?.note ?? "");
    setErr("");
  }, [entryId, verification?.status, verification?.note, verification?.hash]);

  async function mark(state: "verified" | "flagged" | "unverified", thenNext = false) {
    setErr("");
    setBusy(true);
    try {
      const res = await api.verify(entryId, state, state === "unverified" ? "" : note);
      setStatus(res.status);
      if (state === "unverified") setNote("");
      onMarked?.();
      if (thenNext) nav?.onNext?.();
    } catch (e) {
      const fe = (e as EntryRefused).fieldErrors;
      setErr(fe ? fe.map((x) => x.msg).join("; ") : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className={`verify-bar ${status}`} data-testid="verify-bar">
      <div className="verify-row">
        <span className={`verify-chip ${status}`} data-testid="verify-status">{WORDS[status]}</span>
        <input className="verify-note" placeholder="note (required to flag)" value={note}
          onChange={(e) => setNote(e.target.value)} aria-label="verification note" />
        <button className="btn primary" disabled={busy || dirty}
          title={dirty ? "save or discard your edits first — a mark is taken against the saved entry" : "the entry matches the book"}
          onClick={() => mark("verified", true)}>Verified · next</button>
        <button className="btn" disabled={busy || dirty} onClick={() => mark("verified")}>Verified</button>
        <button className="btn" disabled={busy || dirty || !note.trim()}
          title={note.trim() ? "flag with this note" : "write what is wrong first"}
          onClick={() => mark("flagged")}>Flag</button>
        {status !== "unverified" && (
          <button className="btn" disabled={busy} onClick={() => mark("unverified")}>Clear</button>
        )}
        {nav && (
          <span className="verify-nav">
            <button className="btn" disabled={!nav.onPrev} onClick={nav.onPrev} aria-label="previous entry">←</button>
            <span className="dim">{nav.pos ? `${nav.pos} / ${nav.total}` : `– / ${nav.total}`}</span>
            <button className="btn" disabled={!nav.onNext} onClick={nav.onNext} aria-label="next entry">→</button>
          </span>
        )}
      </div>
      {err && <div className="errors" style={{ marginTop: 4 }}>{err}</div>}
    </div>
  );
}
