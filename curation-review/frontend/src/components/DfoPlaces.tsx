import { useState } from "react";
import { api } from "../api";
import type { DfoPlace, DfoWording } from "../types";
import { getReviewer } from "../review";

/* ONE VIEW OF THE RIVER (owner 2026-11-02): every place on this water, live first — a place is the
   same extent however DFO worded it (a current wording, its historic rewordings, a variant), so each
   is ONE item with every wording listed and the page dates it was printed on. A wording the grouping
   missed is attached to the open place as a VARIANT: a DFO mark (`variant_of`) that T185 writes into
   the wording's `duplicate_of` through the write-through — never applied here. */

function dates(w: DfoWording): string {
  if (!w.dates.length) return "no saved page";
  const first = w.dates[0], last = w.dates[w.dates.length - 1];
  return w.dates.length === 1 ? first : `${first} … ${last} (${w.dates.length} pages)`;
}

export function DfoPlaces({ places, href, onAttached }: {
  places: DfoPlace[]; href: (id: string) => string; onAttached: (msg: string) => void;
}) {
  const here = places.find((p) => p.this);
  const [busy, setBusy] = useState<string | null>(null);
  const [err, setErr] = useState("");
  if (!places.length) return null;
  async function attach(w: DfoWording) {
    if (!here || !w.location_id) return;
    const reviewer = getReviewer().trim();
    if (!reviewer) { setErr("Type your name first (the mark names its reviewer)."); return; }
    if (!window.confirm(`Attach the wording “${w.text}” to this place as a variant? It is saved as your mark;`
      + " the curated entry takes it through the write-through.")) return;
    setBusy(w.location_id); setErr("");
    try {
      await api.postMark({ scope: "dfo", target: w.location_id, verdict: "verified", answers: { T: "y" },
        variant_of: here.id, reviewer, note: `wording attached as a variant of ${here.id}` });
      onAttached(`attached “${w.text}” as a variant of this place — saving to curated`);
    } catch (e) { setErr(String(e)); } finally { setBusy(null); }
  }
  return (
    <div className="ext-row dfo-places" data-testid="dfo-places">
      <span className="k">places on this water ({places.length})</span>
      {err && <div className="field-error">{err}</div>}
      <ul className="named-splits">
        {places.map((p) => (
          <li key={p.id} data-testid="dfo-place" className={p.this ? "this-place" : ""}>
            {p.this ? <strong>{p.text || "(whole water)"}</strong>
              : <a href={href(p.id)}>{p.text || "(whole water)"}</a>}
            <span className="dim"> {p.historic_only ? "historic only" : p.status}{p.verify ? ` · ${p.verify}` : ""}
              {p.wordings.length > 1 ? ` · ${p.wordings.length} wordings` : ""}</span>
            <details open={p.this}>
              <summary className="dim">wordings</summary>
              <ul data-testid="dfo-wordings">
                {p.wordings.map((w, i) => (
                  <li key={`${w.location_id}:${w.fingerprint ?? i}`} data-testid="dfo-wording">
                    “{w.text || "(whole water)"}”
                    <span className="dim"> {dates(w)} · {w.status}{w.variant ? " · variant" : ""}</span>
                    {!p.this && here && w.location_id && (
                      <button type="button" className="btn small" data-testid="dfo-attach-variant"
                        disabled={busy === w.location_id}
                        title="this wording is the same place as the open one: attach it as a variant (a mark)"
                        onClick={() => void attach(w)}>{busy === w.location_id ? "saving…" : "attach here"}</button>
                    )}
                  </li>
                ))}
              </ul>
            </details>
          </li>
        ))}
      </ul>
    </div>
  );
}
