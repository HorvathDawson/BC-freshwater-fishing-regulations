import type { DfoPlace, DfoWording } from "../types";

/* ONE VIEW OF THE RIVER (owner 2026-11-02): every place on this water, live first — a place is the
   same extent however DFO worded it (a current wording, its historic rewordings, a variant), so each
   is ONE item with every wording listed and the page dates it was printed on. Read only (T742, owner 2026-11-14:
   "remove the grouping option"): no wording is attached to another here; each is opened and marked on its own. */

function dates(w: DfoWording): string {
  if (!w.dates.length) return "no saved page";
  const first = w.dates[0], last = w.dates[w.dates.length - 1];
  return w.dates.length === 1 ? first : `${first} … ${last} (${w.dates.length} pages)`;
}

export function DfoPlaces({ places, href }: { places: DfoPlace[]; href: (id: string) => string }) {
  if (!places.length) return null;
  return (
    <div className="ext-row dfo-places" data-testid="dfo-places">
      <span className="k">places on this water ({places.length})</span>
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
                    {w.location_id && w.location_id !== p.id && <> · <a href={href(w.location_id)} data-testid="dfo-wording-open">open it on its own</a></>}
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
