import type { AwaitingWater } from "../types";

/* T89 AWAITING WATER (owner Q45, 2026-10-23): this entry's rules bind no water, because no water of
   the printed name is in the printed MU. The leads and the same-name waters elsewhere are on the map
   (violet); the fix path is below. READ ONLY — a same-name water elsewhere is never bound. To
   settle the entry, record a mark with the verdict bar; nothing here writes. */
export function AwaitingWaterPanel({ aw }: { aw: AwaitingWater }) {
  const fix = aw.how_to_fix.split("->").map((s) => s.trim()).filter(Boolean);
  return (
    <div className="section awaiting-water" data-testid="awaiting-water">
      <h3>Awaiting water — T89 <span className="dim" style={{ fontWeight: 400, fontSize: 12 }}>
        — read only; your mark is the only write</span></h3>
      <div><strong>{aw.rules} rule{aw.rules === 1 ? "" : "s"}</strong> bind nothing · printed MU {aw.printed_mu} ·{" "}
        <span data-testid="aw-status">{aw.status}</span></div>
      <div className="dim">{aw.rule}</div>

      <h4>Leads</h4>
      {aw.lead ? (
        <div data-testid="aw-lead">{aw.lead}</div>
      ) : (
        <div className="dim" data-testid="aw-lead">None found.</div>
      )}
      {aw.lead_points.length > 0 && (
        <ul style={{ margin: "2px 0", paddingLeft: 16 }}>
          {aw.lead_points.map((l) => (
            <li key={l.label}>{l.label} <span className="dim">{l.coord[1].toFixed(4)}, {l.coord[0].toFixed(4)} — on the map</span></li>
          ))}
        </ul>
      )}

      <h4>Same name elsewhere ({aw.same_name_candidates_elsewhere.length}) <span className="dim" style={{ fontWeight: 400 }}>
        — never bound; nearest drawn on the map</span></h4>
      {aw.same_name_candidates_elsewhere.length === 0 ? <div className="dim">None.</div> : (
        <ul style={{ margin: "2px 0", paddingLeft: 16 }} data-testid="aw-candidates">
          {aw.same_name_candidates_elsewhere.map((c) => (
            <li key={c.item_id}>{c.name} <code>{c.item_id}</code>{" "}
              <span className="dim">MU {c.mus.join(", ")} · {c.km_from_printed_mu.toFixed(0)} km from MU {aw.printed_mu}</span></li>
          ))}
        </ul>
      )}

      <h4>Fix options</h4>
      <ol style={{ margin: "2px 0", paddingLeft: 18 }} data-testid="aw-fix">
        {fix.map((s, i) => <li key={i}>{s}</li>)}
      </ol>
      <div className="dim">Or leave it unbound and listed (the standing rule): mark it <em>deferred</em> with a note saying so.</div>
      <details><summary className="dim">searched ({aw.searched.length})</summary>
        <ul>{aw.searched.map((s) => <li key={s}>{s}</li>)}</ul>
      </details>
    </div>
  );
}
