import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Protocol } from "pmtiles";
import { layers, LIGHT } from "@protomaps/basemaps";

export const CASCADE_HASH = "cascade/";

/* THE DFO CASCADE VIEW (T229, owner 2026-10-28): one watershed's sections coloured by the DFO salmon places
   whose chain covers them — one colour per place, several places drawn as parallel stripes (in chain
   order). Click a section: its chain in order and, per species, which place answers on each
   day of the year (the bundle's own `verdict_on`). READ ONLY — GET /api/dfo/cascade/<name>. */

interface Place {
  id: string; label: string; water: string; section: string | null; precedence: number; tier: string;
  kind: string; status: string; drawn: boolean; n_sections: number; n_in_frame: number; share: number;
  tributaries: boolean | null; rows: string[]; has_rules: boolean;
}
type Run = [number, number, string, string];            // [day, place index | -1, status, words]
interface Chain {
  places: number[]; undrawn: number[]; n: number; partial: boolean;
  year: Record<string, Run[]>; gear: [number, number, string[]][];
}
interface Cascade {
  frame: { id: string; label: string }; frames: Record<string, string>; reach_file: string;
  n_sections: number; min_order: number; places: Place[]; chains: Chain[];
  geojson: GeoJSON.FeatureCollection;
}

const SPECIES: [string, string][] = [["CH", "Chinook"], ["CO", "Coho"], ["SK", "Sockeye"], ["PK", "Pink"], ["CM", "Chum"]];
const STATUS_COLOR: Record<string, string> = { open: "#16a34a", release: "#2563eb", closed: "#9ca3af", check: "#d97706" };
const MAX_STRIPES = 8;
/** a place covering at least this share of the view is "under everything": listed, not striped (tick to draw) */
const BACKGROUND_SHARE = 0.9;

const BASEMAP_URL = "pmtiles:///basemap/bc.pmtiles";
const g = globalThis as unknown as { __pm?: Protocol; __pmAdded?: boolean };
const protocol = (g.__pm ??= new Protocol());
if (!g.__pmAdded) { maplibregl.addProtocol("pmtiles", protocol.tile); g.__pmAdded = true; }

/** distinct colours: golden-angle hues, alternating lightness; scope defaults get muted greys/browns */
function placeColor(i: number, p: Place): string {
  if (p.precedence === 0) return "#6b7280";
  if (p.precedence === 1) return ["#a16207", "#0e7490", "#7c2d12", "#4d7c0f"][i % 4];
  if (p.precedence === 2) return "#be185d";
  const h = Math.round((i * 137.508) % 360);
  const l = [42, 55, 34][i % 3];
  return `hsl(${h} 80% ${l}%)`;
}

const DAY0 = Date.UTC(2024, 0, 1);                       // the leap year the bundle's day index walks
function dayLabel(d: number): string {
  return new Date(DAY0 + d * 86400000).toLocaleDateString("en-CA", { month: "short", day: "numeric", timeZone: "UTC" });
}
function todayIndex(): number {
  const n = new Date();
  return Math.round((Date.UTC(2024, n.getMonth(), n.getDate()) - DAY0) / 86400000);
}
function runAt<T extends unknown[]>(runs: T[], d: number): T | null {
  let hit: T | null = null;
  for (const r of runs) { if ((r[0] as number) <= d) hit = r; else break; }
  return hit;
}

export function CascadeView({ onLeave, onDfo }: { onLeave: () => void; onDfo: () => void }) {
  const [name, setName] = useState(() => {
    const h = decodeURIComponent(window.location.hash.slice(1));
    return (h.startsWith(CASCADE_HASH) && h.slice(CASCADE_HASH.length)) || "skeena";
  });
  const [minOrder, setMinOrder] = useState(3);
  const [data, setData] = useState<Cascade | null>(null);
  const [error, setError] = useState("");
  const [hidden, setHidden] = useState<Set<number> | null>(null);
  const [chainSel, setChainSel] = useState<number | null>(null);
  const [day, setDay] = useState(todayIndex);
  const [hover, setHover] = useState<number | null>(null);
  const box = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);

  useEffect(() => {
    setData(null); setError(""); setChainSel(null);
    window.history.replaceState(null, "", `#${CASCADE_HASH}${name}`);
    fetch(`/api/dfo/cascade/${encodeURIComponent(name)}?min_order=${minOrder}`)
      .then((r) => (r.ok ? r.json() : r.text().then((t) => Promise.reject(new Error(`${r.status} ${t}`)))))
      .then((d: Cascade) => {
        setData(d);
        // default: hide the places under everything and the ones with no rules today (they never answer)
        setHidden(new Set(d.places.map((p, i) => (p.share >= BACKGROUND_SHARE || !p.has_rules ? i : -1)).filter((i) => i >= 0)));
      })
      .catch((e) => setError(String(e)));
  }, [name, minOrder]);

  const colors = useMemo(() => (data ? data.places.map((p, i) => placeColor(i, p)) : []), [data]);

  // the features with their stripes: c0..cN the shown places of the chain, narrowest first, n how many
  const fc = useMemo(() => {
    if (!data || !hidden) return null;
    return {
      type: "FeatureCollection" as const,
      features: data.geojson.features.map((f) => {
        const ci = (f.properties as { chain: number }).chain;
        const ch = data.chains[ci];
        const shown = ch.places.filter((p) => !hidden.has(p)).slice(0, MAX_STRIPES);
        const props: Record<string, unknown> = { ...f.properties, n: shown.length, partial: ch.partial };
        shown.forEach((p, k) => { props[`c${k}`] = colors[p]; });
        props.fill = shown.length ? colors[shown[0]] : "#d1d5db";
        return { ...f, properties: props };
      }),
    };
  }, [data, hidden, colors]);

  useEffect(() => {
    if (!box.current || map.current) return;
    const m = new maplibregl.Map({
      container: box.current,
      style: {
        version: 8,
        glyphs: "https://cdn.protomaps.com/fonts/pbf/{fontstack}/{range}.pbf",
        sprite: "https://protomaps.github.io/basemaps-assets/sprites/v4/light",
        sources: {
          protomaps: { type: "vector", url: BASEMAP_URL, maxzoom: 15 },
          cascade: { type: "geojson", data: { type: "FeatureCollection", features: [] } },
        },
        layers: [
          ...layers("protomaps", LIGHT),
          { id: "cas-lake", type: "fill", source: "cascade", filter: ["==", ["get", "kind"], "lake"],
            paint: { "fill-color": ["get", "fill"], "fill-opacity": 0.35 } },
          { id: "cas-none", type: "line", source: "cascade",
            filter: ["all", ["==", ["get", "kind"], "line"], ["==", ["get", "n"], 0]],
            paint: { "line-color": "#d1d5db", "line-width": 1 } },
          ...Array.from({ length: MAX_STRIPES }, (_, k) => ({
            id: `cas-${k}`, type: "line" as const, source: "cascade",
            filter: ["all", ["==", ["get", "kind"], "line"], [">", ["get", "n"], k]] as maplibregl.FilterSpecification,
            layout: { "line-cap": "round" as const, "line-join": "round" as const },
            paint: {
              "line-color": ["get", `c${k}`] as maplibregl.ExpressionSpecification,
              "line-width": ["interpolate", ["linear"], ["zoom"], 6, 1.5, 10, 2.5, 13, 4] as maplibregl.ExpressionSpecification,
              // parallel stripes: stripe k of n sits (k - (n-1)/2) widths off the line's centre
              "line-offset": ["interpolate", ["linear"], ["zoom"],
                ...[[6, 1.5], [10, 2.5], [13, 4]].flatMap(([z, w]) => [z, ["*", ["-", k, ["/", ["-", ["get", "n"], 1], 2]], w]])] as maplibregl.ExpressionSpecification,
            },
          })),
          { id: "cas-partial", type: "line", source: "cascade",
            filter: ["all", ["==", ["get", "kind"], "line"], ["==", ["get", "partial"], true]],
            paint: { "line-color": "#111827", "line-width": 1, "line-dasharray": [1, 3], "line-opacity": 0.8 } },
          { id: "cas-sel", type: "line", source: "cascade", filter: ["==", ["get", "chain"], -1],
            paint: { "line-color": "#111827", "line-width": 9, "line-opacity": 0.25 } },
        ],
      },
      center: [-128.0, 55.0], zoom: 6,
    });
    m.addControl(new maplibregl.NavigationControl(), "top-right");
    const hit = Array.from({ length: MAX_STRIPES }, (_, k) => `cas-${k}`).concat(["cas-lake", "cas-none"]);
    m.on("click", (e) => {
      const b = 4;
      const fs = m.queryRenderedFeatures([[e.point.x - b, e.point.y - b], [e.point.x + b, e.point.y + b]], { layers: hit });
      setChainSel(fs.length ? Number(fs[0].properties?.chain) : null);
    });
    m.on("mousemove", (e) => {
      const fs = m.queryRenderedFeatures(e.point, { layers: hit });
      m.getCanvas().style.cursor = fs.length ? "pointer" : "";
    });
    map.current = m;
    return () => { m.remove(); map.current = null; };
  }, []);

  useEffect(() => {
    const m = map.current;
    if (!m || !fc) return;
    const put = () => {
      (m.getSource("cascade") as maplibregl.GeoJSONSource).setData(fc as GeoJSON.FeatureCollection);
      let x0 = 180, y0 = 90, x1 = -180, y1 = -90;
      const walk = (c: unknown): void => {
        if (Array.isArray(c) && typeof c[0] === "number") {
          const [x, y] = c as number[]; x0 = Math.min(x0, x); x1 = Math.max(x1, x); y0 = Math.min(y0, y); y1 = Math.max(y1, y);
        } else if (Array.isArray(c)) c.forEach(walk);
      };
      if (!m.getSource("cascade-fitted")) {
        fc.features.forEach((f) => walk((f.geometry as { coordinates: unknown }).coordinates));
        if (x0 < x1) m.fitBounds([[x0, y0], [x1, y1]], { padding: 30, duration: 0 });
        m.addSource("cascade-fitted", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
      }
    };
    if (m.getSource("cascade")) put(); else m.once("load", put);
  }, [fc]);

  useEffect(() => {
    const m = map.current;
    if (!m || !m.getLayer("cas-sel")) return;
    m.setFilter("cas-sel", ["==", ["get", "chain"], chainSel ?? -1]);
  }, [chainSel, fc]);

  // the places a hovered legend row covers, highlighted
  useEffect(() => {
    const m = map.current;
    if (!m || !m.getLayer("cas-sel") || !data) return;
    if (hover === null) { m.setFilter("cas-sel", ["==", ["get", "chain"], chainSel ?? -1]); return; }
    const cs = data.chains.map((c, i) => (c.places.includes(hover) || c.undrawn.includes(hover) ? i : -1)).filter((i) => i >= 0);
    m.setFilter("cas-sel", ["in", ["get", "chain"], ["literal", cs]]);
  }, [hover, data, chainSel]);

  const toggle = (i: number) => setHidden((h) => {
    const n = new Set(h ?? []);
    if (n.has(i)) n.delete(i); else n.add(i);
    return n;
  });

  const chain = data && chainSel !== null ? data.chains[chainSel] : null;
  return (
    <div className="app" data-testid="cascade-mode" style={{ display: "flex", flexDirection: "column", height: "100vh" }}>
      <div className="filterbar-wrap">
        <div className="filterbar">
          <div className="mode-tabs" role="tablist">
            <button className="btn small" role="tab" aria-selected={false} onClick={onLeave}>Synopsis entries</button>
            <button className="btn small" role="tab" aria-selected={false} onClick={onDfo}>DFO salmon extents</button>
            <button className="btn small primary" role="tab" aria-selected>Cascade</button>
          </div>
          <div>
            <label>watershed</label>
            <select aria-label="watershed" value={name} onChange={(e) => setName(e.target.value)}>
              {Object.entries(data?.frames ?? { skeena: "Skeena" }).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
            </select>
          </div>
          <div>
            <label>streams</label>
            <select aria-label="minimum stream order" value={minOrder} onChange={(e) => setMinOrder(Number(e.target.value))}>
              {[1, 2, 3, 4, 5].map((o) => <option key={o} value={o}>{o === 1 ? "all" : `order ≥ ${o}`}</option>)}
            </select>
          </div>
          <div>
            <label>day</label>
            <input type="range" min={0} max={365} value={day} onChange={(e) => setDay(Number(e.target.value))} aria-label="day of year" />
            <span style={{ marginLeft: 6 }}>{dayLabel(day)}</span>
          </div>
          {data && <span className="muted">{data.n_sections.toLocaleString()} sections · {data.chains.length} distinct chains</span>}
        </div>
      </div>
      {error && <div className="error">{error}</div>}
      <div style={{ display: "flex", flex: 1, minHeight: 0 }}>
        <div style={{ width: 400, overflowY: "auto", borderRight: "1px solid #e5e7eb", padding: 8, fontSize: 12 }}>
          {!data && !error && <div>Loading the cascade… (first load computes every chain; ~1 min)</div>}
          {data && chain && <ChainPanel data={data} chain={chain} colors={colors} day={day} onClose={() => setChainSel(null)} />}
          {data && (
            <>
              <h3 style={{ margin: "8px 0 4px" }}>Places (narrowest first)</h3>
              <div className="muted" style={{ marginBottom: 6 }}>
                A section's chain is every place covering it, in this order; per species and day the first
                place with a rule answers. Tick to stripe a place on the map; hover to light up where it applies.
                Dotted black: the water has a DFO place with rules that is not drawn yet (the app paints it "check").
              </div>
              {data.places.map((p, i) => (
                <div key={p.id} onMouseEnter={() => setHover(i)} onMouseLeave={() => setHover(null)}
                     style={{ display: "flex", gap: 6, alignItems: "flex-start", padding: "3px 0", borderBottom: "1px solid #f3f4f6",
                              opacity: p.drawn ? 1 : 0.65 }}>
                  <input type="checkbox" checked={!hidden?.has(i)} onChange={() => toggle(i)} disabled={!p.drawn}
                         aria-label={`draw ${p.label}`} />
                  <span style={{ width: 14, height: 14, flex: "none", marginTop: 1, borderRadius: 3, background: p.drawn ? colors[i] : "transparent",
                                 border: p.drawn ? "none" : `2px dashed ${colors[i]}` }} />
                  <div style={{ flex: 1 }}>
                    <div><b>{p.label}</b></div>
                    <div className="muted">
                      {p.tier} · {p.section ?? ""} · {p.drawn ? `${p.n_in_frame.toLocaleString()} sections${p.share >= BACKGROUND_SHARE ? " (under everything)" : ""}` : "NOT DRAWN"}
                      {p.tributaries === true ? " · incl. tributaries" : p.tributaries === false ? " · no tributaries" : ""}
                      {!p.has_rules ? " · no rules today" : ""}
                    </div>
                    {p.rows.map((r) => <div key={r} style={{ color: "#374151" }}>{r}</div>)}
                  </div>
                </div>
              ))}
            </>
          )}
        </div>
        <div ref={box} style={{ flex: 1, position: "relative" }} />
      </div>
    </div>
  );
}

function ChainPanel({ data, chain, colors, day, onClose }: {
  data: Cascade; chain: Chain; colors: string[]; day: number; onClose: () => void;
}) {
  const P = data.places;
  const g = runAt(chain.gear, day);
  return (
    <div style={{ border: "1px solid #d1d5db", borderRadius: 6, padding: 8, marginBottom: 10, background: "#f9fafb" }}>
      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <b>This section's chain ({chain.n.toLocaleString()} sections share it)</b>
        <button className="btn small" onClick={onClose}>×</button>
      </div>
      <ol style={{ margin: "4px 0 6px", paddingLeft: 18 }}>
        {chain.places.map((p) => (
          <li key={p}><span style={{ display: "inline-block", width: 10, height: 10, background: colors[p], marginRight: 4 }} />
            {P[p].label} <span className="muted">({P[p].tier}{P[p].has_rules ? "" : ", no rules today"})</span></li>
        ))}
      </ol>
      {chain.undrawn.length > 0 && (
        <div style={{ color: "#b45309", marginBottom: 6 }}>
          Not drawn yet on this water (the app paints the section "check"): {chain.undrawn.map((p) => P[p].label).join("; ")}
        </div>
      )}
      <b>{dayLabel(day)} — who answers</b>
      <table style={{ width: "100%", borderCollapse: "collapse", margin: "4px 0" }}>
        <tbody>
          {SPECIES.map(([sp, nm]) => {
            const r = runAt(chain.year[sp], day);
            const w = r ? r[1] : -1;
            return (
              <tr key={sp} style={{ borderTop: "1px solid #e5e7eb" }}>
                <td style={{ width: 60 }}>{nm}</td>
                <td style={{ color: STATUS_COLOR[r?.[2] ?? "closed"], fontWeight: 600, width: 54 }}>{r?.[2]}</td>
                <td>{w >= 0 ? <><span style={{ display: "inline-block", width: 8, height: 8, background: colors[w], marginRight: 3 }} />{P[w].label}: </> : "nothing opens it: "}
                  <span className="muted">{r?.[3]}</span></td>
              </tr>
            );
          })}
          <tr style={{ borderTop: "1px solid #e5e7eb" }}>
            <td>Gear</td><td />
            <td>{g && g[1] >= 0 ? <>{P[g[1]].label}: <span className="muted">{g[2].join("; ")}</span></> : <span className="muted">none</span>}</td>
          </tr>
        </tbody>
      </table>
      <b>The year — which place answers (colour) and what it says</b>
      {SPECIES.map(([sp, nm]) => (
        <div key={sp} style={{ display: "flex", alignItems: "center", gap: 4, margin: "3px 0" }}>
          <span style={{ width: 54 }}>{nm}</span>
          <YearBar runs={chain.year[sp]} colors={colors} places={P} day={day} />
        </div>
      ))}
      <div className="muted">Top band: the place that answers. Bottom band: open (green) · release (blue) · closed (grey) · check (amber).</div>
    </div>
  );
}

function YearBar({ runs, colors, places, day }: { runs: Run[]; colors: string[]; places: Place[]; day: number }) {
  const W = 300;
  const seg = runs.map((r, i) => ({ r, a: r[0], b: i + 1 < runs.length ? runs[i + 1][0] : 366 }));
  return (
    <svg width={W} height={16} style={{ flex: "none" }}>
      {seg.map(({ r, a, b }) => (
        <g key={a}>
          <rect x={(a / 366) * W} width={((b - a) / 366) * W} y={0} height={9} fill={r[1] >= 0 ? colors[r[1]] : "#e5e7eb"}>
            <title>{`${dayLabel(a)}–${dayLabel(b - 1)}: ${r[1] >= 0 ? places[r[1]].label : "nothing opens it"} — ${r[2]}: ${r[3]}`}</title>
          </rect>
          <rect x={(a / 366) * W} width={((b - a) / 366) * W} y={10} height={5} fill={STATUS_COLOR[r[2]] ?? "#e5e7eb"} />
        </g>
      ))}
      <line x1={(day / 366) * W} x2={(day / 366) * W} y1={0} y2={16} stroke="#111827" strokeWidth={1.5} />
    </svg>
  );
}
