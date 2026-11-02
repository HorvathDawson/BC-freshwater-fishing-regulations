import { TopNav } from "./TopNav";
import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Protocol } from "pmtiles";
import { layers, LIGHT } from "@protomaps/basemaps";

/* EVERY REGION'S DFO CASCADE (owner 2026-11-02): what a region's page says holds BEFORE any table row — its
   region-wide scope sentences and Region 6's lettered sections — as an ordered tree beside what the catalogue
   encodes, the default for a water with no row, PROPOSALS where they differ (DFO's words quoted; nothing is
   accepted here), odd rules, and a map: the region outline and each scope's coverage. READ ONLY —
   GET /api/dfo/cascade-regions and /api/dfo/cascade-regions/<slug>/map. */

export const REGION_PREFIX = "region:";
const SLUGS = ["1", "2", "3", "4", "5a", "5b", "6", "7", "8"];
const WATERSHEDS: Record<string, string> = { skeena: "Skeena watershed — chains", nass: "Nass watershed — chains" };

export function RegionOptions() {
  return (
    <optgroup label="Region cascade">
      {SLUGS.map((s) => <option key={s} value={`${REGION_PREFIX}${s}`}>Region {s.toUpperCase()} — cascade</option>)}
    </optgroup>
  );
}

interface CodeDefault { id: string; status: string; precedence: number; tier: string; scope: string; extents: string[]; rows: string[] }
interface Node {
  id: string; kind: string; label: string; quote: string | null; unlisted?: string; areas?: number[];
  falls_back_to_a?: boolean; n_waters?: number; code: CodeDefault[]; children: Node[];
}
interface Proposal { id: string; scope: string; quote: string; code_now: string; proposal: string }
interface Region {
  slug: string; name: string; url: string; tree: Node; n_waters: number; proposals: Proposal[]; odd: string[];
  unlisted: { page: { scope: number[] | string; says: string; t: string }[]; code: string };
}
interface Legend { scope: string; label: string; color: string; n: number }
interface RegionMap { slug: string; legend: Legend[]; check: Record<string, unknown>; geojson: GeoJSON.FeatureCollection }

const SAYS: Record<string, string> = { closed: "closed", release: "daily limit 0 (release)" };
const BASEMAP_URL = "pmtiles:///basemap/bc.pmtiles";
const g = globalThis as unknown as { __pm?: Protocol; __pmAdded?: boolean };
const protocol = (g.__pm ??= new Protocol());
if (!g.__pmAdded) { maplibregl.addProtocol("pmtiles", protocol.tile); g.__pmAdded = true; }

let regionsCache: Promise<{ regions: Region[] }> | null = null;
function loadRegions() {
  regionsCache ??= fetch("/api/dfo/cascade-regions").then((r) => (r.ok ? r.json() : Promise.reject(new Error(`${r.status}`))));
  return regionsCache;
}

export function RegionCascade({ name, setName }: { name: string; setName: (n: string) => void }) {
  const slug = name.slice(REGION_PREFIX.length);
  const [regions, setRegions] = useState<Region[] | null>(null);
  const [mapData, setMapData] = useState<RegionMap | null>(null);
  const [minOrder, setMinOrder] = useState(4);
  const [error, setError] = useState("");
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const [picked, setPicked] = useState<string>("");
  const box = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);

  useEffect(() => { loadRegions().then((d) => setRegions(d.regions)).catch((e) => { regionsCache = null; setError(String(e)); }); }, []);
  useEffect(() => {
    setMapData(null); setPicked("");
    window.history.replaceState(null, "", `#cascade/${name}`);
    fetch(`/api/dfo/cascade-regions/${encodeURIComponent(slug)}/map?min_order=${minOrder}`)
      .then((r) => (r.ok ? r.json() : r.text().then((t) => Promise.reject(new Error(`${r.status} ${t}`)))))
      .then(setMapData).catch((e) => setError(String(e)));
  }, [slug, name, minOrder]);

  const region = regions?.find((r) => r.slug === slug) ?? null;
  const colorExpr = useMemo(() => {
    const pairs = (mapData?.legend ?? []).flatMap((l) => [l.scope, l.color]);
    return (pairs.length ? ["match", ["get", "scope"], ...pairs, "#111827"] : "#111827") as unknown as maplibregl.ExpressionSpecification;
  }, [mapData]);

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
          rc: { type: "geojson", data: { type: "FeatureCollection", features: [] } },
        },
        layers: [
          ...layers("protomaps", LIGHT),
          { id: "rc-outline-fill", type: "fill", source: "rc", filter: ["==", ["get", "kind"], "outline"],
            paint: { "fill-color": "#0f766e", "fill-opacity": 0.05 } },
          { id: "rc-outline", type: "line", source: "rc", filter: ["==", ["get", "kind"], "outline"],
            paint: { "line-color": "#0f766e", "line-width": 2, "line-dasharray": [3, 2] } },
          { id: "rc-line", type: "line", source: "rc", filter: ["==", ["get", "kind"], "line"],
            layout: { "line-cap": "round", "line-join": "round" },
            paint: { "line-color": "#111827", "line-width": ["interpolate", ["linear"], ["zoom"], 5, 1, 9, 2, 13, 3.5] } },
          { id: "rc-flag", type: "line", source: "rc", filter: ["==", ["get", "kind"], "flag"],
            paint: { "line-color": "#dc2626", "line-width": 6, "line-opacity": 0.8 } },
          { id: "rc-sel", type: "line", source: "rc", filter: ["==", ["get", "scope"], "__none"],
            paint: { "line-color": "#111827", "line-width": 7, "line-opacity": 0.3 } },
        ],
      },
      center: [-127.0, 54.0], zoom: 5,
    });
    m.addControl(new maplibregl.NavigationControl(), "top-right");
    m.on("click", (e) => {
      const b = 4;
      const fs = m.queryRenderedFeatures([[e.point.x - b, e.point.y - b], [e.point.x + b, e.point.y + b]], { layers: ["rc-flag", "rc-line"] });
      const f = fs[0]?.properties;
      if (!f) return;
      new maplibregl.Popup().setLngLat(e.lngLat)
        .setHTML(f.kind === "flag" ? `<b>flagged</b><br>${f.why}<br><code>${f.section}</code>` : `<b>${f.scope}</b>`).addTo(m);
    });
    map.current = m;
    return () => { m.remove(); map.current = null; };
  }, []);

  useEffect(() => {
    const m = map.current;
    if (!m || !mapData) return;
    const put = () => {
      (m.getSource("rc") as maplibregl.GeoJSONSource).setData(mapData.geojson);
      m.setPaintProperty("rc-line", "line-color", colorExpr);
      const ol = mapData.geojson.features.find((f) => (f.properties as { kind: string }).kind === "outline");
      if (ol) {
        let x0 = 180, y0 = 90, x1 = -180, y1 = -90;
        const walk = (c: unknown): void => {
          if (Array.isArray(c) && typeof c[0] === "number") {
            const [x, y] = c as number[]; x0 = Math.min(x0, x); x1 = Math.max(x1, x); y0 = Math.min(y0, y); y1 = Math.max(y1, y);
          } else if (Array.isArray(c)) c.forEach(walk);
        };
        walk((ol.geometry as { coordinates: unknown }).coordinates);
        if (x0 < x1) m.fitBounds([[x0, y0], [x1, y1]], { padding: 30, duration: 0 });
      }
    };
    if (m.isStyleLoaded() && m.getSource("rc")) put(); else m.once("load", put);
  }, [mapData, colorExpr]);

  useEffect(() => {
    const m = map.current;
    if (!m || !m.getLayer("rc-line")) return;
    const shown = (mapData?.legend ?? []).map((l) => l.scope).filter((s) => !hidden.has(s));
    m.setFilter("rc-line", ["all", ["==", ["get", "kind"], "line"], ["in", ["get", "scope"], ["literal", shown]]]);
    m.setFilter("rc-sel", ["==", ["get", "scope"], picked || "__none"]);
  }, [hidden, picked, mapData]);

  const toggle = (s: string) => setHidden((h) => { const n = new Set(h); if (n.has(s)) n.delete(s); else n.add(s); return n; });

  return (
    <div className="app" data-testid="cascade-mode" style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div className="filterbar-wrap">
        <TopNav active="dfo" sub="cascade" right={region ? <span className="muted">{region.n_waters} table rows · {region.proposals.length} proposals</span> : null} />
        <div className="filterbar filterbar-more">
          <div>
            <label>view</label>
            <select aria-label="watershed or region" value={name} onChange={(e) => setName(e.target.value)}>
              {Object.entries(WATERSHEDS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
              <RegionOptions />
            </select>
          </div>
          <div>
            <label>streams</label>
            <select aria-label="minimum stream order" value={minOrder} onChange={(e) => setMinOrder(Number(e.target.value))}>
              {[3, 4, 5, 6].map((o) => <option key={o} value={o}>order ≥ {o}</option>)}
            </select>
          </div>
        </div>
      </div>
      {error && <div className="error">{error}</div>}
      <div style={{ display: "flex", flex: 1, minHeight: 0 }}>
        <div style={{ width: 460, overflowY: "auto", borderRight: "1px solid #e5e7eb", padding: 8, fontSize: 12 }}>
          {!region && !error && <div>Loading the region cascades…</div>}
          {region && <>
            <h3 style={{ margin: "4px 0" }}>Region {region.slug.toUpperCase()} — {region.name}</h3>
            <a href={region.url} target="_blank" rel="noreferrer" className="muted">{region.url}</a>
            <Unlisted r={region} />
            <h4 style={{ margin: "10px 0 4px" }}>The cascade (widest first; the narrowest box with a rule answers)</h4>
            <Tree node={region.tree} depth={0} onPick={setPicked} />
            {region.proposals.length > 0 && <>
              <h4 style={{ margin: "10px 0 4px" }}>Proposals ({region.proposals.length}) — not accepted</h4>
              {region.proposals.map((p) => (
                <div key={p.id} style={{ border: "1px solid #fcd34d", background: "#fffbeb", borderRadius: 4, padding: 6, marginBottom: 6 }}>
                  <div><span style={{ background: "#d97706", color: "#fff", borderRadius: 3, padding: "0 4px", marginRight: 4 }}>PROPOSAL</span><b>{p.scope}</b></div>
                  <blockquote style={{ margin: "4px 0 4px 8px", fontStyle: "italic" }}>“{p.quote}”</blockquote>
                  <div><span className="muted">code now:</span> {p.code_now}</div>
                  <div><span className="muted">proposed:</span> {p.proposal}</div>
                </div>
              ))}
            </>}
            {region.odd.length > 0 && <>
              <h4 style={{ margin: "10px 0 4px" }}>Odd rules</h4>
              <ul style={{ margin: 0, paddingLeft: 16 }}>{region.odd.map((o) => <li key={o}>{o}</li>)}</ul>
            </>}
          </>}
          {mapData && <>
            <h4 style={{ margin: "10px 0 4px" }}>Map — each scope's coverage</h4>
            {mapData.legend.map((l) => (
              <label key={l.scope} style={{ display: "flex", gap: 6, alignItems: "center" }}
                     onMouseEnter={() => setPicked(l.scope)} onMouseLeave={() => setPicked("")}>
                <input type="checkbox" checked={!hidden.has(l.scope)} onChange={() => toggle(l.scope)} />
                <span style={{ width: 14, height: 4, background: l.color, display: "inline-block" }} />
                {l.label} <span className="muted">({l.n.toLocaleString()} sections)</span>
              </label>
            ))}
            <div className="muted">Dashed teal: the region outline (its management units). Red: flagged sections.</div>
            <Check check={mapData.check} />
          </>}
          {!mapData && !error && <div className="muted">Loading the map… (first load of a region ~30 s)</div>}
        </div>
        <div ref={box} style={{ flex: 1, position: "relative" }} />
      </div>
    </div>
  );
}

function Unlisted({ r }: { r: Region }) {
  return (
    <div style={{ border: "1px solid #d1d5db", borderRadius: 4, padding: 6, marginTop: 6, background: "#f9fafb" }}>
      <b>A water with no row</b>
      {r.unlisted.page.length === 0 && <div className="muted">the page says nothing</div>}
      {r.unlisted.page.map((p) => (
        <div key={p.t} style={{ marginTop: 3 }}>
          <span className="muted">{Array.isArray(p.scope) ? `Areas ${p.scope.join(", ")}` : p.scope}:</span>{" "}
          <b>{SAYS[p.says] ?? p.says}</b> — <i>“{p.t}”</i>
        </div>
      ))}
      <div style={{ marginTop: 3 }}><span className="muted">the bundle answers:</span> {r.unlisted.code}</div>
    </div>
  );
}

function Tree({ node, depth, onPick }: { node: Node; depth: number; onPick: (s: string) => void }) {
  return (
    <div style={{ marginLeft: depth ? 12 : 0, borderLeft: depth ? "2px solid #e5e7eb" : "none", paddingLeft: depth ? 6 : 0, marginTop: 4 }}
         onMouseEnter={(e) => { e.stopPropagation(); onPick(node.id); }}>
      <div>
        <b>{node.id === "region" ? node.label : node.id}</b>{" "}
        <span className="muted">{node.kind}{node.falls_back_to_a ? " · falls back to A" : ""}{node.n_waters ? ` · ${node.n_waters} rows on named waters` : ""}</span>
      </div>
      {node.quote && node.id !== "region" && <div style={{ fontStyle: "italic" }}>“{node.quote}”</div>}
      {node.unlisted && <div>no row → <b>{SAYS[node.unlisted] ?? node.unlisted}</b></div>}
      {node.code.length === 0 && node.id !== "region" && <div style={{ color: "#b45309" }}>code: no default location</div>}
      {node.code.map((c) => (
        <div key={c.id} style={{ color: c.status === "active" ? "#374151" : "#9ca3af", marginTop: 2 }}>
          <code>{c.id}</code> · {c.status} · {c.tier}
          {c.extents.map((x) => <div key={x} className="muted" style={{ marginLeft: 8 }}>{x}</div>)}
          {c.status === "active" && c.rows.map((x) => <div key={x} style={{ marginLeft: 8 }}>{x}</div>)}
        </div>
      ))}
      {node.children.map((c) => <Tree key={c.id} node={c} depth={depth + 1} onPick={onPick} />)}
    </div>
  );
}

function Check({ check }: { check: Record<string, unknown> }) {
  if (!check || !Object.keys(check).length) return null;
  const gaps = (check.gaps as { section: string; name: string; area: number | null; wsc: string }[] | undefined) ?? [];
  const named = (check.E_named_outside_areas_3_6 ?? check.elsewhere_named) as [string, number][] | undefined;
  return (
    <div style={{ border: "1px solid #d1d5db", borderRadius: 4, padding: 6, marginTop: 8 }}>
      <b>Check</b>
      {typeof check.verdict === "string" && <div>{check.verdict}</div>}
      {Object.entries(check).filter(([k, v]) => !["verdict", "gaps", "E_named_outside_areas_3_6", "elsewhere_named"].includes(k) && typeof v !== "object")
        .map(([k, v]) => <div key={k} className="muted">{k}: {String(v)}</div>)}
      {Object.entries(check).filter(([, v]) => v && typeof v === "object" && !Array.isArray(v))
        .map(([k, v]) => <div key={k} className="muted">{k}: {Object.entries(v as object).map(([a, b]) => `${a}: ${Number(b).toLocaleString()}`).join(" · ")}</div>)}
      {gaps.length > 0 && <>
        <div style={{ marginTop: 4 }}><b>In the region, in no section ({gaps.length})</b></div>
        {gaps.map((x) => <div key={x.section} className="muted"><code>{x.section}</code> {x.name} {x.area ? `→ Area ${x.area}` : ""} <span>{x.wsc}</span></div>)}
      </>}
      {named && named.length > 0 && <>
        <div style={{ marginTop: 4 }}><b>Named waters draining to no listed Area</b></div>
        <div className="muted">{named.map(([n, c]) => `${n} (${c})`).join(", ")}</div>
      </>}
    </div>
  );
}
