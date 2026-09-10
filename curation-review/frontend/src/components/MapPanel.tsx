import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Protocol } from "pmtiles";
import { layers, LIGHT } from "@protomaps/basemaps";
import { api } from "../api";
import type { EntryReaches, ReachIdentity, RuleResolved } from "../types";

interface Props {
  itemId: string | null;
  /** other registry items this entry also covers (a combined override: Chilliwack + Vedder + Canal).
   *  Their geometry is drawn together with the primary item's, so the map shows the whole water. */
  alsoItemIds?: string[];
  /** split ids any rule currently binds (green) */
  referencedSplitIds?: string[];
  /** curated split ids no rule uses (amber) */
  unusedSplitIds?: string[];
  /** the split id currently selected (ringed + flown-to) */
  selectedSplitId?: string | null;
  /** clicking a split marker selects it */
  onSelectSplit?: (id: string | null) => void;
  /** a live splits.json point coord to preview on the map (the "show on map" toggle) */
  pendingPoint?: { lon: number; lat: number } | null;
  /** bumped after a graph rebuild — refetch geometry even if the item id is unchanged */
  reloadKey?: number;
  /** fly to just this covered item's geometry (a combined entry's per-water focus) */
  focusItemId?: string | null;
  /** per-rule resolved reaches for this entry (GET /api/entries/{id}/reaches) */
  reaches?: EntryReaches | null;
  /** the entry's rules, so the map can offer one reach at a time */
  rules?: { rule_id: string; type: string; label?: string }[];
  /** rule_id -> the shared reach label the rules list shows, so both call a reach the same thing */
  reachOf?: Record<string, ReachIdentity>;
  /** the entry being reviewed — needed to ask for a RULE's tributary expansion */
  entryId?: string | null;
}

// Same basemap the webapp uses: the bc.pmtiles vector source + protomaps LIGHT layers. Served by the
// backend at /basemap/bc.pmtiles (Range-capable) and proxied by Vite. Web-mercator, so the item
// geometry is delivered in EPSG:4326 by the backend.
const BASEMAP_URL = "pmtiles:///basemap/bc.pmtiles";

// Register the pmtiles protocol once for the whole app.
const g = globalThis as unknown as { __pm?: Protocol; __pmAdded?: boolean };
const protocol = (g.__pm ??= new Protocol());
if (!g.__pmAdded) {
  maplibregl.addProtocol("pmtiles", protocol.tile);
  g.__pmAdded = true;
}

function splitColor(id: string, ref: Set<string>, unused: Set<string>): string {
  return ref.has(id) ? "#16a34a" : unused.has(id) ? "#d97706" : "#dc2626";
}

// straight-line (geodesic) distance between two lon/lat points, in metres
function haversine([lng1, lat1]: [number, number], [lng2, lat2]: [number, number]): number {
  const R = 6371000, toR = Math.PI / 180;
  const dLat = (lat2 - lat1) * toR, dLng = (lng2 - lng1) * toR;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * toR) * Math.cos(lat2 * toR) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}
function fmtDist(m: number): string {
  return m < 1000 ? `${m.toFixed(0)} m` : `${(m / 1000).toFixed(2)} km`;
}

// A combined entry covers several waters; give each its own line colour so the reviewer can SEE
// which stream is which instead of one undifferentiated blue blob. Primary keeps the original blue.
export const ITEM_COLORS = ["#2563eb", "#c2410c", "#7c3aed", "#0f766e", "#b91c1c", "#a16207"];

// Tag each split feature with a `_color` + `_note` so the circle layer can data-drive its colour.
function colorize(fc: GeoJSON.FeatureCollection, ref: Set<string>, unused: Set<string>,
                  itemColor: Map<string, string>, reach: Set<string>): GeoJSON.FeatureCollection {
  return {
    type: "FeatureCollection",
    features: (fc.features ?? []).map((f) => {
      // PURE: derive onto a copy, never onto f.properties. Writing `_reach` back into the source
      // features (which `lastFc` holds across renders) meant a highlight could only ever be ADDED —
      // switching to another rule left the previous reach lit and painted both.
      const p = { ...(f.properties ?? {}) } as Record<string, unknown>;
      const own = p.item_id as string | undefined;
      if (own && itemColor.has(own)) p._line = itemColor.get(own);
      p._reach = reach.has(String(p.node_id));
      if (p.kind === "split") {
        const id = String(p.split_id ?? "");
        p._color = p.auto ? "#0891b2" : splitColor(id, ref, unused);   // auto lake boundary = teal
        p._note = p.auto ? "lake" : ref.has(id) ? "bound" : unused.has(id) ? "UNUSED" : "";
      }
      return { ...f, properties: p };
    }),
  };
}

function bounds(fc: GeoJSON.FeatureCollection): maplibregl.LngLatBoundsLike | null {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const walk = (c: unknown) => {
    if (typeof (c as number[])[0] === "number") {
      const [x, y] = c as [number, number];
      minX = Math.min(minX, x); maxX = Math.max(maxX, x);
      minY = Math.min(minY, y); maxY = Math.max(maxY, y);
    } else for (const cc of c as unknown[]) walk(cc);
  };
  for (const f of fc.features ?? []) if (f.geometry && "coordinates" in f.geometry) walk(f.geometry.coordinates);
  return isFinite(minX) ? [[minX, minY], [maxX, maxY]] : null;
}

export function MapPanel({
  itemId, alsoItemIds = [], referencedSplitIds = [], unusedSplitIds = [], selectedSplitId = null,
  onSelectSplit, pendingPoint = null, reloadKey = 0, focusItemId = null,
  reaches = null, rules = [], reachOf = {}, entryId = null,
}: Props) {
  const [showTribs, setShowTribs] = useState(false);
  const [nTribs, setNTribs] = useState<number | null>(null);
  // What the selected rule resolves to (builder's answer), or null when we fell back to the
  // item-level one-level tributary view because no rule is selected.
  const [tribInfo, setTribInfo] = useState<RuleResolved | null>(null);
  const [tribBusy, setTribBusy] = useState(false);
  const itemColor = useMemo(() => {
    const m = new Map<string, string>();
    [itemId, ...alsoItemIds].forEach((id, i) => { if (id) m.set(id, ITEM_COLORS[i % ITEM_COLORS.length]); });
    return m;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [itemId, alsoItemIds.join(",")]);
  const container = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const lastFc = useRef<GeoJSON.FeatureCollection | null>(null);
  const onSelect = useRef(onSelectSplit);
  onSelect.current = onSelectSplit;
  const [count, setCount] = useState<number | null>(null);
  const [measure, setMeasure] = useState(false);
  const [pts, setPts] = useState<[number, number][]>([]);
  const measuring = useRef(false);
  measuring.current = measure;

  const refSet = useMemo(() => new Set(referencedSplitIds), [referencedSplitIds]);
  // Which rule's reach is drawn. The map owns this: it is a way of LOOKING at the geometry, so it
  // belongs with the other view controls rather than scattered down the rules list.
  const [shownReach, setShownReach] = useState<string | null>(null);
  useEffect(() => { setShownReach(null); }, [itemId, reloadKey, reaches]);

  const shownExtents = shownReach ? (reaches?.rules?.[shownReach] ?? []) : [];
  const reachSet = useMemo(
    () => new Set(shownExtents.flatMap((x) => x?.sections ?? [])),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [shownReach, reaches],
  );
  const straddling = shownExtents.flatMap((x) => x?.unclassified ?? []).length;
  const ambiguous = shownExtents.flatMap((x) => x?.ambiguous_cut ?? []);

  // Every OTHER rule that governs the water now highlighted. One reach almost always carries several
  // rules — the Chilliwack's four gear/harvest rules below Vedder Crossing all cover the same
  // sections — and the question a curator is really asking of a stretch of river is "what applies
  // here", not "what does this one rule cover". `all` = the rule covers the whole highlight;
  // otherwise it overlaps part of it.
  const appliesHere = useMemo(() => {
    if (!shownReach || reachSet.size === 0) return [];
    const out: { rule_id: string; type: string; label?: string; all: boolean }[] = [];
    for (const r of rules) {
      if (r.rule_id === shownReach) continue;
      const secs = (reaches?.rules?.[r.rule_id] ?? []).flatMap((x) => x?.sections ?? []);
      if (secs.length === 0) continue;
      const hit = secs.filter((n) => reachSet.has(n)).length;
      if (hit > 0) out.push({ ...r, all: hit === reachSet.size });
    }
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [shownReach, reachSet, reaches, rules]);

  // Only rules that actually resolve to geometry are offerable; the rest would highlight nothing.
  const reachable = rules.filter((r) => {
    const per = reaches?.rules?.[r.rule_id];
    return per && per.length > 0 && per.some((x) => x && x.sections.length > 0);
  });
  const unusedSet = useMemo(() => new Set(unusedSplitIds), [unusedSplitIds]);

  // Create the map once.
  useEffect(() => {
    if (!container.current || map.current) return;
    map.current = new maplibregl.Map({
      container: container.current,
      style: {
        version: 8,
        glyphs: "https://cdn.protomaps.com/fonts/pbf/{fontstack}/{range}.pbf",
        sprite: "https://protomaps.github.io/basemaps-assets/sprites/v4/light",
        sources: {
          protomaps: { type: "vector", url: BASEMAP_URL, maxzoom: 15 },
          item: { type: "geojson", data: { type: "FeatureCollection", features: [] } },
          tribs: { type: "geojson", data: { type: "FeatureCollection", features: [] } },
          pending: { type: "geojson", data: { type: "FeatureCollection", features: [] } },
          measure: { type: "geojson", data: { type: "FeatureCollection", features: [] } },
        },
        layers: [
          ...layers("protomaps", LIGHT),
          // A lake/wetland item (Vedder Canal) is a POLYGON, not a line — drawn under the lines so a
          // stream threading it stays visible on top.
          { id: "item-fill", type: "fill", source: "item",
            filter: ["==", ["geometry-type"], "Polygon"],
            paint: { "fill-color": ["coalesce", ["get", "_line"], "#2563eb"], "fill-opacity": 0.25 } },
          { id: "item-outline", type: "line", source: "item",
            filter: ["==", ["geometry-type"], "Polygon"],
            paint: { "line-color": ["coalesce", ["get", "_line"], "#2563eb"], "line-width": 1.5 } },
          // IN vs OUT, the only question a curator is really asking:
          //   green  = bound by this rule but not in the item layer (tributaries, area members)
          //   red    = removed by an EXCEPT carve-out — visible AS an exclusion, not a smaller total
          //   grey   = the item-level one-level fallback, when no rule is selected
          { id: "trib-lines", type: "line", source: "tribs",
            paint: {
              "line-color": [
                "match", ["get", "kind"],
                "reach_extra", "#16a34a",
                "reach_excluded", "#dc2626",
                "#64748b",
              ],
              "line-width": ["case", ["==", ["get", "kind"], "reach_excluded"], 3, 1.5],
              "line-dasharray": [2, 1.5],
            } },
          // the reach the selected rule resolves to — drawn UNDER the item lines, wide and bright
          { id: "reach-lines", type: "line", source: "item",
            filter: ["==", ["get", "_reach"], true],
            paint: { "line-color": "#22c55e", "line-width": 9, "line-opacity": 0.55 } },
          { id: "item-lines", type: "line", source: "item",
            filter: ["all", ["==", ["geometry-type"], "LineString"],
                     ["!=", ["get", "kind"], "side_channel"]],
            paint: { "line-color": ["coalesce", ["get", "_line"], "#2563eb"], "line-width": 3 } },
          // A NAMED side channel belongs to its own item but is drawn with the river, thinner and
          // lighter: it is part of this water on the map without reading as the mainstem.
          { id: "item-side-channels", type: "line", source: "item",
            filter: ["==", ["get", "kind"], "side_channel"],
            paint: { "line-color": ["coalesce", ["get", "_line"], "#2563eb"], "line-width": 2,
                     "line-opacity": 0.65 } },
          { id: "item-selected", type: "circle", source: "item",
            filter: ["==", ["get", "split_id"], "__none__"],
            paint: { "circle-radius": 11, "circle-color": "rgba(0,0,0,0)",
                     "circle-stroke-color": "#111", "circle-stroke-width": 3 } },
          { id: "item-points", type: "circle", source: "item",
            filter: ["==", ["geometry-type"], "Point"],
            paint: { "circle-radius": 6, "circle-color": ["get", "_color"],
                     "circle-stroke-color": "#fff", "circle-stroke-width": 2 } },
          { id: "item-labels", type: "symbol", source: "item",
            filter: ["==", ["geometry-type"], "Point"],
            layout: { "text-field": ["get", "label"], "text-size": 11, "text-offset": [0, 1.1],
                      "text-anchor": "top", "text-font": ["Noto Sans Regular"] },
            paint: { "text-halo-color": "#fff", "text-halo-width": 1.5 } },
          { id: "pending-point", type: "circle", source: "pending",
            paint: { "circle-radius": 8, "circle-color": "#db2777",
                     "circle-stroke-color": "#fff", "circle-stroke-width": 2 } },
          { id: "measure-line", type: "line", source: "measure",
            filter: ["==", ["geometry-type"], "LineString"],
            paint: { "line-color": "#7c3aed", "line-width": 2, "line-dasharray": [2, 1] } },
          { id: "measure-pts", type: "circle", source: "measure",
            filter: ["==", ["geometry-type"], "Point"],
            paint: { "circle-radius": 5, "circle-color": "#7c3aed",
                     "circle-stroke-color": "#fff", "circle-stroke-width": 2 } },
        ],
      },
      center: [-127, 54],
      zoom: 4,
    });
    map.current.addControl(new maplibregl.NavigationControl({}), "top-right");
    const m = map.current;
    // measure mode: each click drops a point (a 3rd click starts over)
    m.on("click", (e) => {
      if (!measuring.current) return;
      const p: [number, number] = [e.lngLat.lng, e.lngLat.lat];
      setPts((prev) => (prev.length >= 2 ? [p] : [...prev, p]));
    });
    m.on("click", "item-points", (e) => {
      if (measuring.current) return;                 // don't select splits while measuring
      const f = e.features?.[0];
      const sid = f?.properties?.split_id;
      if (sid != null) onSelect.current?.(String(sid));
    });
    m.on("mouseenter", "item-points", () => { m.getCanvas().style.cursor = "pointer"; });
    m.on("mouseleave", "item-points", () => { m.getCanvas().style.cursor = ""; });
    return () => { map.current?.remove(); map.current = null; };
  }, []);

  // Ring the selected split and fly to it.
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const run = () => {
      if (!m.getLayer("item-selected")) return;
      m.setFilter("item-selected", ["==", ["get", "split_id"], selectedSplitId ?? "__none__"]);
      const fc = lastFc.current;
      if (selectedSplitId && fc) {
        const hit = (fc.features ?? []).find(
          (f) => f.properties?.kind === "split" && String(f.properties?.split_id) === selectedSplitId,
        );
        if (hit && hit.geometry?.type === "Point") {
          m.flyTo({ center: hit.geometry.coordinates as [number, number], zoom: Math.max(m.getZoom(), 12), duration: 500 });
        }
      }
    };
    if (m.isStyleLoaded()) run(); else m.once("load", run);
  }, [selectedSplitId]);

  // Live "show on map" point (from the split editor's coord fields).
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const run = () => {
      const src = m.getSource("pending") as maplibregl.GeoJSONSource | undefined;
      if (!src) return;
      if (pendingPoint) {
        src.setData({
          type: "FeatureCollection",
          features: [{ type: "Feature", properties: {},
            geometry: { type: "Point", coordinates: [pendingPoint.lon, pendingPoint.lat] } }],
        });
        m.flyTo({ center: [pendingPoint.lon, pendingPoint.lat], zoom: Math.max(m.getZoom(), 13), duration: 500 });
      } else {
        src.setData({ type: "FeatureCollection", features: [] });
      }
    };
    if (m.isStyleLoaded()) run(); else m.once("load", run);
  }, [pendingPoint]);

  // Load geometry when the item changes. A combined entry covers several registry items, so the
  // map draws the UNION — otherwise "CHILLIWACK / VEDDER RIVERS" showed only the Chilliwack half.
  const alsoKey = alsoItemIds.join(",");
  useEffect(() => {
    let cancelled = false;
    if (!itemId) { lastFc.current = null; setCount(null); return; }
    const ids = [itemId, ...alsoItemIds];
    // one failing item must not blank the whole map — keep whatever the others returned
    Promise.all(ids.map((id) =>
      api.geojson(id).catch(() => ({ type: "FeatureCollection", features: [] } as GeoJSON.FeatureCollection))))
      .then((gjs) => {
        if (cancelled) return;
        const features = gjs.flatMap((gj, i) =>
          ((gj as GeoJSON.FeatureCollection).features ?? []).map((f) => ({
            ...f, properties: { ...(f.properties ?? {}), item_id: ids[i] },
          })));
        lastFc.current = { type: "FeatureCollection", features } as GeoJSON.FeatureCollection;
        setCount(features.length);
        applyData(true);
      })
      .catch(() => { if (!cancelled) setCount(0); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [itemId, alsoKey, reloadKey]);

  // Recolour when the referenced/unused sets change (rule edits) without refetching.
  useEffect(() => { applyData(false); /* eslint-disable-next-line */ }, [refSet, unusedSet, itemColor, reachSet]);

  // Tributaries (opt-in): one level up from every covered water.
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const run = () => {
      const src = m.getSource("tribs") as maplibregl.GeoJSONSource | undefined;
      if (!src) return;
      const clear = () => { src.setData({ type: "FeatureCollection", features: [] }); setTribInfo(null); };
      if (!showTribs || !itemId) { clear(); return; }

      // A rule is selected -> show what THAT RULE resolves to, from the reach builder: the water
      // it covers (including tributaries and `within(area)` sections that belong to OTHER items,
      // which the item layer cannot draw) and the water its EXCEPT clauses remove. This is the
      // set that ships. The item-level call below is only the no-rule-selected fallback.
      if (shownReach && entryId) {
        setTribBusy(true);
        api.ruleResolved(entryId, shownReach)
          .then((info) => {
            setTribInfo(info);
            setNTribs(info.n_added);
            src.setData(info.geojson as unknown as GeoJSON.FeatureCollection);
          })
          .catch(() => clear())
          .finally(() => setTribBusy(false));
        return;
      }

      setTribInfo(null);
      Promise.all([itemId, ...alsoItemIds].map((id) =>
        api.tributaryGeojson(id).catch(() => ({ type: "FeatureCollection", features: [] } as GeoJSON.FeatureCollection))))
        .then((gjs) => {
          const features = gjs.flatMap((g) => (g as GeoJSON.FeatureCollection).features ?? []);
          setNTribs(gjs.reduce((a, g) => a + ((g as unknown as { n_tributaries?: number }).n_tributaries ?? 0), 0));
          src.setData({ type: "FeatureCollection", features });
        });
    };
    if (m.isStyleLoaded()) run(); else m.once("load", run);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showTribs, itemId, alsoKey, reloadKey, shownReach, entryId]);

  // Focus one of a combined entry's waters: fit to just that item's features.
  useEffect(() => {
    const m = map.current, fc = lastFc.current;
    if (!m || !fc || !focusItemId) return;
    const only = {
      type: "FeatureCollection",
      features: (fc.features ?? []).filter((f) => (f.properties ?? {}).item_id === focusItemId),
    } as GeoJSON.FeatureCollection;
    const b = bounds(only);
    if (b) m.fitBounds(b, { padding: 60, maxZoom: 14, duration: 600 });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focusItemId]);

  // draw the measure points/line + set the crosshair cursor
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const run = () => {
      const src = m.getSource("measure") as maplibregl.GeoJSONSource | undefined;
      if (!src) return;
      const feats: GeoJSON.Feature[] = pts.map((p) => ({
        type: "Feature", properties: {}, geometry: { type: "Point", coordinates: p },
      }));
      if (pts.length === 2)
        feats.push({ type: "Feature", properties: {}, geometry: { type: "LineString", coordinates: pts } });
      src.setData({ type: "FeatureCollection", features: feats });
    };
    if (m.isStyleLoaded()) run(); else m.once("load", run);
  }, [pts]);

  useEffect(() => {
    const m = map.current;
    if (m) m.getCanvas().style.cursor = measure ? "crosshair" : "";
  }, [measure]);

  const measureDist = pts.length === 2 ? haversine(pts[0], pts[1]) : null;

  function applyData(fit: boolean) {
    const m = map.current, fc = lastFc.current;
    if (!m || !fc) return;
    const run = () => {
      const src = m.getSource("item") as maplibregl.GeoJSONSource | undefined;
      if (!src) return;
      const colored = colorize(fc, refSet, unusedSet, itemColor, reachSet);
      src.setData(colored);
      if (fit) {
        const b = bounds(colored);
        if (b) m.fitBounds(b, { padding: 40, maxZoom: 14, duration: 0 });
      }
    };
    if (m.isStyleLoaded()) run(); else m.once("load", run);
  }

  return (
    <div className="map-panel">
      <div className="map-bar">
        {itemId == null ? "no matched item — no geometry"
          : count == null ? "loading…"
          : `${count} feature(s) · webapp basemap (bc.pmtiles)`}
      </div>
      <div className="map-tools">
        {/* One rule's reach at a time. Highlighting several at once would just paint the whole
            item green — the question a curator asks here is "what does THIS rule cover". */}
        <label className="reach-pick" title="highlight the water one rule resolves to">
          <span>reach</span>
          <select
            value={shownReach ?? ""}
            disabled={reachable.length === 0}
            onChange={(e) => setShownReach(e.target.value || null)}
          >
            <option value="">
              {reachable.length === 0 ? "none resolvable" : "none"}
            </option>
            {reachable.map((r) => {
              const n = new Set(
                (reaches?.rules?.[r.rule_id] ?? []).flatMap((x) => x?.sections ?? []),
              ).size;
              const rid = r.rule_id.split(".").pop() ?? r.rule_id;
              return (
                <option key={r.rule_id} value={r.rule_id}>
                  {`${reachOf[r.rule_id]?.key ?? "?"} · ${rid} · ${r.type} · ${n} section${n === 1 ? "" : "s"}`}
                </option>
              );
            })}
          </select>
        </label>
        {tribInfo && showTribs && (
          <span
            className={`badge${tribInfo.truncated ? " warn" : ""}`}
            title={[
              `IN: ${tribInfo.n_total} section(s) — this is what the build ships for this rule`,
              `   extents alone: ${tribInfo.n_direct}`,
              tribInfo.n_added
                ? `   + ${tribInfo.n_added} added by ${tribInfo.within_area ? "the area" : "the tributary walk"}`
                : "   nothing added",
              tribInfo.n_offitem
                ? `   ${tribInfo.n_offitem} of them belong to OTHER items — drawn green, they are `
                  + "not in this entry's own geometry"
                : "",
              tribInfo.n_excluded
                ? `OUT: ${tribInfo.n_excluded} section(s) removed by ${tribInfo.carve_outs.length} `
                  + "EXCEPT carve-out(s) — drawn red"
                : tribInfo.n_carve_outs_authored > 0
                  ? `OUT: none. This row authors ${tribInfo.n_carve_outs_authored} EXCEPT carve-out(s), `
                    + "but they only narrow rules that include tributaries — this one does not."
                  : "OUT: no carve-outs on this rule",
              tribInfo.truncated ? `TRUNCATED: only the first ${tribInfo.limit} are drawn` : "",
            ].filter(Boolean).join("\n")}
          >
            in {tribInfo.n_total}
            {tribInfo.n_excluded > 0 ? ` · out ${tribInfo.n_excluded}` : ""}
            {!tribInfo.carve_outs_apply && tribInfo.n_carve_outs_authored > 0
              ? ` · ${tribInfo.n_carve_outs_authored} except n/a` : ""}
            {tribInfo.truncated ? " (truncated)" : ""}
          </span>
        )}
        {shownReach && straddling > 0 && (
          <span
            className="badge warn"
            title="side channels that straddle this reach's end — in on one side, out on the other, so they are neither included nor excluded"
          >
            {straddling} straddling
          </span>
        )}
        {shownReach && ambiguous.length > 0 && (
          <span
            className="badge warn"
            title={ambiguous
              .map((a) => `${a.split_id}: used ${a.used} m, also cuts at ${a.also_at.join(", ")} m`)
              .join("; ")}
          >
            ambiguous cut
          </span>
        )}
        <button
          className={`btn${showTribs ? " primary" : ""}`}
          style={{ padding: "1px 8px" }}
          title={shownReach
            ? `draw what ${shownReach} resolves to — green = covered (tributaries, and area `
              + "members that belong to other items), red = removed by an EXCEPT carve-out"
            : "draw one level of tributaries for the whole water — select a rule above to see "
              + "what that RULE actually covers"}
          onClick={() => setShowTribs((v) => !v)}
        >
          {shownReach ? "🎯 in / out" : "🌿 tributaries"}
          {tribBusy ? " …" : showTribs && nTribs != null ? ` (${nTribs})` : ""}
        </button>
        <button
          className={`btn${measure ? " primary" : ""}`}
          style={{ padding: "1px 8px" }}
          onClick={() => { setMeasure((v) => !v); setPts([]); }}
        >
          📏 measure
        </button>
        {measure && (
          <span className="measure-readout">
            {measureDist != null
              ? `${fmtDist(measureDist)} (straight-line)`
              : `click ${2 - pts.length} point${2 - pts.length === 1 ? "" : "s"}`}
          </span>
        )}
      </div>
      {shownReach && (
        <div className="reach-rules">
          <span className="k">also applies here</span>
          {appliesHere.length === 0 ? (
            <span className="dim">nothing else — this reach is governed by {shownReach.split(".").pop()} alone</span>
          ) : (
            appliesHere.map((r) => (
              <span
                key={r.rule_id}
                className={`badge${r.all ? " all" : ""}`}
                title={`${r.type}: ${r.label ?? ""}${r.all ? "" : " (covers part of this reach)"}`}
              >
                {r.rule_id.split(".").pop()} · {r.label || r.type}
                {r.all ? "" : " (part)"}
              </span>
            ))
          )}
        </div>
      )}
      <div ref={container} style={{ width: "100%", height: "100%" }} />
      <div className="map-legend">
        {shownReach && <span><i style={{ background: "#22c55e", height: 6 }} /> reach</span>}
        <span><i style={{ background: "#16a34a" }} /> bound by a rule</span>
        <span><i style={{ background: "#d97706" }} /> curated · unused</span>
        <span><i style={{ background: "#dc2626" }} /> other split</span>
      </div>
    </div>
  );
}
