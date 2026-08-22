import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Protocol } from "pmtiles";
import { layers, LIGHT } from "@protomaps/basemaps";
import { api } from "../api";

interface Props {
  itemId: string | null;
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

// Tag each split feature with a `_color` + `_note` so the circle layer can data-drive its colour.
function colorize(fc: GeoJSON.FeatureCollection, ref: Set<string>, unused: Set<string>): GeoJSON.FeatureCollection {
  return {
    type: "FeatureCollection",
    features: (fc.features ?? []).map((f) => {
      const p = { ...(f.properties ?? {}) } as Record<string, unknown>;
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
  itemId, referencedSplitIds = [], unusedSplitIds = [], selectedSplitId = null, onSelectSplit,
  pendingPoint = null, reloadKey = 0,
}: Props) {
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
          pending: { type: "geojson", data: { type: "FeatureCollection", features: [] } },
          measure: { type: "geojson", data: { type: "FeatureCollection", features: [] } },
        },
        layers: [
          ...layers("protomaps", LIGHT),
          { id: "item-lines", type: "line", source: "item",
            filter: ["==", ["geometry-type"], "LineString"],
            paint: { "line-color": "#2563eb", "line-width": 3 } },
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

  // Load geometry when the item changes.
  useEffect(() => {
    let cancelled = false;
    if (!itemId) { lastFc.current = null; setCount(null); return; }
    api.geojson(itemId)
      .then((gj) => {
        if (cancelled) return;
        lastFc.current = gj as GeoJSON.FeatureCollection;
        setCount(lastFc.current.features?.length ?? 0);
        applyData(true);
      })
      .catch(() => { if (!cancelled) setCount(0); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [itemId, reloadKey]);

  // Recolour when the referenced/unused sets change (rule edits) without refetching.
  useEffect(() => { applyData(false); /* eslint-disable-next-line */ }, [refSet, unusedSet]);

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
      const colored = colorize(fc, refSet, unusedSet);
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
      <div ref={container} style={{ width: "100%", height: "100%" }} />
      <div className="map-legend">
        <span><i style={{ background: "#16a34a" }} /> bound by a rule</span>
        <span><i style={{ background: "#d97706" }} /> curated · unused</span>
        <span><i style={{ background: "#dc2626" }} /> other split</span>
      </div>
    </div>
  );
}
