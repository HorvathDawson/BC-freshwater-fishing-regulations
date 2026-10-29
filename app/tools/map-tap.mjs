// Tap targets for the tools that tap the map on a fresh profile's first view (smoke-staging, bad-net, request-walk).
// A fixed pixel breaks whenever the layout moves the canvas (T275 made it taller). So each target is a real vertex of
// the water (from data/generated/tiles/layers/stream.geojsonl) and the pixel is PROJECTED at tap time: web-mercator
// from HOME (packages/ui-native/src/shell/useMapColouring.ts, the fresh profile's camera) about the centre of the
// map canvas as it is NOW. Needs no debug hook (window.__map is dev-only); valid while the camera is still at HOME.
export const HOME = { lon: -121.85, lat: 49.15, zoom: 10 };
export const TARGETS = {
  named: { lng: -121.944817, lat: 49.18167, name: 'Gravel Slough' },                       // section 235893, z10+
  unnamed: { lng: -121.766921, lat: 49.031861, name: 'Unnamed stream · Region 2' },        // section 266049, z10+
};
const mx = (lo, z) => (lo + 180) / 360 * 512 * 2 ** z;
const my = (la, z) => (1 - Math.log(Math.tan(Math.PI / 4 + la * Math.PI / 360)) / Math.PI) / 2 * 512 * 2 ** z;
/** the viewport pixel of a coordinate; `evaluate(expr)` runs JS in the page */
export async function project(evaluate, { lng, lat }) {
  const r = await evaluate(`(() => { const c = document.querySelector('canvas.maplibregl-canvas') || document.querySelector('canvas');
    if (!c) return null; const r = c.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; })()`);
  if (!r) throw new Error('no map canvas to project a tap through');
  const z = HOME.zoom;
  return { x: Math.round(r.x + mx(lng, z) - mx(HOME.lon, z)), y: Math.round(r.y + my(lat, z) - my(HOME.lat, z)) };
}
