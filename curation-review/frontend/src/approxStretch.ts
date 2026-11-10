/* AN APPROXIMATE STRETCH, DRAWN IN THE APP (T532 round 3, owner 2026-11-09: "could we make this mock up a new section
   … even just a mock one made in app"): a draft that names a cut not built yet has no reach from the server, so the
   water's own lines (already on the map) are walked from the vertex nearest one point to the vertex nearest the other.
   It is a picture only: never the built stretch, never saved, always labelled approximate. Pure. */

type LL = [number, number];
const key = (c: number[]) => `${c[0].toFixed(6)},${c[1].toFixed(6)}`;
const metres = (a: number[], b: number[]) =>
  Math.hypot((a[0] - b[0]) * 111320 * Math.cos(((a[1] + b[1]) / 2) * Math.PI / 180), (a[1] - b[1]) * 111320);

/** how far (m) a point may sit from the water's lines and still be walked from */
export const SNAP_M = 800;

/** the lines of `fc` walked between the vertices nearest `a` and `b` (shortest way along them), or why not */
export function approxBetween(fc: GeoJSON.FeatureCollection | null, a: LL, b: LL):
    { line: LL[]; km: number } | { why: string } {
  const lines: number[][][] = [];
  for (const f of fc?.features ?? []) {
    const g = f.geometry;
    if (g?.type === "LineString") lines.push(g.coordinates);
    else if (g?.type === "MultiLineString") lines.push(...g.coordinates);
  }
  if (!lines.length) return { why: "the water's lines are not on the map yet" };
  const at = new Map<string, number[]>();
  const adj = new Map<string, [string, number][]>();
  const link = (p: string, q: string, d: number) => { (adj.get(p) ?? adj.set(p, []).get(p)!).push([q, d]); };
  let na = "", nb = "", da = Infinity, db = Infinity;
  for (const l of lines) {
    for (let i = 0; i < l.length; i++) {
      const k = key(l[i]);
      at.set(k, l[i]);
      const x = metres(l[i], a), y = metres(l[i], b);
      if (x < da) { da = x; na = k; }
      if (y < db) { db = y; nb = k; }
      if (i) { const p = key(l[i - 1]), d = metres(l[i - 1], l[i]); link(p, k, d); link(k, p, d); }
    }
  }
  if (da > SNAP_M || db > SNAP_M)
    return { why: `a point is ${Math.round(Math.max(da, db))} m from this water's lines` };
  // Dijkstra over the vertices (a binary heap of [distance, vertex])
  const dist = new Map<string, number>([[na, 0]]);
  const prev = new Map<string, string>();
  const heap: [number, string][] = [[0, na]];
  const pop = () => {
    const top = heap[0], last = heap.pop()!;
    if (heap.length) {
      heap[0] = last;
      for (let i = 0; ;) {
        const l = 2 * i + 1, r = l + 1;
        let m = i;
        if (l < heap.length && heap[l][0] < heap[m][0]) m = l;
        if (r < heap.length && heap[r][0] < heap[m][0]) m = r;
        if (m === i) break;
        [heap[i], heap[m]] = [heap[m], heap[i]]; i = m;
      }
    }
    return top;
  };
  const push = (e: [number, string]) => {
    heap.push(e);
    for (let i = heap.length - 1; i > 0;) {
      const p = (i - 1) >> 1;
      if (heap[p][0] <= heap[i][0]) break;
      [heap[i], heap[p]] = [heap[p], heap[i]]; i = p;
    }
  };
  while (heap.length) {
    const [d, k] = pop();
    if (k === nb) break;
    if (d > (dist.get(k) ?? Infinity)) continue;
    for (const [q, w] of adj.get(k) ?? []) {
      const nd = d + w;
      if (nd < (dist.get(q) ?? Infinity)) { dist.set(q, nd); prev.set(q, k); push([nd, q]); }
    }
  }
  if (!dist.has(nb)) return { why: "this water's lines do not join up between the two points" };
  const line: LL[] = [];
  for (let k: string | undefined = nb; k; k = prev.get(k)) line.push(at.get(k) as LL);
  return { line: line.reverse(), km: Math.round((dist.get(nb) ?? 0) / 100) / 10 };
}
