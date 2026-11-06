/**
 * (T376) THE WATERSHED GROUPS BY NAME AND BY PLACE: which group a point is in, and what the group is called — the
 * release's `conditions` tables `basin`, `basin_frame` and `basin_grid`, written by `pipeline/deliver/bundle/basin_grid.py`
 * (the format is stated there; `pipeline/tests/test_basin_grid.py` pins the same row bytes this file's test decodes).
 *
 * A basin is a watershed group: its id is the tiles' `basin_id` and `basinStandings`' key (the FWA WATERSHED_GROUP_CODE),
 * its name the layer's WATERSHED_GROUP_NAME as sourced ("Chilliwack River") — never reworded here.
 *
 * A grid cell is the group holding its centre (1/32° × 1/64°, ~2 × 1.7 km): a point within about a kilometre of a divide
 * may be answered with the group next door. Outside the grid, or in a cell no group holds (the sea): null.
 */

/** One watershed group: the id the map, `basinMembers` and `basinStandings` use, and its name as the source gives it. */
export interface Basin {
  readonly id: string;
  readonly name: string;
}

/** The grid's frame, in degrees (`basin_frame`). */
export interface BasinFrame {
  readonly w: number;
  readonly s: number;
  readonly dlon: number;
  readonly dlat: number;
  readonly cols: number;
  readonly rows: number;
}

/** The groups, decoded once: the group a point is in, and every group's name. */
export interface BasinGrid {
  /** The group holding the point's cell; null outside every group. A non-finite point is a caller's error: thrown. */
  at(lon: number, lat: number): Basin | null;
  /**
   * (T417) THE GROUP WITH THE LARGEST SHARE OF A BOX (west, south, east, north — the map's visible bounds): every cell
   * the box touches, each counted at its true ground area (a cell's width shrinks with cos(latitude)); null only when no
   * group holds any of them (open sea, outside B.C.). A box that is a single point is that point's cell — `at`. Equal
   * shares go to the group under the box's centre, then to the lower id, so the answer never depends on order.
   * A non-finite or inverted box is a caller's error: thrown.
   */
  mostIn(box: readonly [number, number, number, number]): Basin | null;
  /** Every group, by id. */
  readonly basins: ReadonlyMap<string, Basin>;
}

function varints(b: Uint8Array): number[] {
  const out: number[] = [];
  let i = 0;
  while (i < b.length) {
    let v = 0, mul = 1, x: number;
    do {
      if (i >= b.length) throw new Error("basin_grid: a row ends inside a varint");
      x = b[i++]!;
      v += (x & 0x7f) * mul;
      mul *= 128;
    } while (x >= 0x80);
    out.push(v);
  }
  return out;
}

/** One row's runs as (exclusive end column, ix) — refused unless they cover the frame's columns exactly. */
export function decodeBasinRow(b: Uint8Array, cols: number): { ends: Uint32Array; ix: Uint32Array } {
  const v = varints(b);
  if (v.length % 2) throw new Error("basin_grid: a row ends inside a run");
  const ends = new Uint32Array(v.length / 2), ix = new Uint32Array(v.length / 2);
  let at = 0;
  for (let k = 0; k < v.length; k += 2) {
    at += v[k]!;
    ends[k / 2] = at;
    ix[k / 2] = v[k + 1]!;
  }
  if (at !== cols) throw new Error(`basin_grid: a row covers ${at} cells, not ${cols}`);
  return { ends, ix };
}

/**
 * The release's three tables, decoded. Every row of the frame must be present and every ix a row of `basin` names:
 * a grid that names a group the table does not would answer a group with no name, so it is refused here.
 */
export function decodeBasinGrid(frame: BasinFrame, rows: readonly { row: number; b: Uint8Array }[],
                                names: readonly { ix: number; id: string; name: string }[]): BasinGrid {
  const byIx = new Map<number, Basin>();
  const basins = new Map<string, Basin>();
  for (const n of names) {
    const b: Basin = { id: n.id, name: n.name };
    byIx.set(n.ix, b);
    basins.set(n.id, b);
  }
  const grid: ({ ends: Uint32Array; ix: Uint32Array } | undefined)[] = new Array(frame.rows);
  for (const r of rows) {
    if (!(r.row >= 0 && r.row < frame.rows)) throw new Error(`basin_grid: row ${r.row} is outside the frame`);
    const d = decodeBasinRow(r.b, frame.cols);
    for (const x of d.ix) if (x !== 0 && !byIx.has(x)) throw new Error(`basin_grid: ix ${x} names no basin`);
    grid[r.row] = d;
  }
  for (let r = 0; r < frame.rows; r++) if (!grid[r]) throw new Error(`basin_grid: row ${r} is missing`);
  /** The run holding column `c` of a row: the first run ending past it. */
  const runAt = (ends: Uint32Array, c: number): number => {
    let lo = 0, hi = ends.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (ends[mid]! > c) hi = mid; else lo = mid + 1;
    }
    return lo;
  };
  const at = (lon: number, lat: number): Basin | null => {
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) throw new Error(`basin_grid: (${lon}, ${lat}) is not a point`);
    const r = Math.floor((lat - frame.s) / frame.dlat), c = Math.floor((lon - frame.w) / frame.dlon);
    if (r < 0 || r >= frame.rows || c < 0 || c >= frame.cols) return null;
    const { ends, ix } = grid[r]!;
    const k = ix[runAt(ends, c)]!;
    return k === 0 ? null : byIx.get(k)!;
  };
  return {
    basins,
    at,
    mostIn(box) {
      const [w, s, e, n] = box;
      if (![w, s, e, n].every(Number.isFinite) || e < w || n < s)
        throw new Error(`basin_grid: [${box.join(", ")}] is not a box`);
      const r0 = Math.max(0, Math.floor((s - frame.s) / frame.dlat));
      const r1 = Math.min(frame.rows - 1, Math.floor((n - frame.s) / frame.dlat));
      const c0 = Math.max(0, Math.floor((w - frame.w) / frame.dlon));
      const c1 = Math.min(frame.cols - 1, Math.floor((e - frame.w) / frame.dlon));
      const share = new Map<number, number>();
      for (let r = r0; r <= r1; r++) {
        const { ends, ix } = grid[r]!;
        const area = Math.cos(((frame.s + (r + 0.5) * frame.dlat) * Math.PI) / 180);
        for (let k = runAt(ends, c0), from = c0; k < ends.length && from <= c1; k++) {
          const to = Math.min(ends[k]!, c1 + 1);
          if (ix[k]! !== 0) share.set(ix[k]!, (share.get(ix[k]!) ?? 0) + (to - from) * area);
          from = to;
        }
      }
      if (share.size === 0) return null;
      const centre = at((w + e) / 2, (s + n) / 2);
      let best: Basin | null = null, most = -1;
      for (const [k, v] of share) {
        const b = byIx.get(k)!;
        const wins = v > most || (v === most && best !== null && best.id !== centre?.id
                                  && (b.id === centre?.id || b.id < best.id));
        if (wins) { best = b; most = v; }
      }
      return best;
    },
  };
}
