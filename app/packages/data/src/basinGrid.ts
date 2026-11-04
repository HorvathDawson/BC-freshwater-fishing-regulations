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
  return {
    basins,
    at(lon, lat) {
      if (!Number.isFinite(lon) || !Number.isFinite(lat)) throw new Error(`basin_grid: (${lon}, ${lat}) is not a point`);
      const r = Math.floor((lat - frame.s) / frame.dlat), c = Math.floor((lon - frame.w) / frame.dlon);
      if (r < 0 || r >= frame.rows || c < 0 || c >= frame.cols) return null;
      const { ends, ix } = grid[r]!;
      // the first run ending past the column
      let lo = 0, hi = ends.length - 1;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (ends[mid]! > c) hi = mid; else lo = mid + 1;
      }
      const k = ix[lo]!;
      return k === 0 ? null : byIx.get(k)!;
    },
  };
}
