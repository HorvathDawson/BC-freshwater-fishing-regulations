// Colour-latency harness, option E: a section-id spatial index for the TEST AREA ONLY — every water id each z<Z> tile
// of the atlas carries, keyed "x/y". This is what a shipped E would need for the whole province (built by the
// pipeline from geometry); here it is read from the tiles so the measurement does not wait on a pipeline change.
//   cd app && node ../handoff/PLAN/app-data/colour-latency/harness/build-cell-index.mjs <atlas.pmtiles> <out.json> lon lat z rx ry
import { openSync, readSync, writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { PMTiles } from "../../../../../app/node_modules/pmtiles/dist/esm/index.js";
import { mvtIds } from "./mvt-ids.js";

const [file, out, lon, lat, z, rx, ry] = process.argv.slice(2);
const Z = Number(z);
const fd = openSync(file, "r");
const source = {
  getKey: () => file,
  async getBytes(offset, length) {
    const buf = Buffer.alloc(length);
    readSync(fd, buf, 0, length, offset);
    return { data: buf.buffer.slice(buf.byteOffset, buf.byteOffset + length) };
  },
};
const pm = new PMTiles(source);
const n = 2 ** Z;
const cx = Math.floor(((Number(lon) + 180) / 360) * n);
const latR = (Number(lat) * Math.PI) / 180;
const cy = Math.floor(((1 - Math.log(Math.tan(latR) + 1 / Math.cos(latR)) / Math.PI) / 2) * n);
const cells = {};
let tiles = 0, ids = 0;
const t0 = Date.now();
for (let x = cx - Number(rx); x <= cx + Number(rx); x++)
  for (let y = cy - Number(ry); y <= cy + Number(ry); y++) {
    const t = await pm.getZxy(Z, x, y);
    if (!t) continue;
    let bytes = new Uint8Array(t.data);
    if (bytes[0] === 0x1f && bytes[1] === 0x8b) bytes = new Uint8Array(gunzipSync(bytes));
    const got = mvtIds(bytes, ["stream", "lake", "wetland"]);
    cells[`${x}/${y}`] = got;
    tiles++;
    for (const v of Object.values(got)) ids += v.length;
  }
writeFileSync(out, JSON.stringify({ z: Z, center: [cx, cy], cells }));
console.log(`z${Z} ${tiles} tiles, ${ids} ids, ${Date.now() - t0} ms -> ${out}`);
