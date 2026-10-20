// Access-pattern benchmark over the split packages (copies in pkg/). Two engines:
//   node:sqlite (SQLite C, like expo-sqlite on a phone) and sql.js (wasm, like the web app).
// Prints cold open and warm per-operation medians (ms). Read-only.
import { DatabaseSync } from "node:sqlite";
import fs from "node:fs";
import zlib from "node:zlib";
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/app/package.json");
const initSqlJs = require("sql.js");
const D = new URL("./pkg/", import.meta.url).pathname;
const SQL = await initSqlJs();
const now = () => performance.now();
const med = (a) => { const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; };
const p95 = (a) => { const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length * 0.95)]; };

function open(engine, name) {
  const t0 = now(); let db;
  if (engine === "native") { db = new DatabaseSync(D + name + ".sqlite", { readOnly: true }); db.prepare("select count(*) from sqlite_master").get(); }
  else { const bytes = fs.readFileSync(D + name + ".sqlite"); db = new SQL.Database(bytes); db.exec("select count(*) from sqlite_master"); }
  return { db, ms: now() - t0, engine };
}
function q(h, sql) {
  if (h.engine === "native") { const st = h.db.prepare(sql); return { all: (...a) => st.all(...a), get: (...a) => st.get(...a) }; }
  const st = h.db.prepare(sql);
  const run = (a) => { st.bind(a); const out = []; while (st.step()) out.push(st.getAsObject()); st.reset(); return out; };
  return { all: (...a) => run(a), get: (...a) => run(a)[0] };
}
function time(n, f) { const ts = []; for (let i = 0; i < n; i++) { const t = now(); f(i); ts.push(now() - t); } return [med(ts), p95(ts)]; }
const pick = (arr, i) => arr[(i * 7919) % arr.length];

const report = {};
for (const engine of ["native", "wasm"]) {
  const R = (report[engine] = {});
  const core = open(engine, "core"), topo = open(engine, "topo"), rc = open(engine, "regs_common"), rf = open(engine, "regs_frames"),
        rs = open(engine, "regs_sid"), cond = open(engine, "cond"), near = open(engine, "near"), clim = open(engine, "clim");
  for (const [k, h] of Object.entries({ core, topo, regs_common: rc, regs_frames: rf, regs_sid: rs, cond, near, clim })) R["open " + k] = +h.ms.toFixed(1);
  // samples
  const sids = q(core, "select sid from item_section").all().map((r) => r.sid);
  const parts = q(rc, "select item_ord, part_ix, akey from part where akey is not null").all();
  const zd = Object.fromEntries(q(rc, "select tbl, d from zdict").all().map((r) => [r.tbl, Buffer.from(r.d)]));
  const unpack = (tbl, b) => { b = Buffer.from(b); const h = b[0]; const body = b.subarray(1);
    const raw = h === 2 ? zlib.inflateRawSync(body, { dictionary: zd[tbl] }) : h === 1 ? zlib.inflateRawSync(body) : b; return JSON.parse(raw.toString()); };
  // 1 map tap → water → part → akey
  const qTap = q(core, "select ord from item_section where sid = ?"), qPart = q(topo, "select part_ix from part_section where ord = ? and sid = ?"),
        qAkey = q(rc, "select akey from part where item_ord = ? and part_ix = ?");
  R["tap sid→ord→part→akey"] = time(500, (i) => { const sid = pick(sids, i); const o = qTap.get(sid); const p = qPart.get(o.ord, sid); qAkey.get(o.ord, p.part_ix); });
  // 2 regs page data for (akey, day): segment starts, cell, 5 frames + rows rows, decoded
  const qSeg = q(rc, "select s.starts from akey a join seglist s on s.seg = a.seg where a.akey = ?");
  const qCell = q(rf, "select ladder, rows, gear, licence, display from cell where akey = ? and s = ?");
  const fam = { ladder: q(rf, "select j from ladder_frame where id = ?"), rows: q(rf, "select j from rows_frame where id = ?"), gear: q(rf, "select j from gear_frame where id = ?"),
                licence: q(rf, "select j from licence_frame where id = ?"), display: q(rf, "select j from display_frame where id = ?") };
  const qRow = q(rf, "select j from rows_row where id = ?");
  const nRows = q(rf, "select count(*) n from rows_row").get().n;
  R["regs frames for a part+day (5 frames + 12 rows, inflate+parse)"] = time(300, (i) => {
    const pt = pick(parts, i); const starts = JSON.parse(qSeg.get(pt.akey).starts); const s = starts.length > 1 ? i % starts.length : 0;
    const c = qCell.get(pt.akey, s) ?? qCell.get(pt.akey, 0);
    for (const f of Object.keys(fam)) unpack(f + "_frame", fam[f].get(c[f]).j);
    for (let k = 0; k < 12; k++) unpack("rows_row", qRow.get((i * 31 + k * 97) % nRows).j);
  });
  // 3 unnamed tap: sid → akey via 8 KB block
  const qBlk = q(rs, "select b from section_akey where block = ?");
  R["unnamed sid→akey (block read)"] = time(500, (i) => { const sid = 1 + ((i * 104729) % 1957000); const b = qBlk.get(sid >> 12).b; (b[(sid & 4095) * 2] | (b[(sid & 4095) * 2 + 1] << 8)); });
  // 4 search by name prefix / alias / place
  const pre = ["ved", "cow", "koot", "fras", "lit", "bab", "sk", "che", "moy", "thom"];
  const qName = q(core, "select ord, name from item where name >= ? collate nocase and name < ? collate nocase order by name collate nocase limit 20");
  const qAl = q(core, "select item_id, alias from alias where alias >= ? collate nocase and alias < ? collate nocase limit 20");
  const qPl = q(core, "select place_id, name from place where name >= ? and name < ? limit 20");
  const nxt = (s) => s.slice(0, -1) + String.fromCharCode(s.charCodeAt(s.length - 1) + 1);
  R["search name+alias+place prefix"] = time(300, (i) => { const p = pre[i % pre.length]; qName.all(p, nxt(p)); qAl.all(p, nxt(p)); const P = p[0].toUpperCase() + p.slice(1); qPl.all(P, nxt(P)); });
  // 5 near: waters near a town
  const placeIds = q(near, "select distinct place_id from place_water").all().map((r) => r.place_id);
  const qNear = q(near, "select ckm, ord from place_water where place_id = ? order by ckm limit 50");
  R["near: waters by town (50)"] = time(300, (i) => qNear.all(pick(placeIds, i)));
  // 6 conditions for a section
  const gsids = q(cond, "select sid from section_gauge").all().map((r) => r.sid);
  const qSG = q(cond, "select station, trust, mag from section_gauge where sid = ?"), qSP = q(cond, "select panel_id from section_panel where sid = ?"),
        qPM = q(cond, "select * from panel_member where panel_id = ?"), qG = q(cond, "select * from gauge where station = ?"),
        qCl = q(clim, "select bands from gauge_clim where station = ? and parameter = ?");
  R["conditions: section→gauge+panel+members+gauge row+climatology"] = time(300, (i) => {
    const sid = pick(gsids, i); const g = qSG.get(sid); const p = qSP.get(sid); if (p) qPM.all(p.panel_id); qG.get(g.station); qCl.get(g.station, "discharge"); });
  // 7 trace downstream to the gauge (walk section_down)
  const dsids = q(cond, "select sid from section_down").all().map((r) => r.sid);
  const qDown = q(cond, "select down_sid from section_down where sid = ?");
  R["trace downstream (walk section_down, ≤5000 steps)"] = time(100, (i) => { let s = pick(dsids, i), n = 0; for (; n < 5000; n++) { const r = qDown.get(s); if (!r || !r.down_sid) break; s = r.down_sid; } });
  // 8 part highlight / up-down nav: all sids of a part
  const qPS = q(topo, "select sid from part_section where ord = ? and part_ix = ?");
  R["part sids for highlight (part_section by ord)"] = time(300, (i) => { const pt = pick(parts, i); qPS.all(pt.item_ord, pt.part_ix); });
  // 9 status index: JS decode once + lookup for 5,000 visible sids
  const buf = fs.readFileSync(D + "../status_index.bin");
  const t0 = now(); let pos = 21; const vi = () => { let v = 0, sh = 0, b; do { b = buf[pos++]; v += (b & 0x7f) * 2 ** sh; sh += 7; } while (b & 0x80); return v; };
  const nP = vi(); const profiles = []; for (let k = 0; k < nP; k++) { const nr = vi(); const runs = []; for (let j = 0; j < nr; j++) runs.push(vi()); profiles.push(runs); }
  const nR = vi(); const gap = new Uint32Array(nR), cnt = new Uint32Array(nR), prof = new Uint32Array(nR);
  for (let k = 0; k < nR; k++) gap[k] = vi(); for (let k = 0; k < nR; k++) cnt[k] = vi(); for (let k = 0; k < nR; k++) prof[k] = vi();
  const start = new Uint32Array(nR); let end = 0; for (let k = 0; k < nR; k++) { start[k] = end + gap[k]; end = start[k] + cnt[k]; }
  R["status index decode (once)"] = +(now() - t0).toFixed(1);
  const look = (sid) => { let lo = 0, hi = nR - 1; while (lo <= hi) { const m = (lo + hi) >> 1; if (start[m] > sid) hi = m - 1; else if (start[m] + cnt[m] <= sid) lo = m + 1; else return prof[m]; } return -1; };
  R["status lookup 5,000 visible sids"] = time(50, (i) => { for (let k = 0; k < 5000; k++) look(1 + ((i * 5000 + k) * 389) % 1957000); });
  // 10 build indexes on device instead of shipping them (native only)
  if (engine === "native") {
    fs.copyFileSync(D + "core.sqlite", D + "core_noidx.sqlite"); const w = new DatabaseSync(D + "core_noidx.sqlite");
    w.exec("drop index if exists item_by_name_nc; drop index if exists alias_nc; drop index if exists item_by_name; drop index if exists section_by_item; drop index if exists alias_by_text; drop index if exists place_by_name");
    const t = now(); w.exec("create index a1 on item(name collate nocase); create index a2 on alias(alias collate nocase); create index a3 on item_section(ord); create index a4 on place(name)");
    R["core: build 4 indexes on device"] = +(now() - t).toFixed(1); w.close();
  }
}
console.log(JSON.stringify(report, null, 1));
