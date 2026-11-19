#!/usr/bin/env node
// (T319, owner 2026-11-19: "should make the store photo generator choose the stream automatically based on what rivers are
// doing") THE DAY'S RIVER for the "What the rivers are doing" split: the gauged water whose forecast moves most clearly ahead
// of today, chosen from the day's data, never a fixed water.
//   node app/tools/store-shots/pick-river.mjs <out dir> [--data https://data-staging.canifishthis.ca]
// Reads the data host's live gauge feed (feeds/live/index.json + each candidate's <station>.json) and the release's gauge
// table (release/manifest.json -> conditions-*.sqlite: station -> water, place, tidal). Writes <out>/river-pick.json (the
// pick, and the top ten with every number that ranked them) and prints the table; play-shots.mjs opens that water's chart
// and frames the change map on it. Same inputs, same pick: `rankRivers` is pure, ties go to the lower station id.
//
// THE SCORE (each factor printed): move x models x band x near x named, where
//   move   = the forecast's largest swing from today's flow, relative to it: over the short-range models (CLEVER 10 d,
//            COFFEE 5 d), the line the chart draws (`series.mid`) after the latest reading, max |mid - now| / now
//            (ELF, 30 d, is drawn but not ranked on: its line is a month-scale estimate)
//   models = forecast models with a series / 3 (CLEVER, COFFEE, ELF: each drawn on the chart, T895 colours them)
//   band   = the past-years band behind it (the chart's middle half / 10th-90th, from the gauge's climatology): a station
//            whose feed places today's flow among past years (`now.percentile`) has it; one without is out
//   near   = 1 within 100 km of Vancouver (the Lower Mainland and the Sea-to-Sky), 0.5 within 250 km, else 0.25
//   named  = 1 for an officially named river (a gnis: id, its gauge named "... River ..."), 0.7 for another named water
//            (a creek), 0.5 for an unnamed one: a reader should know the river
// Out before scoring: no water bound to the gauge, a tidal / backwater / diversion gauge, no flow now, today's flow under
// 1 m3/s (a trickle's relative swing says little on a chart), an ELF line more than 5x the highest short-range value or
// today's flow (seen 2026-11-19: 6,125 m3/s on Jump Creek flowing 7.3): with every model on, it would flatten the chart.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import zlib from 'node:zlib';

export const MODELS = ['CLEVER', 'COFFEE', 'ELF'];
const VANCOUVER = [49.2827, -123.1207];
const r6 = (x) => Math.round(x * 1e6) / 1e6;
export function kmFrom([lat1, lon1], [lat2, lon2]) {
  const rad = Math.PI / 180, a = Math.sin(((lat2 - lat1) * rad) / 2) ** 2 + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(((lon2 - lon1) * rad) / 2) ** 2;
  return 6371 * 2 * Math.asin(Math.sqrt(a));
}

/** One gauge's numbers and score, or `out` (why it is not ranked). `feed` is its <station>.json, `g` its gauge-table row. */
export function scoreGauge(g, feed) {
  const base = { station: g.station, item_id: g.item_id, name: g.name };
  if (!g.item_id) return { ...base, out: 'no water bound to the gauge' };
  if (g.tidal || g.backwater || g.diversion) return { ...base, out: 'tidal, backwater or diversion gauge' };
  const now = feed?.now?.discharge == null ? NaN : Number(feed.now.discharge);
  if (!(now > 0)) return { ...base, out: 'no flow now' };
  if (now < 1) return { ...base, out: `flow now ${now} m3/s, under 1` };
  // each model's drawn line after the latest reading (an hourly or daily `mid`, its `at` times beside it)
  const after = Date.parse(feed.now.at ?? '') || -Infinity;
  const ahead = (f) => (f.series?.mid ?? []).map((v, i) => [Date.parse(f.series.at?.[i] ?? ''), v]).filter(([t, v]) => v != null && Number.isFinite(Number(v)) && !(t <= after)).map(([, v]) => Number(v));
  const fc = MODELS.filter((m) => feed.forecasts?.[m] && ahead(feed.forecasts[m]).length);
  const short = fc.filter((m) => m !== 'ELF');
  if (!short.length) return { ...base, out: 'no short-range forecast (CLEVER / COFFEE)' };
  const top = Math.max(now, ...short.flatMap((m) => ahead(feed.forecasts[m])));
  if (fc.includes('ELF') && Math.max(...ahead(feed.forecasts.ELF)) > 5 * top) return { ...base, out: 'ELF line off the chart\'s scale (over 5x the rest)' };
  const swing = Math.max(...short.map((m) => Math.max(...ahead(feed.forecasts[m]).map((v) => Math.abs(v - now) / now))));
  if (typeof feed.now.percentile !== 'number') return { ...base, out: 'no past-years band (no climatology for the station)' };
  const band = 1;
  const km = kmFrom(VANCOUVER, [g.lat, g.lon]);
  const f = { move: r6(swing), models: r6(fc.length / MODELS.length), band, near: km <= 100 ? 1 : km <= 250 ? 0.5 : 0.25,
    named: !String(g.item_id).startsWith('gnis:') ? 0.5 : /\bRiver\b/.test(g.name ?? '') ? 1 : 0.7 };
  return { ...base, now, swing: r6(swing), modelsPresent: fc, percentileNow: r6(feed.now.percentile), km: Math.round(km), ...f,
    score: r6(f.move * f.models * f.band * f.near * f.named) };
}

/**
 * Every WATER ranked, best first (score, then item id): deterministic for the same inputs. The water's Conditions card leads
 * with one of its gauges, picked by the app (by section and day), so a water is only as good as its weakest gauge: every
 * gauge bound to it must qualify, and its score is the lowest of theirs. Whichever gauge the card shows, the chart holds.
 */
export function rankRivers({ gauges, feeds }) {
  const byWater = new Map();
  for (const g of gauges) if (g.item_id) byWater.set(g.item_id, [...(byWater.get(g.item_id) ?? []), g]);
  const ranked = [], out = [];
  for (const [item, gs] of [...byWater].sort(([a], [b]) => (a < b ? -1 : 1))) {
    const sc = gs.map((g) => (feeds[g.station] ? scoreGauge(g, feeds[g.station]) : { station: g.station, item_id: item, name: g.name, out: 'no forecast in the feed' }));
    const bad = sc.filter((x) => x.out);
    if (bad.length) { out.push({ item_id: item, name: gs[0].name, stations: gs.length, out: `gauge ${bad[0].station}: ${bad[0].out}` }); continue; }
    const weakest = sc.reduce((a, b) => (b.score < a.score || (b.score === a.score && b.station < a.station) ? b : a));
    ranked.push({ ...weakest, stations: sc.map((x) => x.station).sort() });
  }
  ranked.sort((a, b) => b.score - a.score || (a.item_id < b.item_id ? -1 : a.item_id > b.item_id ? 1 : 0));
  return { ranked, out };
}

// ---------- the CLI: the day's inputs from the data host ----------
async function main() {
  const args = process.argv.slice(2), val = (k, d) => (args.includes(k) ? args[args.indexOf(k) + 1] : d);
  const OUT = args.find((a, i) => !a.startsWith('--') && args[i - 1] !== '--data');
  if (!OUT) throw new Error('usage: pick-river.mjs <out dir> [--data URL]');
  const DATA = val('--data', 'https://data-staging.canifishthis.ca').replace(/\/$/, '');
  const get = async (u, kind = 'json') => { const r = await fetch(u); if (!r.ok) throw new Error(`pick-river: ${u}: HTTP ${r.status}`); return kind === 'json' ? r.json() : Buffer.from(await r.arrayBuffer()); };
  const manifest = await get(`${DATA}/release/manifest.json`);
  const condUrl = manifest.files?.conditions?.url;
  if (!condUrl) throw new Error('pick-river: the release manifest names no conditions file');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'pick-river-')), db = path.join(tmp, 'conditions.sqlite');
  let gauges;
  try {
    // the host may send it gzip-encoded, which fetch already undoes: gunzip only what still starts with gzip's magic
    const raw = await get(`${DATA}/release/${condUrl}`, 'bin');
    fs.writeFileSync(db, raw[0] === 0x1f && raw[1] === 0x8b ? zlib.gunzipSync(raw) : raw);
    const { DatabaseSync } = await import('node:sqlite');
    const d = new DatabaseSync(db, { readOnly: true });
    gauges = d.prepare('SELECT station, name, item_id, lon, lat, tidal, backwater, diversion FROM gauge ORDER BY station').all();
    d.close();
  } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
  const index = await get(`${DATA}/feeds/live/index.json`);
  const want = gauges.filter((g) => g.item_id && index.stations?.[g.station]?.forecast === true).map((g) => g.station);
  const feeds = {};
  for (let i = 0; i < want.length; i += 8) await Promise.all(want.slice(i, i + 8).map(async (st) => { feeds[st] = await get(`${DATA}/feeds/live/${st}.json`); }));
  const { ranked, out } = rankRivers({ gauges, feeds });
  const why = Object.entries(out.reduce((a, o) => { const k = o.out.replace(/^gauge \S+: /, '').replace(/[\d.]+ m3\/s/, 'N m3/s'); a[k] = (a[k] ?? 0) + 1; return a; }, {}));
  if (!ranked.length) throw new Error(`pick-river: no gauged water qualifies (${why.map(([k, n]) => `${n} ${k}`).join('; ')})`);
  const res = { release: manifest.data_release, feedFetchedAt: index.fetchedAt, forecastIssuedAt: index.forecastIssuedAt, candidates: want.length,
    pick: ranked[0], top: ranked.slice(0, 10), rule: 'score = move x models x band x near x named (pick-river.mjs header)' };
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, 'river-pick.json'), JSON.stringify(res, null, 2) + '\n');
  console.log(`pick-river: release ${res.release}, feed ${res.feedFetchedAt}, ${want.length} gauges with a forecast; waters out: ${why.map(([k, n]) => `${n} ${k}`).join('; ')}`);
  for (const [i, r] of res.top.entries()) console.log(`${String(i + 1).padStart(2)}. ${r.score.toFixed(3)}  ${r.item_id}: weakest gauge ${r.station} ${r.name} (of ${r.stations.length})  now ${r.now} m3/s, swing ${(r.swing * 100).toFixed(0)}%, models ${r.modelsPresent.join('/')}, now at the ${Math.round(r.percentileNow * 100)}th percentile, ${r.km} km`);
  console.log(`pick-river: ${res.pick.name} (${res.pick.item_id}) -> ${path.join(OUT, 'river-pick.json')}`);
}
if (import.meta.url === `file://${process.argv[1]}`) await main();
