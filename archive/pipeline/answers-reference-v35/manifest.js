#!/usr/bin/env node
'use strict';
/* Merge the per-slice manifests golden.js wrote into OUT/manifest.json.
   usage: node manifest.js OUT N   (N = the number of waters the build holds; every slice must be there) */
const fs = require('fs');
const path = require('path');
const [out, n] = process.argv.slice(2);
const parts = fs.readdirSync(out).filter(f => /^manifest\.\d+-\d+\.json$/.test(f)).map(f => JSON.parse(fs.readFileSync(path.join(out, f), 'utf8')));
parts.sort((a, b) => a.range[0] - b.range[0]);
let at = 0; for (const p of parts){ if (p.range[0] !== at) throw new Error(`slices leave a gap at water ${at}`); at = p.range[1]; }
if (+n && at !== +n) throw new Error(`slices cover ${at} of ${n} waters`);
const counts = {}; parts.forEach(p => Object.entries(p.counts).forEach(([k, v]) => { counts[k] = k === 'profiles' ? v : (counts[k] || 0) + v; }));
const files = fs.readdirSync(out).filter(f => f.endsWith('.gz')).sort().map(f => ({ file: f, bytes: fs.statSync(path.join(out, f)).size }));
const m = { what: parts[0].what, page_sha256: parts[0].page_sha256, build: parts[0].build, bundle: parts[0].bundle, node: parts[0].node,
  waters: at, slices: parts.map(p => ({ range: p.range, seconds: p.seconds })), counts,
  bytes_gz: files.reduce((a, f) => a + f.bytes, 0), files,
  note: 'licence_answers.*.jsonl.gz ids are content hashes, shared across slices; licence.*.jsonl.gz maps every profile to one' };
fs.writeFileSync(path.join(out, 'manifest.json'), JSON.stringify(m, null, 1));
console.log(JSON.stringify({ waters: m.waters, counts, bytes_gz: m.bytes_gz }));
