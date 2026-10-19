#!/usr/bin/env node
// regs-diagram: full-page screenshots of index.html for every water × day × theme at phone (390) and desktop (1100)
// widths, through regs-v11/tools/cdp.mjs (headless Chrome over the DevTools protocol). Also records console errors.
//   node app/design/regs-diagram/tools/shoot.mjs [only=<substring>] [clip=<top>,<height>] [at=<selector>] [out=<dir>] [forms=V,J,F,T,K]
// forms=… shoots the gallery section (#G) alone, once per listed form, as <water>-<day>-<theme>-<width>-<form>.png.
// The committed screens/ were then quantized to 256-colour PNGs (Pillow: im.quantize(256, MEDIANCUT, no dither)), ~3× smaller.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { open } from '../../regs-v11/tools/cdp.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const page = pathToFileURL(path.resolve(here, '../index.html')).href;
const arg = k => (process.argv.find(a => a.startsWith(k + '=')) || '').slice(k.length + 1);
const outDir = path.resolve(arg('out') || path.resolve(here, '../screens')); fs.mkdirSync(outDir, { recursive: true });
const only = arg('only'), at = arg('at'); let clip = arg('clip') ? arg('clip').split(',').map(Number) : null;
const dataSrc = fs.readFileSync(path.resolve(here, '../data.js'), 'utf8');
const D = JSON.parse(dataSrc.slice(dataSrc.indexOf('window.DIAG =') + 13).replace(/;\s*$/, ''));

const forms = arg('forms') ? arg('forms').split(',') : [null];
const shots = [];
for (const w of D.waters) for (const theme of ['light', 'dark']) for (const [tag, width] of [['phone', 390], ['desk', 1100]]) for (const f of forms) {
  const name = `${w.w}-${w.md}-${theme}-${tag}${f ? '-' + f : ''}`; if (only && !name.includes(only)) continue;
  shots.push({ name, width, form: f, url: `${page}#w=${w.w}&d=${w.md}&theme=${theme}${f ? '&f=' + f : ''}` });
}
let bad = 0;
for (const s of shots) {
  const b = await open(s.url, { width: s.width, height: 900, init: `window.__errs = []; window.addEventListener('error', e => __errs.push(String(e.message)));` });
  try {
    await b.evaluate('document.fonts ? document.fonts.ready.then(() => 1) : 1');
    const h = await b.evaluate('Math.ceil(document.documentElement.scrollHeight)');
    await b.send('Emulation.setDeviceMetricsOverride', { width: s.width, height: h, deviceScaleFactor: 1, mobile: s.width < 600 });
    const errs = await b.evaluate('JSON.stringify(window.__errs || [])');
    const overflow = await b.evaluate('document.documentElement.scrollWidth - document.documentElement.clientWidth');
    if (at) { const top = await b.evaluate(`Math.floor(document.querySelector(${JSON.stringify(at)}).getBoundingClientRect().top + scrollY)`); clip = [top, clip ? clip[1] : 1400]; }
    if (s.form) { const r = await b.evaluate(`(r => [Math.floor(r.top + scrollY), Math.ceil(r.height) + 8])(document.querySelector('#G').getBoundingClientRect())`); clip = r; }
    const { data } = await b.send('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true, ...(clip ? { clip: { x: 0, y: clip[0], width: s.width, height: clip[1], scale: 1 } } : {}) });
    fs.writeFileSync(path.join(outDir, s.name + (clip && !s.form ? `-${clip[0]}` : '') + '.png'), Buffer.from(data, 'base64'));
    const flag = errs !== '[]' || overflow > 0; if (flag) bad++;
    console.log(`${flag ? 'FAIL' : 'ok  '} ${s.name}  ${s.width}x${h}  overflow=${overflow}  errors=${errs}`);
  } finally { await b.close(); }
}
console.log(`${shots.length} shots, ${bad} flagged, in ${path.relative(process.cwd(), outDir)}`);
process.exit(bad ? 1 : 0);
