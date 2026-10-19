#!/usr/bin/env node
// regs-diagram: full-page screenshots of index.html for every water × day × theme at phone (390) and desktop (1100)
// widths, through regs-v11/tools/cdp.mjs (headless Chrome over the DevTools protocol). Also records console errors.
//   node app/design/regs-diagram/tools/shoot.mjs [only=<substring>] [clip=<top>,<height>] [at=<selector>] [out=<dir>]
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

const shots = [];
for (const w of D.waters) for (const theme of ['light', 'dark']) for (const [tag, width] of [['phone', 390], ['desk', 1100]]) {
  const name = `${w.w}-${w.md}-${theme}-${tag}`; if (only && !name.includes(only)) continue;
  shots.push({ name, width, url: `${page}#w=${w.w}&d=${w.md}&theme=${theme}` });
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
    const { data } = await b.send('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true, ...(clip ? { clip: { x: 0, y: clip[0], width: s.width, height: clip[1], scale: 1 } } : {}) });
    fs.writeFileSync(path.join(outDir, s.name + (clip ? `-${clip[0]}` : '') + '.png'), Buffer.from(data, 'base64'));
    const flag = errs !== '[]' || overflow > 0; if (flag) bad++;
    console.log(`${flag ? 'FAIL' : 'ok  '} ${s.name}  ${s.width}x${h}  overflow=${overflow}  errors=${errs}`);
  } finally { await b.close(); }
}
console.log(`${shots.length} shots, ${bad} flagged, in ${path.relative(process.cwd(), outDir)}`);
process.exit(bad ? 1 : 0);
