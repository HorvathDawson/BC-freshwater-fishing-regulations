// Style-audit screens (round 20c, owner: "an overall format audit … look as a designer would"): every tab (keep / gear /
// licence) and the open sheets (How to tell, a fish, the kokanee row, the date picker), light AND dark, at 390 px, for the
// five audit waters, full height (the page's `tall` mode), into a directory you name.
//   node app/design/regs-v11/tools/audit.mjs OUT_DIR [name-filter]
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { open } from './cdp.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PAGE = pathToFileURL(path.resolve(HERE, '../../regs-v11.html')).href;
const OUT = path.resolve(process.argv[2] || path.join(HERE, '../screens/audit'));
const only = process.argv[3];
fs.mkdirSync(OUT, { recursive: true });
const WATERS = [['vedder-oct10', 'w=vedder&d=1010'], ['bella-coola-may1', 'w=bellacoola&d=501&p=1'], ['kootenay-lwa-oct12', 'w=klwa&d=1012'],
  ['bennett', 'w=x35p&d=1010'], ['glenn-jun15', 'w=glenn&d=615']];
const SHEETS = [['howtell', 'sheet=hatch'], ['fish-rainbow', `sheet=${encodeURIComponent('sp:RB')}`], ['date', 'sheet=date']];
const shots = [];
for (const [w, h] of WATERS) {
  shots.push([`${w}-keep`, h, 'keep'], [`${w}-gear`, `${h}&sec=gear`, 'tab'], [`${w}-methods`, `${h}&sec=methods`, 'tab'], [`${w}-licence`, `${h}&sec=licence`, 'tab']);
  for (const [s, sh] of SHEETS) shots.push([`${w}-sheet-${s}`, `${h}&${sh}`, 'sheet']);
}
shots.push(['kootenay-lwa-oct12-sheet-kokanee', `w=klwa&d=1012&sheet=${encodeURIComponent('other:klwa_1012|o0')}`, 'sheet']);
for (const theme of ['light', 'dark']) for (const [name, hash, what] of shots) {
  const file = `${name}-${theme}`; if (only && !file.includes(only)) continue;
  const b = await open(`${PAGE}#shot=1&tall=1&${hash}${theme === 'dark' ? '&theme=dark' : ''}`, { width: 440, height: 1200, offline: true });
  try {
    await new Promise(r => setTimeout(r, 600));
    const clip = await b.evaluate(`(async () => { await document.fonts.ready;
      const R = e => { const r = e.getBoundingClientRect(); return {top: r.top + scrollY, bottom: r.bottom + scrollY}; };
      const hdr = document.getElementById('hdr');
      if (${JSON.stringify(what)} === 'keep') { const k = document.getElementById('keep'); return k ? {top: 0, bottom: R(k).bottom + 12} : null; }
      if (${JSON.stringify(what)} === 'tab') { const id = (location.hash.match(/sec=(gear|methods|licence)/) || [])[1] || 'gear', sec = document.getElementById(id); if (!sec || !hdr) return null;
        const h = hdr.cloneNode(true); h.removeAttribute('id'); h.style.position = 'relative'; sec.parentNode.insertBefore(h, sec);
        h.querySelectorAll('.segm[role=tablist] button').forEach(b => b.setAttribute('aria-pressed', b.dataset.sec === id));
        await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))); return {top: R(h).top, bottom: R(sec).bottom + 12}; }
      const sh = document.querySelector('#sheet.on'), sc = document.getElementById('sc'); if (!sh || !hdr || !sc) return null;
      const h = hdr.cloneNode(true); h.removeAttribute('id'); h.style.position = 'relative';
      document.querySelectorAll('#scrim, #sc').forEach(e => e.style.display = 'none');
      sh.style.cssText = 'position:relative;max-height:none;transform:none;transition:none'; sh.querySelector('.sbody')?.style.setProperty('overflow', 'visible');
      sh.parentNode.insertBefore(h, sh); await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
      return {top: R(h).top, bottom: R(sh).bottom + 4}; })()`);
    if (!clip) { console.log(`${file}: nothing to shoot`); continue; }
    const height = Math.ceil(clip.bottom - clip.top);
    await b.send('Emulation.setDeviceMetricsOverride', { width: 440, height: Math.max(900, Math.ceil(clip.bottom) + 20), deviceScaleFactor: 1, mobile: false });
    await new Promise(r => setTimeout(r, 300));
    const shot = await b.send('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true, clip: { x: 0, y: Math.floor(clip.top), width: 440, height, scale: 1 } });
    fs.writeFileSync(path.join(OUT, file + '.png'), Buffer.from(shot.data, 'base64'));
    console.log(`${file}  ${height} px`);
  } finally { await b.close(); }
}
