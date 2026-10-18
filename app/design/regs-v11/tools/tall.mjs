// Full-length screens (user round 1): the WHOLE quota cards or the whole licence answer in one tall image, with the
// header (water, part, date) on top, so a panel reads whole cards instead of a phone-height cut.
//   node app/design/regs-v11/tools/tall.mjs [name-filter]   -> app/design/regs-v11/screens/ur1-*.png
// The page's own `tall` mode lets the phone frame grow to its content; the image is clipped from the top of the page to
// the bottom of the last card (`cards`) or, for `licence`, the header followed by the licence section alone.
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { open } from './cdp.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PAGE = pathToFileURL(path.resolve(HERE, '../../regs-v11.html')).href;
const OUT = path.resolve(HERE, '../screens');
const V = 'w=vedder&d=1010';
const TALL = [
  ['ur1-vedder-oct10-cards', V, 'cards'],
  ['ur1-chilliwack-part2-oct10-cards', 'w=chilliwack&d=1010&p=1', 'cards'],
  ['ur1-bella-coola-may1-mainstem-cards', 'w=bellacoola&d=501&p=1', 'cards'],
  ['ur1-kootenay-lwa-oct10-cards', 'w=klwa&d=1010', 'cards'],
  ['ur1-vedder-licence-16-plus', V, 'licence'],
  ['ur1-vedder-licence-under-16', `${V}&age=u16`, 'licence'],
  ['ur1-vedder-licence-outside-canada-open', `${V}&open=${encodeURIComponent('[data-res="non_resident_alien"]')}`, 'licence'],
];
const only = process.argv[2];
for (const [name, hash, what] of TALL) {
  if (only && !name.includes(only)) continue;
  const b = await open(`${PAGE}#shot=1&tall=1&${hash}`, { width: 440, height: 1200, offline: true });
  try {
    await new Promise(r => setTimeout(r, 600));
    const clip = await b.evaluate(`(async () => { await document.fonts.ready;
      const R = e => { const r = e.getBoundingClientRect(); return {top: r.top + scrollY, bottom: r.bottom + scrollY}; };
      if (${JSON.stringify(what)} === 'licence') {
        const sec = document.getElementById('g-licence'), hdr = document.getElementById('hdr');
        if (!sec || !hdr) return null;
        const h = hdr.cloneNode(true); h.removeAttribute('id'); h.style.position = 'relative'; sec.parentNode.insertBefore(h, sec);
        await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
        return {top: R(h).top, bottom: R(sec).bottom + 12}; }
      const cards = [...document.querySelectorAll('#keep article.row.ledger')];
      if (!cards.length) return null;
      const nf = document.querySelector('#keep .olist');            // the other fish under the cards, when there are any
      return {top: 0, bottom: Math.max(R(cards[cards.length - 1]).bottom, nf ? R(nf).bottom : 0) + 12}; })()`);
    if (!clip) throw new Error(`${name}: nothing to shoot`);
    const height = Math.ceil(clip.bottom - clip.top);
    await b.send('Emulation.setDeviceMetricsOverride', { width: 440, height: Math.max(900, Math.ceil(clip.bottom) + 20), deviceScaleFactor: 1, mobile: false });
    await new Promise(r => setTimeout(r, 300));
    const shot = await b.send('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true,
      clip: { x: 0, y: Math.floor(clip.top), width: 440, height, scale: 1 } });
    fs.writeFileSync(path.join(OUT, name + '.png'), Buffer.from(shot.data, 'base64'));
    console.log(`${name}  ${height} px`);
  } finally { await b.close(); }
}
