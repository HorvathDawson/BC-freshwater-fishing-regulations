// Screenfuls (round 20c, owner: "keep in mind screen size and what a user sees as they scroll"): a card as the consecutive
// 390 x 844 viewport frames a phone shows — the top, then each scroll step of the regulations scroller with ~90 px of
// overlap — so each frame can be judged on its own.  node app/design/regs-v11/tools/frames.mjs OUT_DIR [name-filter]
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { open } from './cdp.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PAGE = pathToFileURL(path.resolve(HERE, '../../regs-v11.html')).href;
const OUT = path.resolve(process.argv[2] || path.join(HERE, '../screens/frames'));
const only = process.argv[3];
fs.mkdirSync(OUT, { recursive: true });
const STATES = [['vedder-oct10', 'w=vedder&d=1010'], ['chilliwack-p2-oct10', 'w=chilliwack&d=1010&p=1'], ['bella-coola-may1', 'w=bellacoola&d=501&p=1'],
  ['bennett', 'w=x35p&d=1010'], ['glenn-jun15', 'w=glenn&d=615'], ['vedder-oct10-dark', 'w=vedder&d=1010&theme=dark'],
  // round 20i: the UR4 panel's frames (screens/ur4/frames), re-shot on each result
  ['kootenay-lwa-oct12', 'w=klwa&d=1012'], ['kootenay-lwa-oct12-licence-u16-ab', 'w=klwa&d=1012&sec=licence&age=u16&res=non_resident'], ['vedder-may15', 'w=vedder&d=515']];
// a user-panel round shoots its own waters: STATES_JSON='[["name","hash"],…]' replaces the list
if (process.env.STATES_JSON) STATES.splice(0, STATES.length, ...JSON.parse(process.env.STATES_JSON));
const OVERLAP = 90;
for (const [name, hash] of STATES) {
  if (only && !name.includes(only)) continue;
  const b = await open(`${PAGE}#shot=1&${hash}`, { width: 440, height: 900, offline: true });
  try {
    await new Promise(r => setTimeout(r, 600)); await b.evaluate('document.fonts.ready.then(() => 1)');
    const geo = JSON.parse(await b.evaluate(`JSON.stringify((() => { const app = document.querySelector('.app'), sc = document.getElementById('sc'), r = app.getBoundingClientRect();
      return {x: r.left, y: r.top, w: app.clientWidth, h: app.clientHeight, step: sc.clientHeight - ${OVERLAP}, max: sc.scrollHeight - sc.clientHeight}; })())`));
    let i = 0, lastAt = -1;
    for (let top = 0; ; top += geo.step, i++) {
      let at = Math.min(top, geo.max);
      // round 20i (UR4 fix 3): a frame never starts with a line under the pin stack (step back by the stack's height) and never
      // ends on a heading whose rule is off screen (step back so the heading opens the next frame) — a user reads frames, not pixels
      at = +await b.evaluate(`(async () => { const sc = document.getElementById('sc'), R = e => e.getBoundingClientRect(); let at = ${at}; sc.scrollTop = at; await new Promise(r => setTimeout(r, 250));
        const bar = document.querySelector('#pins .pinbar'), barH = bar && !bar.hidden ? R(bar).height : 0; if (barH && at > 0) { at = Math.max(0, at - barH); sc.scrollTop = at; await new Promise(r => setTimeout(r, 200)); }
        for (let k = 0; k < 3 && at > 0; k++) { const bb = bar && !bar.hidden ? R(bar).bottom : R(sc).top + (document.getElementById('hdr')?.offsetHeight || 0);   /* no text line straddles the stack's bottom edge */
          const str = [...document.querySelectorAll('#keep .tl, #keep .ks, #keep .kmeta, #keep .duty, #keep .kpres, #keep .worked li, #keep .worked p, #keep .nm, #keep .orow, #keep .gorigin, #keep .tkick')].find(e => { const r = R(e); return r.height > 0 && r.top < bb - 1 && r.bottom > bb + 1; });
          if (!str) break; at = Math.max(0, at - (bb - R(str).top) - 4); sc.scrollTop = at; await new Promise(r => setTimeout(r, 200)); }
        const vb = R(sc).bottom, cut = [...document.querySelectorAll('#keep .tier .tl, #keep .krow .ks, #keep .krow .nm, #keep .bkh, #keep .tier .tkick')].find(e => { const r = R(e); return r.top > vb - 80 && r.bottom > vb - 14 && r.top < vb; });
        if (cut && at > 0) { at = Math.max(0, at - (vb - R(cut).top) - 6); sc.scrollTop = at; await new Promise(r => setTimeout(r, 200)); }
        return sc.scrollTop; })()`);
      if (i && at <= lastAt) at = Math.min(lastAt + Math.round(geo.step / 2), geo.max), await b.evaluate(`(() => { const sc = document.getElementById('sc'); sc.scrollTop = ${at}; return sc.scrollTop; })()`), await new Promise(r => setTimeout(r, 250));
      lastAt = at; top = at;
      const shot = await b.send('Page.captureScreenshot', { format: 'png', clip: { x: geo.x, y: geo.y, width: geo.w, height: geo.h, scale: 1 } });
      const file = `${name}-f${String(i + 1).padStart(2, '0')}.png`;
      fs.writeFileSync(path.join(OUT, file), Buffer.from(shot.data, 'base64'));
      console.log(`${file}  scrollTop ${at}`);
      if (at >= geo.max) break;
    }
  } finally { await b.close(); }
}
