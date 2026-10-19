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
  ['bennett', 'w=x35p&d=1010'], ['glenn-jun15', 'w=glenn&d=615'], ['vedder-oct10-dark', 'w=vedder&d=1010&theme=dark']];
const OVERLAP = 90;
for (const [name, hash] of STATES) {
  if (only && !name.includes(only)) continue;
  const b = await open(`${PAGE}#shot=1&${hash}`, { width: 440, height: 900, offline: true });
  try {
    await new Promise(r => setTimeout(r, 600)); await b.evaluate('document.fonts.ready.then(() => 1)');
    const geo = JSON.parse(await b.evaluate(`JSON.stringify((() => { const app = document.querySelector('.app'), sc = document.getElementById('sc'), r = app.getBoundingClientRect();
      return {x: r.left, y: r.top, w: app.clientWidth, h: app.clientHeight, step: sc.clientHeight - ${OVERLAP}, max: sc.scrollHeight - sc.clientHeight}; })())`));
    let i = 0;
    for (let top = 0; ; top += geo.step, i++) {
      const at = Math.min(top, geo.max);
      await b.evaluate(`(() => { const sc = document.getElementById('sc'); sc.scrollTop = ${at}; return sc.scrollTop; })()`);
      await new Promise(r => setTimeout(r, 250));
      const shot = await b.send('Page.captureScreenshot', { format: 'png', clip: { x: geo.x, y: geo.y, width: geo.w, height: geo.h, scale: 1 } });
      const file = `${name}-f${String(i + 1).padStart(2, '0')}.png`;
      fs.writeFileSync(path.join(OUT, file), Buffer.from(shot.data, 'base64'));
      console.log(`${file}  scrollTop ${at}`);
      if (at >= geo.max) break;
    }
  } finally { await b.close(); }
}
