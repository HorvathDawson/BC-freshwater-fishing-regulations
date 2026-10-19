// Rendered word counts (round 20b, owner: "fewer words without missing anything" — measured on what is drawn, not the DOM):
//   node app/design/regs-v11/tools/words.mjs [out.json]
// For each state: the words of the trout/char cards (every `article.row.ledger` on the Keep tab, innerText), the whole Keep
// tab, the Gear section and the Licence section (whichever ids the page has: a combined `#gear` holding `#g-licence` before
// round 20b, `#gear` + `#licence` after). A word is a run of non-space characters.
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { open } from './cdp.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PAGE = pathToFileURL(path.resolve(HERE, '../../regs-v11.html')).href;
const STATES = [['vedder-oct10', 'w=vedder&d=1010'], ['vedder-jun15', 'w=vedder&d=615'], ['chilliwack-p2-oct10', 'w=chilliwack&d=1010&p=1'],
  ['bella-coola-may1-mainstem', 'w=bellacoola&d=501&p=1'], ['bella-coola-oct10-mainstem', 'w=bellacoola&d=1010&p=1'],
  ['kootenay-lwa-oct10', 'w=klwa&d=1010'], ['kootenay-lwa-oct12', 'w=klwa&d=1012'], ['bennett-lake', 'w=x35p&d=1010'],
  ['ape-creek-may1', 'w=ape&d=501'], ['glenn-creek-jun15', 'w=glenn&d=615']];
const out = {};
for (const [name, hash] of STATES) {
  const b = await open(`${PAGE}#shot=1&tall=1&${hash}`, { width: 440, height: 1200, offline: true });
  try {
    await new Promise(r => setTimeout(r, 500));
    out[name] = JSON.parse(await b.evaluate(`(async () => { await document.fonts.ready;
      const wc = el => el ? (el.innerText || '').trim().split(/\\s+/).filter(Boolean).length : null;
      const lic = document.getElementById('licence') || document.getElementById('g-licence'), gear = document.getElementById('gear');
      const gearOnly = () => { if (!gear) return null; const c = gear.cloneNode(true); c.querySelectorAll('#licence, #g-licence').forEach(e => e.remove()); document.body.append(c); const n = wc(c); c.remove(); return n; };
      return JSON.stringify({cards: [...document.querySelectorAll('#keep article.row.ledger')].reduce((a, e) => a + wc(e), 0), keep: wc(document.getElementById('keep')), gear: gearOnly(), licence: wc(lic)}); })()`));
    console.log(`${name.padEnd(28)} cards ${String(out[name].cards).padStart(4)}  keep ${String(out[name].keep).padStart(4)}  gear ${String(out[name].gear).padStart(4)}  licence ${String(out[name].licence).padStart(4)}`);
  } finally { await b.close(); }
}
if (process.argv[2]) fs.writeFileSync(process.argv[2], JSON.stringify(out, null, 1));
