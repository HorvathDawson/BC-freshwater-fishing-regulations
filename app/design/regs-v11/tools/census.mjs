// Style census (round 20c): what the RENDERED Keep tab (and the gear / licence sections) actually uses — distinct font sizes,
// weights and families, text and link colours, link markers, left edges, ALL-CAPS labels, borders / radii and touch targets
// under 44 px — per state, light and dark. Measured from computed styles, so an audit names drift by numbers, not by
// reading CSS.  node app/design/regs-v11/tools/census.mjs [out.json] [name-filter]
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { open } from './cdp.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PAGE = pathToFileURL(path.resolve(HERE, '../../regs-v11.html')).href;
const STATES = [['vedder-oct10', 'w=vedder&d=1010'], ['bella-coola-may1', 'w=bellacoola&d=501&p=1'], ['kootenay-lwa-oct12', 'w=klwa&d=1012'],
  ['bennett', 'w=x35p&d=1010'], ['glenn-jun15', 'w=glenn&d=615']];
const only = process.argv[3];
export const CENSUS = `(() => {
  const vis = e => { const r = e.getBoundingClientRect(); const cs = getComputedStyle(e); return r.width > 0 && r.height > 0 && cs.visibility !== 'hidden' && cs.display !== 'none'; };
  const textEls = root => [...root.querySelectorAll('*')].filter(e => vis(e) && !e.closest('script,style,svg') && [...e.childNodes].some(n => n.nodeType === 3 && n.textContent.trim()));
  const count = (xs) => { const m = new Map(); xs.forEach(x => m.set(x, (m.get(x) || 0) + 1)); return [...m].sort((a, b) => b[1] - a[1]).map(([k, n]) => k + ' ×' + n); };
  const sect = (root, name) => { if (!root) return null; const els = textEls(root), cs = els.map(e => getComputedStyle(e));
    const links = [...root.querySelectorAll('a, button')].filter(vis);
    const small = [...root.querySelectorAll('a, button, [role=button], summary')].filter(vis).map(e => { const r = e.getBoundingClientRect(); return [e.className && String(e.className).split(' ')[0] || e.tagName, Math.round(r.width), Math.round(r.height), (e.textContent || '').trim().slice(0, 24)]; }).filter(x => x[2] < 32);
    const left = count(els.filter(e => !e.closest('.kpics, .tbk, button, a')).map(e => Math.round(e.getBoundingClientRect().left)));
    return {name, sizes: count(cs.map(c => parseFloat(c.fontSize).toFixed(1))), weights: count(cs.map(c => c.fontWeight)),
      families: count(cs.map(c => c.fontFamily.split(',')[0].replace(/"/g, ''))), colours: count(cs.map(c => c.color)),
      linkColours: count(links.map(e => getComputedStyle(e).color)), linkMarks: count(links.map(e => /↗/.test(e.textContent) ? '↗' : /›/.test(e.textContent) ? '›' : getComputedStyle(e).textDecorationLine !== 'none' ? 'underline' : 'plain')),
      caps: count(els.filter(e => getComputedStyle(e).textTransform === 'uppercase' || /^[A-Z0-9 ·&’'\\-]{6,}$/.test(e.textContent.trim())).map(e => (e.className && String(e.className).split(' ')[0]) || e.tagName)),
      borders: count([...root.querySelectorAll('*')].filter(vis).map(e => getComputedStyle(e)).filter(c => c.borderTopStyle !== 'none' && parseFloat(c.borderTopWidth) > 0).map(c => c.borderTopWidth + ' ' + c.borderTopColor + ' r' + c.borderTopLeftRadius)),
      radii: count([...root.querySelectorAll('*')].filter(vis).map(e => getComputedStyle(e).borderTopLeftRadius).filter(r => r !== '0px')),
      leftEdges: left.slice(0, 12), smallTargets: small.slice(0, 20), words: (root.innerText || '').trim().split(/\\s+/).length}; };
  return JSON.stringify({keep: sect(document.getElementById('keep'), 'keep'), gear: sect(document.getElementById('gear'), 'gear'), licence: sect(document.getElementById('licence'), 'licence'), hdr: sect(document.getElementById('hdr'), 'hdr')}); })()`;
const out = {};
for (const theme of ['light', 'dark']) for (const [name, hash] of STATES) {
  const key = `${name}-${theme}`; if (only && !key.includes(only)) continue;
  const b = await open(`${PAGE}#shot=1&tall=1&${hash}${theme === 'dark' ? '&theme=dark' : ''}`, { width: 440, height: 1200, offline: true });
  try { await new Promise(r => setTimeout(r, 600)); await b.evaluate('document.fonts.ready.then(() => 1)');
    out[key] = JSON.parse(await b.evaluate(CENSUS));
    const k = out[key].keep; console.log(`${key.padEnd(28)} keep: ${k.sizes.length} sizes, ${k.weights.length} weights, ${k.families.length} families, ${k.colours.length} colours, links ${k.linkColours.length} colours / ${k.linkMarks.join(' ')}, ${k.smallTargets.length} targets < 32px`);
  } finally { await b.close(); }
}
if (process.argv[2]) fs.writeFileSync(process.argv[2], JSON.stringify(out, null, 1));
