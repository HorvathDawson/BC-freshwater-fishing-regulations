// T41 — the regulations PROTOTYPE (app/design/regs-v11.html), staged at the port target's ref (never the working tree),
// opened in its #shot=1 phone frame: the .app box is 390 × 844 in a 440 × 900 window. Fonts load (not offline): the
// side-by-side is about the design, not the fallback metrics (T41 notes).
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { stage } from '../../design/regs-v11/port/stage.mjs';
import { open } from './cdp.mjs';
import { PHONE } from './states.mjs';
import { sleep } from './drive.mjs';

const APP = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');

/** Stage the prototype at `ref`; its fish pictures are the app's own (`../apps/mobile/assets/fish/…`), linked in. */
export function stageProto(ref) {
  const s = stage(ref);
  const assets = path.join(s.root, 'app/apps/mobile/assets');
  fs.mkdirSync(path.dirname(assets), { recursive: true });
  if (!fs.existsSync(assets)) fs.symlinkSync(path.join(APP, 'apps/mobile/assets'), assets, 'dir');
  return s;
}

/** Open the staged page at a hash; resolves once fonts and pictures are in. */
export async function openProto(s, hash, { tall = false, height = 900 } = {}) {
  const b = await open(`${pathToFileURL(s.page).href}#shot=1${tall ? '&tall=1' : ''}&${hash}`, { width: 440, height, dpr: PHONE.dpr });
  await sleep(700);
  await b.evaluate('document.fonts.ready.then(() => 1)');
  for (let t = 0; t < 50 && !(await b.evaluate('[...document.images].every((i) => i.complete)')); t++) await sleep(200);
  await sleep(300);
  return b;
}

/** the frame's geometry: the .app box, the regs header, the scroller */
export const PROTO_GEO = `(() => { const R = (e) => { if (!e) return null; const r = e.getBoundingClientRect(); return { x: r.x, y: r.y, w: r.width, h: r.height, top: r.top, bottom: r.bottom }; };
  return { app: R(document.querySelector('.app')), hdr: R(document.getElementById('hdr')), sc: R(document.getElementById('sc')),
    sheet: R(document.querySelector('#sheet.on .sbody')), scrollTop: document.getElementById('sc')?.scrollTop ?? 0 }; })()`;
