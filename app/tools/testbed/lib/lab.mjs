// (T420) THE MAP LAB from the host: open the card (a long press on the © chip, `map-credits-chip`), set its switches by
// testID (`map-lab-<key>`, read off a uiautomator dump: RN's testID is the view's resource-id), read the foot
// (`map-lab-state`), close it (`map-lab-close`). Only in a lab build (`EXPO_PUBLIC_MAP_LAB=1`, session.sh sets it). The
// switches are session-only: a cold start is today's map again.
//
//   PAN_LAB=stateGapMs=250,streamsOneBucket=true   (the `pan` scenario; parseLab below)
import * as device from './device.mjs';

/** The switch each key's value means ON (MapLab.tsx's ROWS). */
const ON = { pixelCap: true, prefetch: 2, stateGapMs: 250, streamsOneBucket: true, roads: true, landuse: true, labels: true, hideInvisible: true };
export const LAB_KEYS = Object.keys(ON);

/** `k=v,k=v` → { k: v } (numbers and booleans read as such). */
export function parseLab(s) {
  const out = {};
  for (const part of (s ?? '').split(',').map((p) => p.trim()).filter(Boolean)) {
    const [k, v] = part.split('=');
    if (!(k in ON)) throw new Error(`lab: unknown switch ${k} (one of ${LAB_KEYS.join(', ')})`);
    out[k] = v === 'true' ? true : v === 'false' ? false : Number(v);
    if (out[k] !== ON[k] && !(typeof ON[k] === 'boolean' ? typeof out[k] === 'boolean' : [0, 4].includes(out[k])))
      throw new Error(`lab: ${k}=${v} is not a value its switch has`);
  }
  return out;
}

/** The screen's nodes: resource-id, checked, bounds centre, text. */
function nodes(d = device) {
  const r = d.sh('uiautomator dump /sdcard/tb-lab.xml 2>&1');
  if (!/dumped to/.test(r)) throw new Error(`lab: uiautomator dump failed: ${r.trim()}`);
  const x = d.sh('cat /sdcard/tb-lab.xml');
  return [...x.matchAll(/<node [^>]*>/g)].map(([n]) => {
    const a = (k) => new RegExp(` ${k}="([^"]*)"`).exec(n)?.[1] ?? '';
    const b = /\[(\d+),(\d+)\]\[(\d+),(\d+)\]/.exec(a('bounds'))?.slice(1).map(Number) ?? [0, 0, 0, 0];
    return { id: a('resource-id'), checked: a('checked') === 'true', text: a('text'), x: (b[0] + b[2]) >> 1, y: (b[1] + b[3]) >> 1 };
  });
}
const byId = (ns, id) => ns.find((n) => n.id === id || n.id.endsWith(`/${id}`));

/** Open the card (no-op when it is up). Throws when the build has no lab (the chip's long press does nothing). */
export async function openLab(d = device) {
  let ns = nodes(d);
  if (byId(ns, 'map-lab')) return;
  const chip = byId(ns, 'map-credits-chip');
  if (!chip) throw new Error('lab: no map-credits-chip on screen (is the map up?)');
  d.sh(`input swipe ${chip.x} ${chip.y} ${chip.x} ${chip.y} 1200`);           // a long press
  await d.sleep(1200);
  ns = nodes(d);
  if (!byId(ns, 'map-lab')) throw new Error('lab: the card did not open (a build without EXPO_PUBLIC_MAP_LAB=1?)');
}

/** The foot line ("Today's map." or "Differs from today: …"). */
export function labState(d = device) {
  const ns = nodes(d), foot = byId(ns, 'map-lab-state');
  if (!foot) throw new Error('lab: card not open');
  return foot.text.replace(/&#39;/g, "'").replace(/&amp;/g, '&');
}

/** Set switches ({ key: value }), each checked after its tap. Leaves the card open. */
export async function setLab(change, d = device) {
  await openLab(d);
  for (const [k, v] of Object.entries(change)) {
    const want = v === ON[k];
    let sw = byId(nodes(d), `map-lab-${k}`);
    if (!sw) throw new Error(`lab: no switch map-lab-${k}`);
    if (sw.checked !== want) {
      d.sh(`input tap ${sw.x} ${sw.y}`);
      await d.sleep(800);
      sw = byId(nodes(d), `map-lab-${k}`);
      if (sw.checked !== want) throw new Error(`lab: map-lab-${k} did not turn ${want ? 'on' : 'off'}`);
    }
  }
  return labState(d);
}

export async function closeLab(d = device) {
  const c = byId(nodes(d), 'map-lab-close');
  if (!c) return;
  d.sh(`input tap ${c.x} ${c.y}`);
  await d.sleep(600);
  if (byId(nodes(d), 'map-lab')) throw new Error('lab: the card did not close');
}

/** Open, set, read the foot, close: the one call a scenario makes. */
export async function applyLab(change, d = device) {
  const foot = await setLab(change, d);
  await closeLab(d);
  return foot;
}
