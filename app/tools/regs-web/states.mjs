// T41 — THE STATE TABLE of the browser tools: a golden state id → the same state in the prototype (its #hash) and in the
// app (the web start link, `@app/ui` parseStartLink). The prototype's (w, d, p) and the app's (item, part, date) both come
// from the committed port goldens (port/golden/golden.json + taps.json, generated at the port target's ref), never typed.
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const GOLDEN_DIR = path.resolve(HERE, '../../design/regs-v11/port/golden');
const load = (f) => JSON.parse(fs.readFileSync(path.join(GOLDEN_DIR, f), 'utf8'));
const GOLDEN = load('golden.json'), TAPS = load('taps.json');

/** The phone the tools shoot: 390 × 844 CSS px at DPR 2 (tests.md §4). */
export const PHONE = { width: 390, height: 844, dpr: 2 };
/** The port target's ref (the goldens' own `meta.ref`): the prototype is staged there, never read from the working tree. */
export const REF = GOLDEN.meta.ref;
export const VERSION = GOLDEN.meta.version;
export const APP_URL = process.env.REGS_APP_URL || 'http://localhost:8081';
export const TABS = ['keep', 'gear', 'methods', 'licence'];
/** The prototype's "today": the day its golden headers call Today (its snapshot's day). The tools put the app's clock there,
 *  so both say "Today" on the same day and a weekday on every other. */
export const PROTO_TODAY = (() => {
  const id = Object.keys(GOLDEN.states).find((k) => GOLDEN.states[k].header?.day === 'Today');
  if (!id) throw new Error('no golden state is Today');
  return TAPS.states[id].date;
})();

/** (T115) the golden states whose part the release merged away (taps.json `gone`): the app has no part to open for them */
export const GONE = Object.keys(TAPS.gone ?? {});
/** every golden state id the app can open (all but GONE), in the goldens' order */
export const IDS = Object.keys(GOLDEN.states).filter((id) => !(id in (TAPS.gone ?? {})));
export const stateOf = (id) => {
  const g = GOLDEN.states[id], t = TAPS.states[id];
  if (!g || !t) throw new Error(TAPS.gone?.[id] ? `golden state ${id} has no part in the store: ${TAPS.gone[id].reason}` : `no golden state ${id}`);
  return { id, w: g.w, d: g.d, p: g.p, water: g.water, item: t.item, part: t.partIx, date: t.date, sheets: Object.keys(g.sheets || {}) };
};

/** golden → app sheet key (core `appKeyOf`, the only bridge: T36) — mirrored here for the .mjs tools; states.test.ts
 *  holds the two equal over every golden key. */
export function appKeyOf(goldenKey, sid) {
  const lic = /^licsrc:lic\|([^|]+)\|(\w+)$/.exec(goldenKey);
  if (lic) return lic[1] === sid ? `licsrc:${lic[2]}` : goldenKey;
  if (/^(rule|gl|sp):/.test(goldenKey)) return goldenKey;
  const esc = sid.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return goldenKey.replace(`:${sid}|`, ':').replace(`|${sid}|`, '|').replace(new RegExp(`\\|${esc}$`), '');
}

/**
 * A golden state (+ tab, golden sheet key, who, theme, scroll) → both URLs. `proto` is the hash only (the page path is
 * the staged prototype's, which the caller owns); `app` is the start link on APP_URL.
 * who: { age: 'adult'|'u16', res: 'resident'|'non_resident'|'non_resident_alien' } (either may be left out).
 */
export function urlsOf(id, { tab = 'keep', sheet = null, who = null, theme = 'light', scroll = 0, app = APP_URL } = {}) {
  const s = stateOf(id);
  if (!TABS.includes(tab)) throw new Error(`no tab ${tab}`);
  const h = new URLSearchParams({ w: s.w, d: String(s.d), p: String(s.p), sec: tab });
  const q = new URLSearchParams({ regs: s.item, part: String(s.part), date: s.date, tab });
  if (sheet) { h.set('sheet', sheet); q.set('sheet', appKeyOf(sheet, id)); }
  if (who?.age && who.age !== 'adult') { h.set('age', who.age); q.set('age', who.age); }
  if (who?.res && who.res !== 'resident') { h.set('res', who.res); q.set('res', who.res); }
  if (theme !== 'light') { h.set('theme', theme); q.set('theme', theme); }
  if (scroll) q.set('scroll', String(scroll));
  // the prototype's hash keeps its own spelling of keys (`|` and `:` unescaped, as shoot.mjs writes them)
  const proto = [...h].map(([k, v]) => `${k}=${k === 'sheet' ? encodeURIComponent(v) : v}`).join('&');
  return { proto, app: `${app.replace(/\/$/, '')}/?${q}` };
}

/** check.mjs's state sets (CARDS2 ∪ R17 ∪ R20S ∪ TRS ∪ SBS ∪ STATES_I) as golden ids, + tidal and timed (T42 edges). */
const HASHES = ['w=vedder&d=1010', 'w=klwa&d=1010', 'w=cowichan&d=1010&p=1', 'w=chilliwack&d=1010&p=0', 'w=wilkes&d=1010',
  'w=vedder&d=515', 'w=x12p&d=1010', 'w=x3p&d=1010', 'w=iron&d=720', 'w=x34p&d=1010', 'w=x35p&d=1010', 'w=bellacoola&d=1010&p=1',
  'w=bellacoola&d=501&p=1', 'w=ape&d=1010', 'w=ape&d=501', 'w=x15p&d=1010&p=0', 'w=bellacoola&d=501&p=0', 'w=hamill&d=615',
  'w=klwa&d=1012', 'w=glenn&d=615', 'w=chilliwack&d=1010&p=1'];
export const idOfHash = (hash) => {
  const h = new URLSearchParams(hash), w = h.get('w'), d = Number(h.get('d')), p = Number(h.get('p') || 0);
  const hit = IDS.find((id) => { const g = GOLDEN.states[id]; return g.w === w && g.d === d && g.p === p; });
  if (!hit) throw new Error(`no golden state for ${hash}`);
  return hit;
};
export const WALK_STATES = [...new Set([...HASHES.map(idOfHash), 'x31p_1010', 'x15p2_1010'])];

/** The four states whose Methods must fit one screen under the header (bait-ways-compact). */
export const METHODS_COMPACT = ['vedder_1010', 'klwa_1010', 'cow1_1010', 'vedder_515'];
