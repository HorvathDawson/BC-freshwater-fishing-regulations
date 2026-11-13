// T41 — THE STATE TABLE of the browser tools: a state id → the same state in the prototype (its #hash) and in the
// app (the web start link, `@app/ui` parseStartLink). The prototype's (w, d, p) and the app's (item, part, date) both come
// from the committed tap questions and answers (app/fixtures/golden/states.json + taps.json), never typed. (T439: states.json
// replaces golden.json here; it holds the questions and each state's sheet keys, no prototype text.)
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const GOLDEN_DIR = path.resolve(HERE, '../../fixtures/golden');
const load = (f) => JSON.parse(fs.readFileSync(path.join(GOLDEN_DIR, f), 'utf8'));
const GOLDEN = load('states.json'), TAPS = load('taps.json');

/** The phone the tools shoot: 390 × 844 CSS px at DPR 2 (tests.md §4). */
export const PHONE = { width: 390, height: 844, dpr: 2 };
/** The port target's ref (states.json `meta.ref`): the prototype is staged there, never read from the working tree. */
export const REF = GOLDEN.meta.ref;
export const APP_URL = process.env.REGS_APP_URL || 'http://localhost:8081';
export const TABS = ['keep', 'gear', 'methods', 'licence'];
/** The prototype's "today": the day its snapshot called Today (states.json `meta.today` names a state on it). The tools put
 *  the app's clock there, so both say "Today" on the same day and a weekday on every other. */
export const PROTO_TODAY = TAPS.states[GOLDEN.meta.today].date;

/** (T115) the golden states whose part the release merged away (taps.json `gone`), and (T278) those whose whole part is
 *  tidal now (taps.json `tidal`, off the map): the app has no part to open for them */
export const GONE = [...Object.keys(TAPS.gone ?? {}), ...Object.keys(TAPS.tidal ?? {})];
/** every golden state id the app can open (all but GONE), in the goldens' order */
export const IDS = Object.keys(GOLDEN.states).filter((id) => !GONE.includes(id));
export const stateOf = (id) => {
  const g = GOLDEN.states[id], t = TAPS.states[id];
  const why = TAPS.gone?.[id] ?? TAPS.tidal?.[id];
  if (!g || !t) throw new Error(why ? `golden state ${id} has no part in the store: ${why.reason}` : `no golden state ${id}`);
  return { id, w: g.w, d: g.d, p: g.p, water: g.water, item: t.item, part: t.partIx, date: t.date, sheets: g.sheets ?? [] };
};

/** golden → app sheet key (core `appKeyOf`, the only bridge: T36) — mirrored here for the .mjs tools; states.test.ts
 *  holds the two equal over every golden key. */
export function appKeyOf(goldenKey, sid) {
  // (owner 2026-11-04) the licence sources sheet is removed: a golden licsrc: key has no app key (core's throws)
  if (/^licsrc:/.test(goldenKey)) return null;
  // (T698, owner 2026-11-12 "Remove the screen") the Parts sheet is removed: the golden's `parts` has no app key
  if (goldenKey === 'parts') return null;
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
  // (T369) `ix`: the tools' part index, in the release these states were cut against (an app link carries a place)
  const q = new URLSearchParams({ regs: s.item, ix: String(s.part), date: s.date, tab });
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
  const hit = Object.keys(GOLDEN.states).find((id) => { const g = GOLDEN.states[id]; return g.w === w && g.d === d && g.p === p; });
  if (!hit) throw new Error(`no golden state for ${hash}`);
  return hit;
};
// (T278, r-62e2f3a8c81e) less GONE: x15p3 (the Fraser below Mission) is tidal now, off the map
export const WALK_STATES = [...new Set([...HASHES.map(idOfHash), 'x31p_1010', 'x15p2_1010'])].filter((id) => !GONE.includes(id));

/** The four states whose Methods must fit one screen under the header (bait-ways-compact). */
export const METHODS_COMPACT = ['vedder_1010', 'klwa_1010', 'cow1_1010', 'vedder_515'];
