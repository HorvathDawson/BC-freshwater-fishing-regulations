// STORE SHOT CONTENT CHECKS (T319 / T704): what each shot must show, asserted on the PAGE STATE before the capture (DOM and text,
// never pixels). A shot whose page does not hold its content is not captured: play-shots.mjs throws, the run fails, nothing
// blank or stale reaches the store set. `facts` (b.facts) carries what a scene read mid-way (the part count).
// Each check gets `s` = { text, has(testid), top(testid), salmonTop, ... } read once from the page and returns a list of problems.
export const STATE = `(() => {
  const q = (id) => document.querySelector('[data-testid=' + id + ']');
  const vis = (e) => !!e && e.getBoundingClientRect().width > 0;
  const sal = [...document.querySelectorAll('*')].filter((x) => x.children.length === 0 && /^salmon \\(dfo\\)$/i.test(x.textContent.trim())).pop();
  const pressed = (id) => { const e = q(id); return !!e && ['aria-pressed', 'aria-checked', 'aria-selected'].some((a) => e.getAttribute(a) === 'true'); };
  const leaf = (re) => [...document.querySelectorAll('*')].find((x) => x.children.length === 0 && re.test(x.textContent.trim()) && x.getBoundingClientRect().width > 0);
  const inView = (e) => { if (!e) return false; const r = e.getBoundingClientRect(), sc = q('regs-scroll')?.getBoundingClientRect(); return r.top >= (sc ? sc.top - 1 : 0) && r.bottom <= window.innerHeight + 1; };
  const stripR = q('keep-strip')?.getBoundingClientRect(), scR = q('regs-scroll')?.getBoundingClientRect();
  return { text: document.body.innerText,
    tableHeadInView: inView(leaf(/^Keep \\d+ trout and char today/)), tableRowsInView: ['Brook trout', 'Brown trout'].filter((n) => inView(leaf(new RegExp('^' + n)))),
    stripInView: !!stripR && !!scR && stripR.bottom > scR.top && stripR.top < window.innerHeight, ids: [...new Set([...document.querySelectorAll('[data-testid]')].map((e) => e.dataset.testid))],
    vis: Object.fromEntries(['map-day', 'keep-strip', 'keep-trout', 'salmon-block', 'gauge-card', 'water-map-step-up', 'water-title', 'chart-model-CLEVER'].map((id) => [id, vis(q(id))])),
    keepTop: q('keep-strip')?.getBoundingClientRect().top ?? null, salmonHeadTop: sal?.getBoundingClientRect().top ?? null, salmonBlockTop: q('salmon-block')?.getBoundingClientRect().top ?? null,
    clever: pressed('chart-model-CLEVER'), canvas: !!document.querySelector('canvas.maplibregl-canvas'), scale: document.querySelector('.maplibregl-ctrl-scale')?.textContent.replace(/\\s+/g, ' ').trim() ?? null,
    waterTitle: q('water-title')?.innerText.trim() ?? '',
    modelsOff: ['CLEVER', 'COFFEE', 'ELF'].filter((m) => q('chart-model-' + m)?.getAttribute('aria-checked') === 'false') }; })()`;

const BAD = /still loading|something went wrong|couldn.t load|could not load|no data to show/i;
const need = (cond, msg) => (cond ? [] : [msg]);

export const CHECKS = {
  '01-map': (s) => [...need(s.vis['map-day'] && s.canvas, 'the map (day bar + canvas) is not showing')],
  // shot 2, salmon on top (0cba57a73): the SALMON (DFO) block sits ABOVE the game-fish calendar in the page, both drawn
  // (T810 R) switched by what the page shows: the old card needs nothing more; the trout table (its fold `glance:trout`)
  // is shot OPEN (`keep-trout` drawn), never as a folded row
  '18-regs-trout': (s) => [...need(s.tableHeadInView, 'the limits table\'s heading ("Keep N trout and char today") is not in the viewport'),
    ...need((s.tableRowsInView ?? []).length >= 2, `the table's species rows are not in the viewport (in view: ${(s.tableRowsInView ?? []).join(', ') || 'none'})`),
    ...need(s.stripInView === false, 'the season calendar (keep-strip) is still in the viewport: the shot must show the table, not the calendar')],
  '19-regs-salmon': (s) => [...need(s.salmonHeadTop !== null, 'no "Salmon (DFO)" block on the Vedder'), ...need(s.keepTop !== null, 'no game-fish calendar on the page'),
    ...need(s.salmonHeadTop !== null && s.keepTop !== null && s.salmonHeadTop < s.keepTop, `salmon is not above the game-fish calendar (salmon top ${s.salmonHeadTop}, game fish top ${s.keepTop}): the site serving the shots predates 0cba57a73`),
    ...need(/Open|Closed/.test(s.text), 'no open/closed answer on the page')],
  '11-cond-chart': (s) => [...need(s.vis['gauge-card'], 'no gauge card'), ...need(!(s.modelsOff ?? []).length, `forecast models left off on the chart: ${(s.modelsOff ?? []).join(', ')}`), ...need(s.clever, 'the CLEVER forecast is not switched on the chart (no forecast series; off season or feed empty)'),
    ...need(/\d+(\.\d+)?\s*\n?\s*m³\/s/.test(s.text) || /m³\/s/.test(s.text), 'no flow reading on the card')],
  '12-cond-change': (s) => [...need(s.scale === '20 km', `map scale ${s.scale}, not 20 km`), ...need(/reaches coloured/.test(s.text), 'no coloured reaches (watershed days not loaded)'), ...need(s.ids.includes('map-horizons-change'), 'no Change control on the map')],
  '13-cond-forecast': (s) => [...need(s.scale === '20 km', `map scale ${s.scale}, not 20 km`), ...need(/reaches coloured/.test(s.text), 'no coloured reaches (watershed days not loaded)'), ...need(s.ids.includes('map-horizons-days') && /(Mon|Tue|Wed|Thu|Fri|Sat|Sun) \d+/.test(s.text), 'no forecast days on the map')],
  '14-split-map': (s) => [...need(s.vis['map-day'] && s.canvas, 'the river map is not showing')],
  // the river is split into more than one part and one part's rules are open
  '15-split-rules': (s, facts) => [...need((facts.parts ?? 0) > 1, `the river has ${facts.parts ?? 'no readable number of'} parts, need more than one`), ...need(s.vis['keep-strip'], "the part's rules (keep-strip) are not showing")],
  '04-salmon': (s) => [...need(s.vis['map-day'] && s.canvas, 'the map is not showing')],
  '05-runs': (s) => [...need(/Runs/.test(s.text), 'the Runs tab is not showing')],
  // (T319) the phone's screens, drawn by the store-shots web build (EXPO_PUBLIC_STORE_SHOTS=1): never the website's own
  '07-saved-spot': (s) => [...need(!s.ids.includes('web-spots-notice'), 'the website\'s "get the app" notice is up: the web export was not built with EXPO_PUBLIC_STORE_SHOTS=1'),
    ...need(/Bridge pool/.test(s.text) && /tail-out below the bridge/.test(s.text), 'the sample spot (Bridge pool, its notes) is not showing')],
  '20-help': (s) => [...need(/Offline and updates/i.test(s.text), 'no "Offline and updates" lines on the Help page'),
    ...need(/work with no signal/.test(s.text), 'the Help page shows the website\'s lines, not the app\'s: the web export was not built with EXPO_PUBLIC_STORE_SHOTS=1')],
  // (T319) search: the typed name's waters listed
  '06-search': (s) => [...need(/Vedder River/.test(s.text) && /named waters/i.test(s.text), 'no search results for "Vedder" (Vedder River, the named-waters list)')],
  // (T703/T704) the More data list: each source with its date, the whole rows in view (the Settings screen has no offline control on the web)
  '09-offline': (s) => [...need(/Synopsis/.test(s.text) && /Freshwater Atlas/.test(s.text) && /Map tiles/.test(s.text), 'the More data list (Synopsis, Freshwater Atlas, Map tiles) is not showing'),
    ...need(/Valid to [A-Z][a-z]{2} \d{1,2}, \d{4}/.test(s.text) && /[A-Z][a-z]{2} \d{1,2}, \d{4}/.test(s.text.replace(/Valid to [^\n]*/, '')), 'the sources are not showing their dates')],
};
// every scene: no error/loading wording on the page
export function judge(scene, s, facts = {}) {
  const check = CHECKS[scene];
  const out = check ? check(s, facts) : [];
  if (check && BAD.test(s.text)) out.push(`the page says "${s.text.match(BAD)[0]}"`);
  return out;
}
export async function checkScene(b, scene) {
  const p = judge(scene, await b.evaluate(STATE), b.facts ?? {});
  if (p.length) throw new Error(`content check failed for ${scene}: ${p.join('; ')}`);
}
