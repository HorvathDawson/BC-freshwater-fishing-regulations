// Design tokens and a style census of the regulations prototype, for the app theme mapping (handoff/PLAN/app-integration/tokens.md).
//   node app/design/regs-v11/port/tokens.mjs [--ref HEAD|<sha>|WORKTREE] [--out dir]
//   -> <out>/tokens.json  { meta, tokens: {light: {--name: value}, dark: {...}}, app: {light, dark, cvd: {color.ui.*, color.status.*}},
//                            compare: [{token, light, appLight, dark, appDark, appKey}], census: {light: [...], dark: [...]} }
// tokens: the :root custom properties AS COMPUTED (the cascade resolved, so a later override such as --faint:var(--sub) shows
// its final value), light and dark. census: every distinct (role selector → font family/size/weight/line-height, colour,
// background) on the four tabs and the pin bar, with the colour named by the token it equals — the RN style table is read
// off this, not off the CSS source.
import fs from 'node:fs'; import path from 'node:path'; import { pathToFileURL } from 'node:url';
import { open } from '../tools/cdp.mjs';
import { stage, args, REPO } from './stage.mjs';

const A = args(); fs.mkdirSync(A.out, { recursive: true });
const S = stage(A.ref);

// The roles the RN components draw (prototype selector -> component · part). Extend when the prototype adds a class.
const ROLES = {
  '#hdr .ttl b': 'RegsHeader · name', '#hdr .hdate .hd2 b': 'RegsHeader · date day', '#hdr .hdate .hd2 > span': 'RegsHeader · date',
  '#hdr [data-place]': 'RegsHeader · place label', '#hdr .hgo span': 'RegsHeader · Map/Parts', '#hdr .segm button': 'RegsHeader · tab',
  '#hdr .segm button[aria-pressed=true]': 'RegsHeader · tab (pressed)',
  '.wstrip .wsstat': 'YearStrip · status word', '.wstrip .wsline .muted': 'YearStrip · status note', '.wstrip .smonths > *': 'YearStrip · month', '.wstrip .slegend > span': 'YearStrip · legend',
  '.banner .big': 'ClosedBanner · big', '.banner > p': 'ClosedBanner · para', '.closechain .cd': 'ClosedBanner · chain', '.golink': 'ClosedBanner · link',
  '.inh [data-inherited]': 'InheritedLine · says', '.inh [data-caution]': 'InheritedLine · caution',
  '.ghd.gpin .rtitle': 'GroupHead · name', '.ghd.gpin .rtitle .scope': 'GroupHead · scope', '.ghd .num .ghn': 'GroupHead · big number', '.ghd .num .ghu': 'GroupHead · words',
  '[data-share-lead]': 'GroupHead · share lead (form a)', '[data-share-lead] .gln': 'GroupHead · share lead number', '[data-of-total]': 'GroupHead · of the total', '[data-why-here]': 'GroupHead · why here',
  '.ghd.gpin': 'GroupHead · band', '.gorigin': 'OriginLine', '.gorigin .gtag': 'OriginLine · tag',
  '.tier .tkick': 'TierHeader · kicker', '.tier .tl': 'TierHeader · line', '.tier .tl .tn': 'TierHeader · number', '.tier .tsub': 'TierHeader · why', '.tnode[data-tnode=size]': 'SizeBand',
  '.tnode[data-tnode=size] > .tier .tl': 'SizeBand · head line',
  '.kt .ks': 'KindRow · sentence', '.kt .ks .kn': 'KindRow · number', '.kt .szc': 'KindRow · size chip', '.kt .kq': 'KindRow · qualifier', '.kpics button': 'FishStrip · tile',
  '.kpics img': 'FishStrip · image', '.kcap': 'FishStrip · caption', '.kac': 'KindRow · also counts', '.duty': 'KindRow · duty', '.kpres': 'KindRow · presence', '.linkbtn': 'link button (How to tell ›)',
  '.nkr .nm': 'NotKeptRow · says', '.nkr .s2': 'NotKeptRow · whose', '.nkr .kn': 'NotKeptRow · mark',
  '.worked .bkh': 'HowItAddsUp · head', '.worked .wsays': 'HowItAddsUp · says', '.worked li span': 'HowItAddsUp · q', '.worked li b': 'HowItAddsUp · a', '.worked': 'HowItAddsUp · block',
  '.sech h2': 'section head (Other fish)', '.olist .on': 'OtherFish · name', '.olist .owk': 'OtherFish · week', '.olist .orow b': 'OtherFish · value',
  '.posrow .on': 'PossessionRow · label', '.posrow b': 'PossessionRow · value', '.nofish h3': 'WhereNot · head', '.nofish li': 'WhereNot · item',
  '.gpart > h3': 'tab section head (Gear/Methods/Licence)', '.gday': 'GearLines · day', '.gmode .gval': 'GearLines · mode', '.glab': 'GearLines · label', '.gval b': 'GearLines · value',
  '.chg': 'ChangesLine', '.chg .chgi': 'ChangesLine · chip', '.chg b': 'ChangesLine · when', '.chg em': 'ChangesLine · in force',
  '.isrc .srcs': 'SourceLine · who', 'a.book': 'RuleBookLink', '.gl': 'ⓘ term button',
  '.gxl': 'YesNoList · title', '.ynh': 'YesNoList · group head', '.yn li i': 'YesNoList · mark', '.yn li.ok i': 'YesNoList · ✓', '.yn li.no i': 'YesNoList · ✕', '.yn li b': 'YesNoList · name', '.yn li small': 'YesNoList · note',
  '.gref summary .lab': 'ReferenceFold · label', '.gref summary b': 'ReferenceFold · title', '.gref summary em': 'ReferenceFold · sub', '.gref .hh': 'ReferenceFold · group head',
  '.foot': 'ClosedNote', '.lchoose .lab': 'WhoSelector · question', '.lsel [role=radio]': 'WhoSelector · option', '.lsel [role=radio][aria-checked=true]': 'WhoSelector · option (on)',
  '.resfor': 'LicenceAnswer · label', '.needs .lbl': 'LicenceAnswer · needs head', '.needs li': 'LicenceAnswer · item', '.classbox': 'LicenceAnswer · class box', '.lopen .lab': 'LicenceAnswer · open fold head',
  '#pins .pinbar': 'PinBar · bar', '#pins .pl.g': 'PinBar · group line', '#pins .pl.c': 'PinBar · tier line', '#pins .pl b': 'PinBar · number', '#pins .pl .pls': 'PinBar · total sub-line',
  '.sheet .shead h2': 'Sheet · title', '.sheet .glsays': 'Sheet · says', '.sheet .glex': 'Sheet · example', '.sheet .bookw summary': 'Sheet · fold',
};

function CENSUS(roles) {
  const css = getComputedStyle(document.documentElement), names = [...document.styleSheets].flatMap(s => { try { return [...s.cssRules]; } catch { return []; } })
    .flatMap(r => r.style ? [...r.style].filter(p => p.startsWith('--')) : []);
  const vars = {}; [...new Set(names)].sort().forEach(n => { vars[n] = css.getPropertyValue(n).trim(); });
  const probe = document.createElement('i'); document.body.append(probe);
  const rgb = v => { probe.style.color = ''; probe.style.color = v; return getComputedStyle(probe).color; };
  const byRgb = {}; for (const [n, v] of Object.entries(vars)) if (/^(#|rgb)/.test(v)) (byRgb[rgb(v)] = byRgb[rgb(v)] || []).push(n);
  const tok = c => c === 'rgba(0, 0, 0, 0)' ? 'transparent' : (byRgb[c] || []).join('|') || c;
  const rows = [];
  for (const [sel, role] of Object.entries(roles)) { const els = [...document.querySelectorAll(sel)]; if (!els.length) { rows.push({ sel, role, n: 0 }); continue; }
    const seen = new Map(); els.forEach(e => { const s = getComputedStyle(e);
      const k = JSON.stringify({ family: s.fontFamily.split(',')[0].replace(/"/g, ''), size: +parseFloat(s.fontSize).toFixed(2), weight: +s.fontWeight, lineHeight: s.lineHeight,
        transform: s.textTransform, spacing: s.letterSpacing, color: tok(s.color), background: tok(s.backgroundColor), border: s.borderBottomWidth !== '0px' ? `${s.borderBottomWidth} ${tok(s.borderBottomColor)}` : null,
        padding: s.padding, radius: s.borderRadius });
      seen.set(k, (seen.get(k) || 0) + 1); });
    for (const [k, n] of seen) rows.push({ sel, role, n, ...JSON.parse(k) }); }
  probe.remove();
  return JSON.stringify({ vars, rows });
}

const STATES = [['vedder', 1010, 0], ['vedder', 615, 0], ['klwa', 1010, 0], ['bellacoola', 501, 1], ['glenn', 501, 0], ['iron', 720, 0]];
const b = await open(`${pathToFileURL(S.page).href}#shot=1`, { offline: true });
const out = { meta: { generator: 'app/design/regs-v11/port/tokens.mjs', ref: S.ref, sha: S.sha, version: S.version, states: STATES.map(x => x.join('|')) }, tokens: {}, census: {} };
try {
  await new Promise(r => setTimeout(r, 600));
  for (const theme of ['light', 'dark']) {
    const merged = {}; let vars = null;
    for (const [w, d, p] of STATES) {
      // open a sheet too so the sheet roles are measured (the possession sheet carries says/example/fold)
      await b.evaluate(`go({w:'${w}', d:${d}, p:${p}, theme:'${theme}', sheet:'', age:'adult', res:'resident'}); (()=>{ const k=[...document.querySelectorAll('[data-open^="pos:"]')][0]?.dataset.open; if (k) go({sheet:k}, true); document.getElementById('sc').scrollTop = 600; pinUpdate(); })()`);
      const r = JSON.parse(await b.evaluate(`(${CENSUS.toString()})(${JSON.stringify(ROLES)})`)); vars = r.vars;
      for (const row of r.rows) { const k = JSON.stringify({ ...row, n: undefined }); if (!merged[k]) merged[k] = { ...row, n: 0 }; merged[k].n += row.n; } }
    out.tokens[theme] = vars;
    out.census[theme] = Object.values(merged).filter(r => r.n > 0 || !Object.values(merged).some(x => x.sel === r.sel && x.n > 0))
      .sort((a, b) => Object.keys(ROLES).indexOf(a.sel) - Object.keys(ROLES).indexOf(b.sel));
  }
} finally { await b.close(); S.cleanup(); }

// the app's theme values (the ONE palette source, check-colours) and a side-by-side for the tokens the page uses
const T = n => JSON.parse(fs.readFileSync(path.join(REPO, `app/packages/map/style/themes/${n}.json`), 'utf8')).values;
const app = Object.fromEntries(['light', 'dark', 'cvd'].map(n => [n, Object.fromEntries(Object.entries(T(n)).filter(([k]) => /^(color|opacity)\.(ui|status)\./.test(k)))]));
out.app = app;
const MAP = { '--page': 'color.ui.page', '--card': 'color.ui.card', '--wash': 'color.ui.wash', '--tint': 'color.ui.tint', '--line': 'color.ui.line', '--line2': 'color.ui.line2',
  '--ink': 'color.ui.ink', '--sub': 'color.ui.sub', '--faint': 'color.ui.faint', '--accent': 'color.ui.accent', '--on-accent': 'color.ui.on-accent',
  '--open': 'color.status.open', '--restricted': 'color.status.restricted', '--closed': 'color.status.closed', '--shadow': 'color.ui.shadow', '--scrim': 'opacity.ui.scrim' };
const low = v => String(v ?? '').toLowerCase();
out.compare = Object.entries(MAP).map(([t, k]) => { const key = Object.keys(app.light).find(x => x === k) || Object.keys(app.light).find(x => x.toLowerCase().endsWith('.' + t.slice(2).replace('-', '').toLowerCase())) || null;
  const r = { token: t, appKey: key, light: out.tokens.light[t], appLight: key ? app.light[key] : null, dark: out.tokens.dark[t], appDark: key ? app.dark[key] : null };
  r.same = key ? low(r.light) === low(r.appLight) && low(r.dark) === low(r.appDark) : false; return r; });
const file = path.join(A.out, 'tokens.json'); fs.writeFileSync(file, JSON.stringify(out, null, 1));
console.log(`tokens: regs ${S.version} @ ${S.sha} -> ${path.relative(process.cwd(), file)}`);
for (const r of out.compare) console.log(`${r.same ? '  same ' : '  DIFF '} ${r.token.padEnd(12)} proto ${String(r.light).padEnd(9)} / ${String(r.dark).padEnd(9)}  app ${String(r.appKey).padEnd(22)} ${String(r.appLight).padEnd(9)} / ${r.appDark}`);
