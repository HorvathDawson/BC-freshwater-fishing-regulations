'use strict';
/* Run the consumer page v35's own script (page_v35.js, byte-for-byte the page's <script>) in node.

   NOTHING of the page's logic is rewritten. The script runs whole, as the browser runs it, inside a
   vm context whose only additions are the environment a browser would give it:
     · a stub DOM (document / elements / window) that records innerHTML and event handlers;
     · the two JSON blocks (`data`, `cases`) as the textContent of #data and #cases;
     · a seeded Math.random (the fish drawings take random svg ids; everything else is deterministic).
   The page is then DRIVEN through its own handlers, as a reader drives it: a water button's onclick,
   the #part change event, the date input's onchange (which maps Feb 29 to Feb 28), the angler
   profile's change event. What it shows is read back from the page's own globals (PLACE, MODEL, …)
   and from the HTML it wrote.

   A second script in the same context sees the first one's top-level `const` / `let` / `function`
   bindings (they live in the context's global lexical scope, as for two <script> tags), which is how
   `api` reaches `settle`, `evalSp`, `rowSources`, … without touching the page's source. */
const vm = require('vm');
const fs = require('fs');
const path = require('path');

const PAGE = path.join(__dirname, 'page_v35.js');

function makeDom(blocks){
  const handlers = { document:{}, window:{} };
  const els = new Map();
  class El {
    constructor(id){ this.id = id; this.children = []; this.dataset = {}; this.style = { setProperty(){} };
      this.classList = { add(){}, remove(){}, toggle(){}, contains(){ return false; } };
      this.innerHTML = ''; this.textContent = ''; this.value = ''; this.hidden = true; this.checked = false;
      this.offsetHeight = 0; this.attrs = {}; }
    addEventListener(t, f){ (this._h = this._h || {})[t] = (this._h[t] || []).concat(f); }
    appendChild(c){ this.children.push(c); return c; }
    setAttribute(k, v){ this.attrs[k] = v; }
    getAttribute(k){ return this.attrs[k]; }
    querySelector(){ return null; }
    querySelectorAll(){ return []; }
    closest(){ return null; }
    getBoundingClientRect(){ return { top:0, left:0, width:0, height:0 }; }
    focus(){} scrollIntoView(){}
  }
  const byId = id => { if (!els.has(id)){ const e = new El(id); if (blocks[id] !== undefined) e.textContent = blocks[id]; els.set(id, e); } return els.get(id); };
  const document = {
    getElementById: byId,
    querySelector: () => null,
    querySelectorAll: () => [],
    createElement: tag => new El(null),
    addEventListener: (t, f) => { (handlers.document[t] = handlers.document[t] || []).push(f); },
    documentElement: { style: { setProperty(){} } },
  };
  return { document, els, byId, handlers, El };
}

function seeded(seed){ return () => { seed = (seed * 16807) % 2147483647; return (seed - 1) / 2147483646; }; }

/* load({ data, cases }) -> a running page. `data` / `cases` are the two JSON blocks (objects or strings). */
function load({ data, cases }){
  const src = fs.readFileSync(PAGE, 'utf8');
  const blocks = { data: typeof data === 'string' ? data : JSON.stringify(data), cases: typeof cases === 'string' ? cases : JSON.stringify(cases) };
  const dom = makeDom(blocks);
  const win = {
    document: dom.document, location: { search: '' }, innerHeight: 800,
    getComputedStyle: () => ({ position: 'static' }), matchMedia: () => ({ matches: false }),
    addEventListener: (t, f) => { (dom.handlers.window[t] = dom.handlers.window[t] || []).push(f); },
    setTimeout: () => 0, URLSearchParams, console,
  };
  win.window = win;
  const ctx = vm.createContext(win);
  vm.runInContext(`Math.random = (${seeded.toString()})(7);`, ctx);
  vm.runInContext(src, ctx, { filename: 'page_v35.js' });
  // the page's own bindings, read through a second script in the same context
  const P = vm.runInContext(`({
    get state(){ return state; }, get PLACE(){ return PLACE; }, get MODEL(){ return MODEL; }, get REOPEN(){ return REOPEN; },
    D, WATERS, RULES, LIC, LAD, DAYS, CASES, AXES, TODAY, KEEPISH, mainRes, isBroad, inDates, whenDates, fmtMd,
    settle, makePlace, buildModel, evalSp, ladderAt, speciesAt, renderAll, renderCard, renderKit, setMd,
    gearParts, settleGear, licParts, settleLic, licPlace, rowSources, ladderHtml, rowConds, effCap, rowTitle,
    speciesItems, quotaLines, partLabels, partGroups, groupOf, partPlace, runsLabel, defaultPart, dateChips,
    waterSegs, strip, nextOpen, runCase, runOurs, closedAllYear, sayRule, renderCases,
    get CASE_RESULTS(){ return CASE_RESULTS; },
  })`, ctx);
  const el = id => dom.byId(id);
  const fire = (type, target) => (dom.handlers.document[type] || []).forEach(f => f({ type, target, stopPropagation(){} }));
  const page = {
    P, dom, el,
    /* the reader's actions, through the page's own handlers */
    pickWater(wi){ el('waters').children[wi].onclick(); },
    pickPart(pi){ fire('change', { id:'part', value:String(pi), dataset:{} }); },
    pickDate(m, d){ const e = el('dateIn'); e.value = `2026-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`; e.onchange(); },
    pickWho(axis, value){ fire('change', { id:'', value, dataset:{ axis } }); },
    showKit(kit){ P.state.kit = kit; P.renderKit(); },
    cardHtml(){ return el('card').innerHTML; },
    kitHtml(){ return el('kit').innerHTML; },
  };
  return page;
}

/* page HTML -> plain text, one block per line (svg drawings dropped) */
function htmlText(h){
  return String(h || '').replace(/<svg[\s\S]*?<\/svg>/g, '')
    .replace(/<(br|\/p|\/div|\/li|\/summary|\/h\d|\/section|\/article|\/details|\/ul|\/ol|\/tr)\b[^>]*>/g, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&amp;/g, '&')
    .split('\n').map(s => s.replace(/[ \t ]+/g, ' ').trim()).filter(Boolean).join('\n');
}

module.exports = { load, htmlText, PAGE };
