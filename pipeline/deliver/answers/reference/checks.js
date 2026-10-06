#!/usr/bin/env node
'use strict';
/* The page's Checks tab (Stage 8), run by the page's own renderCases().
   usage: node checks.js DIR      (DIR holds data.json + cases.json from convert.py)
          node checks.js --page PAGE.html   (the blocks embedded in a saved page)
   prints the page's summary line and every failing case; --json writes the per-case results. */
const fs = require('fs');
const path = require('path');
const { load, htmlText } = require('./harness');

function blocks(argv){
  const i = argv.indexOf('--page');
  if (i >= 0){
    const t = fs.readFileSync(argv[i + 1], 'utf8');
    const get = id => t.match(new RegExp(`<script type="application/json" id="${id}">([\\s\\S]*?)</script>`))[1];
    return { data: get('data'), cases: get('cases') };
  }
  const dir = argv.find(a => !a.startsWith('--'));
  return { data: fs.readFileSync(path.join(dir, 'data.json'), 'utf8'), cases: fs.readFileSync(path.join(dir, 'cases.json'), 'utf8') };
}

function run(b){
  const page = load(b);
  page.P.renderCases();
  const R = page.P.CASE_RESULTS;
  const summary = htmlText(page.el('cases-list').innerHTML).split('\n')[0];
  const results = R.map(x => ({ id: x.ours ? x.c.id : `${x.c.mechanism}|${x.c.water.item_id}|${x.c.date}|${x.c.fish}`, ours: !!x.ours, ok: x.ok,
    misses: x.rows.filter(r => !r.same).map(r => ({ rule: r.k, expected: r.es, page: r.gs, why: r.why })) }));
  return { pass: R.filter(x => x.ok).length, total: R.length, summary, results };
}

if (require.main === module){
  const argv = process.argv.slice(2);
  const out = run(blocks(argv));
  console.log(out.summary);
  out.results.filter(r => !r.ok).forEach(r => { console.log(`  FAIL ${r.id}`); r.misses.forEach(m => console.log(`       ${m.rule}: expected ${m.expected}, page ${m.page}${m.why ? ' (' + m.why + ')' : ''}`)); });
  const j = argv.indexOf('--json'); if (j >= 0) fs.writeFileSync(argv[j + 1], JSON.stringify(out, null, 1));
}
module.exports = { run, blocks };
