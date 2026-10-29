// (T255) THE REPORT: one folder per run — report.json (whole), report.md (the table), index.html (every step with its
// screenshot, readable offline). Steps are written as they finish, so a run killed half way still leaves a report.
import fs from 'node:fs';
import path from 'node:path';

const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const ICON = { pass: 'PASS', fail: 'FAIL', skip: 'SKIP', warn: 'WARN' };

export function writeReport(dir, run) {
  fs.writeFileSync(path.join(dir, 'report.json'), JSON.stringify(run, null, 1));
  const md = [`# Test bed run ${run.id}`, '', `${run.device} · APK ${run.apk} · channel ${run.channel}`, '',
    '| scenario | result | steps | time |', '|---|---|---|---|',
    ...run.scenarios.map((s) => `| ${s.id} — ${s.title} | ${ICON[s.result] ?? s.result} | ${s.steps.filter((x) => x.result === 'pass').length}/${s.steps.length} | ${Math.round(s.ms / 1000)} s |`),
    '', ...run.scenarios.flatMap((s) => [`## ${s.id} — ${s.title}: ${ICON[s.result] ?? s.result}`, '',
      ...s.steps.map((x, i) => `${i + 1}. ${ICON[x.result]} ${x.title}${x.note ? ` — ${x.note}` : ''}${x.error ? ` — **${x.error}**` : ''}${x.shot ? ` ([shot](${x.shot}))` : ''}`),
      ...(s.findings?.length ? ['', 'Findings:', ...s.findings.map((f) => `- ${f}`)] : []), ''])].join('\n');
  fs.writeFileSync(path.join(dir, 'report.md'), md);
  const html = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Test bed ${esc(run.id)}</title><style>
:root{--bg:#fff;--fg:#1b1f23;--sub:#5a6470;--line:#e3e6ea;--pass:#1f7a3a;--fail:#b3261e;--skip:#7a6a1f}
@media (prefers-color-scheme:dark){:root{--bg:#14171a;--fg:#e8eaed;--sub:#9aa3ad;--line:#2b3036;--pass:#5cc27a;--fail:#ff8a80;--skip:#d9c56b}}
body{background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif;margin:0 auto;max-width:1100px;padding:16px}
h1{font-size:20px}h2{font-size:16px;margin-top:28px;border-top:1px solid var(--line);padding-top:14px}
.r-pass{color:var(--pass)}.r-fail{color:var(--fail)}.r-skip,.r-warn{color:var(--skip)}
table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid var(--line);padding:6px;text-align:left;vertical-align:top}
.steps{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:12px}
.step{border:1px solid var(--line);border-radius:8px;padding:8px}.step img{width:100%;border-radius:4px}
.sub{color:var(--sub);font-size:12px}pre{white-space:pre-wrap;font-size:11px;color:var(--sub);max-height:140px;overflow:auto}
</style></head><body><h1>Test bed run ${esc(run.id)}</h1>
<p class="sub">${esc(run.device)} · APK ${esc(run.apk)} · channel ${esc(run.channel)} · started ${esc(run.started)}</p>
<table><tr><th>scenario</th><th>result</th><th>steps</th><th>time</th></tr>
${run.scenarios.map((s) => `<tr><td><a href="#${esc(s.id)}">${esc(s.id)}</a> — ${esc(s.title)}</td><td class="r-${s.result}">${ICON[s.result] ?? esc(s.result)}</td><td>${s.steps.filter((x) => x.result === 'pass').length}/${s.steps.length}</td><td>${Math.round(s.ms / 1000)} s</td></tr>`).join('')}
</table>
${run.scenarios.map((s) => `<h2 id="${esc(s.id)}">${esc(s.id)} — ${esc(s.title)} <span class="r-${s.result}">${ICON[s.result] ?? ''}</span></h2>
${s.findings?.length ? `<ul>${s.findings.map((f) => `<li>${esc(f)}</li>`).join('')}</ul>` : ''}
<div class="steps">${s.steps.map((x, i) => `<div class="step"><div><b class="r-${x.result}">${ICON[x.result]}</b> ${i + 1}. ${esc(x.title)}</div>
${x.note ? `<div class="sub">${esc(x.note)}</div>` : ''}${x.error ? `<div class="r-fail">${esc(x.error)}</div>` : ''}
${x.shot ? `<a href="${esc(x.shot)}"><img loading="lazy" src="${esc(x.shot)}" alt="${esc(x.title)}"></a>` : ''}
${x.said ? `<pre>${esc(x.said)}</pre>` : ''}</div>`).join('')}</div>`).join('')}
</body></html>`;
  fs.writeFileSync(path.join(dir, 'index.html'), html);
}
