// Which expected strings quoted in the spec's tickets are no longer in the goldens (after a prototype bump, or a typo).
//   node app/design/regs-v11/port/quotes.mjs [tickets dir] [golden.json]
// Takes every double-quoted (“…”, "…") run of ≥ 14 characters containing a space from each ticket and looks it up in the
// golden file's text (whitespace-normalised, JSON-unescaped). Prints, per ticket, the quotes not found. Strings a ticket
// quotes from elsewhere (code, SQL, CLI output) are expected misses; a sentence of the page that misses is stale.
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';
import { GOLDEN } from './stage.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url)), REPO = path.resolve(HERE, '../../../..');
const DIR = process.argv[2] || path.join(REPO, 'handoff/PLAN/app-integration/tickets');
const G = process.argv[3] || path.join(GOLDEN, 'golden.json');
const norm = t => t.replace(/\s+/g, ' ').trim();
const hay = new Set(); (function walk(x) { if (typeof x === 'string') hay.add(norm(x)); else if (x && typeof x === 'object') Object.values(x).forEach(walk); })(JSON.parse(fs.readFileSync(G, 'utf8')));
const all = [...hay].join('\n');
let total = 0, miss = 0;
for (const f of fs.readdirSync(DIR).filter(f => f.endsWith('.md')).sort()) {
  const src = fs.readFileSync(path.join(DIR, f), 'utf8'), qs = new Set();
  for (const m of src.matchAll(/[“"]([^“”"\n`]{14,300})[”"]/g)) if (/\s/.test(m[1]) && !/[{}=;()<>]|\$\{|=>/.test(m[1])) qs.add(norm(m[1].replace(/\\u00a0/g, ' ')));
  const out = [...qs].filter(q => !all.includes(q) && !all.includes(q.replace(/ /g, ' ')));
  total += qs.size; miss += out.length;
  if (out.length) console.log(`${f}: ${out.length}/${qs.size} not in the goldens\n  ${out.slice(0, 12).map(q => JSON.stringify(q)).join('\n  ')}`);
}
console.log(`quotes: ${total - miss} / ${total} found in ${path.relative(REPO, G)}`);
