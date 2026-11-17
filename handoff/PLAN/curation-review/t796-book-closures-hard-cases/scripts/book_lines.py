"""Find each row's book line in the synopsis PDF text: PDF page -> printed page (PDF-2 to p.40, PDF-6 after: checked
against the page footers), the line found whole or not, and 350 characters of context after it (to see dates or
words the line may have lost). Writes out/book_lines.json. Re-run: python3 -I book_lines.py"""
import json, re
from pathlib import Path
here = Path(__file__).parent
pages = {int(p.stem): p.read_text() for p in sorted((here / 'out/pages').glob('*.txt'))}
norm = lambda s: re.sub(r'\s+', ' ', s.replace('￾', '').replace('‐', '-').replace('–', '-').replace('—', '-')).strip()
npages = {k: norm(v) for k, v in pages.items()}
def printed(i): return i - 2 if i - 2 <= 40 else i - 6
rows = json.load(open(here / 'out/rows.json'))
out = []
for r in rows:
    line = norm(r['bundle']['verbatim'])
    hits = [(i, t.find(line)) for i, t in npages.items() if line in t]
    rec = {"id": r['id'], "line": line, "book_pages_claimed": r['book']['pages'], "hits": []}
    for i, pos in hits:
        t = npages[i]
        rec["hits"].append({"pdf": i, "printed": printed(i), "before": t[max(0, pos - 250):pos], "after": t[pos + len(line):pos + len(line) + 300]})
    if not hits:   # try the first 40 characters
        key = line[:40]
        rec["partial"] = [{"pdf": i, "printed": printed(i), "ctx": t[t.find(key) - 100: t.find(key) + 400]} for i, t in npages.items() if key in t]
    out.append(rec)
json.dump(out, open(here / 'out/book_lines.json', 'w'), indent=1)
for o in out:
    pg = sorted({h['printed'] for h in o['hits']})
    print(o['id'], 'claimed', o['book_pages_claimed'], 'found', pg, 'hits', len(o['hits']), '' if o['hits'] else 'PARTIAL:' + str([(p['printed']) for p in o.get('partial', [])]))
