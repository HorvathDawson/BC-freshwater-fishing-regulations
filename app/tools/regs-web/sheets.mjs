// T82 O5 — the summary of a `regs-heights.mjs --sheets` run: how much taller the app's sheets open than the prototype's,
// and where the difference comes from (the sheet's chrome, the body's words, the cap). Pure, so it is tested without a
// browser. A row: { id, sheet, proto: M | null, app: M | null }, M = { sheet, chrome, body, scrolls, app } (CSS px).
const median = (xs) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const r1 = (v) => (v == null ? null : Math.round(v * 10) / 10);

export function sheetSummary(rows) {
  const both = rows.filter((r) => r.proto && r.app);
  const d = (f) => both.map((r) => f(r.app) - f(r.proto));
  return {
    sheets: rows.length, measured: both.length,
    missing: rows.filter((r) => !r.proto || !r.app).map((r) => `${r.id} ${r.sheet}${r.proto ? '' : ' (prototype)'}${r.app ? '' : ' (app)'}`),
    median: { sheet: r1(median(d((m) => m.sheet))), chrome: r1(median(d((m) => m.chrome))), body: r1(median(d((m) => m.body))) },
    taller: both.filter((r) => r.app.sheet > r.proto.sheet + 1).length,
    capped: { proto: both.filter((r) => r.proto.scrolls).length, app: both.filter((r) => r.app.scrolls).length },
    maxSheet: { proto: r1(Math.max(0, ...both.map((r) => r.proto.sheet))), app: r1(Math.max(0, ...both.map((r) => r.app.sheet))) },
  };
}
