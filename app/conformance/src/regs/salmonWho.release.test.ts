/**
 * (T1031 item 4) A SALMON SPECIES' SOURCES (`SalmonCardVM.whoBy`), over every chain of the release under test on a day per
 * month: every place its table row names as where today's answer comes from (`from`, T668 fix 4) is one of its sources —
 * a DFO place with DFO's rows as printed (some places carry none: named alone), or the rule book's closure by its rule —
 * so the Sources tab never names fewer places than the row.
 */
import { describe, expect, it } from "vitest";
import { SOURCE_AUTHORITIES, SPECIES_SHEET, salmonCard, salmonTable, withSalmonSheets, type SalmonFacts, type SalmonSpecies, type SheetKey, type SheetVM } from "@app/core";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const LIVE = liveRelease();
const DAYS = Array.from({ length: 12 }, (_, m) => new Date(2026, m, 15));

describe.skipIf(!LIVE)(`T1031 each salmon species' sources (${LIVE ?? "no live release"})`, () => {
  it("every place a species row names is among its sources; each source names DFO's rows or the book's rule", async () => {
    const facts = (await openReleaseDir(LIVE!).salmon()) as SalmonFacts;
    const P = facts.payload, wrong: string[] = [];
    let named = 0, book = 0, bare = 0, printed = 0;
    for (let i = 0; i < P.chains.length; i += 7) for (const on of DAYS) {
      const c = salmonCard({ ...facts, chainOf: () => i }, { sids: [0], item: null }, on);
      for (const r of salmonTable(c).species) {
        const who = c.whoBy?.[r.code] ?? [], froms = who.map((w) => w.from);
        // the row's places (`fromSep`-joined; "Region 2 and 3 together": both regions' places, each a source)
        for (const place of (r.from ?? "").split("; ").filter((x) => x && !/ together$/.test(x)))
          if (!froms.includes(place)) wrong.push(`chain ${i} ${on.toDateString()} ${r.code}: ${place} not in ${JSON.stringify(froms)}`);
        for (const w of who) {
          if (w.k === "book") { book++; if (!/::/.test(w.rule)) wrong.push(`chain ${i} ${r.code}: book rule ${w.rule}`); }
          else if (!w.from.endsWith(" · page header") && w.lines.length) printed++;   // a DFO place with DFO's rows as printed
          else if (!w.lines.length) bare++;   // a DFO place the release keeps no printed rows for (Babine Lake B(i)): named alone
        }
        if (r.from) named++;
      }
    }
    expect(wrong.slice(0, 10)).toEqual([]);
    expect([named > 100, printed > 100, book > 0]).toEqual([true, true, true]);
    console.log(`T1031 salmon sources: ${named} rows named a place, ${book} book sources, ${bare} DFO places with no rows`);
  });

  it("the species sheet: `sp:<code>` gains Identification | Sources; `sp:<code>|src` is today's answer then its sources, whole", async () => {
    const facts = (await openReleaseDir(LIVE!).salmon()) as SalmonFacts;
    // the first chain and day (a day per month) where a species is answered by the rule book's closure AND a DFO place
    let hit: { i: number; on: Date; code: SalmonSpecies } | null = null;
    for (let i = 0; i < facts.payload.chains.length && !hit; i++) for (const on of DAYS) {
      const c = salmonCard({ ...facts, chainOf: () => i }, { sids: [0], item: null }, on);
      const code = salmonTable(c).species.find((r) => { const w = c.whoBy?.[r.code] ?? []; return w.some((x) => x.k === "book") && w.some((x) => x.k === "dfo" && x.lines.length); })?.code;
      if (code) { hit = { i, on, code }; break; }
    }
    expect(hit).not.toBeNull();
    const { i, on, code } = hit!, card = salmonCard({ ...facts, chainOf: () => i }, { sids: [0], item: null }, on);
    const row = salmonTable(card).species.find((r) => r.code === code)!, who = card.whoBy![code]!;
    // the page's own sheets: the fish's picture sheet and each rule's sheet (its exact words and its pages)
    const own = (k: SheetKey): SheetVM | null => (k === `sp:${code}` ? { key: k, title: `Fish ${code}`, sub: null, blocks: [] }
      : k.startsWith("rule:") ? { key: k, title: "r", sub: null, blocks: [{ k: "quote", text: `words of ${k.slice(5)}` },
          { k: "pages", line: [{ t: "book", printed: 9, pdf: 11, box: [{ pdf: 11, box: [1, 2, 3, 4], size: [612, 792] }] }] }] } : null);
    const sheet = withSalmonSheets(own, card), id = sheet(`sp:${code}`)!, src = sheet(`sp:${code}|src`)!;
    const tabs = (vm: SheetVM) => vm.tabs!.map((t) => `${t.label}=${t.open}${t.on ? "*" : ""}`);
    expect([tabs(id), tabs(src), src.title]).toEqual([[`${SPECIES_SHEET.ID}=sp:${code}*`, `${SPECIES_SHEET.SRC}=sp:${code}|src`],
      [`${SPECIES_SHEET.ID}=sp:${code}`, `${SPECIES_SHEET.SRC}=sp:${code}|src*`], `Fish ${code}`]);
    const drawn = src.blocks.flatMap((b) => (b.k === "head" ? [`# ${b.text}`] : b.k === "value" || b.k === "para" ? [b.text.map((x) => ("s" in x ? x.s : "")).join("")]
      : b.k === "sources" ? b.groups.flatMap((g) => [`## ${g.where}`, ...g.rows.map((r) => `${r.title} | ${r.quote} | ${r.book.map((x) => (x.t === "book" ? `p.${x.printed}${x.box ? "[hl]" : ""}` : "s" in x ? x.s : "")).join(" ")}`)]) : []));
    expect(drawn).toEqual([`# ${SPECIES_SHEET.HERE}`, row.statusLabel,
      ...[row.quotaRuns ? row.quotaRuns.map((x) => ("s" in x ? x.s : "")).join("") : row.quota, row.window, row.limit].filter((x): x is string => !!x),
      `# ${SPECIES_SHEET.BREAKDOWN}`,
      `## ${SOURCE_AUTHORITIES.synopsis.name}`, ...who.flatMap((w) => (w.k === "book" ? [`${w.from} | words of ${w.rule} | p.9[hl]`] : [])),
      `## ${SOURCE_AUTHORITIES.dfo.name}`, ...who.flatMap((w) => (w.k === "dfo" ? [w.lines.length ? `${w.from} | ${w.lines.join("\n")} | ${SOURCE_AUTHORITIES.dfo.name}` : `null | ${w.from} | ${SOURCE_AUTHORITIES.dfo.name}`] : []))]);
    // mutation: no card, the page's own sheets alone (no tabs, no Sources); a fish the page's lines answer keeps its own tabs
    expect([withSalmonSheets(own, null)(`sp:${code}`)?.tabs, withSalmonSheets(own, null)(`sp:${code}|src`)]).toEqual([undefined, null]);
    const tabbed = (k: SheetKey): SheetVM | null => (k.startsWith("sp:") ? { key: k, title: "x", sub: null, blocks: [], tabs: [] } : null);
    expect(withSalmonSheets(tabbed, card)(`sp:${code}|src`)?.tabs).toEqual([]);
  });
});
