/**
 * T36 — the sheets a Regulations page opens, as `SheetVM`s of blocks (components draw blocks and never compute).
 *
 * Ports (regs-v11.html @ 7fb6809f, inventory names): `sheetHtml` (rule / gl / pos / hatch branches), `glossBody`, `pageLink`,
 * `bookLinks`, `appliesOf`, `head`; page_v36.js: `openRule`, `levelTag`, `clean`, `adiposeHtml` and the hatch sheet's
 * `openSheet` title. `sheetLines` is golden.mjs's sheet extraction (`lines()` per block, `books()`, `details`, nested keys).
 *
 * Owner rule 2026-10-21: nothing here decides a regulation. Every sentence is the store's (`RuleFacts.applies/plain/book`,
 * the export rule's label/verbatim/fields, the glossary term, the rows answers' `group.possession` examples, the Keep tab's
 * possession sentence); this file only selects them for a key and orders them as the prototype's sheet does.
 *
 * Built: `rule:`, `gl:` (one term or several), `pos:pos`, `hatch` (part 1); `srcs:`, `licsrc:`, `src:`, `sp:`, `other:`
 * (part 2: sheets/sources.ts, sheets/fish.ts, sheets/other.ts), `parts` (sheets/parts.ts: each picker choice's STORED display
 * status on the day, read by T09 as `choicesOnDay`; null when the inputs do not carry it — never a guess).
 */
import { ruleBook, ruleBookLine } from "./book";
import type { GlossaryTerm, RegsInputs, RuleView, Statics } from "./inputs";
import { plain, squash, text, type Line, type Run } from "./line";
import type { GroupVM, SheetBlock, SheetCtx, SheetVM } from "./model";
import { parseSheetKey, type ProfileKey, type SheetKey } from "./sheetKey";
import { WORDS } from "./words";
import { ruleSub } from "./level";
import { clean } from "./names";
import { licsrcSheet, srcSheet, srcsSheet } from "./sheets/sources";
import { fishSheet } from "./sheets/fish";
import { otherSheet } from "./sheets/other";
import { partsSheet } from "./sheets/parts";

const para = (s: string, tone?: "small" | "note" | "mono"): SheetBlock => (tone ? { k: "para", text: [text(s)], tone } : { k: "para", text: [text(s)] });

/** sheetHtml rule branch over openRule: applies paragraphs, the exact words, the plain sentence, notes, the technical fold,
 *  the book pages. */
function ruleSheet(key: SheetKey, r: RuleView): SheetVM {
  const f = r.facts;
  const blocks: SheetBlock[] = [
    ...[f?.applies, f?.applies_trib].filter((a): a is string => !!a).map((a) => para(a)),
    { k: "head", text: WORDS.SHEET_EXACT },
    { k: "quote", text: clean(r.export.verbatim) },
    ...(f?.plain ? [para(f.plain, "small")] : []),
    ...r.notes.map((n) => para(WORDS.SHEET_NOTE + n, "note")),
    { k: "fold", summary: WORDS.SHEET_TECH, open: false,
      body: [{ k: "para", tone: "mono", text: [text(r.key), text(" "), text(JSON.stringify(r.export.fields, null, 1))] }] },
    { k: "pages", line: ruleBook(r) },
  ];
  return { key, title: clean(r.export.label || WORDS.SHEET_RULE), sub: ruleSub(r), blocks };
}

/** prototype pageLink: a PRINTED page as a bare book run, its PDF page from the one printed → PDF map (`Statics.pdfPages`);
 *  a page the map lacks is unlinked (pdf null). */
const pageRun = (inp: { readonly statics: Pick<Statics, "pdfPages"> }, printed: number): Run => ({ t: "book", printed, pdf: inp.statics.pdfPages[printed] ?? null, bare: true });

/** prototype glossBody: the plain words, the example, the book's own words folded, then "Rule book" + the pages. */
export function glossBlocks(inp: { readonly statics: Pick<Statics, "pdfPages"> }, t: GlossaryTerm): SheetBlock[] {
  const out: SheetBlock[] = [para(t.says)];
  if (t.example) out.push({ k: "example", text: [text(WORDS.SHEET_FOR_EXAMPLE), text(" "), text(t.example)] });
  if (t.quote) out.push({ k: "fold", summary: WORDS.SHEET_BOOK_WORDS, open: false, body: [{ k: "quote", text: t.quote }] });
  if (t.pages.length) {
    const line: Run[] = [text(WORDS.BOOK)];
    for (const p of t.pages) line.push(text(" "), pageRun(inp, p));
    out.push({ k: "pages", line });
  }
  return out;
}

const termOf = (inp: RegsInputs, id: string): GlossaryTerm | null => inp.statics.glossary.terms.find((t) => t.id === id) ?? null;

/** sheetHtml gl branch: one term → its own sheet; several → "What these words mean" with a section per term. */
function glossSheet(key: SheetKey, inp: RegsInputs, ids: readonly string[]): SheetVM | null {
  const terms = ids.map((id) => termOf(inp, id)).filter((t): t is GlossaryTerm => !!t);
  if (!terms.length) return null;
  if (terms.length === 1) return { key, title: terms[0]!.term, sub: null, blocks: glossBlocks(inp, terms[0]!) };
  return { key, title: WORDS.TERMS_MANY, sub: null,
    blocks: terms.map((t) => ({ k: "term", id: t.id, title: t.term, body: glossBlocks(inp, t) })) };
}

const norm1 = (t: string | null | undefined): string => squash(t || "").toLowerCase();

/** sheetHtml pos branch: the Keep tab's possession sentence, the term's plain words unless they restate it, the rows' own
 *  examples (else the term's generic one), the book's words folded, then "Source ›" + the rule's pages. */
function possessionSheet(key: SheetKey, ctx: SheetCtx): SheetVM | null {
  const o = ctx.page.keep.possession;
  if (!o) return null;
  const t = termOf(ctx.inp, o.term), r = ctx.inp.rules.byKey?.(o.rule) ?? null;
  const blocks: SheetBlock[] = [para(o.says)];
  if (t && t.says && norm1(t.says) !== norm1(o.says)
      && !norm1(t.says).includes(norm1(o.says).replace(/^possession: ?/, "").replace(/\.$/, ""))) blocks.push(para(t.says));
  const own = (ctx.inp.frames?.rows.rows ?? []).map((row) => row.group?.possession).filter((p) => !!p);
  if (own.length) {
    blocks.push({ k: "example", text: [text(WORDS.SHEET_FOR_EXAMPLE)] });
    blocks.push({ k: "list", items: own.map((p): Line => (p!.doubles ? [text(p!.example), text(" "), text(p!.doubles)] : [text(p!.example)])) });
  } else if (t?.example) blocks.push({ k: "example", text: [text(WORDS.SHEET_FOR_EXAMPLE), text(" "), text(t.example)] });
  if (t?.quote) blocks.push({ k: "fold", summary: WORDS.SHEET_RULE_BOOK_WORDS, open: false, body: [{ k: "quote", text: t.quote }] });
  const book = ruleBookLine(r);
  blocks.push({ k: "source", line: [{ t: "link", s: WORDS.SHEET_SOURCE_LINK, open: `rule:${o.rule}` }, ...(book.length ? [text(" "), ...book] : [])] });
  return { key, title: WORDS.POSSESSION, sub: null, blocks };
}

/** sheetHtml hatch branch over page v36 adiposeHtml: the two fins, the plain how-to, then the hatchery_wild term. */
function hatchSheet(key: SheetKey, inp: RegsInputs): SheetVM {
  const hw = termOf(inp, "hatchery_wild");
  const blocks: SheetBlock[] = [
    { k: "adipose", figures: [
      { variant: "wild", caption: [text(WORDS.SHEET_HATCH_WILD), text(WORDS.SHEET_HATCH_WILD_CAP)] },
      { variant: "clipped", caption: [text(WORDS.SHEET_HATCH_CLIPPED), text(WORDS.SHEET_HATCH_CLIPPED_CAP)] }] },
    para(WORDS.SHEET_HATCH_LOOK, "small"),
  ];
  if (hw) blocks.push({ k: "term", id: hw.id, title: hw.term, body: glossBlocks(inp, hw) });
  return { key, title: WORDS.SHEET_HATCH_TITLE, sub: WORDS.SHEET_HATCH_SUB, blocks };
}

/** The sheet a key opens on this page; null when the key does not apply here (or its kind is not built yet). Never throws. */
export function sheetOf(ctx: SheetCtx, key: SheetKey): SheetVM | null {
  if (!parseSheetKey(key)) return null;
  if (key.startsWith("rule:")) { const r = ctx.inp.rules.byKey?.(key.slice(5)) ?? null; return r ? ruleSheet(key, r) : null; }
  if (key.startsWith("gl:")) return glossSheet(key, ctx.inp, key.slice(3).split(","));
  if (key === "pos:pos") return possessionSheet(key, ctx);
  if (key === "hatch") return hatchSheet(key, ctx.inp);
  if (key.startsWith("srcs:")) return srcsSheet(ctx, key);
  if (key.startsWith("licsrc:")) return licsrcSheet(ctx, key, key.slice(7) as ProfileKey);
  if (key.startsWith("src:r")) return srcSheet(ctx, key, +key.slice(5));
  if (key.startsWith("other:o")) return otherSheet(ctx, key, +key.slice(7));
  if (key.startsWith("sp:")) return fishSheet(ctx.inp, key, key.slice(3), ctx.env?.fish);
  if (key === "parts") return partsSheet(key, ctx.inp);
  return null;
}

// ── the golden comparison form ────────────────────────────────────────────────────────────────────────────────────────

export interface SheetLines {
  title: string; sub: string | null; folds: { summary: string; open: false }[];
  pages: { text: string; pdf: number | null }[]; lines: string[]; nested: string[];
}

/** One run's sheet key (golden `nested`: data-open / data-rule / data-gl), in document order. A `src:` link is not one: the
 *  page opens a row's sources by `data-src`, which the golden's nested keys do not list. */
function keysOf(l: Line, out: string[]): void {
  for (const r of l) {
    if (r.t === "link" && typeof r.open === "string") { if (!r.open.startsWith("src:")) out.push(r.open); }
    else if (r.t === "term") out.push(`gl:${r.ids.join(",")}`);
  }
}

/** Every sheet key a drawn VM opens (a group's pictures, ⓘ terms, links), in the VM's field order — the nested keys of a
 *  `group` block, whose lines the caller words. */
function keysDeep(x: unknown, out: string[]): void {
  if (Array.isArray(x)) { for (const y of x) keysDeep(y, out); return; }
  if (!x || typeof x !== "object") return;
  const o = x as Record<string, unknown>;
  if (o.t === "term" && Array.isArray(o.ids)) { out.push(`gl:${(o.ids as string[]).join(",")}`); return; }
  if (o.t === "link") { if (typeof o.open === "string" && !o.open.startsWith("src:")) out.push(o.open); return; }
  if (typeof o.code === "string" && typeof o.open === "string" && o.sil === false) { out.push(o.open); return; }
  for (const [k, v] of Object.entries(o)) if (k !== "more") keysDeep(v, out);
}

/** Every linked book page a drawn VM holds, in its field order (a `group` block's pages). */
function booksDeep(x: unknown, out: SheetLines["pages"]): void {
  if (Array.isArray(x)) { for (const y of x) booksDeep(y, out); return; }
  if (!x || typeof x !== "object") return;
  const o = x as Record<string, unknown>;
  if (o.t === "book") { if (typeof o.pdf === "number") out.push({ text: plain([o as Run]), pdf: o.pdf }); return; }
  for (const v of Object.values(o)) booksDeep(v, out);
}

/** Options of the comparison form: the Keep group's drawn lines (the Keep tab's own projection — a `group` block is drawn
 *  by the Keep group component, so its words are compared the way the Keep parity compares them). */
export interface SheetLinesOpts { readonly groupLines?: (g: GroupVM) => readonly string[] }

/** golden.mjs `books()`: every LINKED page (an `a.book`) in order; an unlinked page is not a link. */
function booksOf(l: Line, out: SheetLines["pages"]): void {
  for (const r of l) if (r.t === "book" && r.pdf !== null) out.push({ text: plain([r]), pdf: r.pdf });
}

/**
 * The sheet as golden.mjs records it: the title and sub, every fold (closed), every linked book page, one text line per
 * block (golden `lines()`: a fold's summary, then its body; a term's title, then its body; a list's items one each), the
 * nested sheet keys. Runs of one Line concatenate as is, then collapse (contracts §1 plain()); separate elements of one
 * drawn line (a ladder line's words, tag, where it applies and pages; a rule row's title, level, applies and pages) join
 * with one space. A `link` block is the prototype's inline `.linkrow` button: it ends the line before it.
 */
export function sheetLines(vm: SheetVM, opts: SheetLinesOpts = {}): SheetLines {
  const out: SheetLines = { title: vm.title, sub: vm.sub, folds: [], pages: [], lines: [], nested: [] };
  const nested: string[] = [];
  const line = (l: Line) => { const s = plain(l); if (s) out.lines.push(s); booksOf(l, out.pages); keysOf(l, nested); };
  const str = (s: string) => { const t = squash(s); if (t) out.lines.push(t); };
  /** one drawn line of separate elements (one space between them) */
  const parts = (...xs: (string | Line | null | undefined)[]) => {
    const s = squash(xs.map((x) => (x == null ? "" : typeof x === "string" ? x : plain(x))).filter(Boolean).join(" "));
    if (s) out.lines.push(s);
    for (const x of xs) if (x && typeof x !== "string") booksOf(x, out.pages);
  };
  const walk = (bs: readonly SheetBlock[]): void => {
    for (const b of bs) {
      switch (b.k) {
        case "para": case "example": case "value": line(b.text); break;
        case "pages": case "source": line(b.line); break;
        case "link": {
          const s = plain(b.line), last = out.lines.length - 1;
          if (s && last >= 0) out.lines[last] = squash(`${out.lines[last]} ${s}`); else if (s) out.lines.push(s);
          booksOf(b.line, out.pages); keysOf(b.line, nested); break;
        }
        case "head": case "quote": str(b.text); break;
        case "list": b.items.forEach(line); break;
        case "fold": out.folds.push({ summary: b.summary, open: false }); str(b.summary); walk(b.body); break;
        case "term": str(b.title); walk(b.body); break;
        case "adipose": b.figures.forEach((f) => line(f.caption)); break;
        case "rule": parts(b.title, b.level, ...b.applies, b.book); nested.push(b.open); break;
        case "image": str(b.credit); break;
        case "pictures":
          str(b.pics.flatMap((p) => (p.sil ? [] : [p.caption])).join(" "));
          for (const p of b.pics) if (!p.sil) nested.push(p.open);
          break;
        case "rowTitle": str(b.title + (b.scope ?? "")); break;
        case "weekBox": parts(b.day, b.scope); str(b.status); str(b.says); break;
        case "ladder":
          if (b.q) str(b.q);
          if (b.result) str(WORDS.CHECK + b.result);
          for (const st of b.steps) {
            str(st.level);
            for (const l of st.lines) {
              parts(l.text, l.tag, ...l.applies, l.book); keysOf(l.text, nested);
              if (l.rule) nested.push(`rule:${l.rule}`);
              (l.notes ?? []).forEach(str);
            }
          }
          break;
        case "sources":
          for (const g of b.groups) {
            if (!b.overruled) str(g.where);
            for (const r of g.rows) {
              if (b.overruled) {
                if (r.key) nested.push(`rule:${r.key}`);   // the card itself opens its rule; its ⓘ sits inside it
                if (r.trib) { str(WORDS.TERM_ONE); nested.push("gl:tributaries"); }
                str(WORDS.SHEET_QUOTED(r.quote)); str(g.where); str(r.role); (r.notes ?? []).forEach((n) => str(WORDS.SHEET_NOTE + n));
              } else { parts(r.title, r.quote); parts(r.role, r.book); }
            }
          }
          break;
        case "group":
          if (!opts.groupLines) throw new Error("sheetLines: a group block's lines are the Keep group's — pass opts.groupLines");
          out.lines.push(...opts.groupLines(b.titled ? b.group : { ...b.group, title: "" }));
          keysDeep(b.group, nested); booksDeep(b.group, out.pages);
          break;
        // the prototype's rows are inline buttons in one body: one line, each row's label, word, long label and its ✓
        case "choice": parts(...b.items.flatMap((i) => [i.text, i.status, i.long, i.checked ? WORDS.CHECK : null])); break;
      }
    }
  };
  walk(vm.blocks);
  out.nested = [...new Set(nested)];
  return out;
}
