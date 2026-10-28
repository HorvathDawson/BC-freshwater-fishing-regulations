// T225 port of pipeline/regs/dfo_salmon/parse.py: one region page → sections + precedence-ranked rows.
// The table is sliced out of the raw source by regex (Regions 4/7 carry an unterminated `<!--` that swallows the DOM),
// then expanded into a dense 5-column grid honouring rowspan/colspan, exactly as the Python does.
import { PAGES, normalizeSlug, pageUrl } from "./fetch";
import { parseHtml, Str, Tag } from "./html";
import { findall, finditer, match, pyInt, pyRe, search, strip, sub } from "./py";

export const N_COLS = 5;

const RE_WS = pyRe("\\s+");
const RE_FN = pyRe("\\bFN\\s?(\\d{3,5})\\b", "i");
export const RE_TABLE = pyRe("<table\\b[^>]*>.*?</table>", "si");
const RE_COMMENT = pyRe("<!--.*?-->", "s");
const RE_ORPHAN_COMMENT = pyRe("<!--.*$", "s");
export const RE_H1 = pyRe("<h1\\b[^>]*\\bid=[\"']wb-cont[\"'][^>]*>", "i");
const RE_PAGE_TAIL = pyRe("<(?:footer|div[^>]*class=[\"'][^\"']*pagedetails)", "i");
export const RE_DATE_MOD = pyRe('<time[^>]*property="dateModified"[^>]*>\\s*([0-9]{4}-[0-9]{2}-[0-9]{2})', "i");
const RE_SECTION = pyRe(
  "^\\s*(?P<letter>[A-H])\\s*(?:\\((?P<roman1>[ivx]+)\\))?\\s*[.:]\\s*(?:Part\\s*\\((?P<roman2>[ivx]+)\\)\\s*[.:]?)?\\s*(?P<title>\\S.*)$",
  "s",
);
export const RE_CATCHALL = pyRe("^\\s*(all\\s+(waters|lakes|streams)|all\\s+region\\b|all\\s+other\\b|all\\s*$)", "i");
const RE_AREAS = pyRe("\\bareas?\\s+(\\d{1,2}(?:(?:\\s*[,&]\\s*|\\s+and\\s+)+\\d{1,2})*)", "i");
const RE_SEE_ALSO = pyRe("^(?P<name>.+?)\\s*[-–—]?\\s*see\\s+(?P<target>.+?)\\s*$", "i");
const RE_DIGITS = pyRe("\\d{1,2}");
const RE_TRAIL_DASH = pyRe("(\\s*—\\s*)+$");
const RE_FALLBACK_A = pyRe('section\\s*"?A"?\\s*applies', "i");
const RE_RESTRICTS = pyRe("\\bno fishing\\b|\\bnon[- ]retention\\b|\\bclosed\\b", "i");

export const norm = (text: string): string => strip(sub(RE_WS, " ", text.replace(/\xa0/g, " ")));

export interface Cell {
  text: string;
  bullets: string[];
  links: { text: string; href: string }[];
}
const cell = (text = "", bullets: string[] = [], links: Cell["links"] = []): Cell => ({ text, bullets, links });

export interface Section {
  key: string;
  letter: string;
  part: string | null;
  title: string;
  falls_back_to_a: boolean;
}

export interface RegRow {
  region: string;
  region_number: number;
  region_name: string;
  row_index: number;
  section_key: string | null;
  section_title: string | null;
  waters: string;
  waters_bullets: string[];
  specific_area: string;
  specific_area_bullets: string[];
  species: string;
  dates: string;
  limits_gear: string;
  precedence: number;
  areas: number[];
  fishery_notices: { text: string; href: string }[];
  see_also: string | null;
  source: string;
}

export interface ParsedRegion {
  region: string;
  region_number: number;
  part: string | null;
  region_name: string;
  url: string;
  date_modified: string | null;
  preamble: string[];
  sections: Section[];
  rows: RegRow[];
  notes: { section: string; text: string }[];
  cross_references: { from: string; to: string; section: string }[];
  table_found: boolean;
}

// ── cells ────────────────────────────────────────────────────────────────────────────────────────────────────────────

function extractCell(td: Tag): Cell {
  // BeautifulSoup(str(td)) — a fresh tree holding a copy of the cell
  const clone = new Tag("[document]");
  const copy = td.reparsed();
  copy.parent = clone;
  clone.children.push(copy);
  for (const nested of clone.findAll(["tr", "tbody", "thead", "table"])) nested.decompose();
  const bullets: string[] = [];
  for (const li of clone.findAll("li")) {
    const t = norm(li.getText(" ", true));
    if (t) bullets.push(t);
  }
  const links = clone
    .findAll("a")
    .filter((a) => a.get("href"))
    .map((a) => ({ text: norm(a.getText(" ", true)), href: a.get("href") ?? "" }));
  for (const ul of clone.findAll(["ul", "ol"])) ul.decompose();
  for (const br of clone.findAll("br")) br.replaceWith(new Str(" — "));
  let text = norm(clone.getText(" ", true));
  text = strip(sub(RE_TRAIL_DASH, "", text));
  return cell(text, bullets, links);
}

// ── grid ─────────────────────────────────────────────────────────────────────────────────────────────────────────────

function rowTags(body: Tag, table: Tag): Tag[] {
  return body.findAll("tr").filter((tr) => tr.findParent("thead") === null && tr.findParent("table") === table);
}

const intAttr = (td: Tag, name: string): number => {
  const v = td.get(name);
  return v === undefined || v === "" ? 1 : pyInt(v); // int(td.get(name, 1) or 1): "0" is 0, "2px" raises
};

export function expandGrid(table: Tag): Cell[][] {
  const grid: Cell[][] = [];
  let pending = new Map<number, [Cell, number]>();
  const body = table.find("tbody") ?? table;
  for (const tr of rowTags(body, table)) {
    const cells = tr.findAll(["td", "th"], false);
    if (!cells.length) continue;

    if (cells.length === 1 && pending.size === 0) {
      const span = intAttr(cells[0]!, "colspan");
      if (span >= N_COLS - 1) {
        grid.push([extractCell(cells[0]!)]);
        continue;
      }
    }

    const spanTotal = cells.reduce((s, td) => s + Math.max(1, Math.min(intAttr(td, "colspan"), N_COLS)), 0);
    let carryFrom: Cell[] | null = null;
    if (pending.size === 0 && grid.length && grid[grid.length - 1]!.length === N_COLS && spanTotal > 1 && spanTotal < N_COLS)
      carryFrom = grid[grid.length - 1]!;

    const row: (Cell | null)[] = new Array(N_COLS).fill(null);
    const still = new Map<number, [Cell, number]>();
    for (const [col, [c, left]] of pending) {
      if (col < N_COLS) row[col] = c;
      if (left - 1 > 0) still.set(col, [c, left - 1]);
    }
    pending = still;

    let col = 0;
    if (carryFrom !== null) {
      const lead = N_COLS - spanTotal;
      const noteRow =
        carryFrom.length === N_COLS &&
        carryFrom[2] === carryFrom[3] &&
        carryFrom[3] === carryFrom[4] &&
        carryFrom[1] === carryFrom[2];
      for (let k = 0; k < lead; k++) row[k] = noteRow && k > 0 ? cell() : carryFrom[k]!;
      col = lead;
    }

    for (const td of cells) {
      while (col < N_COLS && row[col] !== null) col += 1;
      if (col >= N_COLS) break;
      const colspan = Math.max(1, Math.min(intAttr(td, "colspan"), N_COLS - col));
      const rowspan = Math.max(1, intAttr(td, "rowspan"));
      const value = extractCell(td);
      for (let k = 0; k < colspan; k++) {
        row[col + k] = value;
        if (rowspan > 1) pending.set(col + k, [value, rowspan - 1]);
      }
      col += colspan;
    }
    grid.push(row.map((c) => c ?? cell()));
  }
  return grid;
}

// ── flags ────────────────────────────────────────────────────────────────────────────────────────────────────────────

function areasIn(text: string): number[] {
  const out: number[] = [];
  for (const m of finditer(RE_AREAS, text))
    for (const n of findall(RE_DIGITS, m[1]!) as string[]) {
      const v = pyInt(n);
      if (!out.includes(v)) out.push(v);
    }
  return out;
}

/** "B. Part (i): ..." → its section; text after the first "." for the catch-all test */
export const afterFirstDot = (s: string): string => {
  const i = s.indexOf(".");
  return i < 0 ? s : s.slice(i + 1);
};

export function parseSection(text: string): Section | null {
  const m = match(RE_SECTION, text);
  if (!m) return null;
  const g = m.groups!;
  const letter = g.letter!.toUpperCase();
  let part: string | null = g.roman1 || g.roman2 || "" || null;
  if (part) part = part.toLowerCase();
  const key = part ? `${letter}(${part})` : letter;
  return { key, letter, part, title: norm(text), falls_back_to_a: !!search(RE_FALLBACK_A, text) };
}

// ── region ───────────────────────────────────────────────────────────────────────────────────────────────────────────

function stripComments(fragment: string): string {
  return sub(RE_ORPHAN_COMMENT, " ", sub(RE_COMMENT, " ", fragment));
}

export function contentSlice(html: string, tableStart: number | null): string {
  const h1 = search(RE_H1, html);
  const lo = h1 ? h1.index + h1[0].length : 0;
  let hi: number;
  if (tableStart !== null) hi = tableStart;
  else {
    const tail = search(RE_PAGE_TAIL, html, lo);
    hi = tail ? tail.index : html.length;
  }
  return hi > lo ? stripComments(html.slice(lo, hi)) : "";
}

export function parseRegion(html: string, region: string, url = ""): ParsedRegion {
  const slug = normalizeSlug(region);
  const page = PAGES[slug]!;
  if (!url) url = pageUrl(slug);
  const dm = search(RE_DATE_MOD, html);
  const dateModified = dm ? dm[1]! : null;
  const tm = search(RE_TABLE, html);

  const preamble: string[] = [];
  const sliceSoup = parseHtml(contentSlice(html, tm ? tm.index : null));
  for (const node of sliceSoup.findAll(["p", "li"])) {
    if (node.find(["p", "li"])) continue;
    const t = norm(node.getText(" ", true));
    if (t && [...t].length > 2 && !preamble.includes(t)) preamble.push(t);
  }

  const sections: Section[] = [];
  const rows: RegRow[] = [];
  const notes: ParsedRegion["notes"] = [];
  const crossRefs: ParsedRegion["cross_references"] = [];
  const base = { region: slug, region_number: page.region, part: page.part, region_name: page.name, url, date_modified: dateModified };

  if (!tm) return { ...base, preamble, sections, rows, notes, cross_references: crossRefs, table_found: false };

  const table = parseHtml(stripComments(tm[0])).find("table")!;
  let current: Section | null = null;
  let idx = 0;
  const note = (text: string) => notes.push({ section: current ? current.key : "", text });
  const crossref = (text: string): boolean => {
    const m = match(RE_SEE_ALSO, text);
    if (!m) return false;
    crossRefs.push({ from: norm(m.groups!.name!), to: norm(m.groups!.target!), section: current ? current.key : "" });
    return true;
  };

  for (const gridRow of expandGrid(table)) {
    if (gridRow.length === 1) {
      const text = gridRow[0]!.text;
      if (!text) continue;
      const sec = parseSection(text);
      if (sec) {
        sections.push(sec);
        current = sec;
      } else if (!crossref(text)) note(text);
      continue;
    }
    let [waters, area, species, dates, limits] = gridRow as [Cell, Cell, Cell, Cell, Cell];

    const sec = waters.text ? parseSection(waters.text) : null;
    if (sec && match(RE_CATCHALL, strip(afterFirstDot(sec.title)) || "")) {
      if (!sections.some((x) => x.key === sec.key)) sections.push(sec);
      current = sec;
      waters = cell(norm(afterFirstDot(sec.title)), waters.bullets, waters.links);
    }

    if (species === dates && dates === limits && species.text) {
      note(waters.text ? `${waters.text}: ${species.text}` : species.text);
      continue;
    }

    if (!(species.text || dates.text || limits.text)) {
      const joined = gridRow.filter((c) => c.text).map((c) => c.text).join(" ");
      if (joined && !crossref(joined)) note(joined);
      continue;
    }

    let see: string | null = null;
    if (!species.text) {
      const m = match(RE_SEE_ALSO, waters.text);
      if (m) see = norm(m.groups!.target!);
    }

    let scopeText = waters.text || area.text || "";
    if (match(RE_CATCHALL, waters.text || "") && area.text) scopeText = area.text;
    let areas = areasIn(scopeText);
    let precedence: number;
    if (current && current.letter === "A") precedence = 0;
    else if (match(RE_CATCHALL, waters.text || "") || match(RE_CATCHALL, scopeText)) precedence = areas.length ? 2 : 1;
    else {
      precedence = 3;
      areas = [];
    }

    const fns: { text: string; href: string }[] = [];
    for (const c of [limits, dates, area])
      for (const link of c.links)
        if (search(RE_FN, link.text) || link.href.includes("notices.dfo-mpo.gc.ca"))
          if (!fns.some((f) => f.text === link.text && f.href === link.href)) fns.push(link);

    rows.push({
      region: slug,
      region_number: page.region,
      region_name: page.name,
      row_index: idx,
      section_key: current ? current.key : null,
      section_title: current ? current.title : null,
      waters: waters.text,
      waters_bullets: waters.bullets,
      specific_area: area.text,
      specific_area_bullets: area.bullets,
      species: species.text,
      dates: dates.text,
      limits_gear: limits.text,
      precedence,
      areas,
      fishery_notices: fns,
      see_also: see,
      source: "table",
    });
    idx += 1;
  }

  for (const sec of sections) {
    if (rows.some((r) => r.section_key === sec.key)) continue;
    if (!search(RE_RESTRICTS, sec.title)) continue;
    const head = afterFirstDot(sec.title);
    const dash = head.indexOf("-");
    rows.push({
      region: slug,
      region_number: page.region,
      region_name: page.name,
      row_index: idx,
      section_key: sec.key,
      section_title: sec.title,
      waters: norm(dash < 0 ? head : head.slice(0, dash)),
      waters_bullets: [],
      specific_area: sec.title,
      specific_area_bullets: [],
      species: "All",
      dates: "",
      limits_gear: sec.title,
      precedence: 1,
      areas: [],
      fishery_notices: [],
      see_also: null,
      source: "section_banner",
    });
    idx += 1;
  }

  return { ...base, preamble, sections, rows, notes, cross_references: crossRefs, table_found: true };
}
