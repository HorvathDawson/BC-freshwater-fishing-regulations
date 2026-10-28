// T225: one level below the feed — every recorded DFO page through the TS `typePage` + `parseHeader` against the
// Python `type_page` + `header.parse_header` (parity/dfo/typed_dump.py): typed rules, locations (fingerprints,
// scopes, ops, landmarks), structure and header notes must be identical. Catches a parser divergence on a page the
// feed scenarios do not replay.
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { typePage } from "../src/dfo";
import { parseHeader } from "../src/dfo/header";
import { pyStrOf, pyTypeName } from "../src/dfo/py";
import { dumps } from "../src/dfo/pyjson";
import { REPO } from "./dfo.scenario";

const FIXDIR = join(REPO, "app/workers/feeds/test/fixtures/dfo");
const DIRS = (process.env.DFO_CORPUS ?? ["pages", "history", "broken"].map((d) => join(FIXDIR, d)).join(":")).split(":");
const recorded = DIRS.flatMap((d) => readdirSync(d).filter((f) => /^region\d[ab]?[-_].*\.html$/.test(f) && f !== "region5-eng.html").map((f) => join(d, f)));

// SEEDED FUZZ (DFO_FUZZ pages, default 80): recorded pages with markup broken the ways DFO breaks it (unclosed cells
// and comments, stray end tags, entities, truncation) and cells rewritten to dates / limits the validators refuse.
// Both sides must agree on every tree, every rule and every error message.
const SNIPS = [
  "<td>", "</td>", "<tr>", "</tr>", "<!--", "-->", "<br>", "</br>", "<p>", "</p>", "<li>", "<ul>", "&nbsp;", "&amp", "&#8217;",
  "&#x2019", "&#38a", '<td colspan="2">', '<td rowspan="3">', "<th colspan=5>", '<td colspan="2x">', "<script>", "</script>",
  "</table>", "<table>", "<", "&", '"', "Jun 31 to Jul 4", "Eulachon", "snagged", "0 per day", "Aprl 1 to Jun 15",
  "from 05:00 h until 25:00 h", "west half of the river", "adult chinook", "all species", "Nov 01- to Dec 31",
  "less than 5 mm from point to shank", "catch and release", "none over 50 cm", "To be determined", "Feb 30 to Mar 4",
  "No fishing Aug 1-Sep 15", "single, barbless hook", "fly fishing only", "4 hatchery marked per day",
];
function fuzz(n: number): string[] {
  if (!n) return [];
  let seed = 20261028;
  const rnd = () => ((seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648);
  const pick = <T,>(xs: T[]) => xs[Math.floor(rnd() * xs.length)]!;
  const dir = mkdtempSync(join(tmpdir(), "dfo-fuzz-"));
  const out: string[] = [];
  for (let k = 0; k < n; k++) {
    const src = pick(recorded);
    const slug = /region(\d[ab]?)[-_]/.exec(src.split("/").pop()!)![1]!;
    let h = readFileSync(src, "utf-8");
    const t = h.indexOf("<table");
    const e = h.indexOf("</table>");
    for (let m = 1 + Math.floor(rnd() * 6); m > 0; m--) {
      const [lo, hi] = t > 0 && rnd() < 0.8 ? [t, e] : [0, h.length];
      const i = lo + Math.floor(rnd() * Math.max(1, hi - lo));
      const op = rnd();
      if (op < 0.5) h = h.slice(0, i) + pick(SNIPS) + h.slice(i);
      else if (op < 0.85) h = h.slice(0, i) + h.slice(i + 1 + Math.floor(rnd() * 40));
      else if (op < 0.9) h = h.slice(0, i);
      else h = h.replace(/<td([^>]*)>[^<]*<\/td>/g, (all) => (rnd() < 0.04 ? `<td>${pick(SNIPS)} ${pick(SNIPS)}</td>` : all));
    }
    const p = join(dir, `region${slug}_fuzz${k}.html`);
    writeFileSync(p, h);
    out.push(p);
  }
  return out;
}
const pages = [...recorded, ...fuzz(Number(process.env.DFO_FUZZ ?? 80))];
const hasPython = existsSync(join(REPO, ".venv/bin/python"));

describe.skipIf(!hasPython)("DFO typed corpus parity", () => {
  it(`${pages.length} pages`, () => {
    const out = join(mkdtempSync(join(tmpdir(), "dfo-corpus-")), "py.jsonl");
    execFileSync(join(REPO, ".venv/bin/python"), [join(REPO, "app/workers/feeds/parity/dfo/typed_dump.py"), out, ...pages], { stdio: "inherit" });
    const py = readFileSync(out, "utf-8").trim().split("\n");
    const bad: string[] = [];
    pages.forEach((p, i) => {
      const name = p.split("/").pop()!;
      const slug = /region(\d[ab]?)[-_]/.exec(name)![1]!;
      const html = readFileSync(p, "utf-8").replace(/\r\n?/g, "\n");
      const row: Record<string, unknown> = { page: name, slug };
      try {
        const [typed, locations, structure] = typePage(html, slug);
        Object.assign(row, { typed, locations, structure });
      } catch (e) {
        row.error = `${pyTypeName(e)}: ${[...pyStrOf(e)].slice(0, 120).join("")}`;
      }
      try {
        row.header = parseHeader(html, slug);
      } catch (e) {
        row.header_error = pyTypeName(e);
      }
      const ts = dumps(row, { sortKeys: true, ensureAscii: false });
      if (ts !== py[i]) {
        let k = 0;
        while (ts[k] === py[i]![k]) k++;
        bad.push(`${name} at ${k}:\n  py ${py[i]!.slice(Math.max(0, k - 150), k + 150)}\n  ts ${ts.slice(Math.max(0, k - 150), k + 150)}`);
      }
    });
    expect(bad.slice(0, 3).join("\n\n")).toBe("");
    expect(bad.length).toBe(0);
  });
});
