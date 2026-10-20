/**
 * The core word table and the rich-text forms against the goldens themselves (app/design/regs-v11/port/golden/golden.json),
 * so nothing here is a string copied by hand: a prototype bump that renames a tab or reshapes the Rule book link fails
 * this test, not a reader. Core cannot read files (layers.json), so the golden-driven half of T00's tests lives here;
 * T12's harness replaces the loader below.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { WORDS, plain, type Run } from "@app/core";

const GOLDEN = fileURLToPath(new URL("../../../design/regs-v11/port/golden/golden.json", import.meta.url));

interface GoldenPage { readonly text: string; readonly pdf: number | null }
interface Golden {
  readonly meta: { readonly version: string; readonly states: number };
  readonly states: Readonly<Record<string, { readonly header: { readonly tabs: readonly { readonly sec: string; readonly text: string }[] } }>>;
}
const golden = JSON.parse(readFileSync(GOLDEN, "utf8")) as Golden;

/** Every `{text, pdf}` page link anywhere in the golden (tree origins, sources, sheets …). */
function pagesIn(o: unknown, out: GoldenPage[] = []): GoldenPage[] {
  if (Array.isArray(o)) for (const v of o) pagesIn(v, out);
  else if (o && typeof o === "object") {
    const r = o as Record<string, unknown>;
    if (typeof r.text === "string" && "pdf" in r && /p\.\d+/.test(r.text)) out.push(r as unknown as GoldenPage);
    for (const v of Object.values(r)) pagesIn(v, out);
  }
  return out;
}

describe("golden: the tabs", () => {
  it("every state names the tabs as WORDS does, in the same order", () => {
    const ids = Object.keys(golden.states);
    expect(ids.length).toBe(golden.meta.states);
    const words = [WORDS.TAB_KEEP, WORDS.TAB_GEAR, WORDS.TAB_METHODS, WORDS.TAB_LICENCE];
    for (const id of ids) expect(golden.states[id]!.header.tabs.map((t) => t.text), id).toEqual(words);
  });
});

describe("golden: the Rule book link", () => {
  it("plain() of a book run is the golden page text, labelled or bare, on every page link the goldens hold", () => {
    const pages = pagesIn(golden);
    expect(pages.length).toBeGreaterThan(1000);
    let bare = 0;
    for (const p of pages) {
      const printed = Number(/p\.(\d+)/.exec(p.text)![1]);
      const isBare = !p.text.startsWith(WORDS.BOOK);
      if (isBare) bare++;
      const run: Run = isBare ? { t: "book", printed, pdf: p.pdf, bare: true } : { t: "book", printed, pdf: p.pdf };
      expect(plain([run]), p.text).toBe(p.text);
    }
    expect(bare).toBeGreaterThan(0);   // the glossary's bare pages are covered too
  });
});
