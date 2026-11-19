/**
 * (T892, AGENTS 20a) THE SITE'S TROUT TABLE SAYS EVERY TEXT OF A ROW — release-free, so it runs in `pnpm check`. The staging
 * release stopped once at the page-vs-app parity check (`verifyAgainstApp`) because the page dropped a new row text
 * (`TroutRowVM.down`, "Duncan River’s rule for its tributaries"; T907: the tag `TroutRowVM.tag`, "Duncan River’s tributaries",
 * beside the name). Here a synthetic table: its whole composed section is
 * pinned in draw order, and every text field a row carries (whatever its name: a field added later included) must be said.
 */
import { describe, expect, it } from "vitest";
import type { TroutRowVM, TroutVM } from "@app/core";
import { troutWords } from "./model";

const TAG = "Duncan River’s tributaries";
const row: TroutRowVM = {
  key: "DV", fish: "DV", name: "Dolly Varden/bull trout", origin: null, label: "Hatchery only", split: false, tag: TAG,
  sub: "a rainbow over 50 cm (20 in)", lines: ["only 1 over 50 cm (20 in)"], quiet: ["In your possession: no more than 2"],
  src: [0], sheet: { head: "Release", here: [], src: [0] }, coming: ["It reopens on Jun 1."], date: "until May 31, then release",
  a11y: "(the row's label: not a page word)",
};
const table = (r: TroutRowVM): TroutVM => ({
  kind: "table",
  top: { lead: "Release every trout and char today", sub: null, second: null, date: null },
  card: null, fold: { name: "Trout and char", value: "Release", tone: "release", date: null }, hatchLink: null,
  boxes: [{ kind: "release", head: "Release", label: null, strip: null, possession: null, tag: null, date: null, rows: [r], bands: [], inner: [] }],
  together: null, year: null, elsewhere: [], elsewhereLink: null, sourcesLink: "Sources", src: [0], covers: [0],
});
const said = (t: TroutVM) => troutWords(t).filter((x) => x !== null).map((x) => x!.text);

describe("(T892) the site's trout row: every text the app draws, in its order", () => {
  it("the whole section, the tributary tag beside the name (T907)", () => {
    expect(said(table(row))).toEqual([
      "Trout and char", "Release every trout and char today", "Release", "Release",
      "Dolly Varden/bull trout", "Hatchery only", TAG, "a rainbow over 50 cm (20 in)", "only 1 over 50 cm (20 in)",
      "In your possession: no more than 2", "until May 31, then release", "It reopens on Jun 1.",
    ]);
  });
  it("every text field of a row is said (a field the page forgets fails here, not at the release)", () => {
    const NOT_WORDS = new Set(["key", "fish", "a11y", "sheet", "src"]);   // ids, the label read aloud, the sheet's own page
    const texts = Object.entries(row).filter(([k]) => !NOT_WORDS.has(k))
      .flatMap(([, v]) => (typeof v === "string" ? [v] : Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : []));
    const got = said(table(row));
    expect(texts.filter((x) => !got.includes(x))).toEqual([]);
    expect(texts.length).toBeGreaterThanOrEqual(8);
  });
  it("no tag, nothing said for it", () => {
    expect(said(table({ ...row, tag: null }))).not.toContain(TAG);
  });
});
