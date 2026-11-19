/**
 * (T892) THE TABLE SAYS WHAT THE SHEET SAYS. Over a LIVE release, every part of every water: a "What you can keep" row's
 * tributary line ("Dutch Creek’s rule for its tributaries") names exactly the waters its own fish sheet kicks "<water> ·
 * its tributaries" on a deciding step (an Other fish row against its `other:oN` sheet; the trout fold against every
 * trout row's `tf:` sheet); a row whose sheet names none says none. A row whose sheet names one for only some of its
 * fish says none (flow.ts `downNames`): counted, not a mismatch. First run (r-fc137c4040fa, 20,963 parts): 0 mismatches.
 *
 * DATA, so Tier 2 (`scripts/test-full.sh` runs it with TIER2=1):
 *   RELEASE_DIR=data/release/r-… TIER2=1 pnpm -C app exec vitest run release-tributary-lines
 * Under TIER2 a missing release FAILS; in `pnpm check` (no data, no TIER2) it is skipped, by name.
 */
import { describe, expect, it } from "vitest";
import { buildRegsPage, type FlowVM, type RegsPage, type SheetKey } from "@app/core";
import { downLine } from "../packages/core/src/regs/flow";
import { loadRegsInputs } from "@app/data/regs";
import { openReleaseDir } from "@app/data/release/node";
import { loadRegsStore } from "@app/ui";
import { namedRelease } from "../conformance/src/fixtureRelease";

const REL = namedRelease();
const TIER2 = process.env.TIER2 === "1";
const DAY = { month: 11, day: 19 };
const DOWN = " · its tributaries";

const flowOf = (page: RegsPage, key: string): FlowVM | null =>
  page.sheet(key as SheetKey)?.blocks.find((b): b is Extract<typeof b, { k: "flow" }> => b.k === "flow")?.flow ?? null;
/** the waters a sheet's deciding steps name "<water> · its tributaries", once each */
const kicked = (f: FlowVM | null): string[] =>
  [...new Set((f?.steps ?? []).filter((s) => s.tone === "won" && s.kicker.endsWith(DOWN)).map((s) => s.kicker.slice(0, -DOWN.length)))];
const same = (a: readonly string[], b: readonly string[]) => a.length === b.length && a.every((x) => b.includes(x));

describe.skipIf(!REL && !TIER2)("Tier 2 (T892): every table row's tributary line is its sheet's", () => {
  it("has a release to check (RELEASE_DIR)", () => {
    expect(REL, "Tier 2 needs RELEASE_DIR=data/release/r-…").not.toBeNull();
  });

  it("every part of every water: row line = the sheet's deciding tributary kickers", { timeout: 1_800_000 }, async () => {
    const st = await loadRegsStore(openReleaseDir(REL!));
    const items = await (await st.core()).all("SELECT item_id, name FROM item ORDER BY ord");
    const bad: string[] = [];
    let parts = 0, other = 0, partial = 0, folds = 0, groups = 0;
    for (const it of items) {
      const ord = await st.itemOrd(String(it.item_id));
      if (ord === null) continue;
      const n = (await st.parts(ord)).length;
      for (let p = 0; p < n; p++) {
        parts++;
        const inp = await loadRegsInputs(st, { kind: "part", item: String(it.item_id), partIx: p }, DAY, "Thursday", 2026);
        const page = buildRegsPage(inp, { today: DAY, who: { age: "adult", residency: "resident" } });
        const at = `${it.name} (${it.item_id}) part ${p}`;
        for (const o of page.keep.otherFish ?? []) {
          const f = flowOf(page, o.open), names = kicked(f);
          if (o.from) {
            other++;
            if (o.from !== downLine(f?.down ?? []) || !same(f?.down ?? [], names)) bad.push(`${at} ${o.name}: "${o.from}" vs sheet ${names.join(", ")}`);
          } else if (names.length) {
            if (f?.down) bad.push(`${at} ${o.name}: no line, sheet ${names.join(", ")}`);
            else partial++;                            // the sheet names it for some of the row's fish only
          }
        }
        groups += page.keep.groups.filter((g) => g.from).length;
        const t = page.keep.trout?.kind === "table" ? page.keep.trout : null;
        if (t) {
          const rows = t.boxes.flatMap((b) => b.rows).map((r) => kicked(flowOf(page, `tf:${r.key}`)));
          const all = rows.length > 0 && rows.every((x) => x.length > 0 && same(x, rows[0]!));
          if (!!page.keep.troutFrom !== all || (page.keep.troutFrom && page.keep.troutFrom !== downLine(rows[0]!)))
            bad.push(`${at} trout fold: "${page.keep.troutFrom ?? "-"}" vs rows ${rows.map((x) => x.join("+") || "-").join(" | ")}`);
          if (page.keep.troutFrom) folds++;
        }
      }
    }
    console.log(`T892 sweep: ${parts} parts; lines on ${other} Other fish rows, ${groups} groups, ${folds} trout folds; ${partial} rows named for some fish only`);
    expect(parts).toBeGreaterThan(1000);
    expect(bad.slice(0, 20)).toEqual([]);
  });
});
