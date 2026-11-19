/**
 * (T892) THE TABLE SAYS WHAT THE SHEET SAYS. Over a LIVE release, every part of every water, each "What you can keep" row
 * against its own fish sheet's rail as drawn (conformance `railSetters`: the steps that set the answer, per origin):
 * - a row WITH a tributary line ("Dutch Creek’s rule for its tributaries"): the rail's setting steps are all "<water> · its
 *   tributaries", for every origin, and name exactly the line's waters (an Other fish row against `other:oN`; the trout
 *   fold against every trout row's `tf:` sheet);
 * - a row WITHOUT one whose rail still names a deciding tributary step: proved partial — the rail shows another rule
 *   setting some origin's answer, or (an Other fish row, whose rail merges its fish) the stored answers show some fish ×
 *   origin set by a rule that is not a tributary one (its winner, or a rule that lowered its number: `storedElsewhere`).
 *   Anything else fails (e.g. a line lost to an empty entry name).
 *
 * DATA, so Tier 2 (`scripts/test-full.sh` runs it with TIER2=1):
 *   RELEASE_DIR=data/release/r-… TIER2=1 pnpm -C app exec vitest run release-tributary-lines
 * Under TIER2 a missing release FAILS; in `pnpm check` (no data, no TIER2) it is skipped, by name.
 */
import { describe, expect, it } from "vitest";
import { buildRegsPage, type FlowVM, type RegsInputs, type RegsPage, type SheetKey } from "@app/core";
import { loadRegsInputs } from "@app/data/regs";
import { openReleaseDir } from "@app/data/release/node";
import { loadRegsStore } from "@app/ui";
import { downLine } from "../packages/core/src/regs/flow";
import { buildModel } from "../packages/core/src/regs/card";
import { makePlace } from "../packages/core/src/regs/place";
import { settle } from "../packages/core/src/regs/settle";
import { listRows, tableCovers } from "../packages/core/src/regs/others";
import { namedRelease } from "../conformance/src/fixtureRelease";
import { railSetters, storedElsewhere } from "../conformance/src/regs/tributaryEvidence";

const REL = namedRelease();
const TIER2 = process.env.TIER2 === "1";
const DAY = { month: 11, day: 19 };

const flowOf = (page: RegsPage, key: string): FlowVM | null =>
  page.sheet(key as SheetKey)?.blocks.find((b): b is Extract<typeof b, { k: "flow" }> => b.k === "flow")?.flow ?? null;
const same = (a: readonly string[], b: readonly string[]) => a.length === b.length && a.every((x) => b.includes(x));

/** The Other fish list's i-th row: some fish × origin of it whose stored answer another rule sets. */
function otherElsewhere(inp: RegsInputs, i: number): boolean {
  const place = makePlace(inp), card = buildModel(inp, place, settle(inp, place));
  const row = card.rows[listRows(card, tableCovers(inp))![i]!]!, f = inp.year!.rowsBySeg[card.ctx.s]!;
  return row.allMembers.filter((S) => card.R[S]).some((S) =>
    [f.fish[S]?.hatchery, f.fish[S]?.wild].some((d) => !!d && storedElsewhere(d, inp.rules)));
}
/** Some trout row of the table whose stored answer (its origin's, else both) another rule sets. */
function troutElsewhere(inp: RegsInputs, rows: readonly { fish: string; origin: string | null }[]): boolean {
  const place = makePlace(inp), card = buildModel(inp, place, settle(inp, place)), f = inp.year!.rowsBySeg[card.ctx.s]!;
  return rows.some((r) => {
    const a = f.fish[r.fish];
    const ds = r.origin === "hatchery" ? [a?.hatchery] : r.origin === "wild" ? [a?.wild] : [a?.hatchery, a?.wild];
    return ds.some((d) => !!d && storedElsewhere(d, inp.rules));
  });
}

describe.skipIf(!REL && !TIER2)("Tier 2 (T892): every table row's tributary line is its sheet's", () => {
  it("has a release to check (RELEASE_DIR)", () => {
    expect(REL, "Tier 2 needs RELEASE_DIR=data/release/r-…").not.toBeNull();
  });

  it("every part of every water: a line ⇔ the rail's setting steps are all tributary ones; no line ⇒ proved partial", { timeout: 1_800_000 }, async () => {
    const st = await loadRegsStore(openReleaseDir(REL!));
    const items = await (await st.core()).all("SELECT item_id, name FROM item ORDER BY ord");
    const bad: string[] = [];
    let parts = 0, other = 0, otherPartial = 0, folds = 0, troutPartial = 0, groups = 0;
    for (const it of items) {
      const ord = await st.itemOrd(String(it.item_id));
      if (ord === null) continue;
      const n = (await st.parts(ord)).length;
      for (let p = 0; p < n; p++) {
        parts++;
        const inp = await loadRegsInputs(st, { kind: "part", item: String(it.item_id), partIx: p }, DAY, "Thursday", 2026);
        const page = buildRegsPage(inp, { today: DAY, who: { age: "adult", residency: "resident" } });
        const at = `${it.name} (${it.item_id}) part ${p}`;
        (page.keep.otherFish ?? []).forEach((o, i) => {
          const f = flowOf(page, o.open), ev = railSetters(f);
          if (o.from) {
            other++;
            const down = f?.down ?? [];
            if (!ev.setByDown || !ev.names.every((x) => down.includes(x)) || !down.every((x) => ev.mentions.includes(x)) || o.from !== downLine(down))
              bad.push(`${at} ${o.name}: "${o.from}" vs rail setters ${ev.setByDown ? ev.names.join(", ") : "(not all tributary)"}`);
          } else if (ev.mentions.length) {
            if (ev.setByDown && !otherElsewhere(inp, i)) bad.push(`${at} ${o.name}: no line, rail set by ${ev.names.join(", ")}`);
            else otherPartial++;
          }
        });
        groups += page.keep.groups.filter((g) => g.from).length;
        const t = page.keep.trout?.kind === "table" ? page.keep.trout : null;
        if (t) {
          const trows = t.boxes.flatMap((b) => b.rows);
          const rows = trows.map((r) => railSetters(flowOf(page, `tf:${r.key}`), r.origin));
          const all = rows.length > 0 && rows.every((x) => x.setByDown && same(x.names, rows[0]!.names));
          if (page.keep.troutFrom) {
            folds++;
            const names = all ? rows[0]!.names : [], line = page.keep.troutFrom;
            if (!all || !names.every((x) => line.includes(x)))
              bad.push(`${at} trout fold: "${page.keep.troutFrom}" vs rows ${rows.map((x) => (x.setByDown ? x.names.join("+") : "-")).join(" | ")}`);
          } else if (rows.some((x) => x.mentions.length)) {
            if (all && !troutElsewhere(inp, trows)) bad.push(`${at} trout fold: no line, every row set by ${rows[0]!.names.join(", ")}`);
            else troutPartial++;
          }
        }
      }
    }
    console.log(`T892 sweep: ${parts} parts; lines on ${other} Other fish rows, ${groups} groups, ${folds} trout folds; `
      + `proved partial: ${otherPartial} Other fish rows, ${troutPartial} trout tables`);
    expect(parts).toBeGreaterThan(1000);
    expect(bad.slice(0, 20)).toEqual([]);
  });
});
