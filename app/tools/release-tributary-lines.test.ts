/**
 * (T892) THE TABLE SAYS WHAT THE SHEET SAYS. Over a LIVE release, every part of every water, each "What you can keep" row
 * against its own fish sheet's rail as drawn (conformance `railSetters`: the steps that set the answer, per origin):
 * - a row WITH a tributary line ("Dutch Creek’s rule for its tributaries"): the rail's setting steps are all "<water> · its
 *   tributaries", for every origin, and name exactly the line's waters, and no stored answer of it is set or lowered by
 *   another rule the rail draws as deciding (`storedElsewhere`) (an Other fish row against `other:oN`; the trout
 *   fold against every trout row's `tf:` sheet);
 * - a row WITHOUT one whose rail still names a deciding tributary step: proved partial — the rail shows another rule
 *   setting some origin's answer, or (an Other fish row, whose rail merges its fish) the stored answers show some fish ×
 *   origin set by a rule that is not a tributary one (its winner, or a rule that lowered its number: `storedElsewhere`).
 *   Anything else fails (e.g. a line lost to an empty entry name).
 * - (T892, the trout rows) each row of the trout table against its own `tf:` sheet's rail, the same two ways: a row WITH
 *   Python's line (`TRow.down`) — its rail's setting steps, for the origins the row covers, are all tributary ones naming
 *   exactly the line's waters, the line is the Other fish rows' words for them (`downLine`), and no answer of the row is
 *   set or lowered by another rule the rail draws; a row WITHOUT one whose rail names a deciding tributary step — proved
 *   partial, else it fails (a line Python missed). And a trout fold with a line ⇒ every row carries the same line.
 *   A release from before T892 carries no row line: it is named in `BEFORE_T892` (and must then carry none), and its
 *   missing row lines are counted, not failed. (T907) Each row's line is read from the payload (`TRow.down`, no longer
 *   drawn); a row with a line carries its tag (`TRow.tag`, drawn beside the name) naming the same waters ("Trout Lake’s
 *   tributaries"), and a row without one none — except in a release cut before T907 (`BEFORE_T907`), whose rows carry
 *   no tag at all.
 *
 * DATA, so Tier 2 (`scripts/test-full.sh` runs it with TIER2=1):
 *   RELEASE_DIR=data/release/r-… TIER2=1 pnpm -C app exec vitest run release-tributary-lines
 * Under TIER2 a missing release FAILS; in `pnpm check` (no data, no TIER2) it is skipped, by name.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
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
import { railKeys, railSetters, storedElsewhere } from "../conformance/src/regs/tributaryEvidence";
import { ladderOf } from "../packages/core/src/regs/decided";
import { plainRowSeams } from "../packages/core/src/regs/keep";
import { FISH } from "../packages/core/src/regs/fish.generated";

const REL = namedRelease();
/** (T892) the releases cut before trout rows carried their line (`TRow.down`): their rows say none, by construction */
const BEFORE_T892 = new Set(["r-86d43303bee2", "r-fc137c4040fa"]);
/** (T907) the releases cut before trout rows carried their tag (`TRow.tag`): their rows have none */
const BEFORE_T907 = new Set([...BEFORE_T892, "r-379e72fc9524"]);
/** (T907) Python's `words.row_tag` for these waters, in order: "Trout Lake’s tributaries", "A’s and B’s tributaries" — the
 *  line's possessives with "tributaries" (so a name ending in "Tributaries" is said once there too, round 2) */
const tagOf = (names: readonly string[]) => downLine(names)!.replace(/ (?:rule for its|rules for their) tributaries$/, " tributaries");
/** (T907 round 2) a release cut before "Drop the repeat" (owner 2026-11-19 ~12:27) says a name ending in "Tributaries"
 *  twice in its trout rows' lines ("Lake Revelstoke's Tributaries’ rule for its tributaries"): read those with the old
 *  possessive; the app's own words (`downLine`) say it once */
const poss = (n: string) => (/s$/i.test(n) ? `${n}’` : `${n}’s`);
const oldLine = (names: readonly string[]) => names.length === 1 ? `${poss(names[0]!)} rule for its tributaries`
  : `${names.slice(0, -1).map(poss).join(", ")} and ${poss(names[names.length - 1]!)} rules for their tributaries`;
const TIER2 = process.env.TIER2 === "1";
const DAY = { month: 11, day: 19 };

const flowOf = (page: RegsPage, key: string): FlowVM | null =>
  page.sheet(key as SheetKey)?.blocks.find((b): b is Extract<typeof b, { k: "flow" }> => b.k === "flow")?.flow ?? null;
const same = (a: readonly string[], b: readonly string[]) => a.length === b.length && a.every((x) => b.includes(x));

/** A rows-frame row: some fish × origin of it whose stored answer another rule sets (`flow` its sheet's rail). */
function rowElsewhere(inp: RegsInputs, ix: number, flow: FlowVM | null): boolean {
  const place = makePlace(inp), card = buildModel(inp, place, settle(inp, place));
  const row = card.rows[ix]!, f = inp.year!.rowsBySeg[card.ctx.s]!, keys = railKeys(flow);
  return row.allMembers.filter((S) => card.R[S]).some((S) =>
    [f.fish[S]?.hatchery, f.fish[S]?.wild].some((d) => !!d && storedElsewhere(d, inp.rules, keys)));
}
/** The Other fish list's i-th row's index in the rows frame. */
function listIx(inp: RegsInputs, i: number): number {
  const place = makePlace(inp), card = buildModel(inp, place, settle(inp, place));
  return listRows(card, tableCovers(inp))![i]!;
}
/** A quota group's rail: the flow its fish sheet draws (other.ts: `ladderOf` with `flow`). */
function groupFlow(inp: RegsInputs, ix: number): FlowVM | null {
  const place = makePlace(inp), settled = settle(inp, place), card = buildModel(inp, place, settled);
  return ladderOf(card.rows[ix]!, { card, settled, inp, seams: plainRowSeams(inp, FISH), flow: true }).flow ?? null;
}
/** Some trout row of the table whose stored answer (its origin's, else both) another rule sets (`flows` the rows' rails). */
function troutElsewhere(inp: RegsInputs, rows: readonly { fish: string; origin: string | null }[], flows: readonly (FlowVM | null)[]): boolean {
  const place = makePlace(inp), card = buildModel(inp, place, settle(inp, place)), f = inp.year!.rowsBySeg[card.ctx.s]!;
  return rows.some((r, k) => {
    const a = f.fish[r.fish], keys = railKeys(flows[k] ?? null);
    const ds = r.origin === "hatchery" ? [a?.hatchery] : r.origin === "wild" ? [a?.wild] : [a?.hatchery, a?.wild];
    return ds.some((d) => !!d && storedElsewhere(d, inp.rules, keys));
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
    let parts = 0, other = 0, otherPartial = 0, folds = 0, troutPartial = 0, groups = 0, groupPartial = 0;
    let rowLines = 0, rowPartial = 0;
    const missing: string[] = [];                                     // (T892) a trout row the rail sets with no line
    const id = (JSON.parse(readFileSync(join(REL!, "manifest.json"), "utf8")) as { data_release?: string }).data_release ?? "";
    const before = BEFORE_T892.has(id), untagged = BEFORE_T907.has(id);
    let tags = 0;
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
            if (!ev.setByDown || !ev.names.every((x) => down.includes(x)) || !down.every((x) => ev.mentions.includes(x)) || o.from !== downLine(down)
                || rowElsewhere(inp, listIx(inp, i), f))
              bad.push(`${at} ${o.name}: "${o.from}" vs rail setters ${ev.setByDown ? ev.names.join(", ") : "(not all tributary)"}`);
          } else if (ev.mentions.length) {
            if (ev.setByDown && !rowElsewhere(inp, listIx(inp, i), f)) bad.push(`${at} ${o.name}: no line, rail set by ${ev.names.join(", ")}`);
            else otherPartial++;
          }
        });
        // (review 3) each quota group against its own sheet's rail, as the rows
        for (const g of page.keep.groups) {
          const f = groupFlow(inp, g.row), ev = railSetters(f), down = f?.down ?? [];
          if (g.from) {
            groups++;
            if (!ev.setByDown || !ev.names.every((x) => down.includes(x)) || !down.every((x) => ev.mentions.includes(x)) || g.from !== downLine(down)
                || rowElsewhere(inp, g.row, f))
              bad.push(`${at} group ${g.title}: "${g.from}" vs rail setters ${ev.setByDown ? ev.names.join(", ") : "(not all tributary)"}`);
          } else if (ev.mentions.length) {
            if (ev.setByDown && !rowElsewhere(inp, g.row, f)) bad.push(`${at} group ${g.title}: no line, rail set by ${ev.names.join(", ")}`);
            else groupPartial++;
          }
        }
        const t = page.keep.trout?.kind === "table" ? page.keep.trout : null;
        if (t) {
          // (T907) the payload's rows, in the table's order: the line (`down`) is no longer on the drawn row
          const trows = (inp.trout?.table.boxes ?? []).flatMap((b) => b.rows);
          expect(trows.map((r) => r.key)).toEqual(t.boxes.flatMap((b) => b.rows).map((r) => r.key));
          const rows = trows.map((r) => railSetters(flowOf(page, `tf:${r.key}`), r.origin));
          // (T892) each trout row's own line (Python's) against its own sheet's rail
          trows.forEach((r, k) => {
            const f = flowOf(page, `tf:${r.key}`), ev = rows[k]!, down = f?.down ?? [];
            if (r.tag) tags++;
            if (r.down && !untagged && r.tag !== tagOf(down))
              bad.push(`${at} trout row ${r.key}: line "${r.down}" but tag ${JSON.stringify(r.tag ?? null)}`);
            if (!r.down && r.tag) bad.push(`${at} trout row ${r.key}: tag "${r.tag}" without its line`);
            if (r.down) {
              rowLines++;
              if (!ev.setByDown || !ev.names.every((x) => down.includes(x)) || !down.every((x) => ev.mentions.includes(x))
                  || r.down !== (untagged ? oldLine(down) : downLine(down)) || troutElsewhere(inp, [r], [f]))
                bad.push(`${at} trout row ${r.key}: "${r.down}" vs rail setters ${ev.setByDown ? ev.names.join(", ") : "(not all tributary)"}`);
            } else if (ev.mentions.length) {
              if (ev.setByDown && !troutElsewhere(inp, [r], [f])) missing.push(`${at} trout row ${r.key}: no line, rail set by ${ev.names.join(", ")}`);
              else rowPartial++;
            }
          });
          if (page.keep.troutFrom && !before && !trows.every((r) => r.down === page.keep.troutFrom))
            bad.push(`${at} trout fold: "${page.keep.troutFrom}" but rows say ${trows.map((r) => r.down ?? "-").join(" | ")}`);
          const all = rows.length > 0 && rows.every((x) => x.setByDown && same(x.names, rows[0]!.names));
          if (page.keep.troutFrom) {
            folds++;
            const names = all ? rows[0]!.names : [], line = page.keep.troutFrom;
            if (!all || !names.every((x) => line.includes(x)) || troutElsewhere(inp, trows, trows.map((r) => flowOf(page, `tf:${r.key}`))))
              bad.push(`${at} trout fold: "${page.keep.troutFrom}" vs rows ${rows.map((x) => (x.setByDown ? x.names.join("+") : "-")).join(" | ")}`);
          } else if (rows.some((x) => x.mentions.length)) {
            if (all && !troutElsewhere(inp, trows, trows.map((r) => flowOf(page, `tf:${r.key}`)))) bad.push(`${at} trout fold: no line, every row set by ${rows[0]!.names.join(", ")}`);
            else troutPartial++;
          }
        }
      }
    }
    console.log(`T892 sweep: ${parts} parts; lines on ${other} Other fish rows, ${groups} groups, ${folds} trout folds, `
      + `${rowLines} trout rows; proved partial: ${otherPartial} Other fish rows, ${groupPartial} groups, ${troutPartial} trout `
      + `tables, ${rowPartial} trout rows; ${tags} trout row tags`);
    if (untagged) expect(tags, `${id} is named in BEFORE_T907 but its trout rows carry tags`).toBe(0);
    if (before) {
      // a release named as cut before T892: its rows carry no line (else the name is stale), its missing ones are counted
      console.log(`T892 sweep: ${id} predates the trout rows' line; ${missing.length} rows its rails set by tributary rules say none`);
      expect(rowLines, `${id} is named in BEFORE_T892 but its trout rows carry lines`).toBe(0);
    } else bad.push(...missing);
    expect(parts).toBeGreaterThan(1000);
    expect(bad.slice(0, 20)).toEqual([]);
  });
});
