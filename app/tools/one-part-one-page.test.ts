/**
 * T387 on the LIVE release (skipped with the reason when this machine has none; a failure under REGS_STORE_REQUIRED=1):
 * EVERY TAP ON ONE PART OF A WATER GIVES THE SAME REGULATIONS PAGE (owner 2026-11-04: "in the one section not all clicks
 * are equal"). For every part with more than one section, the page a tap on each of its sections opens — the way the
 * water screen opens it: `targetFor` → `loadRegsInputs` → core's page and every sheet it can open, the access notices
 * (`regsAccessNotices`) and the Salmon card (`salmonCardFor`) asked by the page's subject (`pageSubjectOf`) — is equal,
 * minus the POINT-BOUND facts, which are not built here (the ticket lists them, the owner decides):
 *
 *   the sun line   sunrise / sunset at the tapped point (`useSunLine` reads the frame's tap, else the water's point)
 *   scroll memory  where the reader left the page, keyed by the water as opened (`regsScrollKey`) — not a fact
 *
 * COVERAGE: (1) every section of every multi-section part opens its part (`targetFor`) — the one way a tap enters the
 * page's inputs; (2) the WHOLE page, sheets included, built afresh from a tap on one section of EACH distinct per-section
 * signature of the part (paint's and the access file's access rows, the salmon chain, the day's bait / fly class, the
 * section's answer key) and its first and last section — for every part whose sections differ in any of those, plus
 * every `SAMPLE`-th other part (~10,000 pages, ~45 s). `T387_EVERY=1` builds the page from every section of every part
 * (~51,000 pages, ~4 min); a tap enters the page only through its target, which (1) proves for all of them anyway. MUTATION PIN: the T54 read (access and
 * salmon asked by the tapped section) fails the comparison, on Nicomen Slough first.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage, methodClassOn, type RegsPage } from "@app/core";
import { loadRegsInputs, openRegsStore, targetFor, type RegsStore } from "@app/data/regs";
import type { ReleaseLoader } from "@app/data/release";
import { pageSubjectOf, regsAccessNotices, salmonCardFor, type PageSubject } from "@app/ui";
import { liveReleaseAt, openReleaseDir } from "../conformance/src/fixtureRelease";
import { freezePage, sheetKeysOf, stableJson } from "../conformance/src/regs/fixtures";

const at = liveReleaseAt();
const DAY = { month: 10, day: 22 } as const;
const ON = new Date(2026, 9, 22, 12);
const ADULT_RESIDENT = { age: "adult", residency: "resident" } as const;
/** Every SAMPLE-th part whose sections do not differ in any per-section table is built whole too. */
const SAMPLE = 25;
/** `T387_EVERY=1`: build the page from EVERY section of EVERY multi-section part (~51,000 pages, ~4 min). */
const EVERY = !!process.env.T387_EVERY && process.env.T387_EVERY !== "0";
/** Nicomen Slough (a reserve and private land on 4 of its sections): the ticket's case. One part of 9 sections until
 * r-483b295de57c; the two DFO cuts there (Hwy 7 bridge at Dewdney, Siddle Creek) make it 3 parts, so the access line
 * of its first multi-section part (8 sections) speaks of "this part of the water". */
const NICOMEN = "gnis:21105";

it(`T387 one part, one page on the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T387 skipped: ${at.reason}`);
});

interface Part { item: string; ord: number; partIx: number; sids: number[] }

describe.skipIf(!at.dir)("(T387) every tap on a part gives the same page", () => {
  let loader: ReleaseLoader, st: RegsStore, parts: Part[];
  beforeAll(async () => {
    loader = openReleaseDir(at.dir!);
    st = await openRegsStore(loader);
    const items = new Map((await (await loader.db("core")).all("SELECT ord, item_id FROM item"))
      .map((r) => [Number(r.ord), String(r.item_id)]));
    const by = new Map<string, Part>();
    for (const r of await (await loader.db("water")).all("SELECT ord, part_ix, sid FROM part_section ORDER BY ord, part_ix, sid")) {
      const ord = Number(r.ord), partIx = Number(r.part_ix), k = `${ord}:${partIx}`;
      let p = by.get(k);
      if (!p) by.set(k, p = { item: items.get(ord)!, ord, partIx, sids: [] });
      p.sids.push(Number(r.sid));
    }
    parts = [...by.values()].filter((p) => p.sids.length > 1);
  }, 120_000);

  /**
   * THE PAGE A TAP ON `sid` OPENS, as one string: the water screen's reads in its order. `leak` is the mutation: the T54
   * read, access and the Salmon card asked by the tapped section instead of the page's subject.
   */
  async function pageOfTap(p: Part, sid: number, leak = false): Promise<string> {
    const target = await targetFor(st, p.item, sid);
    const inputs = await loadRegsInputs(st, target, DAY, "Thursday", 2026);
    const page = buildRegsPage(inputs, { today: DAY, who: ADULT_RESIDENT });
    const subject: PageSubject = leak ? { kind: "section", sid } : pageSubjectOf(inputs)!;
    const [access, salmon] = await Promise.all([
      regsAccessNotices(loader, st, subject),
      // a card that fails is part of the page too (the app says so in its place, with Try again): compared as its reason
      // (the live release r-ac5a4e4f734c predates T372's in-force days `n`: every water with an undrawn DFO place fails)
      salmonCardFor(loader, st, { subject, akey: inputs.key?.akey ?? null }, ON)
        .catch((e: unknown) => ({ failed: (e as Error).message }))]);
    return stableJson({ page: freezePage(page, sheetKeysOf(page)), access, salmon });
  }

  /**
   * The taps a part's page is built from: one section of each distinct per-section signature (paint's and the access
   * file's rows, the salmon chain, the day's bait / fly class, the section's answer key) — every way the part's sections
   * differ in a table the page could read — and always its first and last section. More than one signature: it differs.
   */
  async function tapsOf(p: Part): Promise<{ sids: number[]; differs: boolean }> {
    const [paint, gentle, salmon, method] = await Promise.all([loader.paint(), loader.access(), loader.salmon(), loader.method()]);
    const one = new Map<string, number>();
    for (const sid of p.sids) {
      const sig = JSON.stringify([paint.access.of(sid), gentle?.of(sid) ?? [], salmon?.chainOf(sid) ?? null,
        method ? methodClassOn(method, sid, ON) : null, await st.sectionAkey(sid)]);
      if (!one.has(sig)) one.set(sig, sid);
    }
    return { sids: [...new Set([p.sids[0]!, ...one.values(), p.sids[p.sids.length - 1]!])], differs: one.size > 1 };
  }

  it("(1) every section of every multi-section part opens its own part", async () => {
    const off: string[] = [];
    for (const p of parts) for (const sid of p.sids) {
      const t = await targetFor(st, p.item, sid);
      if (t.kind !== "part" || t.item !== p.item || t.partIx !== p.partIx) off.push(`${p.item} part ${p.partIx}: section ${sid} → ${JSON.stringify(t)}`);
    }
    expect(off).toEqual([]);
    expect(parts.length).toBeGreaterThan(1000);   // a floor, not a count: 6,816 on r-ac5a4e4f734c — the net ran on parts
  }, 600_000);

  it("(2) the whole page — sheets, access, the Salmon card — is the same from each kind of section", async () => {
    const unequal: string[] = [];
    let differ = 0, sampled = 0, built = 0, sections = 0;
    for (const [i, p] of parts.entries()) {
      const taps = EVERY ? { sids: p.sids, differs: (await tapsOf(p)).differs } : await tapsOf(p);
      if (!EVERY && !taps.differs && i % SAMPLE !== 0) continue;
      if (taps.differs) differ++; else sampled++;
      sections += p.sids.length;
      const pages = new Set<string>();
      for (const sid of taps.sids) { pages.add(await pageOfTap(p, sid)); built++; }
      if (pages.size > 1) unequal.push(`${p.item} part ${p.partIx}: ${pages.size} pages over ${taps.sids.length} taps`);
    }
    console.log(`T387: ${parts.length} multi-section parts; ${differ} differ in a per-section table, ${sampled} more `
      + `${EVERY ? "(every part)" : `sampled (every ${SAMPLE}th)`}; ${built} pages built, standing for ${sections} sections`);
    expect(unequal).toEqual([]);
    expect(differ).toBeGreaterThan(1000);   // a floor, not a count: 2,630 on r-ac5a4e4f734c — the parts that differ were found
  }, 3_600_000);

  it("MUTATION PIN: asked by the tapped section (the T54 read), Nicomen Slough's taps give different pages", async () => {
    const p = parts.find((x) => x.item === NICOMEN);
    expect(p?.sids.length).toBeGreaterThan(1);
    const honest = new Set<string>(), leaked = new Set<string>();
    for (const sid of p!.sids) { honest.add(await pageOfTap(p!, sid)); leaked.add(await pageOfTap(p!, sid, true)); }
    expect([honest.size, leaked.size > 1]).toEqual([1, true]);
    // and the part's page names the reserve the ticket found on 4 of its 9 sections
    const access = (JSON.parse([...honest][0]!) as { access: { text: string }[] }).access.map((n) => n.text);
    expect(access).toHaveLength(1);
    // each with its share once the release ships section lengths (`part_section.m`, the 0.5.0 rebuild); none before
    const share = "(?:all|under 1%|about \\d+%) of it";
    expect(access[0]).toMatch(new RegExp(
      `^This part of the water crosses an Indian reserve \\(Skumalasph 16(?:, ${share})?\\) and private land(?: \\(${share}\\))?\\.$`));
    console.log(`T387 Nicomen Slough: ${access[0]}`);
  }, 120_000);
});
