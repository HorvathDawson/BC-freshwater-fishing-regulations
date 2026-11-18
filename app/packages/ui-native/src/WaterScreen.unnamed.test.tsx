/**
 * T53 — an unnamed stream has regulations. The water screen with no item and the tapped section draws the Regulations
 * page the STORE answers for that section (`RegsTarget` kind "section": its region's base plus whatever override reaches
 * it), on the committed regs fixture release. Every line checked here is read from the store at test time — the page
 * core builds from the same store's inputs, the rule keys from the store's own rule set — never a hand copy.
 *
 * The ticket's own examples (handles 1408, 64035, 59506) are not in the committed fixture (it was cut for the golden
 * states); they run on the live release in conformance/src/regs/unnamed.release.test.ts. Here: the fixture's first
 * unnamed stream that a TRIBUTARY rule of a named water closes all year (`tribClosedUnnamed`) and its first unnamed
 * section answered by its region's base alone (`baseOnlyUnnamed`). Both are chosen by that property: the fixture keeps
 * taps.json's evenly spaced unnamed sample, whose members move with every build — T56 / r-2f33edfe8452 took 817454 out
 * of it for 817700, and r-46e4355a7277 took out stream 354088147:0 (a Harris Creek tributary), which this file named by
 * its stream key until then; the re-cut fixture holds a Dean River tributary instead.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import { buildRegsPage, plain, WORDS, type RegsPage } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, WaterSource, SectionId } from "@app/data";
import { BLOCK_BITS, loadRegsInputs, type RegsStore } from "@app/data/regs";
import { openRegsFixture, openRegsFixtureWithLines, regsFixtureUnnamedTaps, sectionNames } from "@app/data/release/node";
import { DEFAULT_WHO, LayoutWidthContext, loadRegsStore } from "@app/ui";
import { WaterScreen } from "./WaterScreen";
import { LIGHT } from "./theme";

afterEach(cleanup);

const DAY = new Date(2026, 9, 10, 12);   // Sat Oct 10
/** The fixture source, with flow pointers for the tapped reach (`TRIB`, below): it → HOP (unnamed) → END (a named water). */
const NAMES = sectionNames(openRegsFixtureWithLines());
// (T667) the two sections downstream of it are the fixture's own, by name: an unnamed hop (Kootenay Lake's Upper West Arm,
// a lake section) and the named water the trace ends in (its Lower West Arm)
const HOP = await NAMES.lake("wbk:-21", 0), END = await NAMES.lake("wbk:-22", 0);
function sourceFlowing(): WaterSource {
  const base = makeFixtureSource();
  return { ...base,
    traceToGauge: async (from: SectionId) => (from === TRIB ? [TRIB, HOP, END] as SectionId[] : [from]),
    waterFor: async (s: SectionId) => (s === END ? { item: "gnis:9" as ItemId, name: DOWN, kind: "stream" } : base.waterFor(s)),
  };
}

/** The page core builds from the store for the section, on the same day, with the same downstream name. */
async function storePage(handle: number, downstream: string | null): Promise<{ page: RegsPage; trib: string[]; reach: string[] }> {
  const st: RegsStore = await loadRegsStore(openRegsFixture());
  const inp = await loadRegsInputs(st, { kind: "section", handle }, { month: 10, day: 10 }, "Saturday", 2026);
  const key = (r: number) => { const e = inp.rules.get(r)!.export; return `${e.entry_id}::${e.rule_id}`; };
  return { page: buildRegsPage(inp, { today: { month: 10, day: 10 }, who: DEFAULT_WHO, downstream }),
           trib: inp.ruleset!.trib.map(key), reach: inp.ruleset!.reach.map(key) };
}

/** The fixture's unnamed sections (a handle with a key that no water's part holds), ascending; the first whose store
 *  rule set is its region's base alone — no tributary rule, every reach rule a zone's (`z…:`, the region's or the
 *  province's `zp:`). */
async function baseOnlyUnnamed(): Promise<number> {
  const st: RegsStore = await loadRegsStore(openRegsFixture());
  const named = new Set((await (await st.water()).all("SELECT sid FROM part_section")).map((r) => Number(r.sid)));
  const handles: number[] = [];
  for (const r of await (await st.sid()).all("SELECT block, b FROM section_akey ORDER BY block")) {
    const b = r.b as Uint8Array, dv = new DataView(b.buffer, b.byteOffset, b.byteLength);
    for (let i = 0; i < b.byteLength / 2; i++) if (dv.getUint16(2 * i, true)) handles.push((Number(r.block) << BLOCK_BITS) + i);
  }
  for (const h of handles.filter((x) => !named.has(x))) {
    const { trib, reach } = await storePage(h, null);
    if (trib.length === 0 && reach.every((k) => /^z[^:]*:/.test(k))) return h;
  }
  throw new Error("the regs fixture holds no unnamed section answered by its region's base alone");
}

/** The fixture's first unnamed stream (ascending) that a tributary rule closes all year: its banner says so and every
 *  rule of the banner's chain is one of the section's own tributary rules. */
async function tribClosedUnnamed(): Promise<number> {
  for (const h of await NAMES.unnamedStreams()) {
    const { page, trib } = await storePage(h, null);
    const b = page.keep.banner;
    if (b && b.big.join("|") === "Closed all year" && b.chain.length > 0 && b.chain.every((c) => trib.includes(c.rule))) return h;
  }
  throw new Error("the regs fixture holds no unnamed stream that a tributary rule closes all year");
}
/** the tapped reach, by what it IS (never by an id or a stream key: the fixture's sample moves) */
const TRIB = await tribClosedUnnamed();

/** (T768) The named water that OWNS rule `key`: its entry, as the store holds it (`entry.j`), is one water's row of the
 *  book (`kind` "water") and names it; a zone's or an area's rule (the region's, the province's) has no such water, and
 *  throws. (0.8.0 re-cut) It was "the one shown ITEM whose `entries` hold the entry", which asks whether the FIXTURE
 *  holds that water: it holds the golden waters and taps.json's unnamed sample, not every water whose tributary rule
 *  reaches a sampled stream. The sample moved, and the closed stream it holds now is a Cheekye River tributary
 *  (r2:cheekye_river@2-7, "Incl. Tribs"): the Cheekye is no golden water, so 0 items held its entry, though the store
 *  holds the entry and the entry names its water. */
async function ownerOf(key: string): Promise<string> {
  const st = await loadRegsStore(openRegsFixture());
  const r = await (await st.water()).get("SELECT j FROM entry WHERE id = ?", key.split("::")[0]!);
  if (r === undefined) throw new Error(`rule ${key}: the store holds no entry ${key.split("::")[0]}`);
  const e = st.val<{ kind: string; name: string }>("entry", r.j);
  if (e.kind !== "water") throw new Error(`rule ${key}: its entry is a ${e.kind}'s (${e.name}), not a named water's`);
  return e.name;
}
/** The water the tapped reach is a tributary OF: the named water whose own rule closes it (the store's tie between
 *  the rule and its water, not a name typed here). The fixture SOURCE answers the trace with this water
 *  (`sourceFlowing`), so the page says "Tributary of" it. */
const DOWN = await ownerOf((await storePage(TRIB, null)).page.keep.banner!.chain[0]!.rule);

async function open(handle: number, source: WaterSource = makeFixtureSource(), width: number | null = null) {
  render(<LayoutWidthContext.Provider value={width}>
           <WaterScreen source={source} item={null} section={handle as SectionId} palette={LIGHT}
                        loader={openRegsFixture()} day={DAY} />
         </LayoutWidthContext.Provider>);
  await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
  await waitFor(() => expect(within(screen.getByTestId("regs-header")).getByRole("heading").textContent).not.toBe(""));
}

describe("(T53) <WaterScreen> for an unnamed reach: the store's page for the tapped section", () => {
  it("an unnamed stream bound by tributary rules: the store's name, \"Tributary of …\", and the closure the tributary rule makes", async () => {
    const { page } = await storePage(TRIB, DOWN);
    await open(TRIB, sourceFlowing());
    await waitFor(() => expect(screen.getByTestId("regs-place").textContent).not.toBe(""));
    const header = screen.getByTestId("regs-header");
    // the header: the store's place name (core wording), the downstream water in the label slot, no Map / Parts action
    expect([within(header).getByRole("heading").textContent, screen.getByTestId("regs-place").textContent,
            screen.queryByTestId("regs-action")])
      .toEqual([page.header.name, page.header.place!.full, null]);
    // (T768) the name is the PIPELINE's for this section (the golden taps), an answer the app did not compute
    expect(page.header.name).toBe(regsFixtureUnnamedTaps().find((u) => u.handle === TRIB)!.name);
    expect(page.header.place).toEqual({ full: WORDS.TRIBUTARY_OF(DOWN), short: WORDS.TRIBUTARY_OF(DOWN) });
    // the banner, whole, as the store's page has it; its closure is one of the store's TRIBUTARY rules for this section
    const b = page.keep.banner!;
    expect(screen.getByTestId("keep-banner").textContent).toBe(
      b.big.join("") + b.paras.join("") + b.chain.map((c) => `${c.text} ${c.scope}${WORDS.SOURCE}${plain(c.book)}`).join(""));
    // (T768) every rule of the chain is the receiving water's OWN rule (the reach was picked for being closed by
    // tributary rules; that they are all the one named water's, the water the page names, is not part of the pick)
    expect(await Promise.all(b.chain.map((c) => ownerOf(c.rule)))).toEqual(b.chain.map(() => DOWN));
    // the sections of the page, as for any water — (T227) closed all year with no salmon: no Gear / Methods
    expect(within(screen.getByRole("tablist", { name: "Section" })).getAllByRole("tab").map((t) => t.textContent))
      .toEqual(page.header.tabs.filter((t) => t.tab !== "gear" && t.tab !== "methods").map((t) => t.label));
  }, 30_000);

  it("an unnamed section on the region's base alone: no override, no tributary line, every rule the region's or the province's", async () => {
    const handle = await baseOnlyUnnamed();
    const { page, trib, reach } = await storePage(handle, null);
    await open(handle);
    // (T183, 1ca8bfda: taps.json's unnamed sample is half streams) the fixture's one base-only stream (445526, Region 8)
    // lies in a park: the PROVINCE's superior closure (zp:superior_closures, "Federal / parks") closes it all year. A
    // province rule, so the page is still the base alone; the banner, whole, is the store page's, its chain that rule
    // (2026-10-27, r-8b43fb267199) the sample moves with every build: the base-only stream may lie in no park (then no
    // banner, and the strip reads open on the day). Either way the page is the store's, whole.
    const b = page.keep.banner;
    expect([within(screen.getByTestId("regs-header")).getByRole("heading").textContent,
            screen.getByTestId("regs-place").textContent, screen.queryByTestId("keep-banner")?.textContent ?? null])
      .toEqual([page.header.name, "",
                b ? b.big.join("") + b.paras.join("") + b.chain.map((c) => `${c.text} ${c.scope}${WORDS.SOURCE}${plain(c.book)}`).join("") : null]);
    // a banner here is only ever a province or zone rule's (the page is the base alone)
    if (b) expect(b.chain.every((c) => /^z[^:]*:/.test(c.rule))).toBe(true);
    expect(page.header.name).toMatch(/^Unnamed stream · Regions? \d/);
    // the reach rules are the zones of the section's own region(s) and the province's, nothing else
    const zones = new Set(["zp", ...[...page.header.name.matchAll(/\d[AB]?/g)].map((m) => `z${m[0].toLowerCase()}`)]);
    expect([page.header.place, trib, reach.filter((k) => !zones.has(k.split(":")[0]!))]).toEqual([null, [], []]);
    // the "Where you can't fish" lines, whole, as the store's page has them
    const w = page.keep.whereNot!;
    expect(screen.getByTestId("keep-wherenot").textContent)
      .toBe(w.heading + (w.scope ?? "") + w.items.map((x) => plain(x.text)).join(""));
  }, 30_000);

  it("the same reach on a desktop window: the same name and \"Tributary of …\" over the side column and the page", async () => {
    const { page } = await storePage(TRIB, DOWN);
    await open(TRIB, sourceFlowing(), 1400);
    await waitFor(() => expect(screen.getByTestId("regs-place").textContent).not.toBe(""));
    const side = screen.getByTestId("regs-side");
    expect([within(screen.getByTestId("regs-header")).getByRole("heading").textContent, screen.getByTestId("regs-place").textContent,
            screen.queryByTestId("regs-action"), !!within(screen.getByTestId("regs-columns")).getByTestId("regs-reading"),
            page.header.tabs.map((t) => within(side).queryAllByText(t.label).length > 0)])
      // (T227) closed all year with no salmon: the side list offers no Gear / Methods; (owner 2026-11-04) nor Licence
      .toEqual([page.header.name, WORDS.TRIBUTARY_OF(DOWN), null, true, [true, false, false]]);
  }, 30_000);
});
