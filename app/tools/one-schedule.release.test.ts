/**
 * T1207 (owner 2026-11-26, "One block "Salmon and game fish" with one year bar") ON THE LIVE RELEASE: the page the Keep tab
 * draws is ONE status block exactly where `closedWithSalmon`'s gate holds (core sets `banner.schedule`), two otherwise.
 * Nelson Creek (`gnis:20408`, the book's "No Fishing", all year) is one; Kirkup Creek (`gnis:12661`, two closures, the salmon's
 * only part of the year) and Old Kootenay River Channel (`gnis:2123`, salmon closed all year, game fish open in summer) stay two.
 * Same pages as the app builds (`loadRegsInputs`, `buildRegsPage`, `salmonCardFor`), on Nov 26, 2026. No release on the machine: reported, as the
 * other release-backed tests do (`liveReleaseAt`), never a silent pass.
 */
import { describe, expect, it } from "vitest";
import { buildRegsPage, closeForFinfish, closedWithSalmon, salmonWithPdf, tapDay } from "@app/core";
import { loadRegsInputs, openRegsStore } from "@app/data/regs";
import { DEFAULT_WHO, pageSubjectOf, salmonCardFor } from "@app/ui";
import { liveReleaseAt, openReleaseDir } from "../conformance/src/fixtureRelease";

const at = liveReleaseAt();
it(`T1207 one schedule on the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T1207 skipped: ${at.reason}`);
});

async function schedule(item: string, partIx: number) {
  const loader = openReleaseDir(at.dir!);
  const store = await openRegsStore(loader);
  const day = { month: 11, day: 26 };
  const inputs = await loadRegsInputs(store, { kind: "part", item, partIx }, day, tapDay(day, 2026).weekday, 2026);
  const regs = buildRegsPage(inputs, { today: { ...day, year: 2026 }, who: DEFAULT_WHO, downstream: null, names: null, places: null });
  const card0 = await salmonCardFor(loader, store, { subject: pageSubjectOf(inputs)!, akey: inputs.key?.akey ?? null }, new Date(2026, 10, 26, 12));
  const card = card0 ? salmonWithPdf(card0, inputs.statics.pdfPages) : null;
  return closedWithSalmon(card0 ? closeForFinfish(regs, card0.finfish) : regs, card, inputs.rules).keep.banner?.schedule;
}

describe.skipIf(!at.dir)("(T1207) the live release: one status block for both fish, or two", () => {
  it("Nelson Creek: one block, \"Salmon and game fish\", tagged with the rule book", async () => {
    expect(await schedule("gnis:20408", 0)).toEqual({ title: "Salmon and game fish", tag: "B.C. Synopsis", label: "Salmon and game fish: closed all year" });
  }, 120_000);
  it("Kirkup Creek (two closures, the salmon's part-year) and Old Kootenay River Channel (game fish open): two blocks", async () => {
    expect(await schedule("gnis:12661", 0)).toBeUndefined();
    expect(await schedule("gnis:2123", 0)).toBeUndefined();
  }, 120_000);
});
