/**
 * T800 on the LIVE release: an angler closure that holds on some days of the week only (the Skeena's Saturday and
 * Sunday zones for non-guided anglers from outside Canada, p.52; the Zymoetz's Friday to Sunday; the Kitsumkalum's
 * Sunday) is said on the days it does NOT hold as an "At certain times" line. That line is the rule's own sentence, the
 * same words the "some anglers" note shows on the days it holds: who is closed out, when, and where. It was the printed
 * text cut at its first bracket, which named nobody ("…, July 1 - Dec 31, · Sat, Sun") or no zone.
 * Skipped, by name, when this machine has no release.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage, plain, type RegsInputs } from "@app/core";
import { loadRegsInputs, openRegsStore, type RegsStore } from "@app/data/regs";
import { sectionNames, type SectionNames } from "@app/data/release/node";
import { liveReleaseAt, openReleaseDir } from "../fixtureRelease";

const at = liveReleaseAt();

it(`T800 runs against the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T800 skipped: ${at.reason}`);
});

describe.skipIf(!at.dir)(`T800: a some-days angler closure reads the same on the days it rests (${at.dir ?? at.reason})`, () => {
  let st: RegsStore, names: SectionNames;
  beforeAll(async () => { const loader = openReleaseDir(at.dir!); st = await openRegsStore(loader); names = sectionNames(loader); });
  type Note = { what: string; where: string | null; rule: string | null };
  const notes = async (item: string, partIx: number, month: number, day: number, weekday: RegsInputs["weekday"], kind: "angler" | "timed"): Promise<Note[]> => {
    const inp = await loadRegsInputs(st, { kind: "part", item, partIx }, { month, day }, weekday, 2026);
    return buildRegsPage(inp, { today: { month, day }, who: { age: "adult", residency: "resident" } }).keep.caveats
      .filter((c) => c.kind === kind).flatMap((c) => c.items.map((i) => ({ what: plain(i.what), where: i.where, rule: i.rule })));
  };
  // 2026: Aug 12 is a Wednesday, Aug 15 a Saturday
  const both = async (item: string, partIx: number) => [
    (await notes(item, partIx, 8, 12, "Wednesday", "timed")).map((n) => [n.what, n.where]),
    (await notes(item, partIx, 8, 15, "Saturday", "angler")).map((n) => n.what)];

  it("Skeena Section 4, zone (b), Chimdemash Creek to 1.5 km above the Zymoetz (p.52): Wednesday's line says who and where", async () => {
    // (T801 item 4, d0df51fd0; first on the 0.8.0 release r-c7c6b2422c65) the cut's place name reads as the book prints
    // it: "1.5 km upstream of the Zymoetz River confluence", no longer "the Zymoetz River confluence (1500 m upstream)"
    const S = "Angling closed to non-guided non-resident aliens, Jul 1-Dec 31, on Saturdays and Sundays — between the Chimdemash Creek confluence and 1.5 km upstream of the Zymoetz River confluence";
    const part = await names.part("gnis:2936", "skeena_river__zymoetz_river_into_skeena_river_u1500m", "skeena_river__chimdemash_creek_into_skeena_river");
    expect(await both("gnis:2936", part)).toEqual([[[S, "Sat, Sun"]], [S]]);
  });

  it("Skeena Section 4, zone (a), Sedan Creek to the Shegunia River (p.52): Wednesday's line names the zone", async () => {
    const S = "Angling closed to non-guided non-resident aliens, Jul 1-Dec 31, on Saturdays and Sundays — between the Shegunia River confluence and the Sedan Creek confluence";
    const part = await names.part("gnis:2936", "skeena_river__sedan_creek_into_skeena_river", "skeena_river__mill_creek_into_skeena_river");
    expect(await both("gnis:2936", part)).toEqual([[[S, "Sat, Sun"]], [S]]);
  });

  it("Suskwa River (p.52): Wednesday's line is the Saturday note's sentence", async () => {
    const S = "Angling closed to non-guided non-resident aliens, on Saturdays and Sundays";
    expect(await both("gnis:2637", await names.part("gnis:2637", "mouth", "source"))).toEqual([[[S, "Sat, Sun"]], [S]]);
  });

  it("province-wide: every weekday line of a some-days angler closure is, word for word, its note on a day it holds", async () => {
    const ords = (await (await st.water()).all("SELECT item_ord FROM item WHERE shown = 1")).map((r) => Number(r.item_ord));
    // two Wednesdays of 2026 (summer and autumn classified seasons), each with its Friday, Saturday and Sunday
    const weeks = [[8, 12, [14, 15, 16]], [10, 14, [16, 17, 18]]] as const;
    const ends = ["Friday", "Saturday", "Sunday"] as const;
    const differs: string[] = [], waters = new Set<string>();
    let matched = 0;
    for (const ord of ords) {
      const [it2, parts] = await Promise.all([st.item(ord), st.parts(ord)]);
      for (let partIx = 0; partIx < parts.length; partIx += 1) {
        if (!parts[partIx]!.facts || parts[partIx]!.tidal) continue;
        for (const [month, wed, days] of weeks) {
          const lines = (await notes(it2.id, partIx, month, wed, "Wednesday", "timed")).filter((n) => n.rule !== null);
          if (!lines.length) continue;
          const said = new Map<string, string>();
          for (let k = 0; k < days.length; k += 1)
            for (const n of await notes(it2.id, partIx, month, days[k]!, ends[k]!, "angler")) if (n.rule !== null) said.set(n.rule, n.what);
          for (const l of lines) {
            const note = said.get(l.rule!);
            if (note === undefined) continue;   // a timed line of another kind (a closed hour, a closed day for everyone)
            matched += 1; waters.add(it2.name);
            if (l.what !== note) differs.push(`${it2.name} part ${partIx} ${month}-${wed}: "${l.what}" is not "${note}"`);
          }
        }
      }
    }
    expect(differs).toEqual([]);
    // the walk really met them (one that met none would prove nothing): the Skeena, the Suskwa and the Zymoetz at least
    expect(matched).toBeGreaterThan(100);
    expect(["Skeena River", "Suskwa River", "Zymoetz River"].filter((w) => !waters.has(w))).toEqual([]);
  }, 600_000);
});
