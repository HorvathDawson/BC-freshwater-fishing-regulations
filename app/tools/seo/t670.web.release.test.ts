/**
 * (T672, after T670) THE WEB PAGE SAYS WHAT THE APP'S PAGE SAYS, for the lines T670 put on the screen — on the release
 * under test, for the waters `conformance/src/regs/t670.release.test.ts` names (each part by its ends, never a number).
 *
 * The visible water page is the app's own components prerendered (pane.tsx), so it draws them by construction; the
 * site's TEXT MODEL (`model.ts` `partModel`: the hubs, the content hash, /changes) selects presenter strings by hand,
 * and a field it does not select is a regulation the site's model does not know — "Whitefish · 15 a day" with no
 * "Release every mountain whitefish." Two checks:
 *   1. each finding's pinned line is in the model of its water's part, whole;
 *   2. THE GUARD: every string the page's view model carries in the fields T670 added or filled (possession `also`,
 *      a row's `back`, the species key, the boats line, a group's not-kept lines, caveat items, the gear lines' values,
 *      the Methods boats list) is in the model. A new field the presenter fills and the model forgets fails here.
 * And the prerendered pane itself is asked for the lines of the waters whose first part carries them.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { plain, type RegsPage } from "@app/core";
import { openRegsStore, type RegsStore } from "@app/data/regs";
import { openReleaseDir, sectionNames, type SectionNames } from "@app/data/release/node";
import { liveRelease } from "../../conformance/src/fixtureRelease";
import { buildAll } from "./build";
import { appPage, buildDayOf, partModel, type BuildDay } from "./model";

const LIVE = liveRelease();
const on = (iso: string): BuildDay => buildDayOf(new Date(`${iso}T12:00:00`));
const JUL15 = on("2026-07-15");

/** every sentence the site's model holds for the page */
const said = (page: RegsPage): string[] => partModel(page).sections.flatMap((s) => [s.title, ...s.items.map((i) => i.text)]);
const squash = (s: string) => s.replace(/\s+/g, " ").trim();

/** every string the view model carries in the fields the app draws for T670's findings */
function carried(page: RegsPage): string[] {
  const k = page.keep, out: string[] = [];
  out.push(...(k.possession?.also ?? []));
  for (const o of k.otherFish ?? []) out.push(o.name, o.value, ...o.back.map((b) => b.says));
  out.push(...k.otherFishKey.map((x) => x.says));
  out.push(...(page.boats ?? []).map((b) => b.text));
  for (const g of k.groups) for (const n of g.notKept) out.push(n.says);
  for (const c of k.caveats) for (const i of [...c.items, ...(c.later ?? [])]) out.push(plain(i.what), i.where ?? "", ...(i.detail ?? []));
  for (const l of page.gear?.lines ?? []) out.push(...l.values);
  out.push(...(page.methods?.boats?.items ?? []));
  for (const b of page.licence?.blocks ?? []) if (b.cls === "needs") for (const i of [...b.items, ...(b.onlyIf?.items ?? [])]) out.push(...i.book);
  return out.map(squash).filter(Boolean);
}

describe.skipIf(!LIVE)(`T672: the site's model carries what T670 put on the app's page (${LIVE ?? "no live release"})`, () => {
  let st: RegsStore, names: SectionNames;
  beforeAll(async () => { const loader = openReleaseDir(LIVE!); st = await openRegsStore(loader); names = sectionNames(loader); });
  /** [finding, water, the part (by its ends; null: the water's first), the day, the lines the model must hold] */
  const CASES: readonly [string, string, readonly [string, string] | null, BuildDay, readonly string[]][] = [
    ["K1 Charlie Lake", "wbk:329264876", null, JUL15, ["Have no more than 3 walleye in possession. Fish at home don’t count.",
      "Have no more than 3 northern pike in possession. Fish at home don’t count."]],
    ["K2 Peace River", "gnis:14619", ["peace_river__site_c_dam", "peace_river__halfway_river_into_peace_river_d5000m"], JUL15,
      ["Whitefish · 15 a day", "Release every mountain whitefish."]],
    ["K3 Shuswap Lake", "wbk:329518145", null, on("2026-11-11"), ["Release every lake trout, Oct 15–Jan 31."]],
    ["K4 Bruer Creek", "gnis:10061", ["mouth", "source"], JUL15, ["Brook trout · 20 a day"]],
    ["K5 Mat Creek", "gnis:14254", ["mouth", "source"], on("2026-01-15"), ["Dolly Varden/bull trout · 2 a day"]],
    ["G1 Kispiox River", "gnis:2308", null, on("2026-08-15"), ["No angling (from a boat)", "No powered boats"]],
    ["G3 Mahood Lake", "wbk:329354708", null, JUL15, ["Bait ban · Jul 1–Dec 31", "Single barbless hook · Jul 1–Dec 31",
      "Where: Within fishing boundary signs at the western tip of the lake"]],
    ["G4 Qualicum River", "gnis:27804", ["qualicum_river__upper_hatchery_weir_125_m_downstream", "source"], JUL15,
      ["Hooks: One hook, lure or fly, Single barbless hook, no trebles, Single barbless hook; no hook more than 15 mm from point to shank"]],
    // (T670 G7, RULINGS Li13) a classified water's licence terms from p.7, in the book's words (the model's angler is the adult resident)
    ["G7 Wigwam River", "gnis:27703", ["mouth", "wigwam_river__wigwam_km42_ram_fsr"], JUL15,
      ["If you are a B.C. resident: Your Classified Waters Licence is valid"]],
    // (T670, owner 2026-11-12 "Show the fee only") the basic licence's disabled-resident fee sentence, never the one-licence line
    ["G8 basic licence fee", "gnis:16880", null, JUL15,
      ["If you are a resident of B.C. and disabled: You may qualify for a basic freshwater angling licence at the reduced fee of $1.14."]],
  ];
  const pageOf = async (item: string, ends: readonly [string, string] | null, day: BuildDay) =>
    appPage(st, item, ends ? await names.part(item, ends[0], ends[1]) : 0, day);

  it.each(CASES)("%s: the pinned line is in the model, whole", async (_name, item, ends, day, lines) => {
    const got = said(await pageOf(item, ends, day));
    // a line is an item of the model, or (K1: the possession sentence says the limits one after the other) whole inside one
    expect(lines.filter((l) => !got.some((g) => g.includes(l)))).toEqual([]);
  });

  it("the guard: no string the view model carries in those fields is missing from the model, on any of the waters", async () => {
    const missing: string[] = [];
    let asked = 0;
    for (const [name, item, ends, day] of CASES) {
      const page = await pageOf(item, ends, day), text = said(page).map(squash).join("\n");
      for (const s of carried(page)) { asked++; if (!text.includes(s)) missing.push(`${name}: ${s}`); }
    }
    expect(missing).toEqual([]);
    expect(asked).toBeGreaterThan(40);
  });

  it("the prerendered pane (the visible page) draws them, for the waters whose first part carries the line", async () => {
    const first = CASES.filter(([, , ends, day]) => ends === null && day === JUL15);
    const built = await buildAll({ loader: openReleaseDir(LIVE!), on: JUL15, only: new Set(first.map(([, item]) => item)), gauges: null });
    const text = (html: string) => squash(html.replace(/<style[\s\S]*?<\/style>|<script[\s\S]*?<\/script>/g, " ").replace(/<[^>]+>/g, " ")
      .replace(/&#x27;|&#39;/g, "'").replace(/&amp;/g, "&"));
    const missing: string[] = [];
    for (const [name, item, , , lines] of first) {
      const path = `/water/${built.build.slugMap.waters[item]!.slug}`;
      const html = text(built.build.pages.find((p) => p.path === path)!.html);
      for (const l of lines) if (!html.includes(l.replace(/^Where: /, ""))) missing.push(`${name}: ${l}`);
    }
    expect(missing).toEqual([]);
    expect(first.length).toBeGreaterThan(1);
  }, 240_000);
});
