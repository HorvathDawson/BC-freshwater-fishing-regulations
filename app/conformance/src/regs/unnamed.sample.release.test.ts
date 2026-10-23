/**
 * T53 acceptance, on the LIVE release and the bundle it was cut from (skipped with the reason when this machine has
 * neither, or the two are not one build): a SAMPLE OF UNNAMED SECTIONS IN EVERY REGION, each opened the way the water
 * screen opens it (`{kind: "section", handle}` → T09 → core's page), carries exactly the rules the bundle's reach run
 * bound to that section — the region's base (`via` reach) plus every override that reaches it (`via` trib) — and every
 * rule the page cites is one of them. The expected rule set is read from the BUNDLE (`section_ruleset` → `ruleset`), the
 * page from the RELEASE: nothing here is a copy of either.
 *
 * Per region: the first sections (by handle) bound by an override, and the first bound by the base alone. T57 adds two
 * strata, each up to `PER` per region and present somewhere in B.C.: sections NEAR AN MU LINE (stamped with more than one
 * management unit) and sections IN A LAKE'S CATCHMENT (bound by the tributary walk of a lake's row). A section on a
 * region line takes its place's name ("Unnamed stream · Regions 3 and 8").
 */
import { readFileSync, existsSync, mkdtempSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { gunzipSync } from "node:zlib";
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage } from "@app/core";
import { loadRegsInputs, openRegsStore, type RegsStore } from "@app/data/regs";
import { liveReleaseAt, openReleaseDir } from "../fixtureRelease";

const at = liveReleaseAt();
const ADULT_RESIDENT = { age: "adult", residency: "resident" } as const;
const REGIONS = ["1", "2", "3", "4", "5", "6", "7a", "7b", "8"] as const;
const PER = 3;

/** The bundle beside the release (`<root>/data/release/r-*` → `<root>/data/generated/bundle/bundle.sqlite`), when it is
 *  the build the release was cut from (the same reach run and rule ids as the release's regs-sid); else why not. */
function bundleOf(dir: string | null): { path: string; reason: null } | { path: null; reason: string } {
  if (!dir) return { path: null, reason: at.reason ?? "no release" };
  const path = resolve(dir, "../../generated/bundle/bundle.sqlite");
  if (!existsSync(path)) return { path: null, reason: `no bundle at ${path}` };
  const m = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as { files: Record<string, { url: string }> };
  const sidFile = join(mkdtempSync(join(tmpdir(), "t53-sid-")), "regs-sid.sqlite");
  writeFileSync(sidFile, gunzipSync(readFileSync(resolve(dir, "..", m.files["regs-sid"]!.url))));
  const meta = (p: string) => Object.fromEntries((new DatabaseSync(p, { readOnly: true })
    .prepare("SELECT k, v FROM meta").all() as { k: string; v: string }[]).map((r) => [r.k, r.v]));
  const [b, s] = [meta(path), meta(sidFile)];
  const off = ["reach_digest", "rule_ids_sha256", "section_handles"].filter((k) => b[k] !== s[k]);
  return off.length ? { path: null, reason: `bundle ${path} is not the release's build (${off.join(", ")} differ)` }
    : { path, reason: null };
}

const bundle = bundleOf(at.dir);

it(`T53 unnamed sample on the live release${bundle.path ? "" : ` — SKIPPED: ${bundle.reason}`}`, () => {
  if (!bundle.path) console.warn(`T53 unnamed sample skipped: ${bundle.reason}`);
});

describe.skipIf(!bundle.path)("(T53) unnamed sections in every region: the page's rules are the bundle's binding", () => {
  let st: RegsStore, db: DatabaseSync;
  beforeAll(async () => {
    st = await openRegsStore(openReleaseDir(at.dir!));
    db = new DatabaseSync(bundle.path!, { readOnly: true });
  });

  /** The bundle's binding for a section: its rule keys by `via`, and the unnamed part's kind. */
  const bound = (sid: number) => {
    const rows = db.prepare(`SELECT r.via, r.entry_id || '::' || r.rule_id AS k FROM section_ruleset s
                             JOIN ruleset r ON r.set_id = s.set_id WHERE s.sid = ? ORDER BY k`).all(sid) as { via: string; k: string }[];
    const kind = (db.prepare(`SELECT u.kind FROM section_ruleset s JOIN unnamed_part u ON u.up = s.up WHERE s.sid = ?`)
      .get(sid) as { kind: string }).kind;
    return { reach: rows.filter((r) => r.via === "reach").map((r) => r.k),
             trib: rows.filter((r) => r.via === "trib").map((r) => r.k), kind };
  };
  /** The first `PER` unnamed sections of a region (one region only) bound by an override (`trib`), or by none. */
  const sample = (region: string, withTrib: boolean) => (db.prepare(
    `SELECT s.sid FROM section_ruleset s JOIN region_place p ON p.place = s.place
     WHERE s.up IS NOT NULL AND p.regions = ?
       AND ${withTrib ? "" : "NOT "}EXISTS (SELECT 1 FROM ruleset r WHERE r.set_id = s.set_id AND r.via = 'trib')
     ORDER BY s.sid LIMIT ${PER}`).all(region) as { sid: number }[]).map((r) => Number(r.sid));
  /** T57: the first `PER` unnamed sections of a region (alone or with another) near an MU line, or in a lake's
   *  catchment (a lake row's rule reaches them by the walk). */
  const strata = (region: string, which: "mu_line" | "lake_catchment") => (db.prepare(
    `SELECT s.sid FROM section_ruleset s JOIN region_place p ON p.place = s.place
     ${which === "mu_line" ? "JOIN mu_set m ON m.mus = s.mus" : ""}
     WHERE s.up IS NOT NULL AND (',' || p.regions || ',') LIKE ?
       AND ${which === "mu_line" ? "m.codes LIKE '%,%'"
         : `EXISTS (SELECT 1 FROM ruleset r JOIN entry e ON e.entry_id = r.entry_id JOIN item i ON i.item_id = e.item_id
                    WHERE r.set_id = s.set_id AND r.via = 'trib' AND i.kind = 'lake')`}
     ORDER BY s.sid LIMIT ${PER}`).all(`%,${region},%`) as { sid: number }[]).map((r) => Number(r.sid));
  /** The place's regions as the page names them (one region, or "Regions A and B", "Regions A, B and C"). */
  const placeName = (sid: number) => {
    const rs = ((db.prepare(`SELECT p.regions FROM section_ruleset s JOIN region_place p ON p.place = s.place
                              WHERE s.sid = ?`).get(sid) as { regions: string }).regions).split(",").map((r) => r.toUpperCase());
    return rs.length === 1 ? `Region ${rs[0]}` : `Regions ${rs.slice(0, -1).join(", ")} and ${rs[rs.length - 1]}`;
  };
  it.each(REGIONS)("Region %s", async (region) => {
    const extra = [...strata(region, "mu_line"), ...strata(region, "lake_catchment")];
    const sids = [...new Set([...sample(region, true), ...sample(region, false), ...extra])];
    expect(sids.length).toBeGreaterThanOrEqual(2 * PER);
    const got = [], want = [];
    for (const sid of sids) {
      const inp = await loadRegsInputs(st, { kind: "section", handle: sid }, { month: 10, day: 22 }, "Thursday", 2026);
      const key = (r: number) => { const e = inp.rules.get(r)!.export; return `${e.entry_id}::${e.rule_id}`; };
      const page = buildRegsPage(inp, { today: { month: 10, day: 22 }, who: ADULT_RESIDENT });
      const cited = [...(page.keep.whereNot?.items ?? []).map((x) => x.rule),
                     ...(page.keep.banner?.chain ?? []).map((c) => c.rule)];
      const b = bound(sid);
      const all = new Set([...b.reach, ...b.trib]);
      got.push({ sid, item: inp.item, name: page.header.name, reach: inp.ruleset!.reach.map(key).sort(),
                 trib: inp.ruleset!.trib.map(key).sort(), uncited: cited.filter((k) => !all.has(k)) });
      want.push({ sid, item: null, name: `Unnamed ${b.kind} · ${placeName(sid)}`, reach: b.reach, trib: b.trib,
                  uncited: [] });
    }
    expect(got).toEqual(want);
  }, 120_000);

  it("(T57) both new strata were sampled somewhere in B.C.", () => {
    // counted here, not by the Region tests' side effects: this test alone (`-t T57`) must give the same answer
    const n = (which: "mu_line" | "lake_catchment") => REGIONS.reduce((a, r) => a + strata(r, which).length, 0);
    expect([n("mu_line") > 0, n("lake_catchment") > 0]).toEqual([true, true]);
  });
});
