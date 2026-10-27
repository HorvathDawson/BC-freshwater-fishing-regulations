/**
 * The pairing the parity suites hang on (`answersVerdict`: the release carries the answers the oracles were cut from; P20 —
 * no regs.sqlite, taps.py reads the release's own store): a stale release must come back as a skip WITH its reason (never a
 * silent pass, never a fail), and REGS_STORE_REQUIRED=1 must turn that skip into a failure, so a machine that is meant to
 * prove parity cannot exit 0 on a rebuilt release.
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { basename } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { KNOWN_VERSIONS } from "@app/core";
import type { Db, Manifest } from "@app/data";
import type { RegsLoader } from "@app/data/regs";
import { regsFixtureDir } from "@app/data/release/node";
import { liveReleaseAt, openReleaseDir } from "../fixtureRelease";
import { answersVerdict, chooseSource, fixtureVerdict, openPaired, paritySkip, parityMode, resolveSource, summary, taps } from "./golden";

const dir = mkdtempSync(join(tmpdir(), "regs-pairing-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
const want = taps().meta.store.answers_sha256!;

describe("answersVerdict: is the release the build the oracles were cut from, or why not", () => {
  it("taps.json names the answers it was cut from (taps.py reads the release, P20)", () => {
    expect(want).toMatch(/^[0-9a-f]{64}$/);
  });

  it("a release carrying the oracles' answers is the pairing", () => {
    expect(answersVerdict("/r/r-x", want, want)).toBeNull();
  });

  it("a release carrying other answers is a skip naming the release, both digests and the re-cut", () => {
    const why = answersVerdict("/r/r-stale", "0".repeat(64), want)!;
    expect(why).toContain("/r/r-stale");
    expect(why).toContain("000000000000");
    expect(why).toContain(want.slice(0, 12));
    expect(why).toMatch(/sync\.mjs/);
  });

  it("a taps.json cut before P20 (no answers_sha256) pairs with nothing", () => {
    expect(answersVerdict("/r/r-x", want, undefined)).toMatch(/no answers_sha256.*taps\.py/);
    expect(answersVerdict("/r/r-x", want, null)).toMatch(/no answers_sha256/);
  });
});

/**
 * The release a parity suite reads is opened the way the app opens it (`openRegsStore(loader)`), every region file the
 * oracle states read opened up front: each refusal is a skip reason in the app's own words, never 134 failures. Made on the
 * committed regs fixture by tampering with what the loader sees (`openReleaseDir(dir, tamper)`); the files are only read.
 */
describe("openPaired: a release the app refuses is a reason, not a store", () => {
  const FX = regsFixtureDir(), rel = basename(FX), oracle = taps().meta.store, Z = "0000000000000000";
  /** A file whose meta says `k` is `v` (another build's file, as the store reads it). */
  const withMeta = (d: Db, k: string, v: string): Db => ({
    ...d, all: async (q, ...p) => (await d.all(q, ...p)).map((r) => (/\bFROM meta\b/.test(q) && r.k === k ? { ...r, v } : r)),
  });

  it("the committed fixture, untampered: the store, every oracle region opened, nothing refused, the oracles' digests", async () => {
    const at = await openPaired(openReleaseDir(FX));
    expect(at.reason).toBeNull();
    expect({ release: at.store!.meta.release, handles: at.store!.meta.sectionHandles, reach: at.store!.meta.reachDigest,
             rules: at.store!.meta.ruleIdsSha256, refused: at.store!.refused() })
      .toEqual({ release: rel, handles: oracle.section_handles, reach: oracle.reach_digest, rules: oracle.rule_ids_sha256, refused: null });
  });

  it("a release with no water file is the app's refusal, in its own words", async () => {
    const loader = { release: "r-test", manifest: { data_release: "r-test", files: {} } as unknown as Manifest,
      db: async () => { throw new Error("never opened"); }, regionsOf: async () => [], regionDb: async () => { throw new Error("never opened"); },
    } as RegsLoader;
    expect(await openPaired(loader)).toEqual({ store: null, reason: "release r-test refused: regs store: release r-test has no water file — refused" });
  });

  it("wrong tiles: a map of another atlas is refused", async () => {
    const at = await openPaired(openReleaseDir(FX, { manifest: (m) => ({ ...m, tiles: { ...m.tiles, current: { id: `${Z}.000000000000`, url: "t" } } }) }));
    expect(at).toEqual({ store: null,
      reason: `release ${rel} refused: regs store: built for atlas ${oracle.section_handles}, the map is ${Z}.000000000000 — refused` });
  });

  it("wrong handles: a water file of another sid space than its manifest is refused", async () => {
    const at = await openPaired(openReleaseDir(FX, { db: (n, d) => (n === "water" ? withMeta(d, "section_handles", Z) : d) }));
    expect(at).toEqual({ store: null, reason: `release ${rel} refused: regs store: water: format water/1 / handles ${Z} ` +
      `are not the manifest's water/1 / ${oracle.section_handles} — refused` });
  });

  it("a missing region file is refused at open, not as failures later", async () => {
    const at = await openPaired(openReleaseDir(FX, { manifest: (m) => {
      const { "regs-2": _gone, ...files } = m.files;
      return { ...m, files };
    } }));
    expect(at).toEqual({ store: null, reason: `release ${rel} refused: regs store: release ${rel} has no file regs-2 — refused` });
  });

  it("a region file from another build (bad digest) is refused at open", async () => {
    const at = await openPaired(openReleaseDir(FX, { db: (n, d) => (n === "regs-2" ? withMeta(d, "reach_digest", Z) : d) }));
    expect(at).toEqual({ store: null,
      reason: `release ${rel} refused: regs store: a region file is cut for rule bindings ${Z}, water's are ${oracle.reach_digest} — refused` });
  });

  it("a failure that is not a refusal (a bug, a damaged read) is thrown, never a skip", async () => {
    await expect(openPaired(openReleaseDir(FX, { db: (n, d) => (n === "regs-2" ? { ...d, all: async () => { throw new Error("disk"); } } : d) })))
      .rejects.toThrow("disk");
  });
});

describe("paritySkip: a skip is announced, and REGS_STORE_REQUIRED makes it a failure", () => {
  const stale = { path: null, reason: "x.sqlite is store 00, the oracles were cut from 11" } as const;

  it("paired: run, nothing to say", () => {
    expect(paritySkip({ path: "/s.sqlite", reason: null }, {})).toEqual({ skip: false, fail: null });
  });

  it("unpaired: skip, carrying the reason", () => {
    expect(paritySkip(stale, {})).toEqual({ skip: true, fail: null, reason: stale.reason });
  });

  it("unpaired with REGS_STORE_REQUIRED=1: a failure carrying the reason", () => {
    expect(paritySkip(stale, { REGS_STORE_REQUIRED: "1" })).toEqual({ skip: true, fail: expect.stringContaining(stale.reason) });
  });

  it("REGS_STORE_REQUIRED=0 or empty does not require", () => {
    expect(paritySkip(stale, { REGS_STORE_REQUIRED: "0" }).fail).toBeNull();
    expect(paritySkip(stale, { REGS_STORE_REQUIRED: "" }).fail).toBeNull();
  });
});

describe("liveReleaseAt: a release built for another port is a skip with its reason, never tested as if paired", () => {
  const release = (name: string, versions: Record<string, number>): string => {
    const d = join(dir, name);
    mkdirSync(d, { recursive: true });
    writeFileSync(join(d, "manifest.json"), JSON.stringify({ regulations: { valid_until: null, versions } }));
    return d;
  };
  const { glossary: _g, ...sections } = KNOWN_VERSIONS;

  it("a release at this app's section versions is the live release", () => {
    const d = release("r-now", { ...sections });
    expect(liveReleaseAt({ RELEASE_DIR: d })).toEqual({ dir: d, reason: null });
  });

  it("a rows-29 release (before the year strip) is a skip naming the version and the rebuild", () => {
    const d = release("r-rows29", { ...sections, rows: 29 });
    expect(liveReleaseAt({ RELEASE_DIR: d })).toEqual({ dir: null,
      reason: `release ${d} is rows 29 (this app reads 33): rebuild it (deliver all), then re-cut the goldens (sync.mjs)` });
  });

  it("a rows-30 / display-17 release (before the Keep tab's last choices moved to the pipeline) is a skip naming both", () => {
    const d = release("r-rows30", { ...sections, rows: 30, display: 17 });
    expect(liveReleaseAt({ RELEASE_DIR: d })).toEqual({ dir: null, reason: `release ${d} is rows 30 (this app reads 33), `
      + "display 17 (this app reads 18): rebuild it (deliver all), then re-cut the goldens (sync.mjs)" });
  });

  it("a rows-31 / licence-6 release (before the four pipeline answers: Other fish stamps, the steelhead stamp, youth alternatives) is a skip naming both", () => {
    const d = release("r-rows31", { ...sections, rows: 31, licence: 6 });
    expect(liveReleaseAt({ RELEASE_DIR: d })).toEqual({ dir: null, reason: `release ${d} is rows 31 (this app reads 33), `
      + "licence 6 (this app reads 7): rebuild it (deliver all), then re-cut the goldens (sync.mjs)" });
  });

  it("a rows-32 release (before the leading origin was shipped, T161) is a skip naming the version and the rebuild", () => {
    const d = release("r-rows32", { ...sections, rows: 32 });
    expect(liveReleaseAt({ RELEASE_DIR: d })).toEqual({ dir: null,
      reason: `release ${d} is rows 32 (this app reads 33): rebuild it (deliver all), then re-cut the goldens (sync.mjs)` });
  });

  it("with REGS_STORE_REQUIRED=1 it throws that reason", () => {
    const d = release("r-rows29b", { ...sections, rows: 29 });
    expect(() => liveReleaseAt({ RELEASE_DIR: d, REGS_STORE_REQUIRED: "1" })).toThrow(
      `REGS_STORE_REQUIRED is set: release ${d} is rows 29 (this app reads 33): rebuild it (deliver all), then re-cut the goldens (sync.mjs)`);
  });

  it("no release: the reason says so", () => {
    expect(liveReleaseAt({ RELEASE_DIR: join(dir, "nowhere") })).toEqual({ dir: null, reason: "no release on this machine (data/release/r-*)" });
  });
});

describe("T12 parityMode: REGS_PARITY, default auto, a typo refused", () => {
  it("reads the three modes; unset is auto; anything else throws", () => {
    expect([parityMode({}), parityMode({ REGS_PARITY: "province" }), parityMode({ REGS_PARITY: "fixture" }), parityMode({ REGS_PARITY: "auto" })])
      .toEqual(["auto", "province", "fixture", "auto"]);
    expect(() => parityMode({ REGS_PARITY: "fixtures" })).toThrow("REGS_PARITY=fixtures: expected province, fixture or auto");
  });
});

describe("T12 fixtureVerdict: the committed fixture is the oracles' store, or why not", () => {
  const store = { section_handles: "h", reach_digest: "r", rule_ids_sha256: "u" };
  const fx = { sectionHandles: "h", reachDigest: "r", ruleIdsSha256: "u", answersSha256: "a".repeat(64) };
  it("paired on the three digests; answers checked only when the province store is here", () => {
    expect(fixtureVerdict(fx, store, null)).toBeNull();
    expect(fixtureVerdict(fx, store, "a".repeat(64))).toBeNull();
    expect(fixtureVerdict(fx, store, "b".repeat(64)))
      .toBe("the regs fixture release carries answers aaaaaaaaaaaa, the oracles' store bbbbbbbbbbbb: run pnpm -C app regs-fixture");
  });
  it("each digest names itself", () => {
    expect(fixtureVerdict({ ...fx, sectionHandles: "H" }, store, null))
      .toBe("the regs fixture release has section_handles H, the oracles' store h: run pnpm -C app regs-fixture (after port/sync.mjs)");
    expect(fixtureVerdict({ ...fx, reachDigest: "R" }, store, null)).toMatch(/^the regs fixture release has reach_digest R, the oracles' store r:/);
    expect(fixtureVerdict({ ...fx, ruleIdsSha256: "U" }, store, null)).toMatch(/^the regs fixture release has rule_ids_sha256 U, the oracles' store u:/);
  });
});

describe("T12 resolveSource: the committed fixture is held to the oracles' digests through the harness's own path", () => {
  const store = taps().meta.store;
  const facts = { section_handles: store.section_handles, reach_digest: store.reach_digest, rule_ids_sha256: store.rule_ids_sha256 };
  it("REGS_PARITY=fixture over the committed fixture, paired: the fixture", async () => {
    expect(await resolveSource({ REGS_PARITY: "fixture" }, { store: facts, provinceAnswers: () => null }))
      .toEqual({ label: "fixture", dir: expect.stringMatching(/\/regs-release\/r-[0-9a-f]+$/), reason: null });
  });
  it("a digest the fixture does not carry is a refusal naming it, never a pairing", async () => {
    expect(await resolveSource({ REGS_PARITY: "fixture" }, { store: { ...facts, reach_digest: "stale" }, provinceAnswers: () => null }))
      .toEqual({ label: null, dir: null, reason: expect.stringMatching(/^REGS_PARITY=fixture: the regs fixture release has reach_digest .*, the oracles' store stale:/) });
  });
  it("province answers the fixture does not carry are a refusal", async () => {
    expect(await resolveSource({ REGS_PARITY: "fixture" }, { store: facts, provinceAnswers: () => "f".repeat(64) }))
      .toEqual({ label: null, dir: null, reason: expect.stringMatching(/^REGS_PARITY=fixture: the regs fixture release carries answers .*, the oracles' store ffffffffffff:/) });
  });
});

describe("T12 chooseSource", () => {
  it("auto: province first, then the fixture, else both reasons", () => {
    expect(chooseSource("auto", null, null)).toEqual({ label: "province", reason: null });
    expect(chooseSource("auto", "p", null)).toEqual({ label: "fixture", reason: null });
    expect(chooseSource("auto", "p", "f")).toEqual({ label: null, reason: "no paired regs store: province — p; fixture — f" });
  });
  it("a named mode takes only that store", () => {
    expect(chooseSource("fixture", null, null)).toEqual({ label: "fixture", reason: null });
    expect(chooseSource("fixture", null, "f")).toEqual({ label: null, reason: "REGS_PARITY=fixture: f" });
    expect(chooseSource("province", null, null)).toEqual({ label: "province", reason: null });
    expect(chooseSource("province", "p", null)).toEqual({ label: null, reason: "REGS_PARITY=province: p" });
  });
});

describe("T12 summary", () => {
  it("the afterAll line: states equal, then each keyed exclusion with what lifts it", () => {
    // (T115) the 4 states whose part the release merged away, and (T67) the 2 closed states whose merged closed list the
    // release re-cut, are excluded from every block (exclusions PART_GONE_IDS); (T183) so is the West Road's x27p1, its
    // part merged into part 0
    const GONE = " · 7 excluded (x15p14_1010, x15p15_1010, x15p17_1010, x15p18_1010, x19p5_1010, x22p2_1010, x27p1_1010)";
    expect(summary("methods", 134, 134)).toBe("methods parity: 134/134 states equal" + GONE   // (T32) the Set lines note the prototype's DOM lost, asserted instead
      + " · ^boats$ not compared (the page blocks and golden re-cut on drop_on_parts)"   // (T206) the on-parts ruling's Boats blocks, pinned instead
      + " · ^ways\\.list\\.0\\.items\\.\\d+\\.note$ not compared (never — a prototype markup bug, not a regulation)");
    expect(summary("gear", 134, 134)).toBe("gear parity: 134/134 states equal" + GONE   // (T31) the Bella Coola snapshot who-word, asserted instead
      + " · ^bait\\.src\\.who$ not compared (never — the prototype's snapshot is global; the app's level is per part)");
    expect(summary("keep", 133, 134)).toBe("keep parity: 133/134 states equal" + GONE
      + " · ^groups\\.\\d+\\.(tree|worked|lines)$ not compared (never — a re-synced prototype on the ruled data removes it)"   // (T18/T19) the Kitimat ruling; T13 trout line compared
      + " · ^plainRows\\.0$ not compared (never on this golden — D19 (a) shipped: a prototype re-synced to draw the row as a group)");   // (T21) the whole Keep tab; rows 32: Other fish stamps compared; T80: the app's group is D19_GROUPS
    const d19 = (k: string) => ` · ^(other:${k})$ not compared (D19 (a): the pipeline ships a group (headline) for these rows, or D19 (b) ports the legacy path)`;
    expect(summary("sheets", 134, 134)).toBe("sheets parity: 134/134 states equal" + GONE
      + " · ^parts$ not compared (never on this golden — a prototype re-synced on T115's data)"   // (T115) the re-cut Fraser's Parts sheet
      + d19("x18p5_1010\\|o0") + d19("x22p1_1010\\|o1") + d19("x22p3_1010\\|o1")   // (T36 part 2) the legacy pooled card path
      + " · ^rule: not compared (never — T36 test asserts the fold exists, closed, and that its first line starts with the key)"
      + " · ^src:x25p_1010\\|r2$ not compared (the page blocks and golden re-cut on the retired rules)");   // (T206) Lois Lake's fold   // the gl:/hatch pages compared since the release ships pdf_pages
    expect(summary("licence", 133, 134)).toBe("licence parity: 133/134 states equal · 8 excluded (x15p14_1010, x15p15_1010, x15p17_1010, x15p18_1010, x19p5_1010, x22p2_1010, x27p1_1010, x31p_1010)");   // licence 7: the steelhead stamp and the youth alternatives compared
  });
});
