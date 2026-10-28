/**
 * T41 (A) — the browser tools' state table: every golden state id maps to ONE prototype hash and ONE app start link, and
 * the start link reads back (through the app's own parser) as that state's water, part, day, tab, sheet and angler.
 */
import { describe, expect, it } from "vitest";
import { appKeyOf as coreAppKeyOf } from "@app/core";
import { parseStartLink } from "@app/ui";
// @ts-expect-error — a plain .mjs module of the node tools (no declaration file)
import { appKeyOf, GONE, IDS, PROTO_TODAY, REF, stateOf, urlsOf, WALK_STATES } from "./states.mjs";
import golden from "../../fixtures/golden/golden.json";

const G = golden as unknown as { meta: { ref: string }; states: Record<string, { sheets?: Record<string, unknown> }> };
const NOW = new Date(2026, 9, 22, 9);

describe("regs-web states", () => {
  it("vedder_1010 → the prototype hash and the app's start link (item from taps.json), at the goldens' ref", () => {
    expect({ ...urlsOf("vedder_1010", { app: "http://x" }), ref: REF }).toEqual({
      proto: "w=vedder&d=1010&p=0&sec=keep",
      app: "http://x/?regs=gnis%3A3062&part=0&date=2026-10-10&tab=keep",
      ref: G.meta.ref,
    });
  });

  it("a sheet, a tab, an angler and the dark palette ride both links (the app's sheet key through appKeyOf)", () => {
    expect(urlsOf("vedder_1010", { app: "http://x", tab: "licence", sheet: "pos:vedder_1010|pos", who: { age: "u16", res: "non_resident" }, theme: "dark" }))
      .toEqual({
        proto: "w=vedder&d=1010&p=0&sec=licence&sheet=pos%3Avedder_1010%7Cpos&age=u16&res=non_resident&theme=dark",
        app: "http://x/?regs=gnis%3A3062&part=0&date=2026-10-10&tab=licence&sheet=pos%3Apos&age=u16&res=non_resident&theme=dark",
      });
  });

  it(`every one of the ${IDS.length} golden ids maps uniquely, and its start link reads back as that state`, () => {
    const protos = new Set<string>(), apps = new Set<string>(), wrong: string[] = [];
    for (const id of IDS as string[]) {
      const { proto, app } = urlsOf(id, { app: "http://x" });
      protos.add(proto); apps.add(app);
      const s = stateOf(id), l = parseStartLink(new URL(app).search, NOW)!;
      const top = l.route.mode === "water" ? l.route.stack[l.route.stack.length - 1]! : null;
      const [y, m, d] = (s.date as string).split("-").map(Number);
      if (top?.item !== s.item || top.part !== s.part || l.day?.getFullYear() !== y || l.day.getMonth() !== m! - 1 || l.day.getDate() !== d
          || l.regs.tab !== "keep" || l.regs.sheet !== null || l.who !== null || l.theme !== null) wrong.push(id);
    }
    // (T115, T67, T183, T220) 124: the golden's 134 less the 10 whose part the release re-cut (taps.json `gone`; T220 added Dean x13p2, Kootenay x9p2)
    expect({ n: IDS.length, protos: protos.size, apps: apps.size, wrong }).toEqual({ n: 124, protos: 124, apps: 124, wrong: [] });
    expect([...IDS, ...GONE].sort()).toEqual(Object.keys(G.states).sort());
    // (T183) the Fraser's gone states' reasons are T183's now (a straddling piece carries both regions' rules again)
    expect(() => stateOf("x15p14_1010")).toThrow(/has no part in the store: T183/);
  });

  it("the tools' appKeyOf is core's over every golden sheet key of every state, and the start link accepts each", () => {
    const diff: string[] = [];
    for (const id of IDS as string[]) for (const k of Object.keys(G.states[id]!.sheets ?? {})) {
      let core: string; try { core = coreAppKeyOf(k, id); } catch { core = "(throws)"; }
      const ours = appKeyOf(k, id);
      if (ours !== core) diff.push(`${id} ${k}: ${ours} ≠ ${core}`);
      else if (core !== "(throws)" && parseStartLink(new URL(urlsOf(id, { app: "http://x", sheet: k }).app).search, NOW)!.regs.sheet !== core)
        diff.push(`${id} ${k}: the link reads back otherwise`);
    }
    expect(diff).toEqual([]);
  });

  it("PROTO_TODAY: the day the goldens call Today (the prototype's snapshot day) — the tools' clock", () => {
    expect(PROTO_TODAY).toBe("2026-10-10");
  });

  it("WALK_STATES: check.mjs's state sets as golden ids + the tidal and timed edge states", () => {
    expect(WALK_STATES).toEqual(["vedder_1010", "klwa_1010", "cow1_1010", "chw1_1010", "wilkes_1010", "vedder_515", "x12p_1010",
      "x3p_1010", "iron_720", "x34p_1010", "x35p_1010", "bc0_1010", "bc0_501", "ape_1010", "ape_501", "x15p3_1010", "bc1_501",
      "hamill_615", "klwa_1012", "glenn_615", "chw2_1010", "x31p_1010", "x15p2_1010"]);
  });
});
