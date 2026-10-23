/**
 * T16 parity: the header — `headerOf(inp, {today, status})` — against the golden
 * (app/design/regs-v11/port/golden/golden.json, page v38 @ 7fb6809f) `header` on every golden state, inputs from T09
 * `inputsOf` over the harness's paired store (REGS_STORE_REQUIRED=1 turns a skip into a failure).
 *
 * `status` is the day's as T13's strip reads it (null strip → tidal / outside). THE ACTION (T71, owner call 3, 2026-10-22:
 * one Map entry, the view strip's): where the golden header offers Map (exclusions GOLDEN_MAP_ACTION_IDS) the app offers
 * Parts when the picker has a choice to make, else nothing — asserted whole against that rule; every other state's action
 * is the golden's. On the 22 one-part states the prototype could not map (golden place null, no action — exclusions
 * NO_MAP_LABEL_IDS) the app's label is checked against T16's rule, not the golden. `place.shown` is fitLabel's
 * measurement (excluded; T41). Every expected value is the golden's except the ticket's literals.
 */
import { afterAll, describe, expect, it } from "vitest";
import { buildModel, headerOf, makePlace, settle, stripOf, subtitleOf, WORDS, type HeaderVM } from "@app/core";
import { caseIds, golden, inputsOf, parityGate, requestOf, summary } from "./golden";
import { excluded } from "./exclusions";

interface GHeader { name: string; day: string; date: string; dateAria: string; dot: string;
  place: { shown: string; full: string; short: string } | null; action: { kind: string; label: string; aria: string } | null;
  tabs: { sec: string; text: string }[] }
const G = golden();
const gate = await parityGate();
const ids = caseIds("header");

async function headerFor(id: string): Promise<HeaderVM> {
  const inp = (await inputsOf(id))!, place = makePlace(inp), s = settle(inp, place), r = requestOf(id);
  const strip = stripOf(inp, s, buildModel(inp, place, s), r.today);
  return headerOf(inp, { today: r.today, status: strip ? strip.status : inp.key?.tidal ? "tidal" : "outside" });
}

it(`T16 header parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
  if ("reason" in gate) console.warn(`T16 header parity skipped: ${gate.reason}`);
});

describe.skipIf(gate.skip)("T16 header vs golden header", () => {
  let passed = 0;
  afterAll(() => console.info(summary("header", passed, ids.length)));

  it.each(ids)("1–2. %s: name, day, date, aria, dot, place (full, short), action, tabs", async (id) => {
    const v = await headerFor(id), g = G.states[id]!.header as GHeader;
    expect(excluded("header", id, "place.shown")).not.toBeNull();
    // no water map in the prototype → no label there; the app labels the part unless the label is the water's name (T16)
    const noMap = excluded("header", id, "place") !== null;
    if (noMap) {
      // a one-part water (no choice of parts) the prototype could not map
      expect([g.place, g.action, ((await inputsOf(id))!.item?.parts.filter(Boolean).length ?? 0) <= 1]).toEqual([null, null, true]);
      if (v.place) expect([v.place.full, v.place.short !== v.name]).toEqual([v.place.short, true]);
    }
    // (T71) the golden's Map button is the view strip's in the app: Parts when the picker has a choice, else nothing
    const noMapButton = excluded("header", id, "action") !== null;
    expect(noMapButton).toBe(g.action?.kind === "map");
    const many = ((await inputsOf(id))!.item?.picker?.choices.length ?? 0) > 1;
    const action = !noMapButton ? g.action
      : many ? { kind: "parts", label: "Parts", aria: WORDS.PARTS_ARIA(g.action!.aria.replace(/^View on map: where these rules apply(, )?/, "")) }
      : null;
    expect({ name: v.name, day: v.dayWord, date: v.date, dateAria: v.dateAria, dot: v.dot, place: noMap ? null : v.place,
             action: v.action, tabs: v.tabs.map((t) => ({ sec: t.tab, text: t.label })) })
      .toEqual({ ...g, action, place: g.place && { full: g.place.full, short: g.place.short } });
    passed++;
  });

  it.each(ids)("1. %s: the subtitle the full label is tested against is page v36's", async (id) => {
    expect(subtitleOf((await inputsOf(id))!)).toBe(G.states[id]!.sub);
  });

  it("3. vedder_1010: no action — the golden's Map is the view strip's (T71); the Vedder has no choice of parts", async () => {
    expect(await headerFor("vedder_1010")).toEqual({ name: "Vedder River", dayWord: "Today", date: "Oct 10",
      dateAria: "Change date, now Oct 10", dot: "open",
      place: { full: "Mouth → Vedder Crossing Bridge", short: "Mouth → Vedder Crossing Bridge" },
      action: null,
      tabs: [{ tab: "keep", label: "What you can keep" }, { tab: "gear", label: "Gear" }, { tab: "methods", label: "Methods" },
             { tab: "licence", label: "Licence" }] });
  });

  it("4. vedder_615: Mon Jun 15, closed", async () => {
    const v = await headerFor("vedder_615");
    expect([v.dayWord, v.date, v.dot]).toEqual(["Mon", "Jun 15", "closed"]);
  });

  it("5. bc1_501: Fri; the long label is in the subtitle; Parts names it", async () => {
    const v = await headerFor("bc1_501");
    expect([v.dayWord, v.place, v.action]).toEqual(["Fri",
      { full: "4 side channels, joining 55–59.1 km from the mouth", short: "4 side channels" },
      { kind: "parts", label: "Parts", aria: "Other parts — now 4 side channels, joining 55–59.1 km from the mouth" }]);
  });

  it("6. cow1_1010: the long label, and the uncut short one", async () => {
    expect((await headerFor("cow1_1010")).place).toEqual({ full: "From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)",
      short: "CNR Mile 66 Trestle → CNR Bridge (mile 70.2)" });
  });

  it("7. x5p0_1010: the long label is not in the subtitle, the short is uncut: both are the short", async () => {
    expect((await headerFor("x5p0_1010")).place).toEqual({ full: "Josephine Falls → Source", short: "Josephine Falls → Source" });
  });

  it("8. klwa_1010: the label is the water's name — no label slot; one part, so no action either (T71)", async () => {
    const v = await headerFor("klwa_1010");
    expect([v.place, v.action]).toEqual([null, null]);
  });

  it("9. x31p_1010 (tidal): a faint dot; its part is labelled — the prototype's null was its missing water map (review fix)", async () => {
    const v = await headerFor("x31p_1010");
    expect([v.dot, v.place && v.place.short !== v.name]).toEqual(["faint", true]);
  });

  it("2. (T71) where the golden offers Map: a picker of several choices gives Parts, a one-part water nothing", async () => {
    expect([G.states.cow1_1010!.header, G.states.klwa_1010!.header].map((h) => (h as GHeader).action?.kind)).toEqual(["map", "map"]);
    expect((await headerFor("cow1_1010")).action).toEqual({ kind: "parts", label: "Parts",
      aria: "Other parts — now From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)" });
    expect((await headerFor("klwa_1010")).action).toBeNull();
  });
});
