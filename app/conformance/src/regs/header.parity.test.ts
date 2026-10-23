/**
 * T16 parity: the header — `headerOf(inp, {today, status})` — against the golden
 * (app/design/regs-v11/port/golden/golden.json, page v38 @ 7fb6809f) `header` on every golden state, inputs from T09
 * `inputsOf` over the harness's paired store (REGS_STORE_REQUIRED=1 turns a skip into a failure).
 *
 * `status` is the day's as T13's strip reads it (null strip → tidal / outside). THE ACTION (owner call 3): the app's
 * header has none. Where the golden header offers Map (T71, 2026-10-22: one Map entry, the view strip's — exclusions
 * GOLDEN_MAP_ACTION_IDS) or Parts (T150, 2026-10-23: the water's Map view lists the parts — GOLDEN_PARTS_ACTION_IDS) the
 * golden's action is excluded by that entry, and every state asserts the app's header carries no `action` key. On the 22 one-part states the prototype could not map (golden place null, no action — exclusions
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
    // (owner call 3) THE APP'S HEADER HAS NO ACTION: every golden action is excluded by its kind's entry — Map (T71, the
    // view strip's) and Parts (T150, the Map view's list) — and the app's header carries no `action` key on any state
    const excludedAction = excluded("header", id, "action");
    expect([excludedAction === null, excludedAction?.reason.match(/T71|T150/)?.[0] ?? null])
      .toEqual(g.action === null ? [true, null] : [false, g.action.kind === "map" ? "T71" : "T150"]);
    expect("action" in v).toBe(false);
    // (T115) a part that took in a merged piece (exclusions PART_GROWN_IDS): its label is not the prototype's — asserted to differ
    const grown = excluded("header", id, "place.full") !== null;
    if (grown) expect(v.place && { full: v.place.full, short: v.place.short }).not.toEqual(g.place && { full: g.place.full, short: g.place.short });
    const { action: _golden, ...rest } = g;
    expect({ name: v.name, day: v.dayWord, date: v.date, dateAria: v.dateAria, dot: v.dot, place: noMap || grown ? null : v.place,
             tabs: v.tabs.map((t) => ({ sec: t.tab, text: t.label })) })
      .toEqual({ ...rest, place: grown ? null : g.place && { full: g.place.full, short: g.place.short } });
    passed++;
  });

  it.each(ids)("1. %s: the subtitle the full label is tested against is page v36's", async (id) => {
    // (T115) a grown part's subtitle carries its new extent and section count (exclusions PART_GROWN_IDS) — asserted to differ
    if (excluded("header", id, "place.full")) expect(subtitleOf((await inputsOf(id))!)).not.toBe(G.states[id]!.sub);
    else expect(subtitleOf((await inputsOf(id))!)).toBe(G.states[id]!.sub);
  });

  it("3. vedder_1010: no action — the golden's Map is the view strip's (T71); the Vedder has no choice of parts", async () => {
    expect(await headerFor("vedder_1010")).toEqual({ name: "Vedder River", dayWord: "Today", date: "Oct 10",
      dateAria: "Change date, now Oct 10", dot: "open",
      place: { full: "Mouth → Vedder Crossing Bridge", short: "Mouth → Vedder Crossing Bridge" },
      tabs: [{ tab: "keep", label: "What you can keep" }, { tab: "gear", label: "Gear" }, { tab: "methods", label: "Methods" },
             { tab: "licence", label: "Licence" }] });
  });

  it("4. vedder_615: Mon Jun 15, closed", async () => {
    const v = await headerFor("vedder_615");
    expect([v.dayWord, v.date, v.dot]).toEqual(["Mon", "Jun 15", "closed"]);
  });

  it("5. bc1_501: Fri; the long label is in the subtitle; the golden's Parts button is not the app's (T150)", async () => {
    const v = await headerFor("bc1_501");
    expect([v.dayWord, v.place, "action" in v, (G.states.bc1_501!.header as GHeader).action?.kind]).toEqual(["Fri",
      { full: "4 side channels, joining 55–59.1 km from the mouth", short: "4 side channels" }, false, "parts"]);
  });

  it("6. cow1_1010: the long label, and the uncut short one", async () => {
    expect((await headerFor("cow1_1010")).place).toEqual({ full: "From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)",
      short: "CNR Mile 66 Trestle → CNR Bridge (mile 70.2)" });
  });

  it("7. x5p0_1010: the long label is not in the subtitle, the short is uncut: both are the short", async () => {
    expect((await headerFor("x5p0_1010")).place).toEqual({ full: "Josephine Falls → Source", short: "Josephine Falls → Source" });
  });

  it("8. klwa_1010: the label is the water's name — no label slot; no action (T71)", async () => {
    const v = await headerFor("klwa_1010");
    expect([v.place, "action" in v]).toEqual([null, false]);
  });

  it("9. x31p_1010 (tidal): a faint dot; its part is labelled — the prototype's null was its missing water map (review fix)", async () => {
    const v = await headerFor("x31p_1010");
    expect([v.dot, v.place && v.place.short !== v.name]).toEqual(["faint", true]);
  });

  it("2. (T71 / T150) where the golden offers Map or Parts, many parts or one: the app's header offers nothing", async () => {
    expect([G.states.cow1_1010!.header, G.states.klwa_1010!.header, G.states.bc1_501!.header].map((h) => (h as GHeader).action?.kind))
      .toEqual(["map", "map", "parts"]);
    for (const id of ["cow1_1010", "klwa_1010", "bc1_501"]) expect(["action" in (await headerFor(id)), id]).toEqual([false, id]);
  });
});
