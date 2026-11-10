/**
 * T581 (T580 F1, F7) — THE GAUGE CARD IS ABOUT THE QUANTITY ASKED, AND THE ALERT EDITOR READS THE CARD'S OWN FIGURE.
 * Lead kind (flow and level / flow only / level only, and a gauged lake's level station) x Flow / Level x Now / +3:
 * the card's quantity is the asked one, or the refusal words of the row and the pin appear — never the other quantity's
 * figure under this chart.
 */
import { describe, expect, it } from "vitest";
import { OTHER_QUANTITY_WORDS } from "@app/core";
import type { Parameter } from "@app/data";
import { cardQuantity } from "./answers";
import { currentFigure, type ReadingVM } from "./panel/conditions";
import { alertNowValue } from "./notify";

const KINDS: Record<string, { has: readonly Parameter[]; reads: Parameter; figures: Partial<Record<Parameter, number>> }> = {
  both: { has: ["discharge", "level"], reads: "discharge", figures: { discharge: 197, level: 2.18 } },
  flowOnly: { has: ["discharge"], reads: "discharge", figures: { discharge: 41.2 } },
  levelOnly: { has: ["level"], reads: "level", figures: { level: 1.548 } },
  lake: { has: ["level"], reads: "level", figures: { level: 340.2 } },
};

/** what the card says for a lead of this kind, asked in `q`: the quantity of its figure, or the refusal's words */
function card(kind: string, q: Parameter, known = true) {
  const k = KINDS[kind]!;
  const c = cardQuantity(q, k.has, known, k.reads);
  const reading = { figures: k.figures } as Pick<ReadingVM, "figures">;
  const figure = currentFigure(reading, c.shown);
  const words = c.silent ? OTHER_QUANTITY_WORDS[k.reads].reads : null;
  return { shown: c.shown, silent: c.silent, figure, words, options: c.options };
}

describe("the card's quantity: lead kind x asked quantity (x day: the card does not depend on it)", () => {
  for (const day of [0, 3])
    for (const kind of Object.keys(KINDS))
      for (const q of ["discharge", "level"] as const)
        it(`${kind}, ${q}, +${day}: the asked quantity, or the refusal`, () => {
          const c = card(kind, q);
          expect(c.shown).toBe(q);                                   // the toggle and the unit are the asked one
          if (KINDS[kind]!.has.includes(q)) {
            expect(c.silent).toBe(false);
            expect(c.figure).toBe(KINDS[kind]!.figures[q]);
          } else {
            expect(c.silent).toBe(true);
            expect(c.figure).toBeNull();                              // never the other quantity's figure
            expect(c.words).toBe(q === "discharge" ? "Reports water level only today, not flow. Show Level to see it."
                                                   : "Reports flow only today, not water level. Show Flow to see it.");
            expect(c.options).toEqual(["discharge", "level"]);        // the other quantity is one tap away: the one Show choice
          }
        });

  it("is Flow, never Level, while the station's quantities are not yet known", () => {
    expect(cardQuantity(null, [], false, null).shown).toBe("discharge");
    expect(cardQuantity("discharge", [], false, null)).toMatchObject({ shown: "discharge", silent: false });
    expect(cardQuantity(null, ["level"], true, null).shown).toBe("level");   // nothing asked: the station's own
  });
});

describe("the alert editor's value and 'now' are the card's figure", () => {
  it("for every lead kind x quantity the prefill is the figure the card prints", () => {
    for (const kind of Object.keys(KINDS))
      for (const q of ["discharge", "level"] as const) {
        const c = card(kind, q);
        const prefill = currentFigure({ figures: KINDS[kind]!.figures }, q);
        expect(prefill).toBe(c.figure);
        // the editor's own "now" is that figure, whatever the 72 h series' last point says
        if (prefill !== null) expect(alertNowValue(q, { quantity: q, current: prefill }, 999)).toBe(prefill);
      }
  });

  it("Hansard (declares flow) under Level prefills the card's 2.18 m, not nothing", () => {
    expect(currentFigure({ figures: KINDS.both!.figures }, "level")).toBe(2.18);
    expect(alertNowValue("level", { quantity: "level", current: 2.18 }, 2.17)).toBe(2.18);
    // another quantity picked in the editor: the series speaks for it
    expect(alertNowValue("discharge", { quantity: "level", current: 2.18 }, 197)).toBe(197);
  });
});
