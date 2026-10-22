/**
 * T40 (C) — the style-token gate, ported from check.mjs `r20c-style-tokens` (@ 04765ab5 L1071–1085): on the Keep tab of
 * vedder_1010, x35p_1010, klwa_1012 and glenn_615 (the prototype's four states) —
 *   1. at most four font sizes and 2. at most four weights, the strip's bar, month axis and legend left out (`.sbar,
 *      .smonths, .slegend, svg`);
 *   3. one link colour (the accent) and no underlined link;
 *   4. ALL-CAPS only on the kickers — the hits drawn in `textTransform: uppercase` sit in a `kicker:<kind>` test id (the
 *      prototype's `.lab, .scope, .bkh, .nkt .lab, .nofish h3`: the scope, a tier's kicker, the year label, How it adds
 *      up, where-not, the inherited label, the dates kicker, the ladder's question).
 * The prototype's fifth rule (every button or link ≥ 32 px) is a11y.test.tsx's 44 × 44 (the app's floor).
 *
 * A LINK is what the prototype counts as one (`a, button.linkbtn, .golink, .linkrow, .book`): every `role=link` and every
 * ⓘ. A row that is a button (an Other fish row, Possession, a caveat item) or a chip ("Source") draws its own words in
 * the page's colours, as the prototype's `button` rows do — those are not links.
 *
 * Each rule is pinned by a planted mutant it must catch. The census is `testing/textCensus.ts`. The expectations read the
 * palette's tokens (`LIGHT.accent`), so a palette swap (T47) moves no expectation here.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { Text, View } from "react-native";
import type { KeepVM, RegsPage } from "@app/core";
import type { MenuControls } from "@app/ui";
import { MenuContext } from "../menu/MenuContext";
import { ReportLink } from "../menu/ReportLink";
import { LIGHT } from "../theme";
import { TYPE } from "../type";
import { KeepTab } from "./KeepTab";
import { NO_ACTIONS } from "./LineText";
import { textCensus, weightOf, type TextHit } from "./testing/textCensus";
import vedder1010 from "./fixtures/vedder_1010.json";
import x35p1010 from "./fixtures/x35p_1010.json";
import klwa1012 from "./fixtures/klwa_1012.json";
import glenn615 from "./fixtures/glenn_615.json";

afterEach(cleanup);

const STATES = { vedder_1010: vedder1010, x35p_1010: x35p1010, klwa_1012: klwa1012, glenn_615: glenn615 } as unknown as Record<string, RegsPage>;
/** check.mjs leaves out the strip's bar (`.sbar`), month axis (`.smonths`) and legend (`.slegend`) */
const SKIP = ["strip-bar", "strip-months", "strip-legend"] as const;
const LINK_SEL = '[role="link"], [role="button"][aria-label^="What "]';

/** the colour react-native-web writes for a palette token (it normalises the hex) */
function drawn(colour: string): string {
  const { container, unmount } = render(<Text style={{ color: colour }}>x</Text>);
  const c = (container.firstElementChild as HTMLElement).style.color;
  unmount();
  return c;
}

/** the four rules over one rendered tree */
function audit(root: Element) {
  const hits = textCensus(root, SKIP);
  const links = [...root.querySelectorAll(LINK_SEL)];
  const inLink = (h: TextHit) => links.some((l) => l === h.el || l.contains(h.el));
  return {
    hits,
    sizes: [...new Set(hits.map((h) => h.size))].sort(),
    weights: [...new Set(hits.map((h) => h.weight))].sort(),
    linkColours: [...new Set(hits.filter(inLink).map((h) => h.color))],
    underlined: hits.filter((h) => inLink(h) && h.underline).map((h) => h.text),
    caps: hits.filter((h) => h.transform === "uppercase" && !h.testID.startsWith("kicker:")).map((h) => `${h.testID}: ${h.text}`),
  };
}

/**
 * The Keep tab as the app draws it: the tab and, at its foot, the menu's "Report a problem with this page" link (RegsScreen
 * puts it under every tab; it draws only under the Shell's MenuContext, so the gate provides one).
 */
function keepOf(id: string) {
  const page = STATES[id]!;
  const { container } = render(
    <MenuContext.Provider value={{ report() {} } as unknown as MenuControls}>
      <KeepTab keep={page.keep as KeepVM} dateLabel={page.header.date} palette={LIGHT} actions={NO_ACTIONS} />
      <ReportLink page={page} tab="keep" palette={LIGHT} />
    </MenuContext.Provider>);
  expect(container.querySelector('[data-testid="keep-tab"]')).not.toBeNull();
  expect(container.querySelector('[data-testid="report-link-keep"]')).not.toBeNull();
  return container;
}

describe("r20c-style-tokens on the Keep tab", () => {
  for (const id of Object.keys(STATES))
    it(`${id}: ≤ 4 sizes, ≤ 4 weights, one link colour (the accent), no underline, caps only on kickers`, () => {
      const a = audit(keepOf(id));
      expect(a.hits.length).toBeGreaterThan(20);   // not vacuous
      expect(a.sizes.length, `sizes ${a.sizes.join(", ")}`).toBeLessThanOrEqual(4);
      expect(a.weights.length, `weights ${a.weights.join(", ")}`).toBeLessThanOrEqual(4);
      expect(a.linkColours).toEqual([drawn(LIGHT.accent)]);
      expect(a.underlined).toEqual([]);
      expect(a.caps).toEqual([]);
    });

  it("across the four states the type is the page's scale: 12.5 / 15 / 17 / 26 px in 400 / 600 / 700 / 800", () => {
    const all = Object.keys(STATES).map((id) => audit(keepOf(id)));
    expect([...new Set(all.flatMap((a) => a.sizes))].sort()).toEqual(["12.5", "15.0", "17.0", "26.0"]);
    expect([...new Set(all.flatMap((a) => a.weights))].sort()).toEqual([400, 600, 700, 800]);
    // and the kickers the caps rule lets through are kickers in the page's kicker face
    const kickers = all.flatMap((a) => a.hits.filter((h) => h.transform === "uppercase"));
    expect(kickers.length).toBeGreaterThan(0);
    expect([...new Set(kickers.map((h) => `${h.size} ${h.weight}`))]).toEqual([`${TYPE.kicker.fontSize.toFixed(1)} 700`]);
  });
});

describe("the planted mutants are caught", () => {
  const face = (size: number, family: string = TYPE.regsBody.fontFamily) => ({ fontFamily: family, fontSize: size });
  it("a fifth size and a fifth weight", () => {
    const { container } = render(
      <View>
        {[12.48, 15, 17, 26, 14].map((s) => <Text key={s} style={[face(s), { color: LIGHT.ink }]}>{`size ${s}`}</Text>)}
        {["Archivo_400Regular", "Archivo_500Medium", "Archivo_600SemiBold", "Archivo_700Bold", "Archivo_800ExtraBold"].map((f) =>
          <Text key={f} style={[face(15, f), { color: LIGHT.ink }]}>{f}</Text>)}
      </View>);
    const a = audit(container);
    expect([a.sizes.length, a.weights]).toEqual([5, [400, 500, 600, 700, 800]]);
  });

  it("a link in another colour, an underlined link, caps outside a kicker", () => {
    const { container } = render(
      <View>
        <Text accessibilityRole="link" style={[face(15), { color: LIGHT.accent }]}>Good link ›</Text>
        <Text accessibilityRole="link" style={[face(15), { color: LIGHT.ink }]}>Ink link ›</Text>
        <Text accessibilityRole="link" style={[face(15), { color: LIGHT.accent, textDecorationLine: "underline" }]}>Underlined ›</Text>
        <Text testID="kicker:scope" style={[TYPE.kicker, { color: LIGHT.sub }]}>Region 2 streams</Text>
        <Text testID="keep-shout" style={[TYPE.kicker, { color: LIGHT.sub }]}>Shouting</Text>
      </View>);
    const a = audit(container);
    expect(a.linkColours).toEqual([drawn(LIGHT.accent), drawn(LIGHT.ink)]);
    expect(a.underlined).toEqual(["Underlined ›"]);
    expect(a.caps).toEqual(["keep-shout: Shouting"]);
  });

  it("the census reads a nested run's own family and its parent's size (jsdom does not inherit)", () => {
    const { container } = render(<Text style={[TYPE.regsBody, { color: LIGHT.ink }]}>Keep <Text style={{ fontFamily: TYPE.numeral.fontFamily }}>2</Text> a day</Text>);
    expect(textCensus(container).map((h) => [h.text, h.size, h.weight])).toEqual([["Keep a day", "15.0", 400], ["2", "15.0", 800]]);
    expect([weightOf("Archivo_700Bold", ""), weightOf("System", "600"), weightOf("System", "")]).toEqual([700, 600, 400]);
  });
});
