/**
 * T39 — THE PALETTE RENDER GATE (light / dark / CVD): check.mjs `r20g-palette`, `r20h-one-surface`,
 * `r20h-picture-surface` and the bold half of `r20i-glare`, restated over react-native-web's DOM (testing/census.ts).
 *
 * Every tab of every frozen golden page (fixtures/*.json, `UPDATE_REGS_FIXTURES=1` from buildRegsPage over the parity
 * store — never hand-edited; the r20g state set where a frozen page exists: vedder_1010, vedder_515, bc1_501, and
 * klwa_1010 / glenn_501 standing for klwa_1012 / glenn_615, plus the closed vedder_615) is drawn in each palette
 * (`paletteFor(theme)`) and censused: each rule's faults must be none. Then each rule is pinned by a MUTANT tree that
 * breaks it (and the same tree mended, which passes), so a rule that stops looking fails here, not silently.
 *
 * The gate reads the TOKENS, never a hex: the per-tab colour census below is named by palette key, and the "palette
 * swap" case redraws a page with every status colour moved (T47's pick, still open) and finds nothing to fix.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { Text, View } from "react-native";
import type { PinLine, RegsPage, Tab } from "@app/core";
import { CVD, DARK, LIGHT, type Palette } from "../theme";
import { KeepTab } from "./KeepTab";
import { GearTab } from "./GearTab";
import { MethodsTab } from "./MethodsTab";
import { LicenceTab } from "./LicenceTab";
import { PinBar } from "./PinBar";
import type { PinStore } from "./pinStore";
import { NO_ACTIONS } from "./LineText";
import type { FishImages } from "./types";
import {
  ALLOW, borderFaults, census, coloursOf, pictureFaults, rgbOf, roleOf, surfaceFaults, TEXT_RULES, textFaults, tokenOf,
  type TextRule,
} from "./testing/census";
import vedder1010 from "./fixtures/vedder_1010.json";
import vedder515 from "./fixtures/vedder_515.json";
import vedder615 from "./fixtures/vedder_615.json";
import klwa1010 from "./fixtures/klwa_1010.json";
import bc1501 from "./fixtures/bc1_501.json";
import glenn501 from "./fixtures/glenn_501.json";

afterEach(cleanup);

const THEMES = [["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const;
const PAGES = { vedder_1010: vedder1010, vedder_515: vedder515, vedder_615: vedder615, klwa_1010: klwa1010, bc1_501: bc1501, glenn_501: glenn501 } as unknown as Record<string, RegsPage>;
const TABS: readonly Tab[] = ["keep", "gear", "methods", "licence"];
/** every species code has a picture, as the app's FISH_IMAGES gives each code the manifest draws (apps/mobile; a
 *  ui-native test may not import it — layers.json) */
const IMAGES = new Proxy({}, { get: (_t, code) => [{ uri: `fish/${String(code)}.webp` }] }) as FishImages;

function drawTab(tab: Tab, page: RegsPage, palette: Palette): HTMLElement {
  // a tab the page does not draw (null: outside B.C.) mounts nothing — none of the frozen pages here is one
  const el = tab === "keep" ? <KeepTab keep={page.keep} dateLabel="Oct 10" palette={palette} actions={NO_ACTIONS} images={IMAGES} />
    : tab === "gear" ? page.gear && <GearTab vm={page.gear} palette={palette} actions={NO_ACTIONS} />
    : tab === "methods" ? page.methods && <MethodsTab vm={page.methods} palette={palette} actions={NO_ACTIONS} referenceOpen />
    : page.licence && <LicenceTab vm={page.licence} who={{ age: "adult", residency: "resident" }} palette={palette} actions={NO_ACTIONS} onWho={() => {}} />;
  return render(el).container;
}

/** The pin bar over vedder_1010's deepest golden bar (fixtures/golden-pins.json), its size line marked as v38 marks it. */
const PIN_LINES: PinLine[] = [
  { cls: "g two", text: "Trout and char · 2 a day from streams", sub: "(of 4 across Region 2)" },
  { cls: "c", text: "↳ of those, 1 over 50 cm (2 if one is a hatchery steelhead)", sub: null, tier: "size" },
];
const pinStore = (lines: readonly PinLine[]) =>
  ({ subscribe: () => () => {}, getLines: () => lines, getBoxes: () => new Map(), setLineHeight() {}, setStackHeight() {} }) as unknown as PinStore;

type Rule = TextRule | "border" | "surface" | "picture";
const RULES: readonly Rule[] = [...TEXT_RULES, "border", "surface", "picture"];
interface Drawn { faults: Record<Rule, string[]>; colours: Record<Tab, string[]>; roles: Record<string, number>; pics: number }

/** One page in one palette: every tab drawn and censused (each tab its own mount, unmounted after). */
function drawPage(page: RegsPage, palette: Palette): Drawn {
  const faults = Object.fromEntries(RULES.map((r) => [r, [] as string[]])) as Record<Rule, string[]>;
  const colours = {} as Record<Tab, string[]>, roles: Record<string, number> = {};
  let pics = 0;
  for (const tab of TABS) {
    const root = drawTab(tab, page, palette);
    const t = textFaults(root, palette, tab);
    for (const r of TEXT_RULES) faults[r].push(...t[r]);
    faults.border.push(...borderFaults(root, palette));
    faults.picture.push(...pictureFaults(root));
    for (const g of root.querySelectorAll<HTMLElement>('[data-testid="keep-group"], [data-testid="keep-plain"]')) faults.surface.push(...surfaceFaults(g, palette));
    colours[tab] = coloursOf(root, palette);
    for (const h of census(root, palette)) roles[h.role] = (roles[h.role] ?? 0) + 1;
    pics += root.querySelectorAll('[data-testid^="pic:"]').length;
    cleanup();
  }
  return { faults, colours, roles, pics };
}

describe.each(THEMES)("%s palette: every tab of the frozen pages", (_name, palette) => {
  const drawn = new Map<string, Drawn>();
  const of = (id: string) => { let d = drawn.get(id); if (!d) drawn.set(id, d = drawPage(PAGES[id]!, palette)); return d; };

  describe.each(Object.keys(PAGES))("%s", (id) => {
    it("≤ 7 / 5 / 5 / 4 text colours on Keep / Gear / Methods / Licence (rule 1)", () => expect(of(id).faults.count).toEqual([]));
    it("every text colour is a token (rule 2)", () => expect(of(id).faults.token).toEqual([]));
    it("status colours only on status words, and on rules only where keyed (rule 3)", () =>
      expect([of(id).faults.status, of(id).faults.border]).toEqual([[], []]));
    it("card / onAccent text only on filled buttons (rule 4)", () => expect(of(id).faults.filled).toEqual([]));
    it("numbers in ink, the size section's in its rule (rule 5)", () => expect(of(id).faults.number).toEqual([]));
    it("one tint, only on chips (rule 6)", () => expect(of(id).faults.tint).toEqual([]));
    it("one surface inside a group (rule 7)", () => expect(of(id).faults.surface).toEqual([]));
    it("pictures transparent (rule 8)", () => expect(of(id).faults.picture).toEqual([]));
    it("release is bold (rule 9)", () => expect(of(id).faults.release).toEqual([]));
  });

  it("the gate is not vacuous: status words, numbers, chips, filled buttons and pictures were all censused", () => {
    const all = Object.keys(PAGES).map(of);
    const sum = (k: string) => all.reduce((a, d) => a + (d.roles[k] ?? 0), 0);
    expect([sum("status") > 20, sum("number") > 20, sum("chip") > 10, sum("button") >= 12, all.reduce((a, d) => a + d.pics, 0) >= 12])
      .toEqual([true, true, true, true, true]);
  });

  it("the pin bar: one surface (the card's), its numbers in ink and the size line's in the rule", () => {
    const root = render(<PinBar store={pinStore(PIN_LINES)} palette={palette} />).container;
    const bar = root.querySelector<HTMLElement>('[data-testid="pin-bar"]')!;
    const t = textFaults(bar, palette, "keep");
    expect([surfaceFaults(bar, palette), t.number, t.token, t.status]).toEqual([[], [], [], []]);
    expect(census(bar, palette).filter((h) => h.role === "number").map((h) => [h.text, h.token])).toEqual([["2", "ink"], ["4", "ink"], ["1", "rule"]]);
  });
});

describe("the dark census, measured (light beside it): each tab's text colours, by token", () => {
  // the prototype's census at v38 (check.mjs r20g BOUND, tokens.json .census): Keep 7 (ink sub accent open closed
  // restricted rule), Gear 5, Methods 5, Licence 4 — the same in light and dark
  const want = (licenceFilled: "onAccent" | "card") => ({
    keep: ["accent", "closed", "ink", "open", "restricted", "rule", "sub"],
    gear: ["accent", "closed", "ink", "open", "sub"],
    methods: ["accent", "closed", "ink", "open", "sub"],
    licence: ["accent", "ink", licenceFilled, "sub"].sort(),
  });
  it("vedder_1010 in dark: 7 / 5 / 5 / 4, the licence's filled choice in `card` (dark onAccent is not the card)", () => {
    expect(drawPage(PAGES.vedder_1010!, DARK).colours).toEqual(want("card"));
  });
  it("vedder_1010 in light and CVD: the same counts (light onAccent IS the card, named onAccent)", () => {
    expect([drawPage(PAGES.vedder_1010!, LIGHT).colours, drawPage(PAGES.vedder_1010!, CVD).colours]).toEqual([want("onAccent"), want("onAccent")]);
  }, 20_000);   // two whole pages drawn (≈ 1.3 s alone; past the render project's 5 s default under the smoke's load)
});

describe("the gate reads the palette, never a hex (T47 swaps the status colours later)", () => {
  it("vedder_1010 redrawn with every status colour moved: no fault, and the moved colours are still found on status words", () => {
    // the colour-blind theme's trio standing in for any pick: values that are not the light theme's
    const moved: Palette = { ...LIGHT, open: CVD.open, closed: CVD.closed, restricted: CVD.restricted };
    const d = drawPage(PAGES.vedder_1010!, moved);
    expect(RULES.flatMap((r) => d.faults[r])).toEqual([]);
    expect(d.colours.keep).toEqual(["accent", "closed", "ink", "open", "restricted", "rule", "sub"]);
  });
});

/**
 * MUTANTS: each rule against a tree that breaks it, then mended. The fault strings are whole; colours are named through
 * the palette (`rgbOf`) so a token value change moves only the value, never the expectation's shape.
 */
describe("each rule fails its mutant", () => {
  const P = LIGHT;
  /** a colour no single palette key holds: a donor pin's identity hue (a ramp, not a text colour) */
  const STRANGER = LIGHT.donor[1];
  const T = (p: { c: string; id?: string; bg?: string; face?: string; s?: string }) =>
    <Text testID={p.id} style={{ color: p.c, backgroundColor: p.bg, fontFamily: p.face }}>{p.s ?? "x"}</Text>;
  const tree = (kids: React.ReactNode) => render(<View testID="keep-tab">{kids}</View>).container;

  it("roleOf: the nearest prefixed testID (own or an ancestor's); none is text", () => {
    const root = tree(<>
      <Text testID="st:open">Open <Text>today</Text></Text>
      <Text testID="n:big">2</Text><Text testID="chip:size">any size</Text>
      <View testID="btn:adult"><Text>16 or older</Text></View><Text testID="keep-x">plain</Text>
    </>);
    expect(census(root, P).map((h) => [h.text, h.role, h.tag])).toEqual([
      ["Open today", "status", "st:open"], ["today", "status", "st:open"], ["2", "number", "n:big"],
      ["any size", "chip", "chip:size"], ["16 or older", "button", "btn:adult"], ["plain", "text", null]]);
    expect(roleOf(root.querySelector("[data-testid='keep-x']")!)).toBe("text");
  });

  it("tokenOf: every key holding the colour (light onAccent and card share white), none for a stranger", () => {
    expect([tokenOf(P, rgbOf(P.card)).filter((k) => k === "card" || k === "onAccent").sort(), tokenOf(P, rgbOf(STRANGER))])
      .toEqual([["card", "onAccent"], []]);
  });

  it("1 count: an eighth colour on Keep", () => {
    const eight = [P.ink, P.sub, P.accent, P.rule, P.open, P.closed, P.restricted, P.onAccent] as const;
    const ids = [undefined, undefined, undefined, undefined, "st:open", "st:closed", "st:restricted", "btn:x"];
    const root = tree(eight.map((c, i) => <T key={i} c={c} id={ids[i]} />));
    expect(textFaults(root, P, "keep").count).toEqual(["keep: 8 text colours (ink, sub, accent, rule, open, closed, restricted, onAccent) > 7"]);
    expect(textFaults(root, P, "gear").count).toEqual(["gear: 8 text colours (ink, sub, accent, rule, open, closed, restricted, onAccent) > 5"]);
    cleanup();
    expect(textFaults(tree(eight.slice(0, 7).map((c, i) => <T key={i} c={c} id={ids[i]} />)), P, "keep").count).toEqual([]);
  });

  it("2 token: faint (banned as regs text, D3) and a stranger are no text token", () => {
    const root = tree(<><T c={P.faint} s="faint" /><T c={STRANGER} s="stranger" /><T c={P.ink} s="ink" /></>);
    expect(textFaults(root, P, "keep").token).toEqual([
      `keep: ${rgbOf(P.faint)} (faint) is not a text token, on “faint”`, `keep: ${rgbOf(STRANGER)} (no token) is not a text token, on “stranger”`]);
  });

  it("3 status: open / closed / restricted off a status word; on one it passes", () => {
    const bad = tree(<><T c={P.open} s="Open" /><T c={P.restricted} s="release" id="chip:size" /></>);
    expect(textFaults(bad, P, "keep").status).toEqual(["keep: open on a non-status text “Open”", "keep: restricted on a non-status chip “release”"]);
    cleanup();
    expect(textFaults(tree(<><T c={P.open} s="Open" id="st:open" /><T c={P.restricted} s="release" id="st:release" face="Archivo_700Bold" /></>), P, "keep").status).toEqual([]);
  });

  it("3 status (rules): a status-coloured border off a status element, unless keyed (ALLOW: the caveat's restricted rule)", () => {
    const rule = { borderLeftWidth: 3, borderLeftColor: P.restricted };
    const bad = tree(<View testID="keep-somewhere" style={rule}><T c={P.sub} s="Some times" /></View>);
    expect(borderFaults(bad, P)).toEqual(["a restricted left rule on keep-somewhere “Some times”"]);
    cleanup();
    expect(borderFaults(tree(<View testID="keep-caveat" style={rule}><Text testID="kicker:times" style={{ color: P.sub }}>Some times</Text></View>), P)).toEqual([]);
    expect(ALLOW.map((a) => [a.testID, a.rule, a.side ?? null])).toEqual([["keep-caveat", "border", "left"], ["keep-banner", "border", "top"], ["note:paper", "tint", null]]);
  });

  it("3 status (rules): a key excuses its own element and side only — never what it holds (P11: a closed rule on a banner chain row)", () => {
    const root = tree(<>
      <View testID="keep-banner" style={{ borderTopWidth: 1.5, borderTopColor: P.closed, borderLeftWidth: 2, borderLeftColor: P.closed }}>
        <Text testID="st:closed" style={{ color: P.closed }}>Closed</Text>
        <View style={{ borderTopWidth: 1, borderTopColor: P.closed }}><T c={P.ink} s="chain row" /></View>
      </View>
      <View testID="keep-caveat" style={{ borderLeftWidth: 3, borderLeftColor: P.restricted }}>
        <View style={{ borderLeftWidth: 3, borderLeftColor: P.open }}><T c={P.sub} s="caveat item" /></View>
      </View>
    </>);
    expect(borderFaults(root, P)).toEqual([
      "a closed left rule on keep-banner “Closedchain row”", "a closed top rule on div “chain row”", "a open left rule on div “caveat item”"]);
  });

  it("4 filled: card / onAccent text off a filled control", () => {
    expect(textFaults(tree(<T c={DARK.card} s="16 or older" />), DARK, "licence").filled).toEqual(["licence: card text off a filled control: “16 or older”"]);
    cleanup();
    expect(textFaults(tree(<View testID="btn:adult"><T c={DARK.card} s="16 or older" /></View>), DARK, "licence").filled).toEqual([]);
  });

  it("5 number: a number in sub; the rule colour only inside the size section", () => {
    const root = tree(<>
      <T c={P.sub} s="4" id="n:inline" />
      <T c={P.rule} s="1" id="n:big" />
      <Text testID="tier:size" style={{ color: P.rule }}>Of these, only <Text testID="n:big">1</Text></Text>
    </>);
    expect(textFaults(root, P, "keep").number).toEqual(["keep: a number not in ink: sub “4”", "keep: a number not in ink: rule “1”"]);
  });

  it("6 tint: tint behind a non-chip, and any other background; a chip and the keyed paper note pass", () => {
    const root = tree(<>
      <T c={P.ink} bg={P.tint} s="band" />
      <T c={P.ink} bg={P.line2} s="grey chip" id="chip:size" />
      <T c={P.ink} bg={P.tint} s="any size" id="chip:size" />
      <T c={P.ink} bg={P.tint} s="Carry your paper" id="note:paper" />
      <T c={P.ink} bg={P.card} s="on the card" />
    </>);
    expect(textFaults(root, P, "keep").tint).toEqual([
      "keep: tint behind a non-chip text “band”", "keep: a background that is not tint or card (line2) behind “grey chip”"]);
  });

  it("6 tint: the paper note's key excuses the note's own ground, not a tinted run inside it", () => {
    const root = tree(<Text testID="note:paper" style={{ color: P.ink, backgroundColor: P.tint }}>Carry <Text style={{ color: P.ink, backgroundColor: P.tint }}>your paper</Text></Text>);
    expect(textFaults(root, P, "licence").tint).toEqual(["licence: tint behind a non-chip text “your paper”"]);
  });

  it("7 one surface: a painted View inside a group; the card and a chip pass", () => {
    const root = tree(<View testID="keep-group">
      <View style={{ backgroundColor: P.card }}><T c={P.ink} /></View>
      <View style={{ backgroundColor: P.tint }}><T c={P.ink} s="a tinted band" /></View>
      <T c={P.ink} bg={P.tint} s="any size" id="chip:size" />
    </View>);
    expect(surfaceFaults(root.querySelector<HTMLElement>("[data-testid='keep-group']")!, P)).toEqual(["keep-group: div paints tint “a tinted band”"]);
  });

  it("8 pictures: a tile under or around a picture", () => {
    const root = tree(<>
      <View style={{ backgroundColor: P.tint }}><View testID="pic:RB"><View style={{ width: 80, height: 40, backgroundColor: P.line2 }} /></View></View>
      <View><View testID="pic:CT"><View style={{ width: 80, height: 40 }} /></View></View>
    </>);
    expect(pictureFaults(root)).toEqual([`pic:RB: its parent paints ${rgbOf(P.tint)}`, `pic:RB: div paints ${rgbOf(P.line2)}`]);
  });

  it("8 pictures: a fish with no picture (vedder_1010's Crayfish, Protected species) draws the prototype's silhouette — a pic:none, no ground", () => {
    const rows = PAGES.vedder_1010!.keep.otherFish!;
    const root = drawTab("keep", PAGES.vedder_1010!, P);
    const list = root.querySelector<HTMLElement>('[data-testid="keep-otherfish"]')!;
    expect([list.querySelectorAll('[data-testid^="pic:"]').length, list.querySelectorAll('[data-testid="pic:none"]').length])
      .toEqual([rows.length, rows.filter((r) => r.thumb === null).length]);
    expect(rows.filter((r) => r.thumb === null).length).toBeGreaterThan(0);
    expect(list.querySelector('[data-testid="pic:none"] svg')).not.toBeNull();
    expect(pictureFaults(list)).toEqual([]);
  });

  it("9 release: a release word under 700", () => {
    const root = tree(<><T c={P.restricted} s="release" id="st:release" face="Archivo_600SemiBold" /><T c={P.restricted} s="release" id="st:release" face="Archivo_700Bold" /></>);
    expect(textFaults(root, P, "keep").release).toEqual(["keep: release not bold (600) “release”"]);
  });
});
