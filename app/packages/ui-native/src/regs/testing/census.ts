/**
 * T39 — THE PALETTE CENSUS over a rendered regs tree (test-only; T40 imports it too).
 *
 * The prototype's round-20g/20h/20i colour rules (check.mjs `r20g-palette`, `r20h-one-surface`, `r20h-picture-surface`,
 * `r20i-glare`) restated over react-native-web's DOM in jsdom: RN has no classes, so an element's ROLE comes from the
 * testID convention every regs component follows (contracts.md §2.7) — `st:<status>` a status word, `n:<size>` a number,
 * `chip:<kind>` a chip, `btn:<name>` a filled control; anything else is text. jsdom has no layout, so the census reads
 * what RN-web writes INLINE (a colour inherits through the cascade, which jsdom computes), never a size: the prototype's
 * "band-sized" threshold (> 200 × 40 px) becomes "any element", which is stricter, never looser.
 *
 * Every colour is named by the PALETTE it was drawn with (`tokenOf`), never by a hex: a later palette swap (T47) moves
 * the values and the rules read the new ones.
 */
import type { Tab } from "@app/core";
import type { Palette } from "../../theme";

export type Role = "status" | "number" | "chip" | "button" | "text";

export interface CensusHit {
  el: HTMLElement; text: string; color: string; token: keyof Palette | null;
  /** every Palette key holding this colour (several share a value: LIGHT onAccent == card) — rules accept any of them */
  tokens: readonly (keyof Palette)[];
  bg: string | null; bgToken: keyof Palette | null; weight: number; size: number; role: Role;
  /** the testID that gave the role (`st:release`), or null for plain text */
  tag: string | null;
}

/** At most this many distinct text colours per tab, light and dark (check.mjs r20g BOUND at v38: UR7 (owner B) added the
 *  size section's `rule` to Keep, 6 → 7; the ticket's 6 / 5 / 5 / 4 is the v33 bound). */
export const MAX_COLOURS: Readonly<Record<Tab, number>> = { keep: 7, gear: 5, methods: 5, licence: 4 };

/** The tokens a text colour may be (r20g: ink, sub, accent, open, closed, restricted, on-accent, card; v38 + rule). */
export const TEXT_TOKENS: readonly (keyof Palette)[] = ["ink", "sub", "accent", "open", "closed", "restricted", "rule", "onAccent", "card"];
/** The status trio: only on a status word. */
export const STATUS_TOKENS: readonly (keyof Palette)[] = ["open", "closed", "restricted"];

/**
 * KEYED EXCEPTIONS — each a testID, the rule it is excepted from (a border: on which side), and why. Only the keyed
 * element itself is excepted, never what it holds. Nothing else is excepted; a new entry needs a
 * reason the prototype gives (cite its CSS / check), never "the gate failed".
 */
export const ALLOW: readonly { testID: string; rule: "border" | "tint"; side?: "top" | "left"; reason: string }[] = [
  { testID: "keep-caveat", rule: "border", side: "left",
    reason: "T42: the caveat block's 3 px left rule is `restricted` on purpose (prototype .caveats border-left: it marks "
      + "'restricted here at some times, sides or anglers'); its label (testID kicker:<kind>) stays sub" },
  { testID: "keep-banner", rule: "border", side: "top",
    reason: "the closed banner's 1.5 px top rule is `closed` (prototype .banner border-top), over its own st:closed words" },
  { testID: "note:paper", rule: "tint",
    reason: "the licence paper note (prototype .gtopic .paper: background --wash, which IS --tint at v38) — a note "
      + "box outside any card, which r20g/r20h never measured; owner question in the T39 report" },
];

const ROLE_OF_PREFIX: readonly [string, Role][] = [["st:", "status"], ["n:", "number"], ["chip:", "chip"], ["btn:", "button"]];

const testIdOf = (el: Element): string | null => el.getAttribute("data-testid");

/** The nearest element (itself or an ancestor, up to `stop`) whose testID carries a role prefix. */
function tagged(el: Element | null, stop?: Element): Element | null {
  for (let e: Element | null = el; e; e = e.parentElement) {
    const id = testIdOf(e);
    if (id && ROLE_OF_PREFIX.some(([p]) => id.startsWith(p))) return e;
    if (e === stop) break;
  }
  return null;
}

/** role from the testID convention every regs component follows (T23–T35): testID prefixes "st:" status word, "n:"
 *  number, "chip:", "btn:" — the element's own or its nearest ancestor's (a nested run inherits its Text's role). */
export function roleOf(el: HTMLElement): Role {
  const t = tagged(el);
  if (!t) return "text";
  const id = testIdOf(t)!;
  return ROLE_OF_PREFIX.find(([p]) => id.startsWith(p))![1];
}

/** "#RRGGBB" → "rgb(r, g, b)" (jsdom's computed form); anything else unchanged. */
export function rgbOf(hex: string): string {
  const m = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex.trim());
  return m ? `rgb(${parseInt(m[1]!, 16)}, ${parseInt(m[2]!, 16)}, ${parseInt(m[3]!, 16)})` : hex;
}

/** Reverse map: an rgb() string → the Palette key(s) holding that colour (several keys may share one value: v33 sub ==
 *  faint, LIGHT onAccent == card). Only the palette's single colours (not the ramps, the radii or the shadow). */
export function tokenOf(palette: Palette, rgb: string): (keyof Palette)[] {
  const want = rgb.replace(/\s+/g, "").toLowerCase();
  return (Object.keys(palette) as (keyof Palette)[]).filter((k) => {
    const v = palette[k];
    return typeof v === "string" && rgbOf(v).replace(/\s+/g, "").toLowerCase() === want;
  });
}

/** The preferred name among keys sharing a value: the text tokens first, in TEXT_TOKENS order. */
function nameOf(keys: readonly (keyof Palette)[]): keyof Palette | null {
  return TEXT_TOKENS.find((t) => keys.includes(t)) ?? keys[0] ?? null;
}
/** A background's name: the surfaces first (white is both `card` and LIGHT `onAccent`; as a ground it is the card). */
const SURFACES: readonly (keyof Palette)[] = ["card", "tint"];
function groundOf(keys: readonly (keyof Palette)[]): keyof Palette | null {
  return SURFACES.find((t) => keys.includes(t)) ?? nameOf(keys);
}

const clear = (c: string | null | undefined) => !c || c === "transparent" || /^rgba\(0, 0, 0, 0\)$/.test(c);

/** display: none on the element or an ancestor (react-native-web writes it inline; the prototype's census skips it). */
function hidden(el: Element, root: Element): boolean {
  for (let e: Element | null = el; e; e = e.parentElement) {
    if ((e as HTMLElement).style?.display === "none") return true;
    if (e === root) break;
  }
  return false;
}

/** The weight of a face: RN names it in the family ("Archivo_700Bold"); a computed numeric weight otherwise; else 400. */
function weightOf(cs: CSSStyleDeclaration): number {
  const m = /_(\d{3})/.exec(cs.fontFamily);
  if (m) return Number(m[1]);
  const w = Number(cs.fontWeight);
  return Number.isFinite(w) && w > 0 ? w : cs.fontWeight === "bold" ? 700 : 400;
}

/** Every element with its own non-empty text node under `root`, with its inline style (react-native-web renders inline
 *  styles in jsdom — FishSpinner.test.tsx reads `.style.backgroundColor` the same way). */
export function census(root: HTMLElement, palette: Palette): CensusHit[] {
  const out: CensusHit[] = [];
  for (const el of [root, ...root.querySelectorAll<HTMLElement>("*")]) {
    if (el.closest("svg")) continue;
    if (![...el.childNodes].some((n) => n.nodeType === 3 && (n.textContent ?? "").trim())) continue;
    if (hidden(el, root)) continue;
    const cs = getComputedStyle(el);
    const tokens = tokenOf(palette, cs.color);
    const bg = clear(cs.backgroundColor) ? null : cs.backgroundColor;
    const t = tagged(el);
    out.push({
      el, text: (el.textContent ?? "").replace(/\s+/g, " ").trim(), color: cs.color, token: nameOf(tokens), tokens,
      bg, bgToken: bg ? groundOf(tokenOf(palette, bg)) : null, weight: weightOf(cs), size: parseFloat(cs.fontSize) || 0,
      role: roleOf(el), tag: t ? testIdOf(t) : null,
    });
  }
  return out;
}

const say = (h: CensusHit) => `“${h.text.slice(0, 40)}”`;
/** A key excuses its OWN element (and, for a border, its one side) — never a descendant: a status rule on a banner chain
 *  row, or a tinted run inside the paper note, is still a fault. */
const allowed = (el: Element, rule: "border" | "tint", side?: string) =>
  ALLOW.some((a) => a.rule === rule && testIdOf(el) === a.testID && (rule !== "border" || a.side === side));

/**
 * The r20g / r20i rules over one tab's tree (rules 1–6 and 9 of the ticket). Each fault is one string; `[]` is a pass.
 *   1 count     ≤ MAX_COLOURS[tab] distinct text colours
 *   2 token     every text colour is a TEXT_TOKENS token
 *   3 status    open / closed / restricted text only on a status word (role status)
 *   4 filled    card / onAccent text only on a filled control (role button)
 *   5 number    every number in ink (the size section's in its `rule`, UR7 owner B: under testID tier:size or pin:size)
 *   6 tint      a text background is `tint` only on a chip, and no other background but the card's
 *   9 release   every st:release word is bold (≥ 700, r20i-glare)
 */
export type TextRule = "count" | "token" | "status" | "filled" | "number" | "tint" | "release";
export const TEXT_RULES: readonly TextRule[] = ["count", "token", "status", "filled", "number", "tint", "release"];

/** The text rules over one tab's tree, each rule's faults apart (the render test asserts each rule on its own). */
export function textFaults(root: HTMLElement, palette: Palette, tab: Tab): Record<TextRule, string[]> {
  const hits = census(root, palette);
  const bad: Record<TextRule, string[]> = { count: [], token: [], status: [], filled: [], number: [], tint: [], release: [] };
  const colours = [...new Set(hits.map((h) => h.color))];
  if (colours.length > MAX_COLOURS[tab])
    bad.count.push(`${tab}: ${colours.length} text colours (${colours.map((c) => nameOf(tokenOf(palette, c)) ?? c).join(", ")}) > ${MAX_COLOURS[tab]}`);
  for (const h of hits) {
    if (!h.tokens.some((t) => TEXT_TOKENS.includes(t)))
      bad.token.push(`${tab}: ${h.color} (${h.tokens.join("|") || "no token"}) is not a text token, on ${say(h)}`);
    if (h.tokens.some((t) => STATUS_TOKENS.includes(t)) && h.role !== "status")
      bad.status.push(`${tab}: ${h.token} on a non-status ${h.role} ${say(h)}`);
    if (h.tokens.some((t) => t === "card" || t === "onAccent") && h.role !== "button")
      bad.filled.push(`${tab}: ${h.token} text off a filled control: ${say(h)}`);
    if (h.role === "number") {
      const size = h.el.closest('[data-testid="tier:size"], [data-testid="pin:size"]') !== null;
      const want: keyof Palette = size ? "rule" : "ink";
      if (!h.tokens.includes(want)) bad.number.push(`${tab}: a number not in ${want}: ${h.token ?? h.color} ${say(h)}`);
    }
    if (h.bg !== null) {
      if (h.bgToken === "tint") { if (h.role !== "chip" && !allowed(h.el, "tint")) bad.tint.push(`${tab}: tint behind a non-chip ${h.role} ${say(h)}`); }
      else if (h.bgToken !== "card") bad.tint.push(`${tab}: a background that is not tint or card (${h.bgToken ?? h.bg}) behind ${say(h)}`);
    }
    if (h.tag === "st:release" && h.weight < 700) bad.release.push(`${tab}: release not bold (${h.weight}) ${say(h)}`);
  }
  return bad;
}

/** Every text rule's faults in one list (`[]` is a pass). */
export function paletteFaults(root: HTMLElement, palette: Palette, tab: Tab): string[] {
  const f = textFaults(root, palette, tab);
  return TEXT_RULES.flatMap((r) => f[r]);
}

const SIDES = ["Top", "Right", "Bottom", "Left"] as const;

/** Rule 3 for rules (borders): a status colour on a border only on a status element or a keyed exception (ALLOW). */
export function borderFaults(root: HTMLElement, palette: Palette): string[] {
  const bad: string[] = [];
  for (const el of [root, ...root.querySelectorAll<HTMLElement>("*")]) {
    if (el.closest("svg") || hidden(el, root)) continue;
    const s = el.style;
    for (const side of SIDES) {
      const w = parseFloat(s.getPropertyValue(`border-${side.toLowerCase()}-width`) || "0"), c = s.getPropertyValue(`border-${side.toLowerCase()}-color`);
      if (!(w > 0) || !c) continue;
      const t = tokenOf(palette, getComputedStyle(el).getPropertyValue(`border-${side.toLowerCase()}-color`) || c);
      if (t.some((k) => STATUS_TOKENS.includes(k)) && roleOf(el) !== "status" && !allowed(el, "border", side.toLowerCase()))
        bad.push(`a ${nameOf(t)} ${side.toLowerCase()} rule on ${testIdOf(el) ?? el.tagName.toLowerCase()} “${(el.textContent ?? "").slice(0, 30)}”`);
    }
  }
  return bad;
}

/** Rule 7 (r20h-one-surface): inside a group card and the pin bar, no element paints a background but the card's —
 *  except a chip (prototype `.szc, .chgi`). `container` is the group's root (keep-group / keep-plain) or the pin bar. */
export function surfaceFaults(container: HTMLElement, palette: Palette): string[] {
  const bad: string[] = [];
  for (const el of [container, ...container.querySelectorAll<HTMLElement>("*")]) {
    const c = el.style.backgroundColor;
    if (clear(c) || el.closest("svg")) continue;
    if (tokenOf(palette, getComputedStyle(el).backgroundColor).includes("card")) continue;
    if (roleOf(el) === "chip") continue;
    bad.push(`${testIdOf(container)}: ${testIdOf(el) ?? el.tagName.toLowerCase()} paints ${groundOf(tokenOf(palette, getComputedStyle(el).backgroundColor)) ?? c} “${(el.textContent ?? "").slice(0, 30)}”`);
  }
  return bad;
}

/** Rule 8 (r20h-picture-surface, D4 a): a fish picture (`testID="pic:<code>"`), everything in it and its parent paint no
 *  background of their own. */
export function pictureFaults(root: HTMLElement): string[] {
  const bad: string[] = [];
  for (const pic of root.querySelectorAll<HTMLElement>('[data-testid^="pic:"]')) {
    const id = testIdOf(pic)!;
    for (const el of [pic.parentElement, pic, ...pic.querySelectorAll<HTMLElement>("*")])
      if (el && !clear(el.style.backgroundColor)) bad.push(`${id}: ${el === pic.parentElement ? "its parent" : testIdOf(el) ?? el.tagName.toLowerCase()} paints ${el.style.backgroundColor}`);
  }
  return bad;
}

/** Per-tab: the distinct text colours, each by its token name (the D tool's printout and the render test's report). */
export function coloursOf(root: HTMLElement, palette: Palette): string[] {
  return [...new Set(census(root, palette).map((h) => h.token ?? h.color))].sort();
}
