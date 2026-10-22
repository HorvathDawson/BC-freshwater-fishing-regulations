/**
 * Render tests only (T40): the accessibility rules the Regulations view keeps, read off a react-native-web DOM the way a
 * screen reader reads it. Each audit returns the offenders (empty = clean), so a gate asserts `[]` on the real screen and
 * a mutation test asserts the offender a planted violation produces.
 *
 *   pressAudit       every press (role button / link / tab / radio) has a name and reaches 44 × 44 (box + hitSlop, read
 *                    from the `data-hitslop` that `RegsPress` / `hitProps` write — jsdom has no layout)
 *   labelAudit       no labelled CONTAINER swallows text: an element with aria-label has no text outside a press under it
 *   scaleAudit       font scaling (data-afs / data-mfm, written by the test's Text spy): no Text turns scaling off, and the
 *                    ones that must stop at 2× do
 *
 * THE ONE KEY (pressAudit): a link or ⓘ that is a run INSIDE a sentence (a Text nested in a Text: LineText's link / term /
 * book runs). React Native's Text has no `hitSlop` (react-native 0.86 Text.js: `pressRetentionOffset` only) and a nested
 * Text cannot hold a Pressable without leaving the sentence's line, so its target is its own glyph run on the line box —
 * the prototype's inline `<a>` too (check.mjs grew it with a `::before`). It is returned in `inline` (not `bad`), with its
 * name still required, so a gate can pin exactly which runs ride on that key.
 */
import { hitArea, MIN_HIT } from "../hit";

const PRESS = ["button", "link", "tab", "radio"] as const;
const PRESS_SEL = PRESS.map((r) => `[role="${r}"]`).join(", ");

/** hidden from a screen reader: inside aria-hidden or display:none */
function hidden(el: Element): boolean {
  for (let e: Element | null = el; e; e = e.parentElement) {
    if (e.getAttribute("aria-hidden") === "true") return true;
    if ((e as HTMLElement).style?.display === "none") return true;
  }
  return false;
}

/** a react-native-web Text: the outer one is `dir="auto"`, a nested one a span (or the element its role gives it) */
const isText = (e: Element) => e.getAttribute("dir") === "auto" || e.tagName === "SPAN";

/** a run inside a sentence: some ancestor is a Text */
export function isInlineRun(el: Element): boolean {
  for (let e = el.parentElement; e; e = e.parentElement) if (isText(e)) return true;
  return false;
}

/** the name a screen reader says: aria-label, else the text inside */
export function nameOf(el: Element): string {
  return (el.getAttribute("aria-label") ?? el.textContent ?? "").replace(/\s+/g, " ").trim();
}

export interface PressOffence { readonly role: string; readonly name: string; readonly why: string }

export function pressAudit(root: Element): { bad: PressOffence[]; inline: { role: string; name: string }[]; sized: number } {
  const bad: PressOffence[] = [], inline: { role: string; name: string }[] = [];
  let sized = 0;
  for (const el of root.querySelectorAll(PRESS_SEL)) {
    if (hidden(el)) continue;
    const role = el.getAttribute("role")!, name = nameOf(el);
    if (!name) { bad.push({ role, name, why: "no accessible name" }); continue; }
    if (isInlineRun(el)) { inline.push({ role, name }); continue; }
    sized++;
    const data = el.getAttribute("data-hitslop");
    const area = data === null ? null : hitArea(data);
    if (!area) { bad.push({ role, name, why: "no data-hitslop: not a RegsPress / hitProps press" }); continue; }
    if (area.w < MIN_HIT || area.h < MIN_HIT) bad.push({ role, name, why: `reaches ${area.w}×${area.h}` });
  }
  return { bad, inline, sized };
}

/**
 * A labelled container hides its children from VoiceOver / TalkBack: an element with aria-label may hold text only inside
 * a press (whose label IS its name) — or be a Text leaf whose label spells its own shown words in full (the header's place
 * label: the short form drawn, the full one said).
 */
export function labelAudit(root: Element): { label: string; text: string }[] {
  const out: { label: string; text: string }[] = [];
  for (const el of root.querySelectorAll("[aria-label]")) {
    if (hidden(el) || el.matches(PRESS_SEL)) continue;
    if (el.getAttribute("dir") === "auto" && el.children.length === 0) continue;   // a Text leaf
    const walker = el.ownerDocument.createTreeWalker(el, 4 /* NodeFilter.SHOW_TEXT */);
    for (let n = walker.nextNode(); n; n = walker.nextNode()) {
      const t = (n.textContent ?? "").trim();
      if (!t) continue;
      const p = n.parentElement;
      if (p && p !== el && p.closest(PRESS_SEL) && el.contains(p.closest(PRESS_SEL))) continue;
      if (p && hidden(p)) continue;
      out.push({ label: el.getAttribute("aria-label")!, text: t });
      break;
    }
  }
  return out;
}

/** every Text the spy saw under `root`: [testID or its words, allowFontScaling, maxFontSizeMultiplier] */
export function scaleOf(root: Element): { id: string; afs: string; mfm: string }[] {
  return [...root.querySelectorAll("[data-afs]")].map((e) => ({
    id: e.getAttribute("data-testid") ?? (e.textContent ?? "").trim().slice(0, 40),
    afs: e.getAttribute("data-afs")!, mfm: e.getAttribute("data-mfm") ?? "",
  }));
}
