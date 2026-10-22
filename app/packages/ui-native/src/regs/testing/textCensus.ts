/**
 * Render tests only (T40): the TEXT CENSUS of a react-native-web tree — every element that draws words of its own (a
 * direct, non-empty text node), with its type resolved the way the browser cascades it: font size, family, weight,
 * transform, colour and decoration read from the element's inline style and, where it sets none, its nearest ancestor's
 * (a nested Text sets only what differs — LineText's numeral sets a family and a size; jsdom does not inherit).
 *
 * The weight is the FACE's (type.ts: the weight is in the family name, `Archivo_700Bold` → 700), else an explicit
 * `font-weight`, else 400. Hidden text (display: none, opacity 0) is not counted — the prototype's census reads visible
 * boxes only. `skip` drops whole subtrees by test id (the strip's bar, months and legend: check.mjs `r20c-style-tokens`
 * leaves out `.sbar, .smonths, .slegend, svg`).
 *
 * T39 ships the palette census (`testing/census.ts`); this one is the type census `r20c-style-tokens` reads. Both read
 * the same rendered DOM.
 */
export interface TextHit {
  readonly el: Element;
  readonly text: string;
  /** px, one decimal (check.mjs: `parseFloat(fontSize).toFixed(1)`) */
  readonly size: string;
  readonly weight: number;
  readonly family: string;
  readonly transform: string;
  readonly color: string;
  readonly underline: boolean;
  /** the nearest test id on the element or above it ("" when none) */
  readonly testID: string;
}

type Styled = Element & { readonly style?: CSSStyleDeclaration };

function cascade(el: Element, prop: keyof CSSStyleDeclaration & string): string {
  for (let e: Element | null = el; e; e = e.parentElement) {
    const v = (e as Styled).style?.[prop];
    if (typeof v === "string" && v !== "" && v !== "inherit") return v;
  }
  return "";
}

function invisible(el: Element): boolean {
  for (let e: Element | null = el; e; e = e.parentElement) {
    const st = (e as Styled).style;
    if (st?.display === "none" || st?.opacity === "0") return true;
  }
  return false;
}

/** the face's weight: `Archivo_700Bold` → 700; a family with no weight in its name → the style's font-weight, else 400 */
export function weightOf(family: string, fontWeight: string): number {
  const m = /_(\d{3})/.exec(family);
  if (m) return Number(m[1]);
  const w = Number(fontWeight);
  return Number.isFinite(w) && w > 0 ? w : fontWeight === "bold" ? 700 : 400;
}

export function textCensus(root: Element, skip: readonly string[] = []): TextHit[] {
  const out: TextHit[] = [];
  const skipped = (el: Element) => skip.some((id) => el.closest(`[data-testid="${id}"]`) && root.contains(el.closest(`[data-testid="${id}"]`)));
  for (const el of [root, ...root.querySelectorAll("*")]) {
    const own = [...el.childNodes].filter((n) => n.nodeType === 3 && (n.textContent ?? "").trim()).map((n) => n.textContent!.trim());
    if (!own.length || invisible(el) || skipped(el)) continue;
    const family = cascade(el, "fontFamily").replace(/["']/g, "");
    const size = parseFloat(cascade(el, "fontSize"));
    out.push({
      el, text: own.join(" "),
      size: Number.isFinite(size) ? size.toFixed(1) : "?",
      weight: weightOf(family, cascade(el, "fontWeight")),
      family,
      transform: cascade(el, "textTransform") || "none",
      color: cascade(el, "color"),
      underline: /underline/.test(cascade(el, "textDecorationLine") || cascade(el, "textDecoration")),
      testID: el.closest("[data-testid]")?.getAttribute("data-testid") ?? "",
    });
  }
  return out;
}
