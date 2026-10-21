/**
 * Render tests only: the drawn text lines of a react-native-web tree, read the way port/golden.mjs `lines()` reads the
 * prototype's DOM — each block its own line, inline runs one line with one space where two elements abut, whitespace
 * folded, hidden (display: none) text included as the golden includes it. The RN mapping: a Text (a `div dir=auto`) is one
 * line with its nested spans; a column View stacks its children's lines; a ROW View (inline style flex-direction: row) sits
 * its children side by side — the last line of one joins the first line of the next ("Lines" + "1 line" → "Lines 1 line").
 */
const norm = (s: string) => s.replace(/\s+/g, " ").trim();

type El = { readonly nodeType: number; readonly textContent: string | null; readonly childNodes: ArrayLike<El>;
  readonly tagName?: string; getAttribute?(k: string): string | null; readonly style?: { readonly flexDirection?: string } };

function linesOf(n: El): string[] {
  if (n.nodeType === 3) return [n.textContent ?? ""];
  if (n.nodeType !== 1) return [];
  if (n.getAttribute?.("dir") === "auto" || n.tagName === "SPAN") return [inline(n)];
  const kids = Array.from(n.childNodes).map(linesOf).filter((ls) => ls.some((l) => norm(l)));
  if (n.style?.flexDirection === "row")
    return kids.reduce<string[]>((acc, ls) => (acc.length ? [...acc.slice(0, -1), `${acc[acc.length - 1]} ${ls[0]}`, ...ls.slice(1)] : [...ls]), []);
  return kids.flat();
}

/** one Text's words: its runs in order, a space between two abutting elements (golden `gap`) */
function inline(n: El): string {
  let out = "", prevEl = false;
  for (const c of Array.from(n.childNodes)) {
    if (c.nodeType === 3) { out += c.textContent ?? ""; prevEl = false; }
    else if (c.nodeType === 1) { out += (prevEl ? " " : "") + inline(c); prevEl = true; }
  }
  return out;
}

export function domLines(root: unknown): string[] {
  return linesOf(root as El).map(norm).filter(Boolean);
}
