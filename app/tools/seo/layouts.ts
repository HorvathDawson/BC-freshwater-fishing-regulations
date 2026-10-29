/**
 * T269 — ONE MARKUP FOR EVERY LAYOUT. The phone, tablet and desktop renders of a water's pane are the same tree but for a
 * few nodes (the reading column's wrapper, the desktop's side column, a tab's padding): shipped side by side they were
 * three copies of the page (p95 21 KB gzip, over T196's 16 KB). Here they become one tree, each difference kept and
 * switched by the window's width with CSS only (no script: the page reads and looks right before the app runs):
 *
 *   a node only some layouts have          hidden on the others (`vh-<layout>`: display none)
 *   a wrapper only some layouts have       transparent on the others (`vc-<layout>`: display contents — its children are
 *                                          laid out as the parent's, which is what those layouts' own tree is)
 *   a node whose style differs by layout   its styles as per-layout classes (`LayoutStyles`), not inline
 *
 * Nodes match by tag, class and test id (text by its words), in order (longest common subsequence). A class that
 * differs is a different node (both kept, each hidden where it is not drawn). Anything else that differs on a matched
 * node (an ARIA state, a touch slop) is the first layout's: it is not drawn, and the app's own pane replaces it.
 */
import { LAYOUT_BREAKPOINTS, type LayoutClass } from "@app/ui";

type Attr = readonly [name: string, value: string | null];
interface El { readonly t: "el"; readonly tag: string; readonly attrs: Map<LayoutClass, readonly Attr[]>; children: Node[]; on: Set<LayoutClass>; contents: Set<LayoutClass> }
interface Tx { readonly t: "tx"; readonly text: string; on: Set<LayoutClass> }
type Node = El | Tx;

const VOID = new Set(["area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"]);
const ATTR = /\s([^\s=/>]+)(?:="([^"]*)")?/g;

/** React's server markup as a tree (well-formed by construction: every element closed, attributes double-quoted). */
export function parse(html: string, on: LayoutClass): Node[] {
  const root: El = { t: "el", tag: "#root", attrs: new Map(), children: [], on: new Set([on]), contents: new Set() };
  const stack: El[] = [root];
  const re = /<!--[\s\S]*?-->|<\/?[a-zA-Z][^>]*>|[^<]+/g;
  for (let m = re.exec(html); m; m = re.exec(html)) {
    const tok = m[0], top = stack[stack.length - 1]!;
    if (tok.startsWith("</")) {
      const tag = tok.slice(2, -1).trim();
      if (top.tag !== tag) throw new Error(`layouts: </${tag}> closes <${top.tag}>`);
      stack.pop();
    } else if (tok.startsWith("<!--") || !tok.startsWith("<")) {
      top.children.push({ t: "tx", text: tok, on: new Set([on]) });
    } else {
      const tag = /^<([a-zA-Z][\w:-]*)/.exec(tok)![1]!;
      const attrs: Attr[] = [];
      const body = tok.slice(tag.length + 1).replace(/\/?>$/, "");
      for (let a = ATTR.exec(body); a; a = ATTR.exec(body)) attrs.push([a[1]!, a[2] ?? null]);
      ATTR.lastIndex = 0;
      const el: El = { t: "el", tag, attrs: new Map([[on, attrs]]), children: [], on: new Set([on]), contents: new Set() };
      top.children.push(el);
      if (!VOID.has(tag.toLowerCase()) && !tok.endsWith("/>")) stack.push(el);
    }
  }
  if (stack.length !== 1) throw new Error(`layouts: <${stack[stack.length - 1]!.tag}> is never closed`);
  return root.children;
}

const attr = (as: readonly Attr[], k: string): string | null => as.find(([n]) => n === k)?.[1] ?? null;
const firstAttrs = (e: El): readonly Attr[] => e.attrs.values().next().value!;
const key = (n: Node): string => n.t === "tx" ? `#${n.text}` : `${n.tag}|${attr(firstAttrs(n), "class") ?? ""}|${attr(firstAttrs(n), "data-testid") ?? ""}`;

/** The matched pairs of two child lists (longest common subsequence by `key`), as index pairs in order. */
function lcs(a: readonly Node[], b: readonly Node[]): [number, number][] {
  const ka = a.map(key), kb = b.map(key), n = a.length, m = b.length;
  const d: Uint16Array[] = Array.from({ length: n + 1 }, () => new Uint16Array(m + 1));
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--)
    d[i]![j] = ka[i] === kb[j] ? d[i + 1]![j + 1]! + 1 : Math.max(d[i + 1]![j]!, d[i]![j + 1]!);
  const out: [number, number][] = [];
  for (let i = 0, j = 0; i < n && j < m;) {
    if (ka[i] === kb[j]) { out.push([i, j]); i++; j++; } else if (d[i + 1]![j]! >= d[i]![j + 1]!) i++; else j++;
  }
  return out;
}

function mergeNode(a: Node, b: Node, la: ReadonlySet<LayoutClass>, lb: ReadonlySet<LayoutClass>): Node {
  if (a.t === "tx" || b.t === "tx") return { ...a, on: new Set([...a.on, ...b.on]) };
  const attrs = new Map([...a.attrs, ...b.attrs]);
  return { t: "el", tag: a.tag, attrs, children: mergeLists(a.children, b.children, la, lb), on: new Set([...a.on, ...b.on]),
           contents: new Set([...a.contents, ...b.contents]) };
}

/**
 * Two child lists as one — `a` drawn by the layouts `la`, `b` by `lb` — matched nodes merged, a wrapper one side lacks
 * made transparent on that side's layouts, the rest kept (each hidden where it is not drawn).
 */
function mergeLists(a: readonly Node[], b: readonly Node[], la: ReadonlySet<LayoutClass>, lb: ReadonlySet<LayoutClass>): Node[] {
  const pairs = lcs(a, b), out: Node[] = [];
  let i = 0, j = 0;
  for (const [pa, pb] of pairs) {
    out.push(...gap(a.slice(i, pa), b.slice(j, pb), la, lb), mergeNode(a[pa]!, b[pb]!, la, lb));
    i = pa + 1; j = pb + 1;
  }
  out.push(...gap(a.slice(i), b.slice(j), la, lb));
  return out;
}

/**
 * The unmatched runs between two matches. A WRAPPER one side draws around nodes the other side draws bare (the tablet's
 * reading column round the page) is found by its children: most of them, or most of the other side's run (WRAP_SHARE),
 * match that run. (Any choice is drawn right — the wrapper is transparent where it was not, what only it holds is
 * hidden there — so this only decides how much is shipped once instead of twice.)
 * Then that run and the wrapper's children merge inside the wrapper, which is transparent on the other side's layouts,
 * and what is before and after it is matched the same way. Whatever is left is drawn only where it was.
 */
const WRAP_SHARE = 0.6;
function gap(xa: readonly Node[], xb: readonly Node[], la: ReadonlySet<LayoutClass>, lb: ReadonlySet<LayoutClass>): Node[] {
  if (!xa.length || !xb.length) return [...xa, ...xb];
  const find = (ws: readonly Node[], other: readonly Node[]) => {
    for (let w = 0; w < ws.length; w++) {
      const e = ws[w]!;
      if (e.t !== "el" || !e.children.length) continue;
      const m = lcs(other, e.children);
      if (m.length && m.length >= Math.ceil(WRAP_SHARE * Math.min(e.children.length, other.length))) return { w, from: m[0]![0], to: m[m.length - 1]![0] + 1 };
    }
    return null;
  };
  const inB = find(xb, xa);
  if (inB) {
    const w = xb[inB.w] as El;
    return [...gap(xa.slice(0, inB.from), xb.slice(0, inB.w), la, lb),
            { ...w, children: mergeLists(xa.slice(inB.from, inB.to), w.children, la, lb), on: new Set([...w.on, ...la]), contents: new Set([...w.contents, ...la]) },
            ...gap(xa.slice(inB.to), xb.slice(inB.w + 1), la, lb)];
  }
  const inA = find(xa, xb);
  if (inA) {
    const w = xa[inA.w] as El;
    return [...gap(xa.slice(0, inA.w), xb.slice(0, inA.from), la, lb),
            { ...w, children: mergeLists(w.children, xb.slice(inA.from, inA.to), la, lb), on: new Set([...w.on, ...lb]), contents: new Set([...w.contents, ...lb]) },
            ...gap(xa.slice(inA.w + 1), xb.slice(inA.to), la, lb)];
  }
  return [...xa, ...xb];
}

/** Every (layout, style) a merged node wears by layout, each one class (`#prerender .ls<n>` under that layout's width). */
export class LayoutStyles {
  private readonly styles = new Map<string, number>();
  name(layout: LayoutClass, style: string): string {
    const k = `${layout}|${style}`;
    let n = this.styles.get(k);
    if (n === undefined) this.styles.set(k, (n = this.styles.size));
    return `ls${n}`;
  }
  css(): string {
    const by = new Map<LayoutClass, string[]>();
    for (const [k, n] of this.styles) {
      const [l, s] = [k.slice(0, k.indexOf("|")) as LayoutClass, k.slice(k.indexOf("|") + 1)];
      by.set(l, [...(by.get(l) ?? []), `#prerender .ls${n}{${s}}`]);
    }
    return [...by].map(([l, rules]) => `@media ${MEDIA[l]}{${rules.join("")}}`).join("");
  }
}

const { tablet, desktop } = LAYOUT_BREAKPOINTS;
/** The window widths of each layout class, as `layoutClassFor` draws them. */
export const MEDIA: Readonly<Record<LayoutClass, string>> = {
  phone: `(max-width:${tablet - 0.02}px)`,
  tablet: `(min-width:${tablet}px) and (max-width:${desktop - 0.02}px)`,
  desktop: `(min-width:${desktop}px)`,
};
/** Hiding a node, and making a wrapper transparent, per layout. */
export const layoutCss = (): string => (Object.keys(MEDIA) as LayoutClass[]).map((l) =>
  `@media ${MEDIA[l]}{#prerender .vh-${l}{display:none!important}#prerender .vc-${l}{display:contents!important}}`).join("");

function render(nodes: readonly Node[], all: readonly LayoutClass[], styles: LayoutStyles): string {
  let s = "";
  for (const n of nodes) {
    const hidden = all.filter((l) => !n.on.has(l));
    if (n.t === "tx") {
      s += hidden.length && !n.text.startsWith("<!--") ? `<span class="${hidden.map((l) => `vh-${l}`).join(" ")}">${n.text}</span>` : n.text;
      continue;
    }
    const mine = all.filter((l) => n.on.has(l) && n.attrs.has(l));
    const base = n.attrs.get(mine[0]!)!;
    const styleOf = (l: LayoutClass) => attr(n.attrs.get(l)!, "style");
    const varied = new Set(mine.map(styleOf)).size > 1;
    const extra = [...hidden.map((l) => `vh-${l}`), ...[...n.contents].filter((l) => !hidden.includes(l)).map((l) => `vc-${l}`),
                   ...(varied ? mine.flatMap((l) => (styleOf(l) ? [styles.name(l, styleOf(l)!)] : [])) : [])];
    let attrs = base.filter(([k]) => !(varied && k === "style"));
    if (extra.length) {
      const cls = attr(attrs, "class");
      attrs = cls === null ? [...attrs, ["class", extra.join(" ")]] : attrs.map(([k, v]) => (k === "class" ? [k, `${v} ${extra.join(" ")}`] : [k, v]));
    }
    s += `<${n.tag}${attrs.map(([k, v]) => (v === null ? ` ${k}` : ` ${k}="${v}"`)).join("")}>`;
    if (VOID.has(n.tag.toLowerCase())) continue;
    s += `${render(n.children, all, styles)}</${n.tag}>`;
  }
  return s;
}

/** The layouts' renders (in `LAYOUTS` order) as one markup. Throws on markup it cannot read. */
export function mergeLayouts(byLayout: ReadonlyMap<LayoutClass, string>, styles: LayoutStyles): string {
  const all = [...byLayout.keys()];
  let tree: Node[] | null = null;
  const done = new Set<LayoutClass>();
  for (const l of all) {
    const t = parse(byLayout.get(l)!, l);
    tree = tree ? mergeLists(tree, t, done, new Set([l])) : t;
    done.add(l);
  }
  return render(tree ?? [], all, styles);
}
