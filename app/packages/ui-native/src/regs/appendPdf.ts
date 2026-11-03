/**
 * T354 — the cited page first, highlighted, WITHOUT reading the book: a PDF incremental update. A PDF may be extended
 * by appending objects and a new cross-reference section that points back (/Prev) at the old one; a viewer reads the
 * last section first, so the appended objects win. The append here is a few KB:
 *   - a copy of each cited page (its contents and resources are the book's own objects, by reference), with one
 *     /Highlight per box, as `highlightPdf` draws them;
 *   - the root /Pages again, the copies at the front of its /Kids, the book following unchanged;
 *   - an xref STREAM (the book's own xref is one) whose dictionary is the new trailer.
 * Everything it needs from the file is the release's index (`synopsis.append`, pipeline/deliver/release/synopsis.py),
 * so the phone copies the held file natively and writes these bytes on the end: no 24 MB on the JS heap (T351/T360).
 * Pure: index in, bytes out.
 */
import { highlightGeometry, type HighlightGeometry, type SourceBox } from "@app/core";
import type { SynopsisAppend } from "@app/data";

/** The index format this append reads; any other: the book opens the old way (`highlightPdf`). */
export const APPEND_V = 1;
/** As `highlightPdf`: yellow, multiplied without an appearance stream by pdfium and PDFKit. */
const COLOUR = "[1 0.9 0.2]";

const num = (x: number) => String(Math.round(x * 100) / 100);
const nums = (xs: readonly number[]) => `[${xs.map(num).join(" ")}]`;
const ref = ([n, g]: readonly [number, number]) => `${n} ${g} R`;

/**
 * The bytes to write after the shipped synopsis (`bytes` long, the size the index was cut from) so it opens on the
 * page(s) `boxes` name, highlighted; with no box that fits, on PDF page `page` (2 or later) unmarked. Null when there is
 * nothing to do or the index is not one this reads.
 */
export function appendHighlight(ix: SynopsisAppend, bytes: number, boxes: readonly SourceBox[], page?: number): Uint8Array | null {
  if (ix.v !== APPEND_V || !(bytes > ix.startxref)) return null;
  const n = ix.page.length;
  const marks = new Map<number, HighlightGeometry[]>();
  for (const b of boxes) {
    const g = highlightGeometry(b);
    if (!g || !(b.pdf >= 1 && b.pdf <= n)) continue;
    const at = marks.get(b.pdf);
    if (at) at.push(g); else marks.set(b.pdf, [g]);
  }
  if (!marks.size && page && page >= 2 && page <= n) marks.set(page, []);
  if (!marks.size) return null;

  let next = ix.size;
  const objs: { at: [number, number]; body: string }[] = [];
  const copies: number[] = [];
  for (const p of [...marks.keys()].sort((a, b) => a - b)) {
    const copy = next++;
    const annots = marks.get(p)!.map((g) => {
      const a = next++;
      objs.push({ at: [a, 0], body: `<</Type/Annot/Subtype/Highlight/Rect ${nums(g.rect)}/QuadPoints ${nums(g.quadPoints)}`
        + `/C ${COLOUR}/CA 1/F 4/Border [0 0 0]/T (Source)/P ${copy} 0 R>>` });
      return `${a} 0 R`;
    });
    objs.push({ at: [copy, 0], body: `<</Type/Page/Parent ${ref(ix.pages)}${ix.box}${ix.page[p - 1]}`
      + `${annots.length ? `/Annots [${annots.join(" ")}]` : ""}>>` });
    copies.push(copy);
  }
  const kids = ix.kids.trim().replace(/^\[/, "").replace(/\]$/, "").trim();
  objs.push({ at: ix.pages, body: `<</Type/Pages/Kids [${copies.map((c) => `${c} 0 R`).join(" ")} ${kids}]/Count ${ix.count + copies.length}>>` });

  // the objects, each at its offset in the whole file (the book's bytes come first)
  let text = "\n";
  const offsets = new Map<number, [number, number]>();          // object -> [offset, generation]
  for (const o of objs) {
    offsets.set(o.at[0], [bytes + text.length, o.at[1]]);
    text += `${o.at[0]} ${o.at[1]} obj\n${o.body}\nendobj\n`;
  }
  const xref = next++;
  const xrefAt = bytes + text.length;
  offsets.set(xref, [xrefAt, 0]);

  // /W [1 4 2]: type 1 (in use, at an offset), a 4-byte offset, a 2-byte generation; /Index runs of consecutive numbers
  const order = [...offsets.keys()].sort((a, b) => a - b);
  const runs: [number, number][] = [];
  for (const k of order) {
    const last = runs[runs.length - 1];
    if (last && last[0] + last[1] === k) last[1]++; else runs.push([k, 1]);
  }
  const data = new Uint8Array(order.length * 7);
  order.forEach((k, i) => {
    const [off, gen] = offsets.get(k)!;
    data.set([1, (off >>> 24) & 255, (off >>> 16) & 255, (off >>> 8) & 255, off & 255, (gen >>> 8) & 255, gen & 255], i * 7);
  });
  const dict = `<</Type/XRef/Size ${next}/W [1 4 2]/Index [${runs.map(([a, c]) => `${a} ${c}`).join(" ")}]`
    + `/Root ${ref(ix.root)}${ix.info ? `/Info ${ref(ix.info)}` : ""}${ix.id ? `/ID [<${ix.id[0]}><${ix.id[1]}>]` : ""}`
    + `/Prev ${ix.startxref}/Length ${data.length}>>`;
  const head = `${text}${xref} 0 obj\n${dict}\nstream\n`;
  const tail = `\nendstream\nendobj\nstartxref\n${xrefAt}\n%%EOF\n`;
  const out = new Uint8Array(head.length + data.length + tail.length);
  out.set(ascii(head), 0);
  out.set(data, head.length);
  out.set(ascii(tail), head.length + data.length);
  return out;
}

/** The append is PDF syntax and refs only: every char is one byte. */
function ascii(s: string): Uint8Array {
  const b = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) b[i] = s.charCodeAt(i) & 255;
  return b;
}
