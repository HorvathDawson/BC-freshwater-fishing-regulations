// T225 DFO port: `BeautifulSoup(markup, "html.parser")` — Python 3.13's html.parser tokenizer (convert_charrefs=False,
// as bs4 sets it) feeding bs4 4.15's tree builder — and the few Tag methods the feed reads.
//
// WHY NOT parse5: the DFO pages are malformed (unclosed cells, an unterminated `<!--`), and the feed's rows depend on
// exactly how html.parser + bs4 nest them: bs4 closes nothing implicitly (an unclosed <td> NESTS the rows after it —
// parse.py's `_row_tags` exists because of that), where an HTML5 parser would re-parent them. parse5 gives a different
// tree on exactly the pages that matter, so this is a port of the reference parser, not a substitute.
import { CP1252, ENTITIES, NONCHARACTERS } from "./entities";
import { strip } from "./py";

export type StrKind = "text" | "comment" | "doctype" | "decl" | "cdata" | "pi" | "script" | "style" | "template" | "rt" | "rp";

export class Str {
  parent: Tag | null = null;
  constructor(
    public data: string,
    public kind: StrKind = "text",
  ) {}
}

export class Tag {
  parent: Tag | null = null;
  children: Node[] = [];
  constructor(
    public name: string,
    public attrs: Record<string, string> = {},
  ) {}

  get(attr: string): string | undefined {
    return Object.prototype.hasOwnProperty.call(this.attrs, attr) ? this.attrs[attr] : undefined;
  }

  /** Pre-order descendants (bs4 `.descendants`). */
  *descendants(): Generator<Node> {
    const stack: Node[] = [...this.children].reverse();
    while (stack.length) {
      const n = stack.pop()!;
      yield n;
      if (n instanceof Tag) for (let i = n.children.length - 1; i >= 0; i--) stack.push(n.children[i]!);
    }
  }

  findAll(names: string | readonly string[], recursive = true): Tag[] {
    const want = typeof names === "string" ? [names] : names;
    if (!recursive) return this.children.filter((c): c is Tag => c instanceof Tag && want.includes(c.name));
    const out: Tag[] = [];
    for (const n of this.descendants()) if (n instanceof Tag && want.includes(n.name)) out.push(n);
    return out;
  }

  find(names: string | readonly string[]): Tag | null {
    const want = typeof names === "string" ? [names] : names;
    for (const n of this.descendants()) if (n instanceof Tag && want.includes(n.name)) return n;
    return null;
  }

  findParent(name: string): Tag | null {
    let p = this.parent;
    while (p) {
      if (p.name === name) return p;
      p = p.parent;
    }
    return null;
  }

  /** get_text(sep, strip=True): the NavigableString / CData descendants, each stripped, empties dropped. */
  getText(sep = "", doStrip = false): string {
    // the tag's interesting_string_types: a <script>/<style>/<template>/<rt>/<rp> reads its own container strings,
    // every other tag NavigableString + CData (never comments, nor the text inside a <script>)
    const own = STRING_CONTAINERS[this.name];
    const out: string[] = [];
    for (const n of this.descendants()) {
      if (!(n instanceof Str) || (own ? n.kind !== own : n.kind !== "text" && n.kind !== "cdata")) continue;
      const t = doStrip ? strip(n.data) : n.data;
      if (doStrip && !t) continue;
      out.push(t);
    }
    return out.join(sep);
  }

  decompose(): void {
    if (!this.parent) return;
    const i = this.parent.children.indexOf(this);
    if (i >= 0) this.parent.children.splice(i, 1);
    this.parent = null;
  }

  replaceWith(n: Node): void {
    if (!this.parent) return;
    const i = this.parent.children.indexOf(this);
    this.parent.children[i] = n;
    n.parent = this.parent;
    this.parent = null;
  }

  /** A deep copy. */
  clone(): Tag {
    const t = new Tag(this.name, { ...this.attrs });
    for (const c of this.children) {
      const k = c instanceof Tag ? c.clone() : new Str(c.data, c.kind);
      k.parent = t;
      t.children.push(k);
    }
    return t;
  }

  /** What `BeautifulSoup(str(tag), "html.parser")` rebuilds: a copy in which ADJACENT TEXT NODES ARE ONE — the
   * serialised text runs together and the parser reads it as one string ("Coho<" + "/td>" left apart by a stray end
   * tag become "Coho</td>"), whitespace-only runs collapsed to "\n" / " " as bs4 does outside <pre>/<textarea>. */
  reparsed(): Tag {
    const t = this.clone();
    const walk = (tag: Tag, preserve: boolean) => {
      const keep = preserve || PRESERVE_WS.has(tag.name);
      const out: Node[] = [];
      for (const c of tag.children) {
        const last = out[out.length - 1];
        if (c instanceof Str && c.kind === "text" && last instanceof Str && last.kind === "text") {
          last.data += c.data;
          (last as Str & { merged?: boolean }).merged = true;
          continue;
        }
        out.push(c);
        if (c instanceof Tag) walk(c, keep);
      }
      for (const c of out)
        if (c instanceof Str && (c as Str & { merged?: boolean }).merged) {
          delete (c as Str & { merged?: boolean }).merged;
          if (!keep && [...c.data].every((ch) => ASCII_SPACES.includes(ch))) c.data = c.data.includes("\n") ? "\n" : " ";
        }
      tag.children = out;
    };
    walk(t, false);
    return t;
  }
}

export type Node = Tag | Str;

// ── tokenizer (html/parser.py, 3.13) ─────────────────────────────────────────────────────────────────────────────────

const RE = {
  interesting: /[&<]/g,
  starttagopen: /<[a-zA-Z]/y,
  endtagopen: /<\/[a-zA-Z]/y,
  commentclose: /--!?>/g,
  commentabruptclose: /-?>/y,
  tagfind: /([a-zA-Z][^\t\n\r\f />]*)(?:[\t\n\r\f ]|\/(?!>))*/y,
  attrfind:
    /((?<=['"\t\n\r\f /])[^\t\n\r\f />][^\t\n\r\f /=>]*)([\t\n\r\f ]*=[\t\n\r\f ]*('[^']*'|"[^"]*"|(?!['"])[^>\t\n\r\f ]*))?(?:[\t\n\r\f ]|\/(?!>))*/y,
  locatetagend:
    /[a-zA-Z][^\t\n\r\f />]*[\t\n\r\f /]*(?:(?<=['"\t\n\r\f /])[^\t\n\r\f />][^\t\n\r\f /=>]*(?:[\t\n\r\f ]*=[\t\n\r\f ]*(?:'[^']*'|"[^"]*"|(?!['"])[^>\t\n\r\f ]*))?[\t\n\r\f /]*)*>?/y,
  charref: /&#(?:[0-9]+|[xX][0-9a-fA-F]+)[^0-9a-fA-F]/y,
  incompleteCharref: /&#(?:[0-9]|[xX][0-9a-fA-F])/y,
  entityref: /&([a-zA-Z][-.a-zA-Z0-9]*)[^a-zA-Z0-9]/y,
  incomplete: /&[a-zA-Z#]/y,
  attrCharref: /&(#[0-9]+|#[xX][0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*)[;=]?/g,
};

const at = (re: RegExp, s: string, i: number): RegExpExecArray | null => {
  re.lastIndex = i;
  return re.exec(s);
};

const CDATA_CONTENT = ["script", "style", "xmp", "iframe", "noembed", "noframes"];
const RCDATA_CONTENT = ["textarea", "title"];
const VOID = new Set([
  "area", "base", "br", "col", "embed", "hr", "img", "input", "keygen", "link", "menuitem", "meta", "param", "source",
  "track", "wbr", "basefont", "bgsound", "command", "frame", "image", "isindex", "nextid", "spacer",
]);
const PRESERVE_WS = new Set(["pre", "textarea"]);
const STRING_CONTAINERS: Record<string, StrKind> = { rt: "rt", rp: "rp", style: "style", script: "script", template: "template" };
const ASCII_SPACES = " \n\t\f\r";

function numericRef(n: number): string {
  if (n === 0 || n > 0x10ffff || (n >= 0xd800 && n <= 0xdfff)) return "�";
  if ((n >= 0xfdd0 && n <= 0xfdef) || NONCHARACTERS.has(n)) return String.fromCodePoint(n);
  if (n >= 0x80 && n <= 0x9f && CP1252[n] !== undefined) return CP1252[n]!;
  return String.fromCodePoint(n);
}

/** html.unescape for one attribute reference (numeric always; a named one when it is an entity). */
function unescapeAttr(v: string): string {
  return v.replace(RE.attrCharref, (ref: string) => {
    if (ref.startsWith("&#")) {
      const body = ref.slice(2).replace(/[;=]$/, "");
      const n = body[0] === "x" || body[0] === "X" ? parseInt(body.slice(1), 16) : parseInt(body, 10);
      return numericRef(n) + (ref.endsWith("=") ? "=" : "");
    }
    if (ref.endsWith("=")) return ref;
    const name = ref.slice(1).replace(/;$/, "");
    const ch = ENTITIES[name];
    return ch !== undefined ? ch : ref;
  });
}

class Builder {
  root = new Tag("[document]");
  stack: Tag[] = [this.root];
  open = new Map<string, number>();
  data: string[] = [];
  preserve: Tag[] = [];
  containers: Tag[] = [];
  alreadyClosedEmpty: string[] = [];

  get current(): Tag {
    return this.stack[this.stack.length - 1]!;
  }

  endData(kind?: StrKind): void {
    if (!this.data.length) return;
    let s = this.data.join("");
    this.data = [];
    if (!this.preserve.length) {
      let strippable = true;
      for (const c of s)
        if (!ASCII_SPACES.includes(c)) {
          strippable = false;
          break;
        }
      if (strippable) s = s.includes("\n") ? "\n" : " ";
    }
    let k: StrKind = kind ?? "text";
    if (k === "text" && this.containers.length) k = STRING_CONTAINERS[this.containers[this.containers.length - 1]!.name] ?? "text";
    const n = new Str(s, k);
    n.parent = this.current;
    this.current.children.push(n);
  }

  push(t: Tag): void {
    t.parent = this.current;
    this.current.children.push(t);
    this.stack.push(t);
    this.open.set(t.name, (this.open.get(t.name) ?? 0) + 1);
    if (PRESERVE_WS.has(t.name)) this.preserve.push(t);
    if (STRING_CONTAINERS[t.name]) this.containers.push(t);
  }

  pop(): void {
    const t = this.stack.pop()!;
    if (this.open.has(t.name)) this.open.set(t.name, this.open.get(t.name)! - 1);
    if (this.preserve.length && this.preserve[this.preserve.length - 1] === t) this.preserve.pop();
    if (this.containers.length && this.containers[this.containers.length - 1] === t) this.containers.pop();
  }

  popTo(name: string): void {
    if (name === "[document]") return;
    for (let i = this.stack.length - 1; i > 0; i--) {
      if (!this.open.get(name)) break;
      const t = this.stack[i]!;
      if (t.name === name) {
        this.pop();
        break;
      }
      this.pop();
    }
  }

  // BeautifulSoupHTMLParser
  starttag(name: string, attrs: [string, string | null][], handleEmpty = true): void {
    const a: Record<string, string> = {};
    for (const [k, v] of attrs) a[k] = v ?? "";
    this.endData();
    const t = new Tag(name, a);
    this.push(t);
    if (VOID.has(name) && t.children.length === 0 && handleEmpty) {
      this.endtag(name, false);
      this.alreadyClosedEmpty.push(name);
    }
  }

  endtag(name: string, checkAlreadyClosed = true): void {
    const i = checkAlreadyClosed ? this.alreadyClosedEmpty.indexOf(name) : -1;
    if (i >= 0) this.alreadyClosedEmpty.splice(i, 1);
    else {
      this.endData();
      this.popTo(name);
    }
  }

  startendtag(name: string, attrs: [string, string | null][]): void {
    this.starttag(name, attrs, false);
    this.endtag(name, false);
  }

  handleData(d: string): void {
    this.data.push(d);
  }

  special(d: string, kind: StrKind): void {
    this.endData();
    this.data.push(d);
    this.endData(kind);
  }

  charref(name: string): void {
    let base = 10;
    let body = name;
    // bs4's `^([0-9]+)(.*)` with search(): `.` stops at the first "\n" and the rest of the reference is DROPPED
    let re = /^([0-9]+)([^\n]*)/;
    if (name.startsWith("x") || name.startsWith("X")) {
      body = name.slice(1);
      base = 16;
      re = /^([0-9a-f]+)([^\n]*)/;
    }
    let real: number | null = null;
    let extra = "";
    const valid = base === 10 ? /^\s*[0-9](_?[0-9])*\s*$/ : /^\s*[0-9a-fA-F](_?[0-9a-fA-F])*\s*$/;
    if (valid.test(body)) real = parseInt(body.replace(/_/g, "").trim(), base);
    else {
      const m = re.exec(body);
      if (m) {
        real = parseInt(m[1]!, base);
        extra = m[2]!;
      }
    }
    if (real === null) extra = body;
    else this.handleData(numericRef(real));
    this.handleData(extra);
  }

  entityref(name: string): void {
    const ch = ENTITIES[name];
    this.handleData(ch !== undefined ? ch : `&${name}`);
  }

  finish(): Tag {
    this.endData();
    while (this.stack.length > 1) this.pop();
    return this.root;
  }
}

/** BeautifulSoup(markup, "html.parser") — the document root (name "[document]"). */
export function parseHtml(raw: string): Tag {
  const b = new Builder();
  let cdataElem: string | null = null;
  let interesting: RegExp = RE.interesting;
  const n = raw.length;
  let i = 0;

  const setCdata = (tag: string, escapable: boolean) => {
    cdataElem = tag;
    interesting = new RegExp(`${escapable ? "&|" : ""}</${tag}(?=[\\t\\n\\r\\f />])`, "gi");
  };
  const clearCdata = () => {
    cdataElem = null;
    interesting = RE.interesting;
  };

  const bogusComment = (i: number): number => {
    const pos = raw.indexOf(">", i + 2);
    if (pos === -1) return -1;
    b.special(raw.slice(i + 2, pos), "comment");
    return pos + 1;
  };
  const parseComment = (i: number): number => {
    let m = at(RE.commentclose, raw, i + 4);
    if (!m) {
      m = at(RE.commentabruptclose, raw, i + 4);
      if (!m) return -1;
    }
    b.special(raw.slice(i + 4, m.index), "comment");
    return m.index + m[0].length;
  };
  const parseDecl = (i: number): number => {
    if (raw.startsWith("<!--", i)) return parseComment(i);
    if (raw.startsWith("<![CDATA[", i)) {
      const j = raw.indexOf("]]>", i + 9);
      if (j < 0) return -1;
      unknownDecl(raw.slice(i + 3, j));
      return j + 3;
    }
    if (raw.slice(i, i + 9).toLowerCase() === "<!doctype") {
      const gt = raw.indexOf(">", i + 9);
      if (gt === -1) return -1;
      b.special(raw.slice(i + 2, gt).slice("DOCTYPE ".length), "doctype");
      return gt + 1;
    }
    return bogusComment(i);
  };
  const unknownDecl = (data: string) => {
    if (data.toUpperCase().startsWith("CDATA[")) b.special(data.slice(6), "cdata");
    else b.special(data, "decl");
  };
  const parsePi = (i: number): number => {
    const j = raw.indexOf(">", i + 2);
    if (j < 0) return -1;
    b.special(raw.slice(i + 2, j), "pi");
    return j + 1;
  };
  const wholeStartTag = (i: number): number => {
    const m = at(RE.locatetagend, raw, i + 1)!;
    const j = m.index + m[0].length;
    if (raw[j - 1] !== ">") return -1;
    return j;
  };
  const parseStarttag = (i: number): number => {
    const endpos = wholeStartTag(i);
    if (endpos < 0) return endpos;
    const attrs: [string, string | null][] = [];
    const tm = at(RE.tagfind, raw, i + 1)!;
    let k = tm.index + tm[0].length;
    const tag = tm[1]!.toLowerCase();
    while (k < endpos) {
      const m = at(RE.attrfind, raw, k);
      if (!m) break;
      const name = m[1]!;
      const rest = m[2];
      let value: string | null = m[3] ?? null;
      if (!rest) value = null;
      else if (value !== null && ((value[0] === "'" && value.endsWith("'") && value.length >= 1) || (value[0] === '"' && value.endsWith('"')))) {
        value = value.slice(1, -1);
      }
      if (value) value = unescapeAttr(value);
      attrs.push([name.toLowerCase(), value]);
      k = m.index + m[0].length;
    }
    const end = strip(raw.slice(k, endpos));
    if (end !== ">" && end !== "/>") {
      b.handleData(raw.slice(i, endpos));
      return endpos;
    }
    if (end.endsWith("/>")) b.startendtag(tag, attrs);
    else {
      b.starttag(tag, attrs);
      if (CDATA_CONTENT.includes(tag)) setCdata(tag, false);
      else if (tag === "plaintext") {
        cdataElem = tag;
        interesting = /(?![\s\S])/g;
      } else if (RCDATA_CONTENT.includes(tag)) setCdata(tag, true);
    }
    return endpos;
  };
  const parseEndtag = (i: number): number => {
    if (raw.indexOf(">", i + 2) < 0) return -1;
    if (!at(RE.endtagopen, raw, i)) {
      if (raw.slice(i + 2, i + 3) === ">") return i + 3;
      return bogusComment(i);
    }
    const m = at(RE.locatetagend, raw, i + 2)!;
    const j = m.index + m[0].length;
    if (raw[j - 1] !== ">") return -1;
    const tm = at(RE.tagfind, raw, i + 2)!;
    b.endtag(tm[1]!.toLowerCase());
    clearCdata();
    return j;
  };

  // goahead(end=True): equivalent to feed() + close(), which only pauses at the same positions
  while (i < n) {
    let j: number;
    const m = at(interesting, raw, i);
    if (m) j = m.index;
    else {
      if (cdataElem) break;
      j = n;
    }
    if (i < j) b.handleData(raw.slice(i, j));
    i = j;
    if (i === n) break;
    if (raw[i] === "<") {
      let k: number;
      if (at(RE.starttagopen, raw, i)) k = parseStarttag(i);
      else if (raw.startsWith("</", i)) k = parseEndtag(i);
      else if (raw.startsWith("<!--", i)) k = parseComment(i);
      else if (raw.startsWith("<?", i)) k = parsePi(i);
      else if (raw.startsWith("<!", i)) k = parseDecl(i);
      else {
        b.handleData("<");
        k = i + 1;
      }
      if (k < 0) {
        if (at(RE.starttagopen, raw, i)) {
          // pass: the rest is handled as data below
        } else if (raw.startsWith("</", i)) {
          if (i + 2 === n) b.handleData("</");
          else if (at(RE.endtagopen, raw, i)) {
            // pass
          } else b.special(raw.slice(i + 2), "comment");
        } else if (raw.startsWith("<!--", i)) {
          let e = n;
          for (const suffix of ["--!", "--", "-"])
            if (raw.endsWith(suffix) && n - suffix.length >= i + 4) {
              e -= suffix.length;
              break;
            }
          b.special(raw.slice(i + 4, e), "comment");
        } else if (raw.startsWith("<![CDATA[", i)) unknownDecl(raw.slice(i + 3));
        else if (raw.slice(i, i + 9).toLowerCase() === "<!doctype") b.special(raw.slice(i + 2).slice("DOCTYPE ".length), "doctype");
        else if (raw.startsWith("<!", i)) b.special(raw.slice(i + 2), "comment");
        else if (raw.startsWith("<?", i)) b.special(raw.slice(i + 2), "pi");
        k = n; // an unfinished start / end tag at EOF is dropped, as html.parser drops it
      }
      i = k;
    } else if (raw.startsWith("&#", i)) {
      const cm = at(RE.charref, raw, i);
      if (cm) {
        b.charref(cm[0].slice(2, -1));
        let k = cm.index + cm[0].length;
        if (raw[k - 1] !== ";") k -= 1;
        i = k;
        continue;
      }
      if (at(RE.incompleteCharref, raw, i)) {
        b.charref(raw.slice(i + 2));
        i = n;
        break;
      } else if (i + 3 < n) {
        b.handleData("&#");
        i += 2;
      } else break;
    } else if (raw[i] === "&") {
      const em = at(RE.entityref, raw, i);
      if (em) {
        b.entityref(em[1]!);
        let k = em.index + em[0].length;
        if (raw[k - 1] !== ";") k -= 1;
        i = k;
        continue;
      }
      if (at(RE.incomplete, raw, i)) {
        b.entityref(raw.slice(i + 1));
        i = n;
        break;
      } else if (i + 1 < n) {
        b.handleData("&");
        i += 1;
      } else break;
    } else throw new Error("html: interesting.search() lied");
  }
  if (i < n) b.handleData(raw.slice(i, n));
  return b.finish();
}
