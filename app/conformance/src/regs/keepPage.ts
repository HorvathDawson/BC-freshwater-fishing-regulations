/**
 * T21 parity helpers: a whole Keep tab (core `buildRegsPage(…).keep`) read the way the golden reads the prototype's DOM —
 * each block to its golden selector, and every block to its drawn lines (golden `keep.lines`: one string per block
 * element, runs concatenated as they are, separate elements one space; contracts §1). A projection only reads the VM; it
 * never re-derives a value. The per-block forms are the ones each presenter's own parity proved (T13 strip, T14 banner,
 * T15 standing, T15b caveats, T17–T19 groups and Other fish, T19b plain rows); this file is where they live now.
 */
import { plain, WORDS, type BannerVM, type CaveatVM, type Day, type GroupVM, type KeepBlock, type KeepVM, type PlainRowVM } from "@app/core";
import { fmtDay } from "@app/core";
import { norm } from "./eq";
import { headProj, nodeLines, notKeptLines, originProj, otherProj } from "./keepGroups";

/** A KeepBlock → the golden `keep.order` selector (the ticket's map; a caveat by its kind and heat). */
export function selectorOf(b: KeepBlock, keep: KeepVM): string {
  if (typeof b === "string") return ({ strip: "div.wstrip", inherited: "div.inh", banner: "div.banner", otherFish: "section.sec",
    possession: "section.posrow", whereNot: "section.nofish", outside: "p.foot" } as const)[b];
  if ("group" in b) return "article.row.ledger.g28";
  if ("plain" in b) return "article.row.ledger";
  const c = keep.caveats[b.caveat]!;
  return c.kind === "inpart" ? (c.hot ? "details.inpart.hot" : "details.inpart") : c.kind === "uncertain" ? "div.caveats.top.check" : "div.caveats.top";
}

const join = (...xs: (string | null | undefined)[]): string => norm(xs.filter((x) => !!x).join(" "));

/** T15b: one caveat block's lines (the heading, the calm line, each item, "Other dates (N)" and the later items). */
export const caveatItemLine = (i: CaveatVM["items"][number]) => [plain(i.what), i.where, plain(i.book)].filter(Boolean).join(" ");
export const caveatLines = (c: CaveatVM): string[] => [
  (c.kind === "inpart" ? WORDS.INPART_IC : "") + c.title, c.calm ?? "", ...c.items.map(caveatItemLine),
  c.later ? WORDS.OTHER_DATES(c.later.length) : "", ...(c.later ?? []).map(caveatItemLine),
].filter(Boolean);

/** T19b: one plain row's lines, "See every regulation line ›" ending the last. */
export function plainRowLines(v: PlainRowVM): string[] {
  const out: string[] = [
    [v.title, v.pill?.text, v.pill?.term ? WORDS.TERM_ONE : ""].filter(Boolean).join(" "),
    v.sum ?? "",
    ...(v.week ? [[v.week.status, v.week.says].filter(Boolean).join(" "), ...v.week.parts.map((p) => [p.days, p.says, plain(p.book)].filter(Boolean).join(" ")),
      ...v.week.notes.map(plain)] : []),
    v.release ? [v.release.names, ...v.release.pictures.flatMap((p) => (p.sil ? [] : [p.caption]))].join(" ") : "",
    plain(v.presence),
    v.whichFish ?? "", v.names ?? "",
    v.ladder.label,
    ...v.ladder.sections.flatMap((s) => [s.q ?? "", s.result ? WORDS.CHECK + s.result : "",
      ...s.steps.flatMap((st) => [st.level, ...st.lines.flatMap((l) => [[plain(l.text), l.tag, plain(l.book)].filter(Boolean).join(" "), ...(l.notes ?? [])])])]),
    v.ladder.lost.length ? WORDS.RULES_NOT_APPLY(v.ladder.lost.length) : "",
    ...v.ladder.lost.map((l) => [l.text, l.status, plain(l.book)].filter(Boolean).join(" ")),
  ].filter(Boolean);
  out[out.length - 1] += " " + v.more.text;
  return out;
}

/** T17–T19: one g28 group's drawn lines, in draw order (the head pinned, the origin, the tree, the not-kept lines, How it
 *  adds up, the quiet line, "See every regulation line ›" inline after the last line). */
export function groupLines(g: GroupVM): string[] {
  const h = headProj(g.head), out: string[] = [norm(g.title + (g.scope ?? "")), norm(h.text)];
  const o = originProj(g.origin);
  if (o) out.push(...o.lines);
  if (g.year) out.push(join(g.year.label, g.year.says));
  out.push(...(g.tree ?? []).flatMap(nodeLines));
  for (const r of g.notKept) out.push(...notKeptLines(r));
  if (g.worked) out.push(g.worked.label, ...(g.worked.says ? [norm(g.worked.says)] : []), ...g.worked.lines.map((l) => join(l.q, l.a)));
  const q = g.quiet ? norm(plain(g.quiet)) : null;
  if (q) out.push(q);
  out.push(join(out.pop(), g.more.text));
  return out.filter((l) => !!l);
}

/** T14: the banner's lines — the headline, the paragraphs, each chain line ("<when>. <scope> Source <Rule book>") with the
 *  reopening link after the last, then the merged list. */
export function bannerLines(b: BannerVM): string[] {
  const chain = b.chain.map((c) => join(c.text, c.scope, WORDS.SOURCE, plain(c.book)));
  if (b.reopen) { if (chain.length) chain.push(join(chain.pop(), b.reopen.text)); else chain.push(b.reopen.text); }
  const merged = b.merged ? [b.merged.label, ...b.merged.items.map((x) => join(x.text, x.level, plain(x.book)))] : [];
  return [...b.big, ...b.paras, ...chain, ...merged];
}

/**
 * Every drawn line of the Keep tab, block by block in `order`. `groupOverride(i)` replaces group i's lines (a keyed
 * exclusion compares the golden's own there).
 */
export function keepLines(keep: KeepVM, day: Day, groupOverride: (i: number) => string[] | null = () => null): string[] {
  const out: string[] = [];
  for (const b of keep.order) {
    if (b === "strip") {
      const s = keep.strip!;
      out.push(s.line.map((l) => l.text).join(" "), fmtDay(day), s.months.join(" "), s.legend.map((l) => (l.mark ? `${l.mark} ` : "") + l.label).join(" "));
    } else if (b === "inherited") {
      const h = keep.inherited!;
      out.push(h.label, ...h.lines.flatMap((x) => [plain(x.says), ...(x.caution ? [x.caution] : [])]));
    } else if (b === "banner") out.push(...bannerLines(keep.banner!));
    else if (b === "otherFish") {
      out.push(WORDS.OTHER_FISH, ...keep.otherFish!.map((o) => otherProj(o).lines[0]!),
        ...keep.otherFishKey.map((k) => join(k.status, k.says, WORDS.TERM_ONE)));
    } else if (b === "possession") out.push(join(keep.possession!.label, keep.possession!.value));
    else if (b === "whereNot") {
      const w = keep.whereNot!;
      out.push(w.heading, ...(w.scope ? [w.scope] : []), ...w.items.map((x) => plain(x.text)));
    } else if (b === "outside") out.push(keep.outside!);
    else if ("caveat" in b) out.push(...caveatLines(keep.caveats[b.caveat]!));
    else if ("group" in b) out.push(...(groupOverride(b.group) ?? groupLines(keep.groups[b.group]!)));
    else out.push(...plainRowLines(keep.plainRows[b.plain]!));
  }
  return out.map(norm).filter((l) => !!l);
}
