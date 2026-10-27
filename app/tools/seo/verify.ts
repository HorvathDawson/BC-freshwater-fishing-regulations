/**
 * T192 — THE PAGE AGREES WITH THE APP. A water's page model is held to the presenter pages the app draws for the same parts
 * on the same day (the caller builds them itself, through `loadRegsInputs` + `buildRegsPage`, never through `model.ts`):
 *
 *   verdict    each part's status is the app's status (and its header dot), the water's the parts' agreement, and the
 *              sentence opens with the same word;
 *   complete   every sentence the app's Keep / Gear / Methods tabs say (a `Line`, the ladder / worked-example / pin
 *              sub-trees aside, which explain a rule rather than state it) is in the model;
 *   nothing    every model sentence is made of strings the app's page holds, plus the page's own joiners — the model
 *   invented   cannot say what the app does not.
 *
 * Used twice: as the build's gate over every water, and by seo.test.ts, which plants a wrong verdict, a dropped rule and an
 * invented sentence and expects each to be reported (mutation-pinned).
 */
import type { RegsPage } from "@app/core";
import type { PartModel, WaterModel, WaterStatus } from "./model";

const squash = (s: string) => s.replace(/\s+/g, " ").trim();
/** sub-trees that explain how a rule was decided rather than state it */
const EXPLAINS = new Set(["ladder", "worked", "more", "pinLine", "runs", "legend", "months", "sheet", "needs", "lostlist", "axes", "terms", "tabs"]);
const RUN_TYPES = new Set(["text", "num", "term", "book", "link"]);

interface Run { t: string; s?: string }
const isLine = (v: unknown): v is Run[] =>
  Array.isArray(v) && v.length > 0 && v.every((r) => r && typeof r === "object" && RUN_TYPES.has((r as Run).t));

/** every `Line` (as the words it reads) and every string leaf under `v`, skipping the explaining sub-trees */
function harvest(v: unknown, lines: Set<string>, leaves: Set<string>, key = ""): void {
  if (EXPLAINS.has(key)) return;
  if (typeof v === "string") { leaves.add(squash(v)); leaves.add(v); return; }
  if (isLine(v)) {
    const w = squash(v.map((r) => (r.t === "text" || r.t === "num" ? r.s ?? "" : "")).join(""));
    if (/[A-Za-z]/.test(w)) lines.add(w);
    for (const r of v) if (r.s) { leaves.add(squash(r.s)); leaves.add(r.s); }
    return;
  }
  if (Array.isArray(v)) { for (const x of v) harvest(x, lines, leaves, key); return; }
  if (v && typeof v === "object") for (const [k, x] of Object.entries(v)) harvest(x, lines, leaves, k);
}

const JOINERS = /[\s:·.,;—–()'"’“”-]/;
/** is `text` nothing but leaves and joiners? (a memoised walk: a leaf may be a prefix of another, so no greedy match) */
function madeOf(text: string, leaves: ReadonlySet<string>, longest: number): boolean {
  const ok: (boolean | undefined)[] = new Array(text.length + 1);
  ok[text.length] = true;
  for (let i = text.length - 1; i >= 0; i--) {
    let good = JOINERS.test(text[i]!) && ok[i + 1] === true;
    for (let n = Math.min(longest, text.length - i); !good && n > 0; n--) good = ok[i + n] === true && leaves.has(text.slice(i, i + n));
    ok[i] = good;
  }
  return ok[0] === true;
}

const rollup = (ss: readonly RegsPage["status"][]): WaterStatus => (new Set(ss).size === 1 ? ss[0]! : "mixed");
const OPENS: Record<WaterStatus, RegExp> = {
  open: /^Open/, closed: /^Closed/, tidal: /^Tidal/, outside: /^Outside/, unknown: /^We could not/, mixed: /^Open in some parts/,
};

/** The ways `model` differs from the app's own pages for its parts; [] = agrees. */
export function verifyAgainstApp(model: WaterModel, pages: readonly RegsPage[]): string[] {
  const bad: string[] = [];
  const say = (m: string) => bad.push(`${model.id}: ${m}`);
  if (pages.length !== model.parts.length) say(`${model.parts.length} parts, the app has ${pages.length}`);
  const expect = rollup(pages.map((p) => p.status));
  if (model.status !== expect) say(`water status ${model.status}, the app says ${expect}`);
  if (!OPENS[model.status].test(model.answer)) say(`answer "${model.answer}" does not open with its status ${model.status}`);
  for (const part of model.parts) {
    const page = pages.find((p) => p.target.kind === "part" && p.target.partIx === part.partIx);
    if (!page) { say(`part ${part.partIx}: the app has no such part`); continue; }
    checkPart(part, page, say);
  }
  return bad;
}

function checkPart(part: PartModel, page: RegsPage, say: (m: string) => void): void {
  const at = `part ${part.partIx}`;
  if (part.status !== page.status) say(`${at}: status ${part.status}, the app says ${page.status}`);
  const dot = page.header.dot;
  if ((part.status === "open") !== (dot === "open") && (part.status === "closed") !== (dot === "closed") && dot !== "faint")
    say(`${at}: status ${part.status} against the app's header dot ${dot}`);
  const lines = new Set<string>(), leaves = new Set<string>();
  const { sheet: _s, ...bare } = page;
  harvest({ keep: bare.keep, gear: bare.gear, methods: bare.methods, licence: bare.licence?.blocks, header: bare.header }, lines, leaves);
  // the licence blocks are strings: only the water-specific ones are on the page, but all are leaves the page may use
  const texts = part.sections.flatMap((s) => s.items.map((i) => i.text));
  const has = (w: string) => texts.some((t) => t.includes(w));
  const wanted = new Set<string>();
  harvest({ keep: bare.keep, gear: bare.gear, methods: bare.methods }, wanted, new Set<string>());
  for (const w of wanted) if (!has(w)) say(`${at}: the app says "${w.slice(0, 80)}" and the page does not`);
  const longest = Math.max(1, ...[...leaves].map((l) => l.length));
  for (const t of texts) if (!madeOf(t, leaves, longest)) say(`${at}: the page says "${t.slice(0, 80)}", which is not the app's words`);
}
