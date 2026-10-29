/**
 * Report a problem: what the user wrote plus the diagnostics they can SEE and switch off, composed for the platform's
 * share sheet or an email — nothing is ever sent without the user pressing send, and nothing in the background.
 *
 * (T237) And SENT: the same report as JSON (`reportPayload`) POSTed to the report endpoint (the feeds worker's
 * `POST /report`), queued on the device when offline. The payload holds what the reader wrote, the reason, an optional
 * email, and exactly the diagnostics rows they left on — never a field they switched off. Two facts ride along with a
 * row without a line of their own: the tapped SECTION HANDLE (with "Where you were"; internal — meaningful only beside the
 * release and tiles it came from, never shown) and the data ENVIRONMENT (with "App version").
 */
import type { HeaderVM, RegsTarget, Tab } from "../regs/model";
import { MENU_WORDS } from "./words";

/** Where the reader was when they asked to report (the Shell's route, the regs page's own words). Every field optional. */
export interface ReportWhere {
  /** the screen: a browse tab ("map", "search", …) or a water's view ("water · regs") */
  screen?: string | null;
  /** the water's durable id */
  water?: string | null;
  /** the water's name as the page wrote it */
  place?: string | null;
  /** the tapped section (only meaningful with the data release beside it) */
  section?: number | null;
  /** the part's index in its water (the route's), and its label as the page wrote it ("Mouth → Source") */
  part?: number | null;
  partLabel?: string | null;
  /** the day the page answers for, as the page wrote it */
  day?: string | null;
  /** the Regulations tab the report was asked from */
  tab?: string | null;
  /** (T237) the map's regulations colouring: standard / salmon / run timing (`RegsLayer`) */
  mode?: string | null;
}

export interface Diagnostics {
  appBuild: string | null;
  /** (T237) the data environment this run reads (local / staging / production; T232) — sent with "App version" */
  env?: string | null;
  platform: string | null;
  release: string | null;
  tiles: string | null;
  where: ReportWhere;
  /** the recent error log, oldest first */
  errors: readonly string[];
}

export type DiagKey = "app" | "platform" | "release" | "tiles" | "where" | "errors";
export const DIAG_KEYS: readonly DiagKey[] = ["app", "platform", "release", "tiles", "where", "errors"];

export interface DiagnosticRowVM { readonly k: DiagKey; readonly label: string; readonly lines: readonly string[] }

const DIAG_LABEL: Record<DiagKey, string> = MENU_WORDS.report.diag;

/**
 * Where a report asked from the Regulations page was asked from: the page's own words (the water's name, the day as the
 * header writes it, the tab's label when one is named) and its target — prefilled into the report's "Where you were",
 * which the reader still sees and can switch off. `tab` null names the whole page: the page-foot link (T41 Q3) is only
 * reached below Licence, so "the tab in view" there would always say Licence — a wrong fact, so it names none.
 */
export function regsReportWhere(page: { readonly target: RegsTarget;
                                         readonly header: Pick<HeaderVM, "name" | "dayWord" | "date" | "tabs" | "place"> },
                                tab: Tab | null): ReportWhere {
  const t = page.target;
  return {
    screen: MENU_WORDS.report.where.regs,
    tab: tab === null ? null : page.header.tabs.find((x) => x.tab === tab)?.label ?? tab,
    place: page.header.name,
    water: t.kind === "part" ? t.item : null,
    part: t.kind === "part" ? t.partIx : null,
    partLabel: page.header.place?.full ?? null,
    section: t.kind === "section" ? t.handle : null,
    day: `${page.header.dayWord}, ${page.header.date}`,
  };
}

/** "Screen: water · regs" / "Water: Vedder River (item 356…)" / … — one line per thing the reader was looking at. */
export function whereLines(w: ReportWhere): string[] {
  const say = MENU_WORDS.report.where;
  const out: string[] = [];
  if (w.screen) out.push(say.screen(w.screen));
  if (w.tab) out.push(say.tab(w.tab));
  if (w.place || w.water) out.push(say.water([w.place, w.water ? `(${w.water})` : null].filter(Boolean).join(" ")));
  const part = [w.part ?? null, w.partLabel ? `(${w.partLabel})` : null].filter((x) => x !== null).join(" ");
  if (part) out.push(say.part(part));
  // (T237) the section handle is internal (meaningful only beside its release): sent with this row, never shown
  if (w.mode) out.push(say.mode(MENU_WORDS.report.modes[w.mode as keyof typeof MENU_WORDS.report.modes] ?? w.mode));
  if (w.day) out.push(say.day(w.day));
  return out;
}

/** The diagnostics as the reader sees them before sending: one row per switch, its exact lines beneath. Empty rows go. */
export function diagnosticRows(d: Diagnostics): DiagnosticRowVM[] {
  const lines: Record<DiagKey, string[]> = {
    app: [d.appBuild, d.env ? MENU_WORDS.report.envLine(d.env) : null].filter((x): x is string => !!x),
    platform: d.platform ? [d.platform] : [],
    release: d.release ? [d.release] : [],
    tiles: d.tiles ? [d.tiles] : [],
    where: whereLines(d.where),
    errors: [...d.errors],
  };
  return DIAG_KEYS.filter((k) => lines[k].length).map((k) => ({ k, label: DIAG_LABEL[k], lines: lines[k] }));
}

/** The report as it will be sent: what the reader wrote, then exactly the rows they left on. */
export function reportMessage(what: string, d: Diagnostics, on: Partial<Record<DiagKey, boolean>>):
  { subject: string; body: string } {
  const w = MENU_WORDS.report;
  const where = d.where.place && on.where !== false ? [d.where.place, d.where.tab].filter(Boolean).join(" · ") : "";
  const parts = [what.trim()];
  const kept = diagnosticRows(d).filter((r) => on[r.k] !== false);
  if (kept.length) parts.push(["--", ...kept.flatMap((r) => (r.lines.length === 1 ? [`${r.label}: ${r.lines[0]}`]
                                                                    : [`${r.label}:`, ...r.lines.map((l) => `  ${l}`)]))].join("\n"));
  return { subject: where ? w.subjectAt(where) : w.subject, body: parts.join("\n\n") };
}

/** mailto: with the subject and body encoded (RFC 6068: spaces as %20, line breaks as %0D%0A). */
export function mailtoUrl(to: string, m: { subject: string; body: string }): string {
  const enc = (s: string) => encodeURIComponent(s).replace(/%0A/g, "%0D%0A");
  return `mailto:${to}?subject=${enc(m.subject)}&body=${enc(m.body.replace(/\r\n/g, "\n"))}`;
}

// ── (T237) the report, sent ─────────────────────────────────────────────────────────────────────────────────────────

/** Why the reader is reporting: the form's first choice. */
export type ReportReason = "rule" | "place" | "missing" | "name" | "other";
export const REPORT_REASONS: readonly ReportReason[] = ["rule", "place", "missing", "name", "other"];

/** The payload's format tag; the endpoint refuses any other. */
export const REPORT_FORMAT = "report/1";
/** Limits the form and the endpoint share (the endpoint enforces them again). */
export const REPORT_LIMITS = { text: 4000, email: 254, errors: 20, line: 300, field: 200 } as const;
/** A pragmatic email shape (the v1 worker's): something@something.tld, no spaces. */
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** null when the optional email is fine (empty or well formed), else the reason it is refused. */
export function emailProblem(email: string): string | null {
  const e = email.trim();
  if (!e) return null;
  if (e.length > REPORT_LIMITS.email || !EMAIL_RE.test(e)) return MENU_WORDS.report.emailBad;
  return null;
}

/** What the report says about where the reader was and what they run — only what the reader left switched on. */
export interface ReportContext {
  screen?: string; tab?: string; place?: string;
  /** the water's durable item id */
  water?: string;
  part?: number; partLabel?: string;
  /** the tapped section's handle: internal, only meaningful beside `release` and `tiles` */
  section?: number;
  day?: string; mode?: string;
  app?: string; env?: string;
  platform?: string; release?: string; tiles?: string;
  errors?: string[];
}

export interface ReportPayload {
  format: typeof REPORT_FORMAT;
  /** made on the device, so a queued report re-sent after a lost answer is stored once */
  id: string;
  /** when the reader pressed Send (ISO) */
  created: string;
  reason: ReportReason;
  text: string;
  email: string | null;
  context: ReportContext;
}

const cut = (s: string, n: number = REPORT_LIMITS.field) => s.slice(0, n);

/**
 * The report as it is SENT: what the reader wrote, the reason, the optional email, and the context — every field from
 * a row the reader left on, nothing from a row they switched off (the section handle goes with "Where you were", the
 * environment with "App version").
 */
export function reportPayload(i: { id: string; created: string; reason: ReportReason; what: string; email: string;
                                   diagnostics: Diagnostics; on: Partial<Record<DiagKey, boolean>> }): ReportPayload {
  const d = i.diagnostics, on = (k: DiagKey) => i.on[k] !== false;
  const c: ReportContext = {};
  const w = d.where;
  if (on("where")) {
    if (w.screen) c.screen = cut(w.screen);
    if (w.tab) c.tab = cut(w.tab);
    if (w.place) c.place = cut(w.place);
    if (w.water) c.water = cut(w.water);
    if (w.part !== null && w.part !== undefined) c.part = w.part;
    if (w.partLabel) c.partLabel = cut(w.partLabel);
    if (w.section !== null && w.section !== undefined) c.section = w.section;
    if (w.day) c.day = cut(w.day);
    if (w.mode) c.mode = cut(w.mode);
  }
  if (on("app")) {
    if (d.appBuild) c.app = cut(d.appBuild);
    if (d.env) c.env = cut(d.env);
  }
  if (on("platform") && d.platform) c.platform = cut(d.platform);
  if (on("release") && d.release) c.release = cut(d.release);
  if (on("tiles") && d.tiles) c.tiles = cut(d.tiles);
  if (on("errors") && d.errors.length) c.errors = d.errors.slice(-REPORT_LIMITS.errors).map((l) => cut(l, REPORT_LIMITS.line));
  const email = i.email.trim();
  return { format: REPORT_FORMAT, id: i.id, created: i.created, reason: i.reason,
           text: cut(i.what.trim(), REPORT_LIMITS.text), email: email && !emailProblem(email) ? email : null, context: c };
}
