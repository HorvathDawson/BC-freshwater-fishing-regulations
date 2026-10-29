/**
 * (T251) THE UPDATES LOG — every check the update service (`@app/data` `createUpdateService`) makes, per source, as one
 * event: when, what came back (unchanged 304 / changed / failed and why / offline), the bytes, the version or sha before
 * and after, how long it took; and the release's later steps (staged, applied, refused for an old app). A development
 * and staging build keeps the last `UPDATE_LOG_CAP` on the device (`@app/data` `createUpdateLog`) and shows them on the
 * Updates log page, copyable whole for a bug report (`updateLogText`). Production builds keep none.
 */
import { MENU_WORDS } from "./menu";
import type { UpdateSourceKey } from "./updates";

export const UPDATE_LOG_CAP = 200;

export type UpdateEventKind =
  /** the origin said it has not moved (a 304, or the same bytes) */
  | "unchanged"
  /** new content, taken (a feed handed to its loader; a release manifest naming another set) */
  | "changed"
  | "failed"
  /** no request made: the device (or the log's "simulate offline") says there is no network */
  | "offline"
  /** a release's files downloaded and verified beside the set in use */
  | "staged"
  /** a staged release put in use */
  | "applied"
  /** a release this app is too old for: not applied, the data in use kept */
  | "app-too-old"
  /** a tester's action from the log page (force re-download, clear cache, simulate offline) */
  | "action";

export type UpdateErrorClass = "offline" | "http-404" | "http-4xx" | "http-5xx" | "sha-mismatch" | "size-mismatch"
  | "parse" | "apply" | "other";

export type UpdateLogSource = UpdateSourceKey | "app";

export interface UpdateEvent {
  /** epoch ms, this device's clock */
  readonly at: number;
  readonly source: UpdateLogSource;
  readonly kind: UpdateEventKind;
  /** the reader (or a tester) asked for it */
  readonly manual?: boolean;
  /** the HTTP status, when a response came back */
  readonly status?: number | null;
  /** bytes received (0 for a 304) */
  readonly bytes?: number;
  /** what was in use before, and what is after: a short sha for a feed, the data release id for the release */
  readonly before?: string | null;
  readonly after?: string | null;
  /** how long the step took (ms) */
  readonly ms?: number;
  readonly error?: string | null;
  readonly errorClass?: UpdateErrorClass | null;
  /** a free note (the action taken, the minAppVersion asked for) */
  readonly note?: string | null;
}

/** A failure message's class, for the log and a report: the network, the origin's answer, the bytes, the parse. */
export function errorClassOf(error: string, offline: boolean): UpdateErrorClass {
  if (offline) return "offline";
  if (/sha256/i.test(error)) return "sha-mismatch";
  if (/bytes, the manifest says|-byte range/i.test(error)) return "size-mismatch";
  const h = /HTTP (\d{3})/.exec(error);
  if (h) return h[1] === "404" ? "http-404" : h[1]!.startsWith("5") ? "http-5xx" : "http-4xx";
  if (/JSON|manifest:|Unexpected token|not valid/i.test(error)) return "parse";
  return "other";
}

export const UPDATE_LOG_WORDS = {
  title: "Updates log",
  aboutLink: "Updates log",
  /** About's link, its arrow included */
  aboutLinkShown: "Updates log ›",
  intro: "Every check for new data on this device, newest first. Development and staging builds only.",
  sources: { gauges: "Gauges", dfo: MENU_WORDS.sources.names.dfo, inseason: "In-season", release: "Release",
             app: "App" } satisfies Record<UpdateLogSource, string>,
  kinds: {
    unchanged: "unchanged", changed: "changed", failed: "failed", offline: "offline", staged: "staged",
    applied: "applied", "app-too-old": "app too old", action: "action",
  } satisfies Record<UpdateEventKind, string>,
  actions: {
    check: "Check now",
    force: "Force re-download",
    offlineOn: "Simulate offline: on",
    offlineOff: "Simulate offline: off",
    clearCache: "Clear cache",
    apply: "Apply the staged release",
    copy: "Copy log",
    copied: "Copied",
    copyFailed: "Copy failed",
    clear: "Clear log",
  },
  channel: { label: "Update channel", live: "Live", test: "Test (staging test/)" },
  empty: "No checks logged yet.",
  /** the tester's actions, as the log notes them */
  notes: {
    offline: (on: boolean) => `simulate offline ${on ? "on" : "off"}`,
    cache: (web: boolean, dropped: boolean) => (web ? `release cache ${dropped ? "cleared" : "was empty"}` : "validators cleared"),
  },
  state: { ok: "ok", notChecked: "not checked", offline: "failed (offline)", failed: "failed (error)" },
  count: (n: number, cap: number) => `${n} of the last ${cap} kept`,
  exportTitle: "Can I Fish This? updates log",
} as const;

export interface UpdateLogRowVM {
  readonly key: string;
  /** "14:03:22" (local) */
  readonly time: string;
  readonly source: string;
  readonly kind: UpdateEventKind;
  readonly result: string;
  /** "304 · 0 B · 12 ms" */
  readonly detail: string;
  /** "a1b2c3 → d4e5f6", or null */
  readonly change: string | null;
  /** "http-404: …", or null */
  readonly problem: string | null;
  /** the row aloud */
  readonly label: string;
}

const pad = (n: number) => String(n).padStart(2, "0");
const clock = (ms: number) => { const d = new Date(ms); return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`; };

/** "0 B", "812 B", "12.4 kB", "3.1 MB" */
export function byteWords(n: number): string {
  if (n < 1000) return `${n} B`;
  if (n < 1e6) return `${(n / 1e3).toFixed(1)} kB`;
  return `${(n / 1e6).toFixed(1)} MB`;
}

function detailOf(e: UpdateEvent): string {
  return [e.status != null ? String(e.status) : null, e.bytes != null ? byteWords(e.bytes) : null,
          e.ms != null ? `${Math.round(e.ms)} ms` : null, e.manual ? "manual" : null, e.note ?? null]
    .filter(Boolean).join(" · ");
}

const changeOf = (e: UpdateEvent) =>
  e.before != null || e.after != null ? `${e.before ?? "—"} → ${e.after ?? "—"}` : null;

/** The page's rows, newest first. */
export function updateLogVM(events: readonly UpdateEvent[]): UpdateLogRowVM[] {
  const W = UPDATE_LOG_WORDS;
  return events.map((e, i) => {
    const source = W.sources[e.source], result = W.kinds[e.kind], detail = detailOf(e), change = changeOf(e);
    const problem = e.error ? `${e.errorClass ?? "other"}: ${e.error}` : null;
    const time = clock(e.at);
    return { key: `${e.at}-${i}`, time, source, kind: e.kind, result, detail, change, problem,
             label: [time, source, result, detail, change, problem].filter(Boolean).join(", ") };
  }).reverse();
}

export interface UpdateLogHeader {
  /** `versionLine` */
  readonly version: string;
  readonly platform: string;
  readonly env: string;
  readonly origin: string;
  readonly channel: string;
  readonly release: string | null;
}

/** The page's head and the export's: the build, then the data source. */
export function updateLogHead(h: UpdateLogHeader): [string, string] {
  return [`app ${h.version} · ${h.platform}`,
          `data ${h.env} · ${h.origin.replace(/^https?:\/\//, "")} · channel ${h.channel} · release ${h.release ?? "none"}`];
}

export interface UpdateLogStatusInput {
  readonly id: UpdateLogSource;
  readonly checkedAt: number | null;
  readonly error: string | null;
  readonly offline: boolean;
  /** the release only: its phase, the release pending, the app version it asks for */
  readonly phase?: string;
  readonly target?: string | null;
  readonly needsApp?: string | null;
}

/** One line per source: "Gauges: ok", "Release: ok · app-too-old r-2 (needs 0.3.0)". */
export function updateLogStatus(sources: readonly UpdateLogStatusInput[]): string[] {
  const S = UPDATE_LOG_WORDS.state;
  return sources.map((s) => {
    const state = s.error ? (s.offline ? S.offline : S.failed) : s.checkedAt !== null ? S.ok : S.notChecked;
    const phase = s.phase && s.phase !== "current"
      ? ` · ${[s.phase, s.target ?? null, s.needsApp ? `(needs ${s.needsApp})` : null].filter(Boolean).join(" ")}` : "";
    return `${UPDATE_LOG_WORDS.sources[s.id]}: ${state}${phase}`;
  });
}

/** The whole log as text, for a bug report: the build and data source first, then one line per event (ISO time, oldest first). */
export function updateLogText(events: readonly UpdateEvent[], h: UpdateLogHeader, now: number): string {
  const head = [
    `${UPDATE_LOG_WORDS.exportTitle} — exported ${new Date(now).toISOString()}`,
    ...updateLogHead(h),
    "",
  ];
  const lines = events.map((e) => [new Date(e.at).toISOString(), e.source, e.kind, detailOf(e), changeOf(e),
                                    e.error ? `${e.errorClass ?? "other"}: ${e.error}` : null]
    .filter(Boolean).join(" | "));
  return [...head, ...(lines.length ? lines : [UPDATE_LOG_WORDS.empty])].join("\n");
}
