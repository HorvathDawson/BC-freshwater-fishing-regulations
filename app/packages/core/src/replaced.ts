/**
 * (T1016) A PAGE OLDER THAN WHAT THE SERVER NOW SERVES — a KNOWN state, not a defect.
 *
 * Production serves the live release and the previous one (`deploy/prod/upload.sh` prunes the rest a day on), and a site
 * deploy replaces the web app's code chunks. A browser tab opened before two releases (or before a deploy) and still open
 * after asks for files that are gone: a region's regs / water slice, the in-season section lines, the tile folder's mask
 * and sprites (HTTP 404), or a code chunk (`AsyncRequireError`). Each was reported as a defect and shown as a failed read.
 *
 * Here are the words the reader is told (one line, one action), the two error names the crash server classes
 * `expected` (the feeds worker's `crashes.ts` EXPECTED, by NAME: `tools/wire.test.ts` proves the app's own reports class
 * so), and the platform-free half of telling them apart: what 404 an error carries (`notFoundUrl`) and whether it is a
 * code chunk that would not load (`missingChunk`). WHICH set the pointer still names is the data package's
 * (`release/replaced.ts`); the web's probes and the reload are `apps/mobile` `replaced.web.ts`. A 404 on a file the live
 * release DOES name stays a defect (no silent fallback): only a file of a set the pointer no longer names is this state.
 */

/** what was replaced: the data release this page opened, or the site's code */
export type ReplacedKind = "data" | "code";

/** (T1016) the reader's words: one line and its one action (owner's pick pending: T1016 report) */
export const REPLACED_WORDS = {
  data: {
    line: "This page’s data was replaced by a newer release.",
    action: "Load the new data ›",
    a11y: "Load the new data",
  },
  code: {
    line: "The app was updated since this page opened.",
    action: "Reload ›",
    a11y: "Reload the page",
  },
} as const satisfies Record<ReplacedKind, { line: string; action: string; a11y: string }>;

/** the error names the crash server classes `expected` (workers/feeds/src/crashes.ts; tools/wire.test.ts pins both) */
export const REPLACED_NAMES = { data: "ReleaseReplaced", code: "SiteReplaced" } as const satisfies Record<ReplacedKind, string>;

/** what the server holds now, for the report: the release the pointer names, or that the site's entry moved */
export function replacedError(kind: ReplacedKind, o: { opened: string | null; live: string | null; cause: unknown }): Error {
  const was = o.cause instanceof Error ? o.cause.message : String(o.cause);
  const message = kind === "data"
    ? `a file of release ${o.opened ?? "?"} is no longer served (HTTP 404); the server's release is ${o.live ?? "?"}: ${was}`
    : `a code chunk of the site this page opened is no longer served (HTTP 404); the site was deployed since: ${was}`;
  const e = new Error(message, { cause: o.cause });
  e.name = REPLACED_NAMES[kind];
  return e;
}

/** `<url>: HTTP 404` — how the app's fetches say a 404 (`NetError`, `VerifyError`, and every message that wraps one) */
const SAID_404 = /\b(https?:\/\/[^\s'"`]+?): HTTP 404\b/;

/** The errors an error was made from, itself first (`cause`, as the in-season failure carries its read's; at most 5). */
function chain(e: unknown): unknown[] {
  const out: unknown[] = [];
  for (let x = e, i = 0; x && typeof x === "object" && i < 5; x = (x as { cause?: unknown }).cause, i++) out.push(x);
  return out;
}

/**
 * The URL a failure got a 404 for, or null (not a 404, or one that names no URL): a status of 404 with its `url`
 * (MapLibre's `AJAXError`, the outside mask's), or the words "<url>: HTTP 404" in its message — on it or on its cause.
 * A failed line (no answer at all) is never a 404: those are the environment's, and never reach here.
 */
export function notFoundUrl(e: unknown): string | null {
  for (const x of chain(e)) {
    const o = x as { status?: unknown; url?: unknown; message?: unknown };
    if (o.status === 404 && typeof o.url === "string" && o.url) return o.url;
    const m = typeof o.message === "string" ? SAID_404.exec(o.message) : null;
    if (m) return m[1]!;
  }
  return null;
}

/**
 * A code chunk that would not load (Expo's `AsyncRequireError`, "Loading module <url> failed"): its URL (the script's
 * `request`, else the one in the message), or null when it is not one. The script tag cannot say WHY (a 404 and a failed
 * line look the same): the web's probe asks (`replaced.web.ts`).
 */
export function missingChunk(e: unknown): { url: string | null } | null {
  for (const x of chain(e)) {
    const o = x as { name?: unknown; request?: unknown; message?: unknown };
    if (o.name !== "AsyncRequireError") continue;
    if (typeof o.request === "string" && o.request) return { url: o.request };
    const m = typeof o.message === "string" ? /Loading module (\S+) failed/.exec(o.message) : null;
    return { url: m ? m[1]! : null };
  }
  return null;
}
