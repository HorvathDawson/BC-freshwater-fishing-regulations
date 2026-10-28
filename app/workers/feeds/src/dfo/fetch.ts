// T225 port of pipeline/regs/dfo_salmon/fetch.py: the region pages, their slugs, and the validated fetch with retries.
import type { Fetch } from "../io";
import { sha256Bytes } from "./hash";
import { PyError, repr } from "./py";

export interface RegionPage {
  slug: string;
  region: number;
  part: string | null;
  name: string;
  isStub: boolean;
}

const page = (slug: string, region: number, part: string | null, name: string, isStub = false): RegionPage => ({
  slug,
  region,
  part,
  name,
  isStub,
});

export const PAGES: Record<string, RegionPage> = {
  "1": page("1", 1, null, "Vancouver Island"),
  "2": page("2", 2, null, "Lower Mainland"),
  "3": page("3", 3, null, "Thompson-Nicola"),
  "4": page("4", 4, null, "Kootenays"),
  "5": page("5", 5, null, "Cariboo (index)", true),
  "5a": page("5a", 5, "A", "Cariboo Part A, Fraser River Watershed"),
  "5b": page("5b", 5, "B", "Cariboo Part B, Coastal Watershed"),
  "6": page("6", 6, null, "Skeena"),
  "7": page("7", 7, null, "Omineca-Peace"),
  "8": page("8", 8, null, "Okanagan"),
};

/** numeric-then-letter: "5" < "5a" < "5b" < "6" */
export const ALL_SLUGS: string[] = Object.keys(PAGES).sort((x, y) => {
  const nx = Number(x.replace(/[ab]+$/, "") || 0);
  const ny = Number(y.replace(/[ab]+$/, "") || 0);
  return nx - ny || (x < y ? -1 : x > y ? 1 : 0);
});

export const REAL_SLUGS = ALL_SLUGS.filter((s) => !PAGES[s]!.isStub);

export function normalizeSlug(value: string | number): string {
  const slug = String(value).trim().toLowerCase();
  if (!(slug in PAGES)) throw new PyError("ValueError", `unknown region ${repr(String(value))}; known: [${ALL_SLUGS.map((s) => repr(s)).join(", ")}]`);
  return slug;
}

export const pageUrl = (slug: string): string => `https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region${slug}-eng.html`;

const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const REQUEST_TIMEOUT_MS = 30_000;
const MAX_ATTEMPTS = 4;
const BACKOFF_BASE_S = 2.0;
const SENTINELS = ['id="wb-cont"', "Recreational salmon fishing limits", 'property="dateModified"'];
const MIN_BYTES = 8_000;
const RE_DATE_MODIFIED = /<time[^>]*property="dateModified"[^>]*>\s*([0-9]{4}-[0-9]{2}-[0-9]{2})/i;
const RE_TITLE = /<title>([^<]*)<\/title>/i;

export class FetchError extends PyError {
  constructor(message: string) {
    super("FetchError", message);
  }
}

export interface RegionSnapshot {
  region: string;
  region_number: number;
  part: string | null;
  region_name: string;
  url: string;
  fetched_at: string;
  http_status: number;
  bytes: number;
  sha256: string;
  date_modified: string | null;
  title: string;
  has_table: boolean;
  attempts: number;
  elapsed_s: number;
  changed: boolean;
  path: string | null;
  warnings: string[];
}

/** `_validate`: a reason the body is not a real region page, else null. */
export function validate(body: string): string | null {
  const len = [...body].length;
  if (len < MIN_BYTES) return `body too small (${len} B < ${MIN_BYTES} B)`;
  const missing = SENTINELS.filter((s) => !body.includes(s));
  if (missing.length) return `missing sentinel(s): ${missing.join(", ")}`;
  return null;
}

/** datetime.now(timezone.utc).isoformat(timespec="seconds") — "2026-10-28T04:55:39+00:00" */
export const isoSeconds = (d: Date, z = false): string => d.toISOString().slice(0, 19) + (z ? "Z" : "+00:00");

export interface FetchRegionOptions {
  /** wait between attempts; injectable so tests do not sleep */
  sleep?: (ms: number) => Promise<void>;
  now?: () => Date;
  random?: () => number;
  maxAttempts?: number;
  timeoutMs?: number;
}

const realSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** `fetch_region` without the disk: the validated page text and its snapshot. Throws FetchError when every attempt fails. */
export async function fetchRegion(
  fetch: Fetch,
  region: string,
  o: FetchRegionOptions = {},
): Promise<{ html: string; snap: RegionSnapshot }> {
  const slug = normalizeSlug(region);
  const pg = PAGES[slug]!;
  const url = pageUrl(slug);
  const sleep = o.sleep ?? realSleep;
  const now = o.now ?? (() => new Date());
  const rnd = o.random ?? Math.random;
  const max = o.maxAttempts ?? MAX_ATTEMPTS;
  const warnings: string[] = [];
  const started = Date.now();
  let body: string | null = null;
  let status = 0;
  let attempt = 0;
  const backoff = (attempt: number, retryAfter: number | null = null) =>
    sleep(((retryAfter ?? BACKOFF_BASE_S * 2 ** (attempt - 1)) + rnd() * 0.75) * 1000);

  for (attempt = 1; attempt <= max; attempt++) {
    try {
      const ctl = new AbortController();
      const timer = setTimeout(() => ctl.abort(), o.timeoutMs ?? REQUEST_TIMEOUT_MS);
      let resp: Response;
      try {
        resp = await fetch(url, {
          headers: {
            "User-Agent": USER_AGENT,
            Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-CA,en;q=0.9",
          },
          signal: ctl.signal,
        });
      } finally {
        clearTimeout(timer);
      }
      status = resp.status;
      if ([429, 500, 502, 503, 504].includes(status)) {
        let retryAfter: number | null = null;
        const ra = resp.headers.get("Retry-After");
        if (ra && /^\d+$/.test(ra.trim())) retryAfter = Math.min(Number(ra.trim()), 60);
        warnings.push(`attempt ${attempt}: HTTP ${status}`);
        if (attempt < max) {
          await backoff(attempt, retryAfter);
          continue;
        }
        throw new FetchError(`region ${slug}: HTTP ${status} after ${attempt} attempts`);
      }
      if (status >= 400) throw new PyError("HTTPError", `HTTP Error ${status} for url: ${url}`);
      const candidate = await resp.text();
      const reason = validate(candidate);
      if (reason) {
        warnings.push(`attempt ${attempt}: ${reason}`);
        if (attempt < max) {
          await backoff(attempt);
          continue;
        }
        throw new FetchError(`region ${slug}: ${reason} (after ${attempt} attempts)`);
      }
      body = candidate;
      break;
    } catch (e) {
      if (e instanceof FetchError) throw e;
      const name = e instanceof PyError ? e.pyType : e instanceof Error ? e.name : "Exception";
      const msg = e instanceof Error ? e.message : String(e);
      warnings.push(`attempt ${attempt}: ${name}: ${msg}`);
      if (attempt >= max) throw new FetchError(`region ${slug}: ${name}: ${msg}`);
      await backoff(attempt);
    }
  }
  if (body === null) throw new FetchError(`region ${slug}: no body`);
  const bytes = new TextEncoder().encode(body);
  const dm = RE_DATE_MODIFIED.exec(body);
  const title = RE_TITLE.exec(body);
  return {
    html: body,
    snap: {
      region: slug,
      region_number: pg.region,
      part: pg.part,
      region_name: pg.name,
      url,
      fetched_at: isoSeconds(now()),
      http_status: status,
      bytes: bytes.length,
      sha256: sha256Bytes(bytes),
      date_modified: dm ? dm[1]! : null,
      title: title ? title[1]!.split("|")[0]!.trim() : "",
      has_table: body.includes("<table"),
      attempts: Math.min(attempt, max),
      elapsed_s: Math.round((Date.now() - started)) / 1000,
      changed: true,
      path: null,
      warnings,
    },
  };
}
