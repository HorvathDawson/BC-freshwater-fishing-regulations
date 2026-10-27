/**
 * T196 — THE PERFORMANCE GATE. A static page must beat the app, so the release fails when a built page, the shared stylesheet
 * or the p95 page exceeds the budget (beside T129's WEB_BUDGET), or when a page ships blocking JavaScript. Sizes are the
 * gzip of what is on disk; the numbers are recorded in `budget.json` next to the pages. The Lighthouse mobile run (LCP, CLS,
 * TBT as INP's lab proxy, Performance, SEO, Accessibility) is `lighthouserc.json`, generated here for the pages that exist,
 * and run by the release job with `lhci autorun` (the CI tool is not a dependency of this workspace).
 */
import { gzipSync } from "node:zlib";

export const BUDGET = {
  /** every HTML page, gzip */ htmlGzipBytes: 30 * 1024,
  /** the 95th-percentile page, gzip (a typical water page is a few KB; this catches a template that bloats them all) */ p95HtmlGzipBytes: 16 * 1024,
  /** the shared stylesheet, gzip */ cssGzipBytes: 10 * 1024,
  /** the inlined critical CSS, raw */ criticalCssBytes: 2 * 1024,
  /** lab targets, mobile profile, 4G throttled */ lcpMs: 1800, cls: 0.05, tbtMs: 200, performance: 0.95, seo: 1, accessibility: 0.95,
} as const;

export interface Measured {
  readonly pages: number; readonly largest: { readonly path: string; readonly gzipBytes: number };
  readonly p95GzipBytes: number; readonly cssGzipBytes: number; readonly criticalCssBytes: number;
}

const gz = (s: string) => gzipSync(s, { level: 9 }).length;

/** Sizes of the HTML pages (`files` keyed by output path, `pages` the HTML ones) and the stylesheet. */
export function measure(files: ReadonlyMap<string, string>, htmlPaths: readonly string[], cssPath: string): Measured {
  const sizes = htmlPaths.map((p) => ({ path: p, gzipBytes: gz(files.get(p)!) })).sort((a, b) => a.gzipBytes - b.gzipBytes);
  const critical = /<style>([\s\S]*?)<\/style>/.exec(files.get(htmlPaths[0]!) ?? "")?.[1] ?? "";
  return {
    pages: sizes.length, largest: sizes.at(-1) ?? { path: "", gzipBytes: 0 },
    p95GzipBytes: sizes.length ? sizes[Math.min(sizes.length - 1, Math.ceil(sizes.length * 0.95) - 1)]!.gzipBytes : 0,
    cssGzipBytes: gz(files.get(cssPath) ?? ""), criticalCssBytes: Buffer.byteLength(critical),
  };
}

/** A `<script>` that blocks the parser: not JSON-LD, not async, not deferred, not a module. */
export function blockingScripts(html: string): string[] {
  return [...html.matchAll(/<script\b([^>]*)>/g)].map((m) => m[1]!)
    .filter((a) => !/type="application\/ld\+json"/.test(a) && !/\b(async|defer)\b/.test(a) && !/type="module"/.test(a));
}

/** What is over budget; [] = within. */
export function checkBudget(m: Measured, files: ReadonlyMap<string, string>, htmlPaths: readonly string[]): string[] {
  const bad: string[] = [];
  if (m.largest.gzipBytes > BUDGET.htmlGzipBytes) bad.push(`${m.largest.path} is ${m.largest.gzipBytes} B gzip, over ${BUDGET.htmlGzipBytes}`);
  if (m.p95GzipBytes > BUDGET.p95HtmlGzipBytes) bad.push(`p95 page is ${m.p95GzipBytes} B gzip, over ${BUDGET.p95HtmlGzipBytes}`);
  if (m.cssGzipBytes > BUDGET.cssGzipBytes) bad.push(`the stylesheet is ${m.cssGzipBytes} B gzip, over ${BUDGET.cssGzipBytes}`);
  if (m.criticalCssBytes > BUDGET.criticalCssBytes) bad.push(`the inlined critical CSS is ${m.criticalCssBytes} B, over ${BUDGET.criticalCssBytes}`);
  for (const p of htmlPaths) if (blockingScripts(files.get(p)!).length) bad.push(`${p} ships blocking JavaScript`);
  return bad;
}

/** The Lighthouse CI config for the built pages (mobile, simulated 4G); `urls` are site paths of pages in the dist folder. */
export function lighthouseConfig(urls: readonly string[]): object {
  const err = (n: number) => ["error", { minScore: n }];
  const max = (n: number) => ["error", { maxNumericValue: n }];
  return {
    ci: {
      collect: {
        staticDistDir: ".", url: urls, numberOfRuns: 3,
        settings: { formFactor: "mobile", throttlingMethod: "simulate", throttling: { rttMs: 150, throughputKbps: 1638.4, cpuSlowdownMultiplier: 4 } },
      },
      assert: {
        assertions: {
          "categories:performance": err(BUDGET.performance), "categories:seo": err(BUDGET.seo), "categories:accessibility": err(BUDGET.accessibility),
          "largest-contentful-paint": max(BUDGET.lcpMs), "cumulative-layout-shift": max(BUDGET.cls), "total-blocking-time": max(BUDGET.tbtMs),
        },
      },
    },
  };
}
