/**
 * (T368, owner 2026-11-03/04) NO SILENT FALLBACK, APP-WIDE — the net. Every place the app catches a failure (`catch {}`,
 * `catch (e) {}`, `.catch(…)`, a two-argument `.then(ok, fail)`) in core, data, ui, ui-native, map and the app must
 * visibly HANDLE it: throw it on, or report it through core's one reporter (`reportFailure`, `reportOnce`,
 * `reported(v)`, `offlineAs(v)`, ui's `mapError`; in the map package its own `report` / `fail` / `onError`, which
 * every host turns into `mapError`). The environment (offline, a refused permission) is filtered in `reportFailure`
 * itself, so a handler never decides that. What is left is listed in FINE — per file, how many sites and why
 * (a preference, a cache, a failure that is not cached, a probe, a bounded retry, an environment state shown).
 * A NEW CATCH THAT DOES NONE OF THESE FAILS HERE: report it (and show it where the reader can act), throw it, or —
 * only when it truly is one of the FINE kinds — raise its file's count here with the reason.
 *
 * The scan is a net, not a proof: what a failure SHOWS is pinned by the render tests; the reporter by crashReport.test.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

export const APP = fileURLToPath(new URL("..", import.meta.url));
export const ROOTS = ["packages/core/src", "packages/data/src", "packages/ui/src", "packages/ui-native/src",
  "packages/map/src", "apps/mobile/src", "workers/feeds/src"];

function sources(dir: string, out: string[] = []): string[] {
  let names: string[];
  try { names = readdirSync(dir); } catch { return out; }
  for (const f of names) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f) && !/\.fixture\.ts$/.test(f)
             && !/\.generated\.ts$/.test(f)) out.push(p);
  }
  return out;
}
export const files = (): string[] => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();

/** Comments out (lines kept, so a site's line is its line in the file), strings kept (a URL's `//` is not a comment:
 *  only one at a line's start or after a space, `;` or a bracket). */
export const code = (src: string): string =>
  src.replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, "")).replace(/(^|[\s;{}),])\/\/.*$/gm, "$1");

/** The text from `open` (an index of `(` or `{`) to its matching close, strings and templates skipped roughly. */
function balanced(src: string, open: number): string {
  const pairs: Record<string, string> = { "(": ")", "{": "}", "[": "]" };
  const stack: string[] = [];
  for (let i = open; i < src.length; i += 1) {
    const c = src[i]!;
    if (c === '"' || c === "'" || c === "`") {
      for (i += 1; i < src.length && src[i] !== c; i += 1) if (src[i] === "\\") i += 1;
      continue;
    }
    if (pairs[c]) stack.push(pairs[c]!);
    else if (c === stack[stack.length - 1]) { stack.pop(); if (!stack.length) return src.slice(open, i + 1); }
  }
  return src.slice(open);
}

/** A handler that HANDLES: it throws on, or reports (core's `reportFailure` / `reportOnce` / `reported` / `offlineAs`, ui's
 *  `mapError`). */
export const HANDLES = /\bthrow\b|\breportFailure\b|\breportOnce\(|\breported\(|\bofflineAs\(|\bmapError\b/;

export interface Site { line: number; kind: "try" | ".catch" | ".then"; handler: string }

/**
 * Every catch site in `src` (comments already out): `catch {…}` / `catch (e) {…}`, `.catch(…)`, and a two-argument
 * `.then(ok, fail)` whose second argument is a function.
 */
export function catchSites(src: string): Site[] {
  const out: Site[] = [];
  const lineOf = (i: number) => src.slice(0, i).split("\n").length;
  for (const m of src.matchAll(/\bcatch\s*(?:\([^)]*\)\s*)?\{/g)) {
    if (src.slice(Math.max(0, m.index! - 1), m.index!) === ".") continue;
    out.push({ line: lineOf(m.index!), kind: "try", handler: balanced(src, m.index! + m[0].length - 1) });
  }
  for (const m of src.matchAll(/\.catch\s*\(/g))
    out.push({ line: lineOf(m.index!), kind: ".catch", handler: balanced(src, m.index! + m[0].length - 1) });
  for (const m of src.matchAll(/\.then\s*\(/g)) {
    const args = balanced(src, m.index! + m[0].length - 1);
    const second = secondArg(args.slice(1, -1));
    if (second !== null && /=>|^\s*[A-Za-z_$][\w$.]*\s*$/.test(second))
      out.push({ line: lineOf(m.index!), kind: ".then", handler: second });
  }
  return out.sort((a, b) => a.line - b.line);
}

/** The second top-level argument of an argument list (null when there is one argument). */
function secondArg(args: string): string | null {
  let depth = 0;
  for (let i = 0; i < args.length; i += 1) {
    const c = args[i]!;
    if (c === '"' || c === "'" || c === "`") { for (i += 1; i < args.length && args[i] !== c; i += 1) if (args[i] === "\\") i += 1; continue; }
    if ("([{".includes(c)) depth += 1;
    else if (")]}".includes(c)) depth -= 1;
    else if (c === "," && depth === 0) {
      const rest = args.slice(i + 1).trim();
      return rest ? rest : null;
    }
  }
  return null;
}

/** The sites in `src` that neither throw nor report: each must be listed as FINE (with its reason) or fixed. */
export const unhandled = (src: string, path = ""): Site[] =>
  catchSites(code(src)).filter((s) => !HANDLES.test(s.handler) && !(path.startsWith("packages/map/") && MAP_HANDLES.test(s.handler))
    && !(path.startsWith("workers/feeds/") && WORKER_HANDLES.test(s.handler)));

/** (T431) In the feeds worker (no core reporter: it is a server) a failure is handled when it is LOGGED (`log(…)`,
 *  console.error / warn: the run's log lines, `wrangler tail`), ANSWERED as an HTTP error (`json({ ok: false, … })`), or
 *  RECORDED in the job's result (an `error` field, `res.ok = false`, a `failed` count) — the run's status page and the
 *  owner's email carry those. */
export const WORKER_HANDLES = /\blog\(|\blogErr\(|\bconsole\.(?:error|warn)\(|\bjson\(\{ ok: false|\berror\s*[=:]|\.ok = false|\.failed\+\+|\bfailed\.push\(|\bhold\(|\browHolds\.set\(|\bnotices\.push\(/;

/** In the map package a failure goes to the map's own `report` / `fail` / `onError`, which is the host's `onError` —
 *  `mapError` (reported) unless a host passes its own (MapScreen defaults to it; pinned in the test). */
export const MAP_HANDLES = /\breport\b|\bfail\b|\bonError\b/;

export const read = (p: string): string => readFileSync(join(APP, p), "utf8");

/** Why each remaining site is fine, per file: `n` sites (exactly — a new one, or one fixed, moves the count). */
const PREF = "a preference or a kept setting: unreadable → its default; a refused write costs only the next start";
const CACHE = "a cache: unreadable → asked again; a refused write costs only the next offline open";
const UNCACHE = "a failure is not cached (the caller still gets the rejection, X4)";
const WARM = "warming / prefetch: the real ask shows and reports its own failure";
const PROBE = "a test-bed probe or instrumentation";
const CAP = "a capability the platform may not have (shown as that state)";
const ENV = "the environment (offline, no fix, a refused permission, a share sheet closed): shown, never reported (owner Q1)";
const W_STATE = "the worker's own kept state in R2: unreadable → started fresh this run (as the Python publisher does)";
const W_TORN = "a torn or unreadable stored object or line is skipped (the next write replaces it)";
const W_ABSENT = "a station or source that did not answer is absent this run (the run's source health counts it)";
export const FINE: Record<string, { n: number; why: string }> = {
  "apps/mobile/src/App.tsx": { n: 3, why: `the Updates-log tools' cache delete / copy (shown: copyFailed; ${ENV}); a spots refresh shown by spotsHook.error, reported in ui/spots.ts` },
  "apps/mobile/src/envStore.native.ts": { n: 2, why: PREF },
  "apps/mobile/src/envStore.web.ts": { n: 2, why: PREF },
  "apps/mobile/src/locate.native.ts": { n: 2, why: ENV },
  "apps/mobile/src/locate.web.ts": { n: 1, why: CAP },
  "apps/mobile/src/memoryTrim.native.ts": { n: 1, why: "hands the failure to `failed` (App.tsx passes reportFailure)" },
  "apps/mobile/src/splash.ts": { n: 1, why: "the splash hides early; nothing lost" },
  "apps/mobile/src/startLink.web.ts": { n: 1, why: "a bad start link opens the plain map (owner Q9: acceptable), named in the Updates log" },
  "packages/core/src/appUpdate.ts": { n: 1, why: "T390: the kept once-a-day check, unreadable → asked again today" },
  "packages/core/src/inseason.ts": { n: 1, why: "T517: an end display.ends cannot say becomes the cover's `whereFailure` — the notice stays, only \"Where\" goes, and ui/inseason.ts reports it" },
  "packages/core/src/layers.ts": { n: 1, why: PREF },
  "packages/core/src/paint.ts": { n: 1, why: "turned into the reader's typed failure (`r.fail`)" },
  "packages/core/src/recentSearches.ts": { n: 1, why: PREF },
  "packages/core/src/runTiming.ts": { n: 1, why: PREF },
  "packages/data/src/feed/history.ts": { n: 3, why: CACHE },
  "packages/data/src/feed/http.ts": { n: 3, why: `${CACHE} (history notes, observation sync — its own outbox)` },
  "packages/data/src/feed/inseason.ts": { n: 2, why: CACHE },
  "packages/data/src/feed/runtiming.ts": { n: 2, why: CACHE },
  "packages/data/src/feed/salmon.ts": { n: 4, why: `${CACHE}; latest() never rejects (defensive); ${UNCACHE}` },
  "packages/data/src/net/fetch.ts": { n: 1, why: "the retry loop: the last NetError is thrown after the final try" },
  "packages/data/src/net/gate.ts": { n: 1, why: "hands the rejection to the waiting caller (`e.fail`)" },
  "packages/data/src/net/share.ts": { n: 2, why: "hands the rejection to each sharing caller" },
  "packages/data/src/regs/place.ts": { n: 1, why: UNCACHE },
  "packages/data/src/regs/reads.ts": { n: 2, why: `${UNCACHE}; a batch failure rejects every waiter` },
  "packages/data/src/release/assets.ts": { n: 3, why: "verified either way (re-download / VerifyError); a bounded retry whose failure is thrown" },
  "packages/data/src/release/background.ts": { n: 3, why: `${PROBE}; a prefetch failure goes to the Updates log as an event` },
  "packages/data/src/release/boot.ts": { n: 9, why: "typed rethrows / defect classification; the manifest unread offline (the held set opens; a first run throws BootError); a permission prompt; cleanup without a manifest; the update service owns staging failures; (T403) a held manifest that will not read is left to openSet, which refuses it" },
  "packages/data/src/release/drivers/cache.web.ts": { n: 4, why: "Cache Storage refused → a session copy in memory (a best-effort cache)" },
  "packages/data/src/release/drivers/install.native.ts": { n: 1, why: "a bounded retry, thrown at the end" },
  "packages/data/src/release/drivers/open.web.ts": { n: 1, why: UNCACHE },
  "packages/data/src/release/drivers/serial.ts": { n: 2, why: "the queue's tail: the caller holds the job's own promise" },
  "packages/data/src/release/drivers/source.web.ts": { n: 1, why: "a priority raise (the original promise carries the outcome); (T409) cached bytes that fail verification are fetched again, any other error is rethrown" },
  "packages/data/src/release/install.ts": { n: 3, why: "the follower hash falls back to the authoritative check; a held manifest costs only a download; the assets failure is thrown at the end" },
  "packages/data/src/release/loader.ts": { n: 17, why: `${UNCACHE}; a routing hint (stored); ${WARM}; T360: a retired set's close hands its failure to retire's own reporter (\`retired\`), and a connection that never opened is its opener's failure to say (\`shut\`, \`trim\`)` },
  "packages/data/src/release/prefetch.ts": { n: 2, why: WARM },
  "packages/data/src/report/outbox.ts": { n: 4, why: "the outbox's own storage; offline → queued (shown: saved on this device)" },
  "packages/data/src/spots/core.ts": { n: 2, why: "a photo that cannot be removed is an orphan, not a lost record; a torn write keeps the old file" },
  "packages/data/src/spots/dayFlow.ts": { n: 3, why: `${UNCACHE}; a live fill stays pending, asked again by the next refresh` },
  "packages/data/src/spots/fastFail.ts": { n: 2, why: "hands the rejection on (`reject`)" },
  "packages/data/src/spots/likeVisit.ts": { n: 3, why: CACHE },
  "packages/data/src/spots/photoStore.web.ts": { n: 1, why: "downscaling failed: the original photo is kept, larger" },
  "packages/data/src/spots/queue.ts": { n: 2, why: "the queue's tail: the caller holds `run`" },
  "packages/data/src/spots/runTimingStore.native.ts": { n: 1, why: CACHE },
  "packages/data/src/spots/runTimingStore.web.ts": { n: 1, why: CACHE },
  "packages/data/src/spots/store.web.ts": { n: 1, why: PREF },
  "packages/data/src/update/check.ts": { n: 2, why: "a value, not a swallow: `{kind: failed}` goes to failSource (shown on the sync badge; reported unless offline)" },
  "packages/data/src/update/log.ts": { n: 2, why: "the Updates log's own storage" },
  "packages/data/src/update/service.ts": { n: 8, why: "an event listener's guard; validator caches; a refused feed / an unparsable manifest go to failSource (shown on the sync badge, reported there); a staging failure emitted as an event (shown); the outbox keeps and shows what is queued" },
  "packages/map/src/Map.native.tsx": { n: 6, why: "a fresh copy starts clean (mirrored after); label placement (project/unproject); a test-bed target finder; (T445) an atlas source not yet mounted has no tag yet (learnt on its next attach)" },
  "packages/map/src/native/probe.ts": { n: 1, why: PROBE },
  "packages/map/src/native/useColouring.ts": { n: 2, why: "the native tag before mount: not mounted yet" },
  "packages/map/src/web/tileIds.ts": { n: 1, why: "the next tile event asks again" },
  "packages/ui-native/src/FirstRunNotice.tsx": { n: 2, why: PREF },
  "packages/ui-native/src/LeadUpStrip.tsx": { n: 1, why: "not laid out yet" },
  "packages/ui-native/src/Walkthrough.tsx": { n: 3, why: PREF },
  "packages/ui-native/src/coachScroll.ts": { n: 1, why: "not laid out yet (T466: the coach's measure moved here from CoachMarks)" },
  "packages/ui-native/src/chartInput.web.ts": { n: 1, why: CAP },
  "packages/ui-native/src/imageryProbe.ts": { n: 1, why: `satellite imagery down: shown as the imagery state (${ENV})` },
  "packages/ui-native/src/menu/ReportPage.tsx": { n: 1, why: `share refused: shown (noShare); ${ENV}` },
  "packages/ui-native/src/motion.ts": { n: 1, why: CAP },
  "packages/ui-native/src/netInfoConnectivity.ts": { n: 3, why: `${CAP} (no NetInfo: never metered)` },
  "packages/ui-native/src/part.tsx": { n: 2, why: "the chunk loader reports (part.tsx's `.catch` → reportFailure) and the part shows Try again; these read its outcome" },
  "packages/ui-native/src/regs/SynopsisLoading.tsx": { n: 1, why: "Retry: openBook shows and reports" },
  "packages/ui-native/src/regs/synopsisPdf.native.ts": { n: 4, why: "held check / bounded retry / alternate path, thrown at the end; T360: the next open waits for this one to END (its failure is its own caller's)" },
  "packages/ui-native/src/regs/synopsisPdf.web.ts": { n: 1, why: "a label write, then close and rethrow" },
  "packages/ui-native/src/regs/useAnchors.ts": { n: 1, why: "not laid out yet" },
  "packages/ui-native/src/shell/SpotHost.tsx": { n: 1, why: `no fix for parking: said (parkingNoFix); ${ENV}` },
  "packages/ui-native/src/shell/prewarm.native.ts": { n: 1, why: WARM },
  "packages/ui/src/chart.ts": { n: 2, why: `${WARM} (askOnce keeps no failure; the chart's own ask shows and reports)` },
  "packages/ui/src/crashReport.ts": { n: 15, why: "the reporter's own guards: reporting cannot report itself" },
  "packages/ui/src/errorLog.ts": { n: 2, why: "logging never breaks logging" },
  "packages/ui/src/hooks/water.ts": { n: 2, why: `${UNCACHE}; ${WARM}` },
  "packages/ui/src/imagery.ts": { n: 1, why: `the imagery probe: shown as the imagery state (${ENV})` },
  "packages/ui/src/layerChoice.ts": { n: 2, why: PREF },
  "packages/ui/src/mapRail.ts": { n: 2, why: `permission / locate: shown as a state (${ENV})` },
  "packages/ui/src/myLocation.ts": { n: 1, why: `location refused: said (refusedLine); ${ENV}` },
  "packages/ui/src/net.ts": { n: 1, why: PREF },
  "packages/ui/src/photos.ts": { n: 1, why: "an orphaned photo file, not a lost record" },
  "packages/ui/src/place.ts": { n: 1, why: "hands it to `onMissing`, which reports (systemBack.web.ts)" },
  "packages/ui/src/prefetch.ts": { n: 3, why: `${PROBE}; ${CAP} (battery)` },
  "packages/ui/src/recentSearches.ts": { n: 2, why: PREF },
  "packages/ui/src/regs.ts": { n: 6, why: `${WARM}; ${UNCACHE}; a probe` },
  "packages/ui/src/release.ts": { n: 1, why: UNCACHE },
  "packages/ui/src/runTiming.ts": { n: 3, why: PREF },
  "packages/ui/src/spots.ts": { n: 2, why: "T314: one spot's fill kept and asked again next refresh; the retry runner (write() shows and reports)" },
  "packages/ui/src/sync.ts": { n: 2, why: "stamps: the update service shows these failures (SyncErrorSheet) and reports them (failSource)" },
  // (T431) the feeds worker: a server job, so "handled" is WORKER_HANDLES (logged / answered / recorded); what is left is
  "workers/feeds/src/backup.ts": { n: 1, why: "(T559) a personal record whose date cannot be read: a report is aged by its folder day, an error kind is never copied" },
  "workers/feeds/src/crashes.ts": { n: 4, why: W_TORN },
  "workers/feeds/src/dfo/live.ts": { n: 3, why: `${W_STATE}; page texts unread leave only the text-change notice out (the header parse failure has its own notice)` },
  "workers/feeds/src/dispatch.ts": { n: 1, why: "GitHub's reply shown as its text when it is not JSON (T558: the dispatch record's read throws)" },
  "workers/feeds/src/gauges/forecast.ts": { n: 1, why: W_ABSENT },
  "workers/feeds/src/gauges/publish.ts": { n: 9, why: `${W_ABSENT}; ${W_STATE}; an unreadable forecast file is skipped as the Python skips it` },
  "workers/feeds/src/gauges/score.ts": { n: 4, why: `${W_STATE}; ${W_TORN}` },
  "workers/feeds/src/gauges/skill.ts": { n: 2, why: `${W_STATE}; ${W_TORN}` },
  "workers/feeds/src/guard.ts": { n: 1, why: "the edge rate limiter failing opens (T249): the R2 per-IP gate still holds" },
  "workers/feeds/src/health.ts": { n: 3, why: "a URL that will not parse gets the unknown source's policy (the fetch itself then throws and is counted); freeing a retried connection" },
  "workers/feeds/src/index.ts": { n: 3, why: "a daily job's status file that cannot be read is served as { unreadable: why } (T609 W12); a bad POST body is answered 400" },
  "workers/feeds/src/inseason.ts": { n: 2, why: W_STATE },
  "workers/feeds/src/io.ts": { n: 1, why: `${W_STATE} (the write manifest: unreadable → every file written again)` },
  "workers/feeds/src/notify.ts": { n: 1, why: `${W_STATE} (what was sent: unreadable → may be sent again, never lost)` },
  "workers/feeds/src/reports.ts": { n: 6, why: `${W_TORN}; ${W_STATE} (the per-IP gate); the edge limiter failing never blocks (the R2 gate holds); a test-report delete the next day's purge tries again` },
};

const sites = () => files().map((f) => [f, unhandled(read(f), f)] as const).filter(([, u]) => u.length);

describe("no silent fallback (T368): every catch throws, reports, or is listed", () => {
  it("scans the app", () => expect(files().length).toBeGreaterThan(300));

  it("every file's unhandled catch sites are exactly its FINE count (a new one is reported, thrown, or listed)", () => {
    const got = Object.fromEntries(sites().map(([f, u]) => [f, `${u.length} @ ${u.map((s) => s.line).join(",")}`]));
    const want = Object.fromEntries(Object.entries(FINE).map(([f, v]) => [f, v.n]));
    const off = Object.keys({ ...got, ...want }).sort()
      .filter((f) => Number((got[f] ?? "0").split(" ")[0]) !== (want[f] ?? 0))
      .map((f) => `${f}: ${got[f] ?? "0"} unhandled, FINE lists ${want[f] ?? 0}`);
    expect(off).toEqual([]);
  });

  it("every FINE entry says why", () => {
    for (const [f, v] of Object.entries(FINE)) expect([f, v.why.length > 10]).toEqual([f, true]);
  });

  it("the map's own report goes to mapError unless a host passes its own (MapScreen's default)", () => {
    expect(code(read("packages/ui-native/src/MapScreen.tsx"))).toMatch(/onError=\{onError \?\? mapError\}/);
    expect(code(read("packages/ui/src/crashReport.ts"))).toMatch(/export function mapError[\s\S]{0,120}reportFailure\(e\)/);
  });

  it("every link out of the app goes through openLink (a refused open is reported)", () => {
    const raw = files().filter((f) => f !== "packages/ui-native/src/openLink.ts" && /\bLinking\.openURL\(/.test(code(read(f))));
    expect(raw).toEqual([]);
  });
});

describe("the scan itself (mutation pins)", () => {
  it("finds a swallowing try/catch, .catch and two-argument .then", () => {
    const src = [
      "try { a(); } catch { return null; }",
      "try { a(); } catch (e) { console.warn(e); }",
      "p.catch(() => null);",
      "p.then((v) => v, () => undefined);",
    ].join("\n");
    expect(unhandled(src).map((s) => [s.line, s.kind])).toEqual([[1, "try"], [2, "try"], [3, ".catch"], [4, ".then"]]);
  });

  it("passes a handler that throws or reports, and a one-argument .then", () => {
    const src = [
      "try { a(); } catch (e) { reportFailure(e); return null; }",
      "try { a(); } catch (e) { if (x) return; throw e; }",
      "p.catch(reported(null));",
      "p.catch(offlineAs(undefined));",
      "p.then((v) => v, reportFailure);",
      "p.then((v) => v);",
      "x.catch(mapError);",
    ].join("\n");
    expect(unhandled(src)).toEqual([]);
  });

  it("a comment naming catch is not a site, and a site's line is its line in the file", () => {
    expect(unhandled("/* p.catch(() => null)\n */\n// try {} catch {}\nq.catch(() => 1);").map((s) => s.line)).toEqual([4]);
  });

  it("a new swallowing catch in a listed file moves its count (the net fails)", () => {
    const f = "packages/ui/src/regs.ts";
    const before = unhandled(read(f), f).length;
    expect(before).toBe(FINE[f]!.n);
    const mutated = `${read(f)}\nexport const later = (p: Promise<unknown>) => p.catch(() => null);\n`;
    expect(unhandled(mutated, f).length).toBe(before + 1);
  });

  it("an unlisted file with a swallowing catch would be named", () => {
    const f = "packages/ui/src/async.ts";
    expect(FINE[f]).toBeUndefined();
    expect(unhandled(read(f), f)).toEqual([]);
    expect(unhandled(read(f).replace("reportFailure(e);", ""), f).length).toBe(1);
  });

  it("(T431) the feeds worker is scanned: a 'logged below' catch is named, a logged one passes", () => {
    expect(files().some((f) => f.startsWith("workers/feeds/src/"))).toBe(true);
    const w = "workers/feeds/src/x.ts";
    expect(unhandled("try { await deliver(e); } catch { /* logged below */ }", w).length).toBe(1);
    expect(unhandled("try { await deliver(e); } catch (e) { console.error(String(e)); }", w)).toEqual([]);
    expect(unhandled("try { await deliver(e); } catch (e) { console.error(String(e)); }", "packages/ui/src/x.ts").length).toBe(1);
  });

  it("map handlers count only in the map package", () => {
    expect(unhandled("p.catch(report);", "packages/map/src/x.ts")).toEqual([]);
    expect(unhandled("p.catch(report);", "packages/ui/src/x.ts").length).toBe(1);
  });
});
