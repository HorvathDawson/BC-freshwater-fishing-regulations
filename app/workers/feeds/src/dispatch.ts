// T413: THE WORKER STARTS A GITHUB WORKFLOW ITSELF when a slow source moved (a new HYDAT today; run timing next, T412).
// Owner 2026-11-05: GitHub is never the scheduler — the worker notices the change and starts the run.
//
// Needs the secret GITHUB_DISPATCH_TOKEN (a fine-grained token, Actions: write on this repo only; the OWNER makes it).
// Without it nothing is started and the email keeps today's "press the button" steps. A start is recorded per workflow +
// version in R2 (private/feeds/dispatch/<workflow>.json), so one version starts one run; a failed start is retried at
// most every RETRY_MS and the email says why it failed (never silent).
import { readStored, type Store } from "./io";

export const REPO = "HorvathDawson/BC-freshwater-fishing-regulations";
/** workflow_dispatch runs the file as it is on this ref (the dev branch until the workflows are on main) */
export const DEFAULT_REF = "redesign/stream-sections";
export const RETRY_MS = 6 * 3600_000;
/**
 * (T554 item 5) GitHub's 204 means "request accepted", not "the run succeeded". A caller that is still asking this long
 * after a start (the HYDAT notice stays up while clim.json is behind; the workflow's own limit is 60 minutes) has a run
 * that failed or uploaded nothing: it is started ONE more time, and after that never again — the owner is told daily.
 */
export const OVERDUE_MS = 6 * 3600_000;
export const MAX_STARTS = 2;

export type Dispatch =
  | { state: "no-token" }
  | {
      state: "started"; at: string; workflow: string; ref: string;
      /** (T554 item 5) how many times this version was started (absent = 1) and when the first start was */
      starts?: number; first?: string;
      /** (T554 item 5) started MAX_STARTS times, the last one more than OVERDUE_MS ago, and the caller still asks */
      overdue?: boolean;
    }
  /** `starts` / `first` (T554 item 5): a start GitHub refused AFTER an earlier start of this version went overdue */
  | { state: "failed"; at: string; workflow: string; ref: string; error: string; starts?: number; first?: string };

/** (T554 item 5) Did a started run not do its job: it was started again (or that second start was refused), or it is overdue. */
export const unfinished = (d: Dispatch | undefined): boolean =>
  !!d && ((d.state === "started" && ((d.starts ?? 1) > 1 || d.overdue === true)) || (d.state === "failed" && (d.starts ?? 0) >= 1));

export interface DispatchEnv {
  GITHUB_DISPATCH_TOKEN?: string;
  GITHUB_DISPATCH_REF?: string;
  /** (T404) "production": the run is asked for the production bucket (`target: production`); anything else, the
   *  workflow's default (staging). Until the owner enables the workflows' production option, GitHub refuses that input
   *  and the failed start is emailed — a production worker never quietly updates staging's bucket. */
  FEEDS_ENV?: string;
}

type Fetch = (url: string, init?: RequestInit) => Promise<Response>;

/**
 * Start `workflow` once for `version` (a repeat call for a started version returns the recorded start).
 * `opts.redoAfterMs` (T554 item 5): the caller calls only WHILE THE WORK IS STILL DUE, so a start older than this did not
 * do it: one more start (MAX_STARTS in all), then `overdue` and no further start.
 */
export async function dispatchOnce(
  env: DispatchEnv,
  fetch_: Fetch,
  store: Store,
  workflow: string,
  version: string,
  inputs: Record<string, string>,
  now: Date,
  opts: { redoAfterMs?: number } = {},
): Promise<Dispatch> {
  if (!env.GITHUB_DISPATCH_TOKEN) return { state: "no-token" };
  const ref = env.GITHUB_DISPATCH_REF || DEFAULT_REF;
  const key = `${workflow}.json`;
  // (T558) absent = never started; a failed read THROWS (readStored): read as "never", the same version starts twice
  const last = await readStored<Dispatch & { version?: string }>(store, key);
  const prior = last && last.version === version && last.state !== "no-token" ? last : null;
  // the starts GitHub accepted so far for this version, and the first of them
  const starts = prior ? prior.starts ?? (prior.state === "started" ? 1 : 0) : 0;
  const first = prior ? prior.first ?? (prior.state === "started" ? prior.at : undefined) : undefined;
  if (prior && prior.state === "started") {
    const late = opts.redoAfterMs !== undefined && now.getTime() - Date.parse(prior.at) >= opts.redoAfterMs;
    if (!late) return prior;
    if (starts >= MAX_STARTS) return { ...prior, overdue: true };
  }
  if (prior && prior.state === "failed" && now.getTime() - Date.parse(prior.at) < RETRY_MS) return prior;

  const at = now.toISOString();
  let out: Dispatch;
  try {
    const r = await fetch_(`https://api.github.com/repos/${REPO}/actions/workflows/${workflow}/dispatches`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.GITHUB_DISPATCH_TOKEN}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "bc-fishing-feeds-worker",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ref, inputs: env.FEEDS_ENV === "production" ? { ...inputs, target: "production" } : inputs }),
    });
    out = r.status === 204
      ? { state: "started", at, workflow, ref, ...(starts ? { starts: starts + 1, first: first! } : {}) }
      : { state: "failed", at, workflow, ref, error: `GitHub answered ${r.status}: ${(await r.text()).slice(0, 300)}`, ...(starts ? { starts, first } : {}) };
  } catch (e) {
    out = { state: "failed", at, workflow, ref, error: String(e), ...(starts ? { starts, first } : {}) };
  }
  await store.put(key, JSON.stringify({ ...out, version }), { contentType: "application/json" });
  return out;
}

/** The "what to do" words for an email, from a dispatch result (the button steps when nothing was started). */
export function dispatchSteps(d: Dispatch | undefined, button: string): { text: string; code?: string }[] {
  if (!d || d.state === "no-token") return [{ text: button }];
  if (d.state === "started" && d.overdue)
    return [
      { text: `The worker started "${d.workflow}" on ${d.ref} ${d.starts ?? 1} times (first ${d.first ?? d.at}, last ${d.at}) and its work is still not done. It does not start it again: look at the failed runs in GitHub → Actions, fix the cause, then start it by hand:` },
      { text: button },
    ];
  if (d.state === "started" && (d.starts ?? 1) > 1)
    return [{ text: `The run the worker started at ${d.first} did not finish its work within ${OVERDUE_MS / 3600_000} h, so it started "${d.workflow}" on ${d.ref} again at ${d.at} (the last time it will). Check both runs in GitHub → Actions.` }];
  if (d.state === "started")
    return [{ text: `Nothing to press: the worker started "${d.workflow}" on ${d.ref} at ${d.at}. Check the run in GitHub → Actions.` }];
  return [
    { text: (d.starts ? `The run the worker started at ${d.first} did not finish its work within ${OVERDUE_MS / 3600_000} h, and ` : "") +
        `${d.starts ? "the" : "The"} worker could not start "${d.workflow}"${d.starts ? " again" : ""} (${d.error}). It retries every ${RETRY_MS / 3600_000} h; or start it by hand:` },
    { text: button },
  ];
}

/** (T413) Is the token good for this repo? Reads the HYDAT workflow with it — starts nothing. GitHub cannot show a
 *  fine-grained token's write permission without using it: a read that answers 200 proves the token and the repo. */
export async function checkToken(env: DispatchEnv, fetch_: Fetch): Promise<{ ok: boolean; status: number | null; says: string }> {
  if (!env.GITHUB_DISPATCH_TOKEN) return { ok: false, status: null, says: "GITHUB_DISPATCH_TOKEN is not set" };
  const r = await fetch_(`https://api.github.com/repos/${REPO}/actions/workflows/feeds-hydat.yml`, {
    headers: { Authorization: `Bearer ${env.GITHUB_DISPATCH_TOKEN}`, Accept: "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "bc-fishing-feeds-worker" },
  });
  const body = await r.text();
  let says = body.slice(0, 200);
  try { const j = JSON.parse(body); says = r.ok ? `${j.name} (${j.state})` : String(j.message ?? says); } catch { /* the text */ }
  return { ok: r.status === 200, status: r.status, says };
}
