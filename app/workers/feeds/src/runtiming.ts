// T412: RUN TIMING IS A SLOW FEED. The worker watches its DFO source (NuSEDS: the open.canada.ca record names its
// "All Areas NuSEDS_<YYYYMMDD>.zip") once a week against the date the live feed was built from (`runtiming/index.json`
// sources.dfo.updated). A newer one starts feeds-runtiming.yml (src/dispatch.ts, the owner's token) and tells the owner;
// without the token the email names the button. The PSF curves change about yearly with no machine-readable date: their
// refresh is the yearly run in handoff/PLAN/runbooks (the same workflow, by hand).
import { dispatchOnce, dispatchSteps, OVERDUE_MS, unfinished, type Dispatch, type DispatchEnv } from "./dispatch";
import type { FeedEvent } from "./events";
import type { Store } from "./io";

export const NUSEDS_API = "https://open.canada.ca/data/api/action/package_show?id=c48669a3-045b-400d-b730-48aafe8c5ee6";
// T552 item 2: since T485 the workflow builds from its KEPT copy of the sources unless `refresh` is set, so a run started
// for a new NuSEDS must refresh, or it rebuilds the old one and publishes nothing; `expect_dfo` makes the run fail when
// the fresh fetch still is not that version.
export const BUTTON = 'GitHub → Actions → "feeds — run timing (PSF + DFO) → R2 runtiming/", with upload: true and refresh: true.';

/** The workflow inputs for a run started because NuSEDS `latest` is out (T552 item 2). */
export const runTimingInputs = (latest: string): Record<string, string> =>
  ({ upload: "true", refresh: "true", expect_dfo: latest });

type Fetch = (url: string, init?: RequestInit) => Promise<Response>;

/** The newest "All Areas NuSEDS_<date>.zip" the record lists, or throws (a changed record is a failure to see). */
export async function nusedsLatest(fetch_: Fetch): Promise<string> {
  const r = await fetch_(NUSEDS_API, { headers: { "User-Agent": "bc-fishing-feeds-worker" } });
  if (!r.ok) throw new Error(`NuSEDS record: HTTP ${r.status}`);
  const names: string[] = ((await r.json()) as any)?.result?.resources?.map((x: any) => String(x?.name ?? "")) ?? [];
  const dates = names.map((n) => /^All Areas NuSEDS_(\d{8})\.zip$/.exec(n)?.[1]).filter((d): d is string => !!d).sort();
  if (!dates.length) throw new Error("NuSEDS record lists no 'All Areas NuSEDS_<date>.zip'");
  return dates[dates.length - 1]!;
}

/** One weekly check: an event when NuSEDS is newer than the live feed's (null when it is not). */
export async function checkRunTiming(env: DispatchEnv, fetch_: Fetch, bucket: { get(k: string): Promise<{ text(): Promise<string> } | null> },
                                     dispatchStore: Store, now: Date): Promise<FeedEvent | null> {
  const latest = await nusedsLatest(fetch_);
  const o = await bucket.get("runtiming/index.json");
  const current: string | null = o ? (JSON.parse(await o.text())?.sources?.dfo?.updated ?? null) : null;
  if (current && latest <= current) return null;
  const dispatch: Dispatch = await dispatchOnce(env, fetch_, dispatchStore, "feeds-runtiming.yml", latest, runTimingInputs(latest), now);
  const at = now.toISOString();
  return {
    key: `runtiming:${latest}`,
    kind: "runtiming",
    at,
    title: `New NuSEDS ${latest} — refresh run timing`,
    text: `NuSEDS ${latest} is out; run timing was built from ${current ?? "nothing"}.\n\n` +
      dispatchSteps(dispatch, BUTTON).map((s) => s.text).join("\n"),
    data: { latest, current, dispatch },
  };
}

// T485 step 3: THE SOURCES' REFRESH IS ITS OWN JOB, off the release path. A release-triggered run builds from the kept
// copy (private/runtiming/src/sources.tar.gz) and makes no outside request; this check — inside the weekly run above,
// worker-started, never a GitHub schedule — starts feeds-runtiming.yml with `refresh` once the kept copy's fetch date
// (private/runtiming/src/FETCHED, written by the workflow beside the copy) is REFRESH_DAYS old. At most one start per
// month (version `refresh:<YYYY-MM>`); a start that did not bring the copy up to date is redone once, then the owner is
// told every week it is still due. A failed or overdue refresh only emails: no release waits on it (the workflow's
// `concurrency` queues a release run behind it, it never fails one), and the copy is replaced only by a run that passed.
export const REFRESH_DAYS = 30;
export const SRC_DATE_KEY = "private/runtiming/src/FETCHED";
export const refreshInputs = (): Record<string, string> => ({ upload: "true", refresh: "true" });

/** The monthly refresh check: an event when the kept sources are due (null when they are fresh enough). A missing date
 *  file is "due" and said so (the copy seeded by hand before this check had none). */
export async function checkSourceRefresh(env: DispatchEnv, fetch_: Fetch, bucket: { get(k: string): Promise<{ text(): Promise<string> } | null> },
                                         dispatchStore: Store, now: Date): Promise<FeedEvent | null> {
  const o = await bucket.get(SRC_DATE_KEY);
  const fetched: string | null = o ? (await o.text()).trim() : null;
  if (fetched !== null && !/^\d{4}-\d{2}-\d{2}$/.test(fetched)) throw new Error(`${SRC_DATE_KEY} reads "${fetched.slice(0, 40)}", not a date`);
  const age = fetched === null ? null : Math.floor((now.getTime() - Date.parse(fetched + "T00:00:00Z")) / 86_400_000);
  if (age !== null && age < REFRESH_DAYS) return null;
  const month = now.toISOString().slice(0, 7), day = now.toISOString().slice(0, 10);
  const dispatch: Dispatch = await dispatchOnce(env, fetch_, dispatchStore, "feeds-runtiming.yml", `refresh:${month}`, refreshInputs(), now,
                                                { redoAfterMs: OVERDUE_MS });
  const bad = unfinished(dispatch);
  const why = age === null ? "no fetch date is kept with them" : `they were fetched ${fetched} (${age} days ago)`;
  return {
    key: bad ? `runtiming-refresh:${month}:unfinished:${day}` : `runtiming-refresh:${month}`,
    kind: "runtiming",
    at: now.toISOString(),
    title: bad ? `Run timing sources: the ${month} refresh did not finish` : `Refreshing the run timing sources (${month})`,
    text: `The kept run-timing sources (PSF + NuSEDS) are due for their monthly refresh: ${why}. Releases are not waiting on it.\n\n` +
      dispatchSteps(dispatch, BUTTON).map((s) => s.text).join("\n"),
    data: { fetched, age, dispatch },
  };
}
