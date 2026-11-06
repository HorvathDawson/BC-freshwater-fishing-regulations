// T412: RUN TIMING IS A SLOW FEED. The worker watches its DFO source (NuSEDS: the open.canada.ca record names its
// "All Areas NuSEDS_<YYYYMMDD>.zip") once a week against the date the live feed was built from (`runtiming/index.json`
// sources.dfo.updated). A newer one starts feeds-runtiming.yml (src/dispatch.ts, the owner's token) and tells the owner;
// without the token the email names the button. The PSF curves change about yearly with no machine-readable date: their
// refresh is the yearly run in handoff/PLAN/runbooks (the same workflow, by hand).
import { dispatchOnce, dispatchSteps, type Dispatch, type DispatchEnv } from "./dispatch";
import type { FeedEvent } from "./events";
import type { Store } from "./io";

export const NUSEDS_API = "https://open.canada.ca/data/api/action/package_show?id=c48669a3-045b-400d-b730-48aafe8c5ee6";
export const BUTTON = 'GitHub → Actions → "feeds — run timing (PSF + DFO) → R2 runtiming/", with upload: true.';

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
  const dispatch: Dispatch = await dispatchOnce(env, fetch_, dispatchStore, "feeds-runtiming.yml", latest, { upload: "true" }, now);
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
