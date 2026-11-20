/**
 * (T739) A PUSH WAKE-UP: our worker said "check now" (a feed changed, or a new release is out). The phone runs the SAME
 * background check its periodic task runs (`runBackgroundCheck`), and before it the update service's own check when
 * the app's runtime has one (the app open or held in the background): that check takes the feeds and, for a newer
 * release, STARTS ITS DOWNLOAD when the reader's mobile-data setting allows it now (T722's system downloader, the app
 * in the background or not). A release whose download started is not also announced; one held (Wi-Fi only, Ask on
 * mobile data, no runtime to download in) is announced by the check's "new data ready" notice — once per release
 * (`toldRelease`), and only with that switch on. Platform-free; the app hands in both halves (`apps/mobile` notify.native.ts).
 */
import { reportFailure } from "@app/core";
import type { UpdateService } from "../update/service";
import { runBackgroundCheck, type BgResult, type CheckDeps } from "./check";

export interface WakeDeps extends CheckDeps {
  /** the app's update service, when this runtime has one (null: a headless start, nothing mounted) */
  updates: Pick<UpdateService, "check" | "snapshot"> | null;
}

export interface WakeResult extends BgResult {
  /** the release whose download this wake started (or found under way / staged), else null */
  readonly downloading: string | null;
}

export async function runWake(d: WakeDeps): Promise<WakeResult> {
  let downloading: string | null = null;
  if (d.updates) {
    try {
      await d.updates.check({ feeds: true, release: true });
      const r = d.updates.snapshot().release;
      if (r && (r.phase === "downloading" || r.phase === "ready") && r.target) downloading = r.target;
    } catch (e) { reportFailure(e); }
  }
  // a release being taken is not "ready to download": the check must not announce it
  if (downloading) {
    const rel = downloading;
    try { d.settings.update((s) => ({ ...s, toldRelease: rel })); } catch (e) { reportFailure(e); }
  }
  const r = await runBackgroundCheck({ ...d, woke: { downloading } });
  return { ...r, downloading };
}
