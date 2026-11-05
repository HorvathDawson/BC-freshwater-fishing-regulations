/**
 * (T390) A NEWER APP, HEADLESS: asks the platform's store (`AppUpdatePort`, the app's: `apps/mobile`
 * `appUpdater.native.ts` / `.web.ts`) at most once a device day and never offline, keeps the answer on the device, and
 * gives the bell's "App update" section its view model (`@app/core` `appUpdateVM`) and its one button.
 *
 * ONE LOAD PATH (T369): the check runs from here only — on opening, and again when the phone comes back online or the
 * device day turns; the bell, its badge and its label all read this one state.
 * FAILURES (T368): a check or a start that fails is said in the section and reported; the environment's (offline, a
 * dropped line) is said and never reported, and an offline check says nothing at all.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { appUpdateVM, isEnvironment, now, readAppUpdateSaved, reportFailure, shouldCheckAppUpdate, wallIso,
         type AppUpdateChannel, type AppUpdateCheck, type AppUpdateSaved, type AppUpdateStart,
         type AppUpdateVM } from "@app/core";

/** The platform's way to a newer app. */
export interface AppUpdatePort {
  readonly channel: AppUpdateChannel;
  /** Is a newer version there? `version`: what the store says (a semver, a Play version code), or null. */
  check(): Promise<{ available: boolean; version: string | null; notes?: readonly string[] | null }>;
  /**
   * Start it: Play's in-app update (`immediate`: the full-screen flow, for data that needs the app; else the flexible
   * download), the store page, or the test-build page. "downloading" | "opened" | "none" (nothing newer to install).
   */
  start(o: { immediate: boolean }): Promise<"downloading" | "opened" | "none">;
}

/** Where the last check is kept (a small file of its own). */
export interface AppUpdateStore { read(): string | null; write(text: string): void }

export interface AppUpdateControl {
  readonly vm: AppUpdateVM | null;
  /** the button */
  update(): void;
}

/** The device's own day ("today" is the device's, owner 2026-11-03). */
const deviceDay = () => wallIso(new Date(now()));

function readSaved(store: AppUpdateStore | null): AppUpdateSaved | null {
  try { return readAppUpdateSaved(store?.read() ?? null); }
  catch (e) { reportFailure(e); return null; }   // an unreadable file: asked again, and reported
}

function asCheck(s: AppUpdateSaved | null): AppUpdateCheck {
  return !s ? { state: "unknown" } : s.available ? { state: "available", version: s.version, notes: s.notes ?? null }
    : { state: "current" };
}

export function useAppUpdate(port: AppUpdatePort | null, store: AppUpdateStore | null,
                             o: { offline: boolean; tooOld: boolean;
                                  /** (T234 v1.1) a new-version notification was tapped: ask now, whatever the day */
                                  ask?: number }): AppUpdateControl | null {
  const channel = port?.channel ?? "none";
  const [check, setCheck] = useState<AppUpdateCheck>(() => asCheck(readSaved(store)));
  const [start, setStart] = useState<AppUpdateStart>("idle");
  const asking = useRef(false);
  const today = deviceDay();
  const asked = useRef(0);
  useEffect(() => {
    if (!port || asking.current) return;
    const saved = readSaved(store);
    const tapped = (o.ask ?? 0) > asked.current && !o.offline && channel !== "none";
    if (!tapped && !shouldCheckAppUpdate({ channel, saved, today, offline: o.offline })) return;
    asked.current = o.ask ?? 0;
    asking.current = true;
    port.check().then((r) => {
      const next: AppUpdateSaved = { day: today, available: r.available, version: r.version, notes: r.notes ?? null };
      try { store?.write(JSON.stringify(next)); } catch (e) { reportFailure(e); }   // asked again next opening
      setCheck(asCheck(next));
    }, (e: unknown) => {
      const offline = isEnvironment(e);
      // a failed check is still the day's ask (a store that refuses would otherwise be asked on every opening);
      // the connection's failure is not: asked again when the phone is back online
      if (!offline) {
        reportFailure(e);
        try { store?.write(JSON.stringify({ day: today, available: false, version: null })); } catch (w) { reportFailure(w); }
      }
      setCheck({ state: "failed", offline });
    }).finally(() => { asking.current = false; });
  }, [port, store, channel, today, o.offline, o.ask]);

  const update = useCallback(() => {
    if (!port) return;
    setStart("working");
    port.start({ immediate: o.tooOld }).then((r) => setStart(r), (e: unknown) => {
      reportFailure(e);   // the environment's is not sent (failure.ts)
      setStart(isEnvironment(e) ? "offline" : "failed");
    });
  }, [port, o.tooOld]);

  if (!port && !o.tooOld) return null;
  // offline says nothing (owner): only the strong prompt stays, since the data it holds back is still held back
  if (o.offline && !o.tooOld) return { vm: null, update };
  return { vm: appUpdateVM({ channel, check, tooOld: o.tooOld, start }), update };
}
