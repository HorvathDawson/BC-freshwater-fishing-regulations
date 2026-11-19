/**
 * (T390) A NEWER APP, HEADLESS: asks the platform's way to a newer app (`AppUpdatePort`, the app's: `apps/mobile`
 * `appUpdater.native.ts` / `.web.ts`; on Play and the APK page that is OUR version file, tiny) on every opening and every
 * return to the app, never offline, keeps the answer on the device (shown at once on the next opening), and gives the
 * bell's "App update" section its view model (`@app/core` `appUpdateVM`) and its one button.
 *
 * ONE LOAD PATH (T369): the check runs from here only — on opening, on each return to the app (`resume`), when the phone
 * comes back online, and when a new-version notification is tapped (`ask`); the bell, its badge and its label all read
 * this one state. (2026-11-05: a once-a-day cap here is why 0.6.0 did not notice 0.6.1.)
 * FAILURES (T368): a check or a start that fails is said in the section and reported; the environment's (offline, a
 * dropped line) and a phone with no Play service (T595) are said and never reported, and an offline check says nothing at all.
 * (T888) THE POPUP ON OPENING reads the same state (`@app/core` `updatePopupVM`): `held` while the first-run notice or the
 * walkthrough is up (the Shell says), once per version — Not now (`dismiss`) or a started update keeps the version in the
 * same file (`popped`), so it does not come back; the bell keeps the update as before.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { appUpdateVM, isEnvironment, isNoPlayService, NEEDED_KEY, now, readAppUpdateSaved, reportFailure, shouldCheckAppUpdate,
         updatePopupVM, wallIso, type AppUpdateChannel, type AppUpdateCheck, type AppUpdateSaved, type AppUpdateStart,
         type AppUpdateVM, type UpdatePopupVM } from "@app/core";

/** The platform's way to a newer app. */
export interface AppUpdatePort {
  readonly channel: AppUpdateChannel;
  /** Is a newer version there? `version`: what the store says (a semver, a Play version code), or null. */
  check(): Promise<{ available: boolean; version: string | null; notes?: readonly string[] | null }>;
  /**
   * Start it: Play's in-app update (`immediate`: the full-screen flow, for data that needs the app; else the flexible
   * download), the store page, the test-build page, or (Play not offering it yet) the Play listing. "downloading" | "opened" | "listing" | "none" (nothing newer to install).
   */
  start(o: { immediate: boolean }): Promise<"downloading" | "opened" | "listing" | "none">;
}

/** Where the last check is kept (a small file of its own). */
export interface AppUpdateStore { read(): string | null; write(text: string): void }

export interface AppUpdateControl {
  readonly vm: AppUpdateVM | null;
  /** the button */
  update(): void;
  /** (T888) the popup on opening, or null (none due, held, answered) */
  readonly popup: UpdatePopupVM | null;
  /** (T888) the popup's Update: the same start; once it is under way the popup's version is answered */
  popupUpdate(): void;
  /** (T888) Not now (or the popup closed any other way): its version is answered, the bell keeps the update */
  dismiss(): void;
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
                                  /** (T234 v1.1) a new-version notification was tapped: ask now */
                                  ask?: number;
                                  /** (T390) bumped on each return to the app: ask again */
                                  resume?: number;
                                  /** (T888) the popup waits (the first-run notice, the walkthrough, a sheet already up) */
                                  held?: boolean }): AppUpdateControl | null {
  const channel = port?.channel ?? "none";
  const [saved0] = useState(() => readSaved(store));
  const [check, setCheck] = useState<AppUpdateCheck>(() => asCheck(saved0));
  const [start, setStart] = useState<AppUpdateStart>("idle");
  /** (T888) the version whose popup was answered (kept in the file), and the key answered in this run */
  const popped = useRef<string | null>(saved0?.popped ?? null);
  /** what the file holds now (the popup's answer is written into it) */
  const kept = useRef<AppUpdateSaved | null>(saved0);
  const [dismissed, setDismissed] = useState<string | null>(null);
  const asking = useRef(false);
  const today = deviceDay();
  useEffect(() => {
    if (!port || asking.current || !shouldCheckAppUpdate({ channel, offline: o.offline })) return;
    asking.current = true;
    port.check().then((r) => {
      const next: AppUpdateSaved = { day: today, available: r.available, version: r.version, notes: r.notes ?? null,
                                     popped: popped.current };
      kept.current = next;
      try { store?.write(JSON.stringify(next)); } catch (e) { reportFailure(e); }   // asked again next opening
      setCheck(asCheck(next));
    }, (e: unknown) => {
      const noPlay = isNoPlayService(e);
      const offline = isEnvironment(e);
      // shown, and reported unless it is the connection's (asked again on the next return or when back online) or a
      // phone with no Play service (T595: a known state, said as one)
      if (!offline && !noPlay) reportFailure(e);
      setCheck({ state: "failed", offline, ...(noPlay ? { noPlay } : {}) });
    }).finally(() => { asking.current = false; });
  }, [port, store, channel, today, o.offline, o.ask, o.resume]);

  /** (T888) a popup answered: its key for this run, and a version kept in the file (the strong prompt's never is) */
  const answer = useCallback((key: string) => {
    setDismissed(key);
    if (key === NEEDED_KEY || popped.current === key) return;
    popped.current = key;
    // a popup comes only from a check, kept in the file (`kept`), so there is always a file to write it into
    if (!kept.current) return;
    kept.current = { ...kept.current, popped: key };
    try { store?.write(JSON.stringify(kept.current)); }
    catch (e) { reportFailure(e); }   // not kept: it pops up once more on the next opening
  }, [store]);
  const run = useCallback((answered: string | null) => {
    if (!port) return;
    setStart("working");
    port.start({ immediate: o.tooOld }).then((r) => { setStart(r); if (answered !== null) answer(answered); }, (e: unknown) => {
      reportFailure(e);   // the environment's is not sent (failure.ts)
      setStart(isEnvironment(e) ? "offline" : "failed");
    });
  }, [port, o.tooOld, answer]);
  const update = useCallback(() => run(null), [run]);

  if (!port && !o.tooOld) return null;
  // offline says nothing (owner): only the strong prompt stays, since the data it holds back is still held back
  const vm = o.offline && !o.tooOld ? null : appUpdateVM({ channel, check, tooOld: o.tooOld, start });
  const popup = updatePopupVM({ vm, channel, check, popped: popped.current, dismissed, held: !!o.held });
  return { vm, update, popup, popupUpdate: () => run(popup?.key ?? null), dismiss: () => { if (popup) answer(popup.key); } };
}
