/**
 * (T264) ONE NATIVE SQLITE CALL AT A TIME, app-wide. This is what `open.native.ts` runs every expo-sqlite call through.
 *
 * WHY: expo-modules-core's Android `SharedObjectRegistry.toNativeObject` reads its id map WITHOUT the lock, on the IO
 * thread that runs the call. The JS thread meanwhile adds to that map under the lock, once per new statement. Fire a
 * burst of reads at once and they race. A town's list did exactly that: ~140 waters, three statements each. The map
 * resizes as it grows, and an IO-thread lookup made during a resize misses an id that is still live:
 * "prepareAsync has been rejected → Cannot use shared object that was already released". It happened on the first
 * burst after a cold start (the map is small and still growing); once the map had grown, later bursts got through,
 * which is why opening a water's regulations first hid it. Running one call at a time means no new statement is
 * created while another call's arguments are being looked up, so the race cannot happen. SQLite already ran a
 * connection's statements one at a time, so nothing is lost.
 *
 * A failed call rejects its own caller only; the next call still runs.
 */
export function serialQueue(): <T>(run: () => Promise<T>) => Promise<T> {
  let tail: Promise<unknown> = Promise.resolve();
  return <T>(run: () => Promise<T>): Promise<T> => {
    const p = tail.then(run, run);
    tail = p.catch(() => {});
    return p;
  };
}
