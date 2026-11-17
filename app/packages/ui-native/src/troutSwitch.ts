/**
 * (T810 O) THE TROUT TABLE'S DEV SWITCH, read in one place: on only in a build made with `EXPO_PUBLIC_TROUT_TABLE=1`
 * (apps/mobile index.js sets `globalThis.__troutTable`), never a store build. On: the page is built with
 * `RegsPageOpts.troutTable` (N's table, O's drawing) and the salmon and trout folds end in the arrow. Off (the default):
 * the app is today's. It exists only between tickets O and S; S removes it with the old trout path.
 */
export const troutTableOn = (): boolean => (globalThis as { __troutTable?: boolean }).__troutTable === true;
