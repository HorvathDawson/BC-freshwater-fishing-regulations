// (T255) THE ACCOUNT ADAPTER — for T223. Starts the fake account server (fake-server.mjs) on the host, which the
// emulator reaches at 10.0.2.2:<port>, and answers the seam's questions from the server's state. Until the app has
// accounts (a sign-in and a sync client pointed at `EXPO_PUBLIC_ACCOUNT_URL`), each question SKIPs with that reason.
// T223's lane: build the app's client against fake-server.mjs's API (or change both), add a `sign-in` flow, then fill
// `setup` (sign in through the UI) and drop the `notYet` guard below.
import { createFakeServer } from './fake-server.mjs';

export const ACCOUNT_PORT = Number(process.env.TESTBED_ACCOUNT_PORT ?? 39255);

export function accountAdapter(ctx) {
  let server = null;
  const state = async () => (await fetch(`http://localhost:${ACCOUNT_PORT}/__testbed/state`)).json();
  const notYet = () => ctx.skip('accounts are not in the app yet (T223): the account adapter has nothing to check');
  const find = async (spot) => (await state()).spots.find((s) => s.title === spot.title || s.name === spot.title);
  return {
    name: 'account',
    async setup() {
      server = createFakeServer();
      await new Promise((ok) => server.listen(ACCOUNT_PORT, ok));
      ctx.device.adb(['reverse', `tcp:${ACCOUNT_PORT}`, `tcp:${ACCOUNT_PORT}`]);   // localhost on the device, too
    },
    async expectStored(spot) { notYet(); if (!(await find(spot))) throw new Error(`${spot.title} is not on the account server`); },
    async expectPending(spot) { notYet(); if (await find(spot)) throw new Error(`${spot.title} reached the server while offline`); },
    async expectSynced(spot) {
      notYet();
      const s = await find(spot);
      if (!s) throw new Error(`${spot.title} is not on the account server after reconnect`);
      if (spot.photo && !(await state()).photos.some((p) => p.k.startsWith(`${s.id}/`))) throw new Error('its photo did not upload');
    },
    async teardown() {
      try { ctx.device.adb(['reverse', '--remove', `tcp:${ACCOUNT_PORT}`]); } catch { /* gone */ }
      await new Promise((ok) => (server ? server.close(ok) : ok()));
    },
  };
}
