// (T255) THE SYNC-ADAPTER SEAM — where a scenario asks "did what the reader made on the device land where it should?"
// without knowing where that is. Today spots are DEVICE-LOCAL (the `local` adapter: "synced" = persisted on the
// device, kept across a cold start, and its weather/gauge context filled in after the network returns). When accounts
// land (T223) the `account` adapter answers the same questions against the account server — for the test bed, the fake
// one in fake-server.mjs (`--sync account`) — and every spot scenario runs unchanged against it.
//
// THE CONTRACT (every method may use ctx.flow / ctx.device / ctx.waitText; each throws with the reader's words when the
// answer is no; `ctx.skip(msg)` when this adapter cannot answer yet):
//   name                          'local' | 'account'
//   setup()                       before the scenario (start a server, sign in…)
//   expectStored(spot)            the spot (by title) is held where this adapter keeps spots
//   expectPending(spot)           the spot is made but not yet complete/synced (offline): what the app says about it
//   expectSynced(spot)            after reconnect: complete (context filled) and stored (account: on the server)
//   teardown()                    after the scenario
import { localAdapter } from './local.mjs';
import { accountAdapter } from './account.mjs';

const ADAPTERS = { local: localAdapter, account: accountAdapter };

export function adapterFor(name, ctx) {
  const make = ADAPTERS[name];
  if (!make) throw new Error(`--sync ${name}: one of ${Object.keys(ADAPTERS).join(', ')}`);
  return make(ctx);
}
