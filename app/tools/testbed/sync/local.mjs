// (T255) THE LOCAL ADAPTER — spots are device-local today: "stored" is the Spots list (the spots file), "pending" is
// what the spot itself says it is missing (`refreshReason`: no weather, the week before or the day's flow not read),
// "synced" is that line gone after the network returned (the reconnect refresh, T255) and the spot still listed.

/** What a spot says while something it records is still to be read (`@app/data/spots` refreshReason words). */
export const MISSING = /No weather was recorded|it can be filled in|not recorded|still to read|days? (are )?pending|Refresh this spot/i;

export function localAdapter(ctx) {
  const open = async (spot) => {
    ctx.flow('tab', { TAB: 'Spots' });
    await ctx.waitText(new RegExp(spot.title), 15_000);
    ctx.flow('tap-text', { TEXT: `.*${spot.title}.*` });
    await ctx.device.sleep(2000);
  };
  const back = () => ctx.device.sh('input keyevent 4');
  return {
    name: 'local',
    async setup() {},
    async expectStored(spot) {
      ctx.flow('tab', { TAB: 'Spots' });
      return ctx.waitText(new RegExp(spot.title), 15_000);
    },
    async expectPending(spot) {
      await open(spot);
      const said = await ctx.waitText(MISSING, 10_000);
      back();
      return said;
    },
    async expectSynced(spot, ms = 120_000) {
      const end = Date.now() + ms;
      let left = null;
      while (Date.now() < end) {
        await open(spot);
        left = ctx.device.screenText().find((w) => MISSING.test(w)) ?? null;
        back();
        if (!left) return 'filled in: nothing missing';
        await ctx.device.sleep(10_000);
      }
      throw new Error(`still missing after reconnect: "${left}"`);
    },
    async teardown() {},
  };
}
