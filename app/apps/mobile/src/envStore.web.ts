/** (T232) The development environment switch on the web: one browser-storage key; a reload applies it. */
const KEY = "canifishthis.appEnv.v1";

export const envStore = {
  read: (): string | null => { try { return globalThis.localStorage?.getItem(KEY) ?? null; } catch { return null; } },
  write: (env: string): void => { try { globalThis.localStorage?.setItem(KEY, env); } catch { /* a preference */ } },
};

/** (T251) The update channel (live / test), one key of its own; a reload applies it. */
const CHANNEL_KEY = "canifishthis.updateChannel.v1";
export const channelStore = {
  read: (): string | null => { try { return globalThis.localStorage?.getItem(CHANNEL_KEY) ?? null; } catch { return null; } },
  write: (c: string): void => { try { globalThis.localStorage?.setItem(CHANNEL_KEY, c); } catch { /* a preference */ } },
};

export function restart(): boolean {
  if (typeof window === "undefined") return false;
  window.location.reload();
  return true;
}
