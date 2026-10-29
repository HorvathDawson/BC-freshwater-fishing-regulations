/** (T232) The development environment switch on the web: one browser-storage key; a reload applies it. */
const KEY = "canifishthis.appEnv.v1";

export const envStore = {
  read: (): string | null => { try { return globalThis.localStorage?.getItem(KEY) ?? null; } catch { return null; } },
  write: (env: string): void => { try { globalThis.localStorage?.setItem(KEY, env); } catch { /* a preference */ } },
};

export function restart(): boolean {
  if (typeof window === "undefined") return false;
  window.location.reload();
  return true;
}
