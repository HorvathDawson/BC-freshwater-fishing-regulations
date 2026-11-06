/**
 * (T391) Metro's `import()` first asks for the module at once and fetches the chunk only when that throws. With
 * `global.ErrorUtils` installed (the web bundle has it), the miss is not thrown: Metro reports it as a FATAL "Requiring
 * unknown module" and hands back nothing — every web visit filed a fatal crash and the part needed a second try. So the
 * asking moment runs without `ErrorUtils`: the miss throws, the chunk is fetched, and only a real load failure is reported
 * (by `part`'s `preload`, part.tsx). Only the synchronous call: the module's own code runs later, guarded as before.
 */
export function unguarded<T>(load: () => Promise<T>, g: Record<string, unknown> = globalThis as never): Promise<T> {
  if (!("ErrorUtils" in g)) return load();
  const eu = g.ErrorUtils;
  g.ErrorUtils = undefined;
  try { return load(); } finally { g.ErrorUtils = eu; }
}
