/** (T282) A phone's bundle is one file: every part is asked for at once, in by the first render (the web twin waits). */
export const ASK_AT_ONCE = true;
/** the module's own `import()`, plainly (the web twin steps round Metro's `ErrorUtils`, T391) */
export function askPart<T>(load: () => Promise<T>): Promise<T> { return load(); }
