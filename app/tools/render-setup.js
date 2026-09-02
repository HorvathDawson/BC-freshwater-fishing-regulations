globalThis.reduceMotion = false;
const listeners = new Set();
globalThis.matchMedia = ((query) => ({
    get matches() {
        return query.includes("prefers-reduced-motion") ? globalThis.reduceMotion : false;
    },
    media: query,
    onchange: null,
    addEventListener: (_, fn) => { listeners.add(fn); },
    removeEventListener: (_, fn) => { listeners.delete(fn); },
    addListener: (fn) => { listeners.add(fn); },
    removeListener: (fn) => { listeners.delete(fn); },
    dispatchEvent: () => true,
}));
/** Flip the preference and tell everyone listening, the way a real browser would. */
export function setReduceMotion(on) {
    globalThis.reduceMotion = on;
    for (const fn of listeners)
        fn({ matches: on });
}
