/**
 * Opening a bundle, whichever platform you are on.
 *
 * The split is on the RELATIVE import below, not on this file's name, and that is
 * load-bearing: a package `exports` map naming `open.ts` exactly defeats platform
 * resolution — Metro hands every target the same file and the `.web` variant is never
 * reached. It showed up as the web app loading expo-sqlite's OPFS worker and hanging on a
 * spinner with no error. A relative specifier gets the platform treatment; an exports
 * target does not.
 */
export { loadBundle, setSqlWasmUrl, type LoadedBundle } from "./engine";
