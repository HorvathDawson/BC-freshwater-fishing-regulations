/**
 * What the regulations half of a release must be for this app to read it (T03). A release that says anything else is
 * refused — its frames may not mean what the presenters read (a rows-29 presenter reading rows-30 frames draws plausible
 * wrong numbers). Bump together with a port: `conformance/src/regs/store.release.test.ts` holds these to the golden's own
 * store (taps.json `meta.store.layout`), so a re-sync that moves a version fails until this file moves with it.
 *
 * The three files are the release's (README §0, D22): `water` (the store's common tables: keys, segments, parts, rules,
 * statics, the dictionaries), `regs-<region>` (cells + the frames they reach), `regs-sid` (section → key / place).
 */
export const REGS_FORMATS = Object.freeze({ water: "water/1", region: "regs-region/1", sid: "regs-sid/1" } as const);
export type SectionName = "ladder" | "rows" | "gear" | "licence" | "display";
export const SECTION_NAMES: readonly SectionName[] = Object.freeze(["ladder", "rows", "gear", "licence", "display"] as const);
/** The answers section versions (water `meta.layout.versions`) and the glossary's (`static answers.glossary`). */
export const KNOWN_VERSIONS = Object.freeze({ ladder: 1, rows: 33, gear: 12, licence: 7, display: 18, glossary: 5 } as const);
/** The blob codecs the data layer can read (`meta.blob_codec`). */
export const BLOB_CODECS: readonly string[] = Object.freeze(["json", "deflate", "zdict"]);
