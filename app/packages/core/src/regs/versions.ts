/**
 * What the regulations half of a release must be for this app to read it (T03). A release that says anything else is
 * refused — its frames may not mean what the presenters read (a rows-29 presenter reading rows-30 frames draws plausible
 * wrong numbers). Bump together with a port: `conformance/src/regs/store.release.test.ts` holds these to the golden's own
 * store (taps.json `meta.store.layout`), so a re-sync that moves a version fails until this file moves with it; and on
 * the writer's side `pipeline/tests/test_answers_versions.py` (T810 I0) holds every section the pipeline writes, required
 * and optional, to `KNOWN_VERSIONS` and `OPTIONAL_VERSIONS` here, so neither side bumps alone. It parses the two lines
 * below: keep each an `Object.freeze({ name: n, … } as const)` on one line.
 *
 * The three files are the release's (README §0, D22): `water` (the store's common tables: keys, segments, parts, rules,
 * statics, the dictionaries), `regs-<region>` (cells + the frames they reach), `regs-sid` (section → key / place).
 */
export const REGS_FORMATS = Object.freeze({ water: "water/3", region: "regs-region/1", sid: "regs-sid/1" } as const);
export type SectionName = "ladder" | "rows" | "gear" | "licence" | "display";
export const SECTION_NAMES: readonly SectionName[] = Object.freeze(["ladder", "rows", "gear", "licence", "display"] as const);
/** The answers section versions (water `meta.layout.versions`) and the glossary's (`static answers.glossary`). */
export const KNOWN_VERSIONS = Object.freeze({ ladder: 1, rows: 33, gear: 12, licence: 7, display: 20, glossary: 5 } as const);
/**
 * (T810 M0) THE OPTIONAL ANSWERS SECTIONS this reader was written for, at the version it reads. A release may leave one
 * out (absent: the page reads as without it); one present at another version is refused, at install (`readsRelease`)
 * and by the store (`openRegsStore`). The section's column and table names come generated (`PACK_OPTIONAL_SECTION_TABLES`
 * in `@app/data`'s pack.generated.ts); the version stays typed here on purpose: a reader says what it was WRITTEN for.
 * RULE: a change of the `trout` shape bumps `Section("trout", n)` (pipeline `answers.SECTIONS`) and `trout` here in the
 * same change, or the I0 pin fails. At the cutover (S) `trout` moves into `KNOWN_VERSIONS` and this becomes `{}`.
 */
export const OPTIONAL_VERSIONS = Object.freeze({ trout: 1 } as const);
export type OptionalSectionName = keyof typeof OPTIONAL_VERSIONS;
/** The blob codecs the data layer can read (`meta.blob_codec`). */
export const BLOB_CODECS: readonly string[] = Object.freeze(["json", "deflate", "zdict"]);
