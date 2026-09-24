/**
 * Whatever the caller uses to name a section. `core` never looks inside it — it compares
 * nothing, parses nothing, and only carries it through — so it must not have an opinion:
 * the bundle names sections by an integer handle, and a test or a fixture may name them
 * with a string. Pinning this to `string` made the id's SHAPE a core concern, which it is
 * not, and would have forced every caller to launder it.
 */
export type SectionKey = string | number;
