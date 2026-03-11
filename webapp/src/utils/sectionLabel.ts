/**
 * Returns the ordinal letter label for a regulation section tab.
 *   index 0 → "A", 1 → "B", 2 → "C", …
 *
 * This is the SINGLE source of label derivation. Import and call it from
 * both InfoPanel tab labels and the map badge symbol layer. Never reimplement
 * the derivation independently — mismatched implementations will produce
 * map badge "B" while the InfoPanel shows tab "A" for the same section.
 */
export function sectionLabel(index: number): string {
    return String.fromCharCode(65 + index); // 65 = 'A'
}
