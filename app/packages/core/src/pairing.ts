/**
 * THE PAIRING WORDS (P11): what the map says when the data and the tiles are not a PROVEN pair. Colour only on a proven
 * pair: a mixed pair is refused and said (the "out of step" banner); an UNPROVEN pair — tiles with no version stamped,
 * or none known — colours nothing either, and this line says why, so an uncoloured map never reads as a slow feed.
 */

/** Said where the mixed-pair banner is drawn, when no tile version is known. A name it does not have is left out. */
export function unprovenLine(data: string | null): string {
  return "Status colours are off: these map tiles carry no version, so they cannot be proven to match the data" +
    `${data ? ` (${data})` : ""}.`;
}
