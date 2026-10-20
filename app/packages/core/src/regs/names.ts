/**
 * Species names — replaces page_v36's globals FISH, GROUPS, PROT, SALM, GAME (static `export.species`).
 *
 * T00 declares the book's type only (Statics.exportSpecies needs it); T06 adds the functions that read it.
 */
export interface SpeciesBook {
  readonly fish: Readonly<Record<string, { readonly name: string }>>;
  readonly groups: Readonly<Record<string, { readonly name: string; readonly members: readonly string[]; readonly open?: boolean }>>;
  readonly protected?: Readonly<Record<string, { readonly name: string }>>;
  readonly salmon?: Readonly<Record<string, { readonly name: string }>>;
}
