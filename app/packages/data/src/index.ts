/**
 * @app/data — ONE interface, and (later) one implementation per platform.
 *
 * The interface is the contract the conformance suite tests. Both implementations
 * must answer identically; that is what stops the two apps drifting apart.
 *
 * ⚠️ THE QUERY SHAPES BELOW ARE PLACEHOLDERS. The real ones come from the content
 * store (pipeline/docs/13-build-plan.md steps 6-7) and the packaging spike (step 6).
 * Do not fill them in by guessing — the point of this file today is that the SHAPE
 * of the contract exists and is enforced, not its content.
 */

/** Opaque until the schema lands. Deliberately not modelled here. */
export type ItemId = string & { readonly __brand: "ItemId" };

export interface BundleInfo {
  /** Content-addressed bundle version. Tiles and data are pinned together. */
  version: string;
  /** Synopsis edition expiry. Past this the client must degrade loudly. */
  validUntil: string | null;
}

export interface RegsSource {
  /** Which bundle is this source serving? */
  info(): Promise<BundleInfo>;

  /** Durable address -> whatever this bundle currently says it means. */
  itemExists(id: ItemId): Promise<boolean>;

  // TODO(step 7): regsForItem, regsForPoint, search, gauges, stocking.
  // Each one added here must be added to the conformance suite in the same commit.
}
