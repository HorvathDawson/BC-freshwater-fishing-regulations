/**
 * (T445) A CALL THAT OUTLIVED ITS VIEW. Every call into the binding (feature-state, a query, a watch) is resolved by the
 * view's tag on the UI thread, later; when that view has gone by then (the map left the screen, or a style load made the
 * atlas source anew) the binding rejects with "`reactTag` 2298 resolved to `view` null which is null or a wrong type"
 * (`ReactTagResolver.kt`). That is not a defect: what it carried was for a view that no longer exists, and the new one
 * is sent everything again (`swapped`, the attach's watch). Two reports from 0.6.1/0.6.2 were this.
 *
 * Only a tag KNOWN to be gone is dropped: one this map once held and no longer does, or any after the map unmounted. A
 * live view's tag that resolves to nothing is still a defect, and reported.
 */

/** The view tag a "resolved to null" rejection names, or null for any other failure. */
export function unresolvedTag(e: unknown): number | null {
  const m = e instanceof Error || (typeof e === "object" && e !== null && "message" in e)
    ? String((e as { message: unknown }).message) : typeof e === "string" ? e : "";
  const r = /`reactTag` (\d+) resolved (?:to `view` \S+ which is null or a wrong type|null)/.exec(m);
  return r ? Number(r[1]) : null;
}

/** The tags of views this map held and let go (the atlas source re-created by a style load), and whether it is gone. */
export class GoneViews {
  private readonly was = new Set<number>();
  private now: number | null = null;
  private unmounted = false;

  /** The atlas source now has `tag` (null: none); the one before it is gone. */
  held(tag: number | null): void {
    if (this.now !== null && this.now !== tag) this.was.add(this.now);
    this.now = tag;
    if (tag !== null) this.was.delete(tag);
  }

  /** The map itself left: every view under it is gone. */
  unmount(): void { this.unmounted = true; }
  mount(): void { this.unmounted = false; }

  /** Is `e` a call to a view known to be gone (dropped), rather than a failure to report? */
  stale(e: unknown): boolean {
    const tag = unresolvedTag(e);
    return tag !== null && (this.unmounted || this.was.has(tag));
  }
}
