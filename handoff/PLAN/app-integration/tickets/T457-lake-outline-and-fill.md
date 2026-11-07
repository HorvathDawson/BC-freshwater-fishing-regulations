# T457 — Lakes: an opaque outline sized by the lake, over a slightly transparent fill

Status: OPEN (owner 2026-11-06, chat 5: an idea to try).

Owner: "lakes in the map could look better if they have an outline that is opaque then a fill that is slightly
transparent? with the outline sized by the size of the lake so it is proportional?"

## Work
1. Today: the `lake` / `lake_flowing` polygon layers (`app/packages/map/style/layers.source.json`) fill with the colour
   mode's colour. `under_lake`'s note says "the lake's own outline already carries the size": check what outline is
   drawn now and at which width.
2. Proposal: the colour mode's colour at full opacity as the OUTLINE, and the same colour at reduced opacity (start
   ~0.55, tune by eye on light and dark basemaps and satellite) as the FILL. The outline width is set by the lake's
   size, so a big lake reads like a big river and a pond like a creek. Use the lake's area (or perimeter) through
   breaks in a zoom×size `interpolate`, the same idea as stream width by Strahler. If the tiles carry no size property,
   add one at the tile build (`area_ha` on the lake features, rounded) and record it in the tile schema. The style
   uses tokens only (rule 28: edit layers.source.json, then `pnpm style:build`).
3. Every colour mode still colours every member (rule 29), and the "missing"/ungauged colours stay readable at the
   lower fill opacity. Highlight/selection must still stand out against the new outline.
4. Look: before/after shots at z8, z11, z14 (big lake: Harrison or Chilliwack Lake; small lakes near Hope; a lake cut
   into parts like Kootenay), light and dark, plain and "Flow for the date" modes. Send them to the owner. The look is
   the owner's call (a proposal until accepted). Check the zoomed-out frame cost (T420: an extra line pass over
   detailed polygons cost 14–15 % before): measure, and if it costs, only draw the outline from a zoom where it pays.
Related: T420 (basemap fill outline removed for speed), rule 28/29. Size: S–M. Owner: approve the look.
