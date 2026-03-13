# Stream Scoping & Sub-Segment Clipping

## The Problem

Regulations often scope to **portions** of a stream, not the whole thing:

- *"KOOTENAY RIVER (downstream of Idaho border)"*
- *"BURNT BRIDGE CREEK (upstream of Sitkatapa Creek)"*

The current metadata groups all edges of a stream under one `gnis_id`. This is
fine for **name matching** ("which FWA features are called Kootenay River?") but
not for **regulation application** ("which specific edges does this rule cover?").

MU boundaries help sometimes (a stream crosses MU lines), but not always — Burnt
Bridge Creek is entirely within one MU, so the MU boundary can't serve as the
scoping mechanism.

## Proposed Solution: `splits.pickle` Overlay

A future **regulation-application layer** builds a `splits.pickle` that contains
sub-segments of streams, clipped at scoping boundaries.

### How it works

1. **Identify scoping landmarks** from the parsed regulation text (confluences,
   bridges, highway crossings, borders).
2. **Snap each landmark** to the nearest point on the stream's edge geometry.
3. **Split the stream edges** at those snap points, producing sub-segments.
4. Each sub-segment references its **parent `edge_id`** from the FWA graph so
   it can be joined back to the full network.
5. The regulation is then applied only to sub-segments that satisfy the
   directional qualifier ("upstream of X", "downstream of Y").

### Data shape

```python
# splits.pickle
{
    gnis_id: {
        "parent_edge_ids": [...],       # original edge_ids from metadata
        "sub_segments": [
            {
                "sub_id": "...",
                "parent_edge_id": "...",
                "geometry": LineString,  # clipped geometry
                "from_landmark": "...", # nullable
                "to_landmark": "...",   # nullable
            },
            ...
        ],
    },
}
```

### Where `polygon_filter.py` fits

`polygon_filter.py` (already written, 13 tests passing) implements a two-pass
spatial filter with prep/buffer support. It will be used in this layer to:

- Clip stream sub-segments to WMU polygon boundaries
- Determine which sub-segments fall within an admin polygon (park, WMA, etc.)

### Current status

**Deferred.** The metadata layer only does name matching. This scoping layer is
the next major piece of work after the base entry builder is stable.


info

=== length_m (LENGTH_METRE) stats ===
  Count:  4907441
  Mean:   398.36 m
  Median: 250.21 m
  Std:    499.71 m
  Min:    0.01 m
  Max:    263103.05 m
  25th:   85.49 m
  75th:   553.85 m
  95th:   1253.60 m
  99th:   2073.84 m

=== HISTOGRAM of LENGTH_METRE (meters) ===
         0-      83 m | ██████████████████████████████████████████████████ 1197366
        83-     166 m | ███████████████████████████████ 746396
       166-     249 m | ████████████████████ 502758
       249-     332 m | █████████████████ 410988
       332-     415 m | ██████████████ 351518
       415-     498 m | ████████████ 297844
       498-     581 m | ██████████ 249415
       581-     664 m | ████████ 205779
       664-     747 m | ███████ 168843
       747-     830 m | █████ 138292
       830-     912 m | ████ 111602
       912-     995 m | ███ 91223
       995-    1078 m | ███ 74493
      1078-    1161 m | ██ 60642
      1161-    1244 m | ██ 49899
      1244-    1327 m | █ 40768
      1327-    1410 m | █ 33370
      1410-    1493 m | █ 27491
      1493-    1576 m |  22830
      1576-    1659 m |  19221
      1659-    1742 m |  15829
      1742-    1825 m |  13468
      1825-    1908 m |  11123
      1908-    1991 m |  9368
      1991-    2074 m |  7840
  (>2074 m: 49075 segments clipped from display)
(fish) dawson.horvath@AB-R914HQ44:/mnt/c/Users/DawsonHorvath/Documents/Workspace/BC-freshwater-fishing-regulations$ 