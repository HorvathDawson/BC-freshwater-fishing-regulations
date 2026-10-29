# T287 — No freeze when a download finishes: updates apply in the background

Status: OPEN (owner 2026-10-29: "after a download finishes the app freezes for a tiny bit. this feels really bad. it should
not freeze. it should update in background cleanly").

## Work
1. Reproduce on the testbed emulator: an update (and a region/offline download) finishing while the user pans/taps; record
   JS-thread long tasks and dropped frames around the finish (T273 pan probe).
2. Find what blocks (likely: hashing/verify or gunzip on the JS thread, opening/attaching the new SQLite files, swapping the
   store and re-rendering every consumer at once, map source reload, big JSON parse).
3. Move the work off the JS thread (native hash/unzip, chunked/yielding work, worker thread), prepare the new release fully in
   the background, then swap atomically at an idle moment; only the components whose data changed re-render; the map keeps
   its camera and doesn't flash.

## Acceptance
No JS long task > 50 ms and no dropped-frame burst at download finish on the emulator (before/after numbers); same for the
first-install finish (T272 path).

Depends: T272, T273. Size: M. Owner: no.
