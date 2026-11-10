# T593 — Keep DFO's heading bytes verbatim in the scrape

Status: OPEN — ticket only (needs a scrape change; do not scrape for it)

Source: T532 round 7 (the Morice heading), leftover h.

## Today
The stored rows keep the water's name and the tributary qualifier apart, and the review app re-assembles the heading
(`checklist.dfo_heading`). It is right for every row seen, but it is a reconstruction: DFO's own heading bytes
("Morice River (including tributaries)") are not on file.

## Work
In the scrape (T549's lane, not the review app's): store the heading as DFO wrote it beside the parsed fields; the
review app then shows the stored bytes and drops the reconstruction. No live fetch is to be run for this ticket by an
agent; it lands with the next scrape the owner runs.

Depends: T549. Size: S. Owner: runs the scrape.
