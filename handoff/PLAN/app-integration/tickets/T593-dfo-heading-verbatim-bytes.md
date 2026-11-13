# T593 — Keep DFO's heading bytes verbatim in the scrape

Status: OPEN — ticket only; waits on the owner's next scrape (human run)

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

## 2026-11-12 (pipeline lane): not built now, why
The parsed rows already hold DFO's heading (`RegRow.waters`, `untangle.split_name` cuts it into name + qualifier), so the
change is small in code: carry the raw cell as `source_text.heading` in `entries.py`. Not done here because writing a new
field into the stored DFO entries re-keys them: the reach key carries `dfo_entries_digest`, and the review marks'
changed-extent guard reads those rows (memory: a re-emitted region staled 6 owner marks, T361). It goes with the owner's
next scrape, with a check that no mark's fingerprint moves.
