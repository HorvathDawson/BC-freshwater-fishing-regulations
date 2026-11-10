# T600 — One writer for curated data

Status: OPEN

Source: T599 findings step 2 (`handoff/PLAN/app-integration/T599-findings.md` §4, §5); owner 2026-11-10: "the
recommendation looks good and is cleaner than what we have now"; "keeping curated data more strictly managed".

## Work
1. One module (`curated_write.py`): validate against the file's schema, back up, write atomically, append a write log,
   stamp provenance (who / when / source) on the record.
2. Move every tool that writes a curated path onto it (the findings count about ten; confirm the list first). The
   review app's write-through belongs to chat 10's paths: list those call sites and hand them over, do not edit them.
3. A test that fails when any module writes under the curated tree without the writer; no pipeline build step may
   write curated data at all (T547 adds the read-only-tree test).

## Acceptance
- One atomic-write copy left; every curated change has a log line and a backup; the grep test is in.

Depends: T547. Size: M. Owner: no.
