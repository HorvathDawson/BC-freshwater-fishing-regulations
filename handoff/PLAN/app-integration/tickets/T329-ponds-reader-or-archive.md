# T329 — Ponds (Q154): close the domain, no reader, no export

Status: OPEN — DECIDED (owner 2026-10-30: "The pond marks can be not exported tho it just means lakes? They don't need
to be saved, build will never use").

A pond call only confirms the default (a lake), so nothing reads it. Close the `pond` domain on the closed-domains list
(T325, option a — marks.jsonl untouched), stop exporting `decided/ponds.json` and delete it, close the review tab.
Depends: T325. Size: S.
