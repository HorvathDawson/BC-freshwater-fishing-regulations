# T496 — Cleanup sweep before launch

Status: OPEN — owner 2026-11-07 (part of T495).

Code, config and docs that the launch should not carry. Owner rule: delete freely, git is the safety net; never drop or
rename DATA content without asking.

## Do
- v1 leftovers: old production website config/scripts/docs and references to the v1 site that the T404 switch
  replaces (keep the T198 crosswalk/301s — they ARE the v1 transition).
- Dead pipeline code (no knip there): modules, CLIs and flags nothing calls; stale `smoke_pending` entries.
- Stale docs: MAP.md files vs the code, runbooks naming removed scripts, handoff pages no longer cited.
- Release folders / generated outputs past live + previous; scratch tools in app/tools that were one-offs.
- Each removal: one line in the commit saying why it is dead (grep / graphify evidence). Full suite once at the end.
