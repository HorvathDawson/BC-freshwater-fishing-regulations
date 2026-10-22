# T57 — Unnamed streams: real-data sweep that their regulations are right (incl. tributary rules)

Status: OPEN. Depends on T53 (unnamed streams open a Regulations tab).

Owner 2026-10-22: "making sure unnamed streams have proper regs etc (including the regs they get from being a trib
of something with rules that apply)".

## Work
- For a sample of unnamed sections in EVERY region (stratified: base only, with a trib rule, near a region/MU line,
  in a lake's catchment), the Regulations page lines == the region base + every override the reach run binds to
  that section. Read both sides from the bundle/store; never from a copy.
- Count unnamed sections an "and tributaries" / "incl. tribs" rule of a named water SHOULD reach but does not (the
  reach trib walk): by rule, by region, with examples. Each miss is either fixed (reach) or explained.
- Check MU-scoped rules on unnamed waters that cross an MU line (straddling: mark, don't cut).

## Acceptance
A needs_bundle test pins: 0 unnamed sections without a page; sampled pages equal base + bound overrides; the trib-rule
miss count is recorded and each remaining miss has a reason in the ticket.
