# T67 — Braid splitting as a build step

Status: DONE — owner accepted every cut on round 3 and it was promoted 2026-10-24 (decisions.md "braids accepted"); 2026-10-28 5a6e7911 made the braid cuts their own named atlas phase `cut_braids` (after `cut_curated`, before `cut_areas`; the sweep is still judged on the uncut graph in `cut_curated`; the length cap keeps its own sweep gate). Proof: full side atlas parity IDENTICAL, braid_sweep.json + braid_sweep_length.json byte-identical to live (56 braid cuts, 31 edges side-consistent, 2 residual straddles: Skeena/Kitsumkalum, Squamish/Ashlu), reach on full_next: binding_diff reaches/full → full_next = no binding changed. Not promoted (nothing to promote: identical).
Assessment: scratchpad sloughs/braids-assessment.md (lane wf_937ae915-731) — the problem today with examples, where
splitting happens, whether the build step is the right place, cost (atlas/reach/tiles), risks (split fix changes the
trib walk → binding diff; end cuts halve a water; atlas build non-deterministic → subtract an old-vs-old diff), plan.
