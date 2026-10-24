"""Audits and one-off reports: each reads a build, the corpus or the curated tree and PRINTS (or writes a report under
data/generated/); none is a pipeline stage and none writes curated data. `check_curated` is the one a build runs
(preflight, CI); `build_parity` compares two atlases."""
