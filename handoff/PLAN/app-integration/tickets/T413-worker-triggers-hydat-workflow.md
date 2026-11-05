# T413 — The feeds worker starts the HYDAT workflow when it sees a new HYDAT

Status: OPEN (owner 2026-11-05: "if the r2 feed worker detects hydat change could it auto trigger the hydat workflow too?").

The worker already detects a new HYDAT release (the T301 notice). On that notice it calls GitHub's workflow_dispatch for
`feeds-hydat.yml` (staging), with a fine-grained token that can only start workflows in this repo (worker secret, set by
the owner). The workflow already stops if the HYDAT is not newer, so a repeat costs nothing; the worker records the
version it triggered for so it fires once. Same hook for any slow feed (T412) whose source the worker can watch.

Ships with the worker (with an app release, never ahead). Size: S. Owner: create the token.
