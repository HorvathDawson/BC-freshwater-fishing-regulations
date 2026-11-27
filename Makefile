# ONE VERB PER TASK (T1193, owner 2026-11-26: "A Makefile (Recommended)"). Every verb wraps a command the repo already
# has: nothing here is a second copy of a script's work, and nothing moved. `make` (or `make help`) lists them, read
# from the `## ` comments below, so the list cannot drift from the verbs. scripts/README.md says what each one calls;
# pipeline/tests/test_makefile.py checks every verb resolves and is listed.
#
#   make test-py ARGS="pipeline/tests/test_x.py -k foo"     a part, in one process (ARGS: pytest's own arguments)
#   make test-app ARGS="packages/core/src/x.test.ts"         a part of the app suite (paths from app/)
#
# A verb whose prerequisite is missing (no .venv, no node_modules, not macOS for scripts/heavy.sh) stops, naming the
# fix. Production has no verb that runs: `make prod` only prints the owner's command (AGENTS A7).

.DEFAULT_GOAL := help
.PHONY: help setup fetch-data test test-py test-app check test-all build build-first app review release prod

#: the pipeline's Python, named as scripts/test-env.sh names it: $PIPELINE_PY, else the checkout's .venv
PY ?= $(or $(PIPELINE_PY),.venv/bin/python)
#: the machine (scripts/heavy.sh is macOS-only); the app's install (its pnpm workspace)
UNAME ?= $(shell uname -s)
APP_MODULES ?= app/node_modules
HEAVY := bash scripts/heavy.sh
ARGS ?=

need_py = command -v "$(PY)" >/dev/null 2>&1 || { echo "make $@: no Python '$(PY)': run 'make setup' (it makes .venv), or set PIPELINE_PY" >&2; exit 1; }
need_node = test -d "$(APP_MODULES)" || { echo "make $@: no $(APP_MODULES): run 'make setup' (pnpm -C app install --frozen-lockfile)" >&2; exit 1; }
# $(call need_mac,<the same command without heavy.sh>)
need_mac = test "$(UNAME)" = Darwin || { echo "make $@: scripts/heavy.sh is macOS-only (memory_pressure); on $(UNAME) run instead, one heavy job at a time: $(1)" >&2; exit 1; }

help: ## this list: every verb and what it runs (from this file's ## comments)
	@echo "make <verb>   (Makefile; scripts/README.md says what each calls)"
	@awk '/^[a-z][a-z-]*:[^=]*## / {split($$0, t, ":"); printf "  %-12s %s\n", t[1], substr($$0, index($$0, "## ") + 3)}' \
	  $(firstword $(MAKEFILE_LIST))

setup: ## once per clone: git hooks, .venv, the app's pnpm install; fetches nothing (bootstrap.sh --setup-only, ~35 s)
	@command -v "$${PYTHON:-python3.13}" >/dev/null 2>&1 || { echo "make setup: no $${PYTHON:-python3.13}: brew install python@3.13, or PYTHON=/path/to/python3.13 make setup" >&2; exit 1; }
	bash scripts/bootstrap.sh --setup-only

fetch-data: ## the pinned inputs the live release was built from: about 16 GB, resumable (bootstrap.sh)
	@command -v "$${PYTHON:-python3.13}" >/dev/null 2>&1 || test -x .venv/bin/python || { echo "make fetch-data: no Python: run 'make setup' first" >&2; exit 1; }
	bash scripts/bootstrap.sh

test: _no-args test-py test-app ## the whole no-data tier, as CI: the pipeline suite in chunks (~14 min) then the app's vitest (~5 min)

_no-args:
	@test -z "$(strip $(ARGS))" || { echo "make test takes no ARGS: make test-py ARGS=\"...\" or make test-app ARGS=\"...\"" >&2; exit 2; }

test-py: ## the pipeline tests, CI's no-data tier: all in chunks via heavy.sh, or ARGS="<file> -k <expr>"
	@$(need_py)
ifeq ($(strip $(ARGS)),)
	@$(call need_mac,bash scripts/pytest-chunked.sh -- -q)
	PIPELINE_PY="$(PY)" $(HEAVY) tests 30 -- bash scripts/pytest-chunked.sh -- -q
else
	$(PY) -m pytest -q $(ARGS)
endif

test-app: ## the app's vitest suite: all via heavy.sh, or ARGS="<path from app/>"
	@$(need_node)
ifeq ($(strip $(ARGS)),)
	@$(call need_mac,pnpm -C app exec vitest run)
	$(HEAVY) app-suite 30 -- pnpm -C app exec vitest run
else
	pnpm -C app exec vitest run $(ARGS)
endif

check: ## what a PR's required checks ask, for this tree (scripts/before_push.py; ARGS="--changed" or "--no-tier")
	@$(need_py)
	@$(need_node)
ifeq ($(findstring --no-tier,$(ARGS)),)
	@$(call need_mac,$(PY) scripts/before_push.py --no-tier)
endif
	$(PY) scripts/before_push.py $(ARGS)

test-all: ## Tier 2, before a merge: the app gates strict, the pipeline's data tier and -m slow, the card gate (needs a build)
	@$(need_py)
	@$(need_node)
	@$(call need_mac,./scripts/test-full.sh)
	$(HEAVY) test-full 40 -- ./scripts/test-full.sh

build: ## rebuild what changed (python -m pipeline build via heavy.sh; up to ~1h15 and ~24 GB free memory)
	@$(need_py)
	@$(call need_mac,$(PY) -m pipeline build)
	$(HEAVY) build 50 -- $(PY) -m pipeline build

build-first: ## the first build on a fresh clone, after fetch-data: --atlas --promote (~1h15, ~24 GB free memory)
	@$(need_py)
	@$(call need_mac,$(PY) -m pipeline build --atlas --promote --jobs 2)
	$(HEAVY) first-build 50 -- $(PY) -m pipeline build --atlas --promote --jobs 2

# The tile server runs in the background and the web server in front; the trap stops the tile server (its own PID:
# pnpm passes the TERM on to node) whichever way the recipe ends, Ctrl-C included.
app: ## the app in a browser, http://localhost:39218: the tile server and Expo web together; Ctrl-C stops both
	@$(need_node)
	pnpm -C app tiles & tiles=$$!; trap 'kill $$tiles 2>/dev/null' EXIT INT TERM; pnpm -C app dev:web

review: ## the owner's review tool: backend :8787 + Vite :5173 (tools/curation-review/run.sh; Ctrl-C stops both)
	@test -x .venv/bin/uvicorn || { echo "make review: no .venv/bin/uvicorn: run 'make setup'" >&2; exit 1; }
	bash tools/curation-review/run.sh

release: ## a release, by hand: prints the steps of docs/runbooks/release-by-hand.md, then the cut's dry run
	@$(need_py)
	@echo "A release, step by step: docs/runbooks/release-by-hand.md (the flags: docs/runbooks/release.md)"
	@grep '^## Step' docs/runbooks/release-by-hand.md | sed 's/^## /  /'
	@echo "The cut's dry run (fetches origin/main and the v* tags; makes no branch, commit, tag or push):"
	$(PY) -m pipeline release --cut --dry-run

prod: ## production is the owner's: prints his command (AGENTS A7); runs nothing
	@echo "Production is the owner's alone (AGENTS A7): he types CONFIRM_PROD. Nothing here runs it."
	@echo "  the data:     CONFIRM_PROD=<r-id> $(PY) -m pipeline release --prod <r-id>"
	@echo "  every step:   docs/runbooks/release-by-hand.md, Step 7"
