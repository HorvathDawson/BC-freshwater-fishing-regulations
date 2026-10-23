# T161 — The pipeline ships each fish's leading origin; the app's mainRes only reads it

Status: OPEN

Source: Q66 decided by Claude 2026-10-23 (owner rule: computed where the data lives). The app picks a fish's "main"
origin itself; common.lead_origin decides it in the pipeline. Ship it in the store/release, make mainRes read it, pin
no second copy. Ships: deliver + fixture re-cut + app. Size: S.
