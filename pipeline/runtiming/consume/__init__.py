"""Reads the frozen index. Needs no atlas, no tiles, no network.

Runs on every build, and CANNOT reach `generate` — a build that could re-derive the
section->CU join is a build that can silently change one. Same construction as
`pipeline/gauges/consume`.
"""
