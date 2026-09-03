"""Shared with everything and depending on nothing: the models, the IO, the paths.

`curated` is the one that matters — every path in the project resolves through it, validated
by pydantic at first access, so a wrong one fails at import naming the key rather than
returning an empty list four builds later.
"""
