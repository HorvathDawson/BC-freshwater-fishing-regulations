"""The bundler: build artifacts in, one `bundle.sqlite` out.

See `pipeline/deliver/bundle/build.py`. The format is defined once, in `schema.sql`, and every
packager — this one and the app's development fixture — executes that same file.
"""
from pipeline.deliver.bundle.build import build, SCHEMA, INDEXES  # noqa: F401
