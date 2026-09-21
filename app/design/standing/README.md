# Standing tables — the review page

The province's and each region's quota and gear rules **before any water changes them**,
each line beside the printed synopsis sentence it was read from. A section's table is its
region's base plus named overrides, so a line wrong here is wrong on every water in the
region at once — which is why this page exists and why it is checked against the book.

Published: https://claude.ai/artifact/TzMSTVCayHe7zWNDbrs4k9

## Build

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.emit_base_tables -o /tmp/base.json
{ cat app/design/standing/head.html
  printf '<script id="base" type="application/json">'
  cat /tmp/base.json
  printf '</script>\n<script>window.__BASE__=JSON.parse(document.getElementById("base").textContent);</script>\n'
  cat app/design/standing/body.html
} > /tmp/standing.html
```

Then publish `/tmp/standing.html` to the URL above — the same artifact, never a new one.

## Why these two files are in the repository

They lived only in a session scratchpad, which is ephemeral: one was cleared between
sessions and took the print-check PDFs with it, turning the gating suite red with
`FileNotFoundError`. Had it taken these, the published page could never have been edited
again. Nothing the build needs may live outside the repository.

`head.html` is the styles; `body.html` is the markup and the renderer. Both are authored,
not generated — the generated half is `base.json`, from `pipeline/tools/emit_base_tables.py`.
