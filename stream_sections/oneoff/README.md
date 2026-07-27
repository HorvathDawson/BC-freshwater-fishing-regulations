# stream_sections/oneoff — one-off / bootstrap scripts

Run-once tooling, kept for reproducibility but NOT part of the build. Run from the repo root.

| Script | What it does | Output |
|--------|--------------|--------|
| `name_variants_compile.py` | Bootstrap the unified name-variations file from the current sources (feature_display_names + overrides + anglerinfo). Future-format sources get their own appenders (docs/13). | `stream_sections/name_variants.json` |
| `complex_regs_report.py` | One-time scan of overrides + parsed synopsis for section-language / tributary / multi-rule complexity (split-candidate triage). | `output/v2/complex_regulations.md` |

```
.venv/bin/python -m stream_sections.oneoff.name_variants_compile --out stream_sections/name_variants.json
.venv/bin/python -m stream_sections.oneoff.complex_regs_report
```

The durable outputs (`name_variants.json`, the regs report) are what the pipeline/docs consume;
these scripts only regenerate them.
