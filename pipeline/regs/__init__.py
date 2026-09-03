"""The regulations corpus: the synopsis, DFO salmon, and binding their words to water.

`extraction` reads the PDF. `parsing` turns pages into EntryFiles (the LLM step — HUMAN
ONLY, it spends credits). `matching` binds a named water to a registry item. `dfo_salmon`
is the second corpus, same shape, separate models.
"""
