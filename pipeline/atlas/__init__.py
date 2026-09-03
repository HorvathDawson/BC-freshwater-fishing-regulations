"""Building the graph of BC's waters: geometry in, a sectioned registry out.

`graph` merges blue lines into stream nodes. `splits` cuts them where a curator, an area
boundary or a gauge says a river changes. `registry` names the pieces. `reach` binds a
regulation's words to them. `waters` mints the features the FWA does not have.

This is the expensive half of the project — ~27 minutes and ~10 GB — and everything here
needs the atlas on disk.
"""
