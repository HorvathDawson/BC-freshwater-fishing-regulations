# T75 — The basemap in the release manifest (audit N18, P18b)

Status: OPEN.

Source: the live `r-98ded5f443e1/manifest.json` has `basemap: null`; `release/build.py` takes a `basemap` argument but
deliver never passes one; `release/manifest.py` allows `{id,url,bytes,sha256} | null`. Native cannot install the ground
map (~918 MB, D1), and native is "ALWAYS OFFLINE" (Q5).

## Work
The tile step names the basemap in the manifest; native install/update fetch it with a sha check; tile tests and the
vintage check cover it.

## Acceptance
Manifest basemap non-null; P11 install/update/rollback tests include it; an offline native launch draws ground.

Depends: T64 + T85 (the basemap size cut lands first so it ships once). Size: M. Owner: no.
