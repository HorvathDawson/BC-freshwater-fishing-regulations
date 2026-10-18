From the repo root:
- Snapshot page v36 (adds the Chilliwack and the Kootenay Lake parts the map steps to): `node app/design/regs-v11/tools/snap.mjs`
- The water map's data (bundle + status index + tile layers of one build, ~10 s): `~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/handoff/tools/heavy.sh watermap 25 -- .venv/bin/python -m pipeline.tools.water_map_export --group vedder=gnis:3062,gnis:8634 --group cowichan=gnis:12227 --group klwa=wbk:-22,wbk:-21,wbk:-20 --date 10-10 --date 10-12 --date 05-15 --date 06-15 --braids --out app/design/regs-v11/watermap.js` (`--braids`: a review map per braid the reach builder placed across a cut, bundle `section_straddle`)
- The cap system's own answers for check.mjs's internal test card-is-the-caps (after every snap; needs .venv; never drawn): `node app/design/regs-v11/tools/oracle.mjs` -> `app/design/regs-v11/oracle.json`
- Re-shoot the screens: `node app/design/regs-v11/tools/shoot.mjs [name-filter]`
- Check the page (also in app CI): `node app/design/regs-v11/tools/check.mjs [page.html]`
- Full-length screens (whole cards / the whole licence answer, header on top): `node app/design/regs-v11/tools/tall.mjs [name-filter]` -> `screens/ur1-*.png`, `screens/ur2-*.png` (modes: cards, licence, keep, sheet)
