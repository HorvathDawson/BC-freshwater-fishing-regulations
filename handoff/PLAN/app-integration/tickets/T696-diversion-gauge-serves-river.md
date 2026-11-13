# T696 — Diversion gauges: "Canal gauge also serves river" (Q103); the Theodosia's 08GC005 above the canal

Status: IN REVIEW — built (08GC005 above the Theodosia's canal; the 28 left gaugeless); owner: OWNER-TODO "T696: a cut at the Theodosia intake?"

Source: owner 2026-11-12 (dialog option, verbatim): "Canal gauge also serves river" — the diversion gauge is shown on the
nearby river sections too, labelled as measuring a diversion; it must not outrank a real river gauge, only fill sections
that have none. RULINGS P54.

## Measured (2026-11-12, live atlas `full`, the bundle's own `build_gauge_sheds` run twice: climb guard on vs off)
- Sections that lose every gauge because of the climb guard: **28** (the 24 of T117 in October). Not one of them was served
  by a diversion gauge. Who served them with the guard off: 10CD001 Muskwa near Fort Nelson 6, 08NG065 Kootenay at Fort
  Steele 6, 08NM116 Mission Creek near East Kelowna 5, 08MH032 North Arm Fraser at Vancouver 3, 08EB003 Skeena at Glen
  Vowell 2, 08HD007 Salmon above Memekay 2, 08HB083 Courtenay at 5th Ave 1, 08HD023 Little Oyster at Yorke Rd 1, 08LC042
  Bessette above Lumby Lagoon outfall 1, 08MC040 San Jose above Borland 1. All river gauges (refused at a barrier, a code
  divide or a bigger river, T117), none a canal.
- The diversion gauges' only effect is a speaker change; every section keeps a gauge: 08GC005 Theodosia River Diversion
  above Olsen Lake → 08GC006 Theodosia River Diversion Bypass on 175 sections; 08HD034 Quinsam River Diversion Headpond →
  08HD021 Quinsam River at Argonaut Bridge on 112; 08HD026 Quinsam Diversion near Campbell River → 08HD027 Quinsam River
  below Lower Quinsam Lake on 2.
- So the chosen rule ("only fills sections that have none") fills nothing, and building the "measures a diversion" marker
  would ship a label no section carries. Nothing built; no app file touched.

## For the owner
1. The 28 gaugeless sections are a different question (a river gauge refused above a barrier / code divide): leave them
   gaugeless (today), or let those river gauges climb?
2. Theodosia: today 08GC006 "Diversion Bypass" (itself a diversion-works gauge) speaks for the 175 sections above the
   canal instead of 08GC005. Is that the river's gauge, or should 08GC005 speak there, labelled as a diversion gauge?

## Answered 2026-11-12 (dialog options, verbatim)
1. The 28 gaugeless river sections stopped at a barrier or code divide: **"Leave gaugeless"**. Closed: the T117 climb
   guard stands; nothing changes for them.
2. The Theodosia: **"Show 08GC005 above the canal"** (labelled as measuring a diversion).

## Built 2026-11-12 (RULINGS P54)
- `data/curated/gauges/diversion_stations.json` (authored, one row: 08GC005, river blue line 360856269, in place of
  08GC006 + 08GC007 above the intake), read by `pipeline/gauges/consume/diversion.py` (checked against HYDAT: the name
  says DIVERSION). The graph finds the intake (the river section with an edge into the canal) and "above" (its upstream
  closure); nothing about sections is written by hand.
- Bundle: 08GC005 placed AT THE INTAKE for the donor panels, the route and `gauge.sid`; its walks only climb; 08GC006 and
  08GC007 leave every section above the intake; `gauge.diversion` = 1 (added last, defaulted: still conditions/1).
- App: map key "08GC005 · 70% · measures a diversion" (`donorMapKey`); one sentence under the answer, Flow and Level
  (`diversionNote`): "One of these gauges measures a diversion: it stands on a canal that takes water out of the river at
  an intake, so it reads what the canal takes, not everything the river carries."
- Measured on the live atlas (scratch run of the bundle's own functions, Theodosia stations): the main-stem panel
  (1195804, 13.3 km up to the lake) goes from 08GC007 lead / 08GC006 / 08GC008 to **08GC005 lead / 08GC008**; 117
  one-km² headwater panels drop a seat 08GC005 held only because the canal's modelled catchment is 1 km²; the match
  record moves 180 sections 08GC006 → 08GC005; below the intake nothing changes. Size: one 0/1 column on 2,324 gauge rows
  and a handful of panel rows — no gate moves.
- Positions (Esri imagery, `~/bcfish-session-backup-2026-11-12/theodosia/`): the canal leaves the river at the intake
  dam, ~160 m above the 08GC006/08GC007 gauge cut, where the section 1195804 begins. Nothing is ambiguous on the ground;
  the section grid is: those ~160 m (where the bypass gauges stand) are in the same section as the river above.

## For the owner
- Add a cut at the intake, so the ~160 m between the intake and the bypass gauge cut stay 08GC006/08GC007's? It rides
  the next atlas build (the T690/T691 cuts already need one). Without it that stretch reads 08GC005 too.

## The final release must rebuild
- deliver (bundle → status index → export) for the new `gauge.diversion` column and the panels; the app build for the
  label. No atlas rebuild for this ticket unless the owner wants the intake cut.
