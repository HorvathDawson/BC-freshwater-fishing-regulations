Item 4 (Core Web Vitals / bundle) — how we'd do it
Your main JS chunk is ~1.9 MB (600 KB gzipped). LCP/INP are ranking factors, so trimming it helps. The approach, roughly in order of payoff:

Measure first — run Lighthouse (or PageSpeed Insights on the deployed URL) to get real LCP/CLS/INP numbers, and npx vite-bundle-visualizer to see what's actually big. No point optimizing blind.
Lazy-load heavy, non-critical deps — pdfjs-dist + pdf-lib (bathymetry PDFs) and likely maplibre-gl plugins are prime candidates for dynamic import() so they're not in the initial bundle. I already code-split chart.js out via the gauge tab; same technique.
manualChunks in vite.config — split vendor libs (maplibre, pmtiles, pdf) into separate chunks so the app shell loads first and the browser caches vendors across deploys.
Defer the map init until after first paint if possible, and make sure the inline splash → LCP element is fast (it already is).
Preload the critical font/tiles, ensure images use loading="lazy".
It's a measure-then-cut loop rather than a single change, and it risks touching load-bearing map code — so I'd want to do it as its own focused pass with Lighthouse numbers before/after, not bundled with the SEO work.

Want me to run the bundle visualizer + a Lighthouse pass and come back with a concrete, prioritized list of what to split (and do the safe dynamic-import wins)? Or leave item 4 for now — the content fixes in 1–3 are the bigger SEO lever anyway.

future hard:
3) split regulations more
4) add a way to filter the target species. 

next todos
1) adjustable opacity on all togglable layers like base layer. refer to item below this got refined
2) layer manifest should drive the layer color and appreance in the front end. aka border/fill type and color. also which zoom level specific appearance disappears at. look at front end too so we can see if any zoom level stuff needs to be added into the tile generation instead? again refer below for more context in the planning.

5) query the osm trails (offroads) to put in a seperate layer instead of having always on in the basemap. then it will be added to the layer manifest with the bc fsr layer. the fsr layer will be default off and trails default on. all layers in toggle menu will have ability to have opacity change. 
6) need to change the legend on the forecast so the line ones are not block and show up as lines with proper pattern. also add in the model info to disclaimer. 
7) make it so the layer toggle menu hides the opacity filter in a settings cog on the right of the toggle. so you can open settings cog for a layer which then opens a pop up with a back button to close the popup (make back button on webbrowser also do it) in here it will give info about the layer (which will exist in the layer manifest) it should link to the sources of the data here too. then it should be able to change the opacity/color of a layer? then we should get ride of the basemap toggle in the menu and get rid of the way it sets global opacity. instead now when the basemap is switched to satellite we will change the attributes of each layer. this will all be defaulted in the layer manifest. this will need more thought to actually make this into a good ui experience because this still seems clunky. i think we actually do not want the basemap to change the appearance of the other layers are all. instead we will just make the lakes and streams and other layers like that toggleable too. the layer menu should become a lot bigger and have the ability to scroll if enough layers are there. this will need a lot of planning. for web app we will want to make it so any appreance changes a user makes color wise is stored. also for polygons we need a way to set fill and external. user will not adjust when it gets cut off though aka the border being removed before the fill. we really need to plan this with a ui expert agent



info to add to disclaimer:
The ELF Model is an empirical mathematical model without any climate data input. Therefore, the ELF Model forecasts are only the worst (lowest flow) scenarios without any rainfall during the forecast period. The model and data have limitations, inaccuracies and errors. As such, the forecast should only be treated as estimates, are provided for guidance only, and are subject to change. The actual discharges or water levels observed will be different from the forecasts. Users of this data must accept all responsibility for their use and interpretation.

 This forecast is derived from the CLEVER Model, a hydrological model using third-party data as inputs. The model has two categories of uncertainty or forecast errors, systematic errors from the model’s intrinsic limitations and random errors inherited from the input data. Therefore, it can be expected that the model forecasts are different from the observations. It is also possible that the actual flow is higher than the forecast upper bound or lower than the forecast lower bound. Users of this forecast must accept all responsibility for their use and interpretation. Please click here (https://bcrfc.env.gov.bc.ca/freshet/for_chart.pdf) for more information.

 