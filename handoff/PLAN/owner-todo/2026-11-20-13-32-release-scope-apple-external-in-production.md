order: 470

## 2026-11-20 13:32: release scope: Apple External in, production data in

- Owner, verbatim: "Apple external can be touched with this release as a product build. Also we will want to update
  prod data too". This release now also puts the 0.8.4 production build (reading production data) on TestFlight
  EXTERNAL testing (Apple's beta review applies). Production data was already in. Order: merge → map rebuild →
  staging → production data → Play Internal + TestFlight internal and external → production website. Play Closed
  stays untouched. An App Store production submission stays yours.
