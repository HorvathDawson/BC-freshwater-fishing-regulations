# (T362) the iOS half of modules/exit-info: why the last run ended, recorded by the app itself at the crash (iOS keeps no
# ApplicationExitInfo for an app to read). Local module: autolinked from apps/mobile/modules.
Pod::Spec.new do |s|
  s.name           = 'ExitInfo'
  s.version        = '1.0.0'
  s.summary        = 'Why the last run of the app ended (crash records for the next start)'
  s.description    = s.summary
  s.license        = 'UNLICENSED'
  s.author         = 'canifishthis'
  s.homepage       = 'https://canifishthis.ca'
  s.platforms      = { :ios => '16.4' }
  s.swift_version  = '5.9'
  s.source         = { :git => '' }
  s.static_framework = true
  s.dependency 'ExpoModulesCore'
  s.pod_target_xcconfig = { 'DEFINES_MODULE' => 'YES' }
  s.source_files = "**/*.{h,m,swift}"
end
