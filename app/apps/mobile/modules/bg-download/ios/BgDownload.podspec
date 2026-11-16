# (T722) the iOS half of modules/bg-download: the release's big files on a background URLSession (on with the app in
# the background or the phone locked), the same JS surface as the Android DownloadManager half. Local module: autolinked
# from apps/mobile/modules.
Pod::Spec.new do |s|
  s.name           = 'BgDownload'
  s.version        = '1.0.0'
  s.summary        = 'Release downloads on a background URLSession, hashed natively'
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
