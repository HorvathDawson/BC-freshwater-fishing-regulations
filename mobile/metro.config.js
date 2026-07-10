// Metro config — allow bundling the SQLite database and PMTiles/PDF assets.
const { getDefaultConfig } = require('expo/metro-config');

/** @type {import('expo/metro-config').MetroConfig} */
const config = getDefaultConfig(__dirname);

// Treat these binary payloads as assets so they can be `require()`d and bundled.
config.resolver.assetExts.push('sqlite', 'pmtiles', 'pdf', 'db');

module.exports = config;
