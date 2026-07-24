/**
 * prerender.mjs — SEO prerender pass (run after `vite build`)
 *
 * For each named waterbody in tier0.json search_index, writes:
 *   dist/waterbody/<wbg>/index.html
 *
 * The file is a copy of dist/index.html (hashed Vite assets intact)
 * with waterbody-specific <title>, <meta name="description">, canonical,
 * and Open Graph tags patched into <head>.
 *
 * Also writes dist/sitemap.xml listing all canonical URLs.
 *
 * URL slug = waterbody_group (wbg):
 *   - Streams: fwa_watershed_code (stable across regulation reruns)
 *   - Lakes/polygons: waterbody_key (stable integer)
 * This ensures SEO authority accumulates across annual regulation updates.
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT = resolve(__dirname, '..');

// --- Load environment file (no dotenv dependency) ---
// Vite sets --mode; map it to the right .env file.
const mode = process.env.VITE_MODE || process.argv.find(a => a.startsWith('--mode='))?.split('=')[1] || 'production';
const envCandidates = [
    resolve(ROOT, `.env.${mode}`),
    resolve(ROOT, '.env.production'),
];
const envPath = envCandidates.find(p => existsSync(p));
if (envPath) {
    console.log(`[prerender] Loading env from ${envPath}`);
    for (const line of readFileSync(envPath, 'utf8').split('\n')) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        const idx = trimmed.indexOf('=');
        if (idx === -1) continue;
        const key = trimmed.slice(0, idx).trim();
        const val = trimmed.slice(idx + 1).trim();
        if (!(key in process.env)) process.env[key] = val;
    }
}

const DEPLOY_DIR = resolve(ROOT, '..', 'output', 'pipeline', 'deploy');
const JSON_PATH = resolve(DEPLOY_DIR, 'tier0.json');
const TEMPLATE_PATH = resolve(ROOT, 'dist', 'index.html');
const OUT_DIR = resolve(ROOT, 'dist', 'waterbody');
const SITEMAP_PATH = resolve(ROOT, 'dist', 'sitemap.xml');
const SITE_URL = 'https://canifishthis.ca';
const R2_DATA_URL = process.env.VITE_TILE_BASE_URL
    ? `${process.env.VITE_TILE_BASE_URL}/tier0.json`
    : 'https://bc-fishing-r2.horvath-dawson.workers.dev/tier0.json';

// --- Load tier0 JSON: local deploy/ first, then R2 ---
async function loadTier0Data() {
    if (existsSync(JSON_PATH)) {
        console.log('[prerender] Loading tier0.json from local deploy/.');
        return JSON.parse(readFileSync(JSON_PATH, 'utf8'));
    }
    console.log('[prerender] Local file not found, fetching from R2...');
    const res = await fetch(R2_DATA_URL);
    if (!res.ok) {
        throw new Error(`Failed to fetch tier0.json from R2: ${res.status} ${res.statusText}`);
    }
    return res.json();
}

// --- Main (async for R2 fetch support) ---
const json = await loadTier0Data();
const searchIndex = json.search_index ?? [];

// --- Guards ---
if (!existsSync(TEMPLATE_PATH)) {
    console.error(`\nERROR [prerender]: ${TEMPLATE_PATH} not found. Run vite build first.\n`);
    process.exit(1);
}

// --- Load template ---
const template = readFileSync(TEMPLATE_PATH, 'utf8');

// Short-key field names mirror waterbodyDataService.ts decodeWaterbody().
// Both short and long keys are handled to be forward-compatible.
const TYPE_LABEL = {
    stream: 'Stream',
    lake: 'Lake',
    wetland: 'Wetland',
    manmade: 'Reservoir',
    ungazetted: 'Waterbody',
};

/** Escape special HTML characters for safe attribute/text insertion. */
function escapeHtml(str) {
    return str
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

/**
 * Strip trailing -000000 padding from FWA watershed codes for URL slugs.
 * Mirrors collapseWbg() in urlState.ts — must stay in sync.
 *   900-105574-000000-...-000000  →  900-105574
 */
function collapseWbg(wbg) {
    return wbg.replace(/(-000000)+$/, '');
}

/**
 * Patch the Vite-built index.html template for a specific waterbody.
 * Replaces title/description in place; inserts canonical + og tags before </head>.
 */
function patchTemplate(tmpl, { title, description, canonicalUrl, bodyHtml, breadcrumbJsonLd }) {
    let html = tmpl;

    // Replace the crawlable #seo-content block with waterbody-specific markup
    // (H1 + description + a link home). Function replacer avoids `$` issues.
    if (bodyHtml) {
        html = html.replace(
            /(<div id="seo-content"[^>]*>)[\s\S]*?(<\/div>)/i,
            () => `<div id="seo-content" class="visually-hidden">${bodyHtml}</div>`,
        );
    }

    // BreadcrumbList structured data (Home → Waterbody) before </head>.
    if (breadcrumbJsonLd) {
        const headEnd0 = html.indexOf('</head>');
        if (headEnd0 !== -1) {
            const tag = `  <script type="application/ld+json">${breadcrumbJsonLd}</script>\n`;
            html = html.slice(0, headEnd0) + tag + html.slice(headEnd0);
        }
    }

    // The homepage template (index.html) already carries site-level canonical /
    // og tags, so per-waterbody pages must REPLACE the page-varying ones (not
    // append) — otherwise a page ends up with two <title>s / two canonicals /
    // two og:titles and crawlers pick unpredictably. Site-level tags
    // (og:site_name, og:type, og:locale, og:image, robots, keywords, JSON-LD)
    // are correctly inherited unchanged.
    // Function replacers avoid `$` in a waterbody name being treated as a
    // replacement special.
    const inserts = [];
    const upsert = (regex, tag) => {
        if (regex.test(html)) html = html.replace(regex, () => tag);
        else inserts.push(tag);
    };

    upsert(/<title>[\s\S]*?<\/title>/i, `<title>${escapeHtml(title)}</title>`);
    upsert(/<meta\s+name="description"[^>]*\/?>/i, `<meta name="description" content="${escapeHtml(description)}" />`);
    upsert(/<link\s+rel="canonical"[^>]*\/?>/i, `<link rel="canonical" href="${canonicalUrl}" />`);
    upsert(/<meta\s+property="og:title"[^>]*\/?>/i, `<meta property="og:title" content="${escapeHtml(title)}" />`);
    upsert(/<meta\s+property="og:description"[^>]*\/?>/i, `<meta property="og:description" content="${escapeHtml(description)}" />`);
    upsert(/<meta\s+property="og:url"[^>]*\/?>/i, `<meta property="og:url" content="${canonicalUrl}" />`);

    if (inserts.length) {
        const headEnd = html.indexOf('</head>');
        if (headEnd !== -1) {
            html = html.slice(0, headEnd) + inserts.map(t => '  ' + t).join('\n') + '\n' + html.slice(headEnd);
        }
    }

    return html;
}

// Cloudflare Pages free tier: 20,000 files per deployment.
// Reserve headroom for Vite's base output (JS, CSS, HTML, fonts, images).
// Override via PRERENDER_MAX_PAGES in .env.production.
// Set to 0 or remove to render all pages (no cap).
const MAX_PAGES = parseInt(process.env.PRERENDER_MAX_PAGES, 10) || 0;

// --- Build wbg → primary raw entry map (deduplicate: one HTML page per wbg) ---
// Multiple search entries can share the same wbg (e.g., different regulation
// segments of the same river). We use the first-seen entry per wbg as the page source.
const wbgEntries = new Map(); // wbg → raw tier0 search entry
for (const raw of searchIndex) {
    const wbg = collapseWbg(raw.waterbody_group ?? '');
    if (!wbg) continue;
    if (wbgEntries.has(wbg)) continue; // keep first/primary entry
    wbgEntries.set(wbg, raw);
}

// --- Prioritise pages: regulated waterbodies first, then named ---
let entries = [...wbgEntries.entries()].filter(
    ([, raw]) => (raw.display_name ?? '') !== '',
);
entries.sort((a, b) => {
    const aRegs = (a[1].segments ?? []).length;
    const bRegs = (b[1].segments ?? []).length;
    return bRegs - aRegs; // more regulations → higher priority
});
if (MAX_PAGES > 0 && entries.length > MAX_PAGES) {
    console.log(`[prerender] Capping from ${entries.length} to ${MAX_PAGES} pages (PRERENDER_MAX_PAGES).`);
    entries = entries.slice(0, MAX_PAGES);
}

// --- Write per-waterbody HTML files ---
let written = 0;
const sitemapUrls = [];

for (const [wbg, raw] of entries) {
    const segments = raw.segments ?? [];

    // Primary name: use the most common segment display_name so the page title
    // reflects the name that covers the most regulation sections.
    const segNameCounts = new Map();
    for (const seg of segments) {
        const n = seg.display_name || '';
        if (n) segNameCounts.set(n, (segNameCounts.get(n) || 0) + 1);
    }
    // Pick the most common reach name; break ties by preferring non-"unnamed"
    const displayName = segNameCounts.size > 0
        ? [...segNameCounts.entries()].sort((a, b) => {
            if (b[1] !== a[1]) return b[1] - a[1]; // most frequent first
            const aUnnamed = /unnamed/i.test(a[0]) ? 1 : 0;
            const bUnnamed = /unnamed/i.test(b[0]) ? 1 : 0;
            return aUnnamed - bUnnamed; // named wins ties
        })[0][0]
        : (raw.display_name ?? '');

    const type = raw.feature_type ?? '';
    const typeLabel = TYPE_LABEL[type] ?? 'Waterbody';

    // Name variants (alternate spellings) — include only direct variants in meta description for SEO.
    // Also include any segment display_names that differ from the primary.
    const nameVariants = (raw.name_variants ?? [])
        .filter(v => v && v.source === 'direct')
        .map(v => v.name)
        .filter(v => v && v !== displayName);
    // Add any less-common segment display_names as additional name variants
    for (const [segName] of segNameCounts) {
        if (segName !== displayName && !nameVariants.includes(segName)) {
            nameVariants.push(segName);
        }
    }
    const akaText = nameVariants.length > 0
        ? ` Also known as: ${nameVariants.join(', ')}.`
        : '';

    // Regulation summary for meta description (keep under 160 chars total)
    const regCount = segments.length;
    const regText = regCount > 1
        ? `${regCount} regulation zones.`
        : regCount === 1 ? 'Has fishing regulations.' : '';

    const title = nameVariants.length > 0
        ? `${displayName} (${nameVariants.join(', ')}) Fishing Regulations | BC Freshwater`
        : `${displayName} Fishing Regulations | BC Freshwater`;
    const description = `BC freshwater fishing regulations for ${displayName} (${typeLabel}).${akaText} ${regText} View catch limits, closures, gear restrictions, and seasons.`
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 160);

    const encodedWbg = encodeURIComponent(wbg);
    const canonicalUrl = `${SITE_URL}/waterbody/${encodedWbg}/`;

    // Crawlable body content (visually hidden; replaces the homepage placeholder).
    const h1 = `${displayName} Fishing Regulations`;
    const aka = nameVariants.length > 0 ? ` Also known as ${nameVariants.join(', ')}.` : '';
    const bodyHtml =
        `<h1>${escapeHtml(h1)}</h1>` +
        `<p>Current British Columbia freshwater fishing regulations for ${escapeHtml(displayName)} (${escapeHtml(typeLabel)}).${escapeHtml(aka)} ` +
        `${escapeHtml(regText)} View catch limits, closures, gear and bait restrictions, seasons, fish stocking, and live water gauge levels on the interactive map.</p>` +
        `<p><a href="/">BC Fishing Regulations Map</a> — search any BC lake, river, or stream.</p>`;

    // BreadcrumbList: Home → Waterbody.
    const breadcrumbJsonLd = JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'BreadcrumbList',
        itemListElement: [
            { '@type': 'ListItem', position: 1, name: 'BC Fishing Regulations Map', item: `${SITE_URL}/` },
            { '@type': 'ListItem', position: 2, name: h1, item: canonicalUrl },
        ],
    });

    const html = patchTemplate(template, { title, description, canonicalUrl, bodyHtml, breadcrumbJsonLd });

    const outPath = resolve(OUT_DIR, encodedWbg);
    mkdirSync(outPath, { recursive: true });
    writeFileSync(resolve(outPath, 'index.html'), html, 'utf8');

    sitemapUrls.push(canonicalUrl);
    written++;
}

// --- Write sitemap.xml ---
const now = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
const sitemap = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    `  <url><loc>${SITE_URL}/</loc><changefreq>weekly</changefreq><priority>1.0</priority><lastmod>${now}</lastmod></url>`,
    ...sitemapUrls.map(u => `  <url><loc>${u}</loc><changefreq>yearly</changefreq><priority>0.6</priority><lastmod>${now}</lastmod></url>`),
    '</urlset>',
].join('\n');

writeFileSync(SITEMAP_PATH, sitemap, 'utf8');
console.log(`[prerender] ${written} waterbody pages → dist/waterbody/`);
console.log(`[prerender] sitemap.xml → ${sitemapUrls.length + 1} URLs`);
