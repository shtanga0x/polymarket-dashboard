#!/usr/bin/env node
/**
 * Build config/midterms.json — the tracked events that Polymarket files under
 * its Midterms hub (https://polymarket.com/midterms = Gamma tag `midterms`).
 *
 * Only events already in our data are considered (the union of eventSlugs in
 * each site's aggregated_portfolio.json), so small untracked midterm markets
 * never show up. Each tracked slug is looked up on Gamma in batches and kept
 * when its tags include `midterms`. The Portfolio "Midterms" filter reads the
 * result through build_site_config.js.
 *
 * Usage: node scripts/build_midterms.js [dataDir ...]
 *   dataDir defaults to out/core/data and out/watch/data (local_refresh layout).
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.join(__dirname, '..');
const GAMMA_EVENTS = 'https://gamma-api.polymarket.com/events';
const TAG = 'midterms';
const BATCH = 20;

const dataDirs = process.argv.length > 2
  ? process.argv.slice(2)
  : ['core', 'watch'].map(s => path.join(ROOT_DIR, 'out', s, 'data'));

const slugs = new Set();
for (const dir of dataDirs) {
  const file = path.join(dir, 'aggregated_portfolio.json');
  const positions = JSON.parse(fs.readFileSync(file, 'utf-8')).positions || [];
  for (const p of positions) if (p.eventSlug) slugs.add(p.eventSlug);
}
console.log(`${slugs.size} tracked events in ${dataDirs.length} data dir(s)`);

const events = [];
const all = [...slugs];
for (let i = 0; i < all.length; i += BATCH) {
  const qs = all.slice(i, i + BATCH).map(s => `slug=${encodeURIComponent(s)}`).join('&');
  const res = await fetch(`${GAMMA_EVENTS}?${qs}`, { signal: AbortSignal.timeout(60_000) });
  if (!res.ok) throw new Error(`Gamma HTTP ${res.status} on batch ${i / BATCH + 1}`);
  for (const e of await res.json()) {
    if ((e.tags || []).some(t => t.slug === TAG)) events.push({ slug: e.slug, title: e.title });
  }
}
events.sort((a, b) => a.slug.localeCompare(b.slug));

const out = path.join(ROOT_DIR, 'config', 'midterms.json');
fs.writeFileSync(out, JSON.stringify({ tag: TAG, updated: new Date().toISOString(), events }, null, 2) + '\n');
console.log(`Wrote ${out}: ${events.length} midterm events`);
