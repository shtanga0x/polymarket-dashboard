#!/usr/bin/env node
/**
 * Main entry point for fetching Polymarket data and generating JSON files.
 *
 * Site-parameterized: pass the site via --site <core|watch> or SITE env var.
 * Reads config/<site>.json and data/<site>/traders.csv, writes out/<site>/data/.
 *
 * Previous-run state (for recent-changes diffing and the PnL scrape cache) is
 * expected in the output dir BEFORE this script runs — the CI workflow downloads
 * the live files from R2 into it. Locally, a previous run's output works as-is.
 *
 * The SNAPSHOT_ID env var (set by CI to the workflow run id) is embedded in
 * metadata.json so the frontend can fetch immutable per-snapshot files.
 *
 * Usage:
 *   SITE=watch node scripts/fetch_data.js
 *   node scripts/fetch_data.js --site core
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { computeAll } from './compute_aggregates.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.join(__dirname, '..');

function parseSite() {
  const idx = process.argv.indexOf('--site');
  const site = idx !== -1 ? process.argv[idx + 1] : process.env.SITE;
  if (!site || !fs.existsSync(path.join(ROOT_DIR, 'config', `${site}.json`))) {
    console.error('Specify a valid site: --site <core|watch> or SITE env var');
    process.exit(1);
  }
  return site;
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function writeJSON(filepath, data, { pretty = false } = {}) {
  // Big data files ship minified: ~40% smaller, faster to parse for every
  // consumer (frontend, Telegram bot). metadata.json stays human-readable.
  fs.writeFileSync(filepath, JSON.stringify(data, null, pretty ? 2 : 0), 'utf-8');
  console.log(`Wrote: ${filepath}`);
}

async function main() {
  const site = parseSite();
  const config = JSON.parse(fs.readFileSync(path.join(ROOT_DIR, 'config', `${site}.json`), 'utf-8'));
  const dataDir = path.join(ROOT_DIR, 'out', site, 'data');

  console.log('═══════════════════════════════════════════════════════');
  console.log(`  ${config.site_name} - Data Refresh`);
  console.log('═══════════════════════════════════════════════════════');
  console.log(`Started at: ${new Date().toISOString()}\n`);

  try {
    ensureDir(dataDir);

    const {
      metadata,
      aggregatedPortfolio,
      traderPortfolios,
      recentChanges
    } = await computeAll({ config, dataDir, rootDir: ROOT_DIR });

    // Embed the snapshot id so the frontend can fetch immutable snapshot files.
    metadata.snapshot = process.env.SNAPSHOT_ID ? `snap/${process.env.SNAPSHOT_ID}` : null;

    // Rotate: current → previous before overwriting
    const portfolioPath = path.join(dataDir, 'aggregated_portfolio.json');
    if (fs.existsSync(portfolioPath)) {
      fs.copyFileSync(portfolioPath, path.join(dataDir, 'previous_portfolio.json'));
      console.log('Rotated: previous_portfolio.json');
    }

    writeJSON(path.join(dataDir, 'metadata.json'), metadata, { pretty: true });
    writeJSON(path.join(dataDir, 'aggregated_portfolio.json'), aggregatedPortfolio);
    writeJSON(path.join(dataDir, 'trader_portfolios.json'), traderPortfolios);
    writeJSON(path.join(dataDir, 'recent_changes.json'), recentChanges);

    // Slim feed for the Telegram bot: top positions with only the fields it
    // reads (~100KB vs multi-MB full files — the bot runs on a tight Workers
    // CPU budget and must not parse the big JSONs).
    const botFeed = {
      last_updated: metadata.last_updated,
      summary: { totalExposure: aggregatedPortfolio.summary?.totalExposure ?? 0 },
      positions: (aggregatedPortfolio.positions || []).slice(0, 400).map(p => ({
        conditionId: p.conditionId,
        outcomeIndex: p.outcomeIndex,
        outcome: p.outcome,
        title: p.title,
        slug: p.slug,
        eventSlug: p.eventSlug,
        endDate: p.endDate,
        totalExposure: p.totalExposure,
        totalSize: p.totalSize,
        traderCount: p.traderCount,
        avgEntry: p.avgEntry,
        curPrice: p.curPrice,
        priceChangePct: p.priceChangePct,
        ...(p.windowChanges
          ? { windowChanges: { h1: p.windowChanges.h1, d1: p.windowChanges.d1, w1: p.windowChanges.w1 } }
          : {})
      })),
      // Minimal change events — fallback delta source for positions that
      // dropped out of the portfolio (no windowChanges available).
      changes: (recentChanges.changes || []).slice(0, 600).map(c => ({
        conditionId: c.conditionId,
        outcomeIndex: c.outcomeIndex,
        timestamp: c.timestamp,
        delta: c.delta
      }))
    };
    writeJSON(path.join(dataDir, 'bot_feed.json'), botFeed);

    console.log('\n═══════════════════════════════════════════════════════');
    console.log('  Summary');
    console.log('═══════════════════════════════════════════════════════');
    console.log(`  Traders tracked: ${metadata.trader_count}`);
    console.log(`  Traders fetched: ${metadata.traders_fetched}`);
    console.log(`  Markets held: ${metadata.market_count}`);
    console.log(`  Total exposure: $${metadata.total_exposure.toLocaleString()}`);
    console.log(`  Recent activities: ${metadata.activity_count}`);
    console.log(`  Last updated: ${metadata.last_updated}`);
    console.log('═══════════════════════════════════════════════════════\n');

    if (metadata.traders_fetched === 0) {
      console.error('ERROR: 0 traders successfully fetched — API is unreachable (blocked IPs or no proxies configured).');
      console.error('Configure the PROXIES GitHub secret with valid proxy credentials to fix this.');
      process.exit(1);
    }

    console.log('Data refresh completed successfully!');
    process.exit(0);
  } catch (error) {
    console.error('\nFailed to refresh data:', error);
    process.exit(1);
  }
}

main();
