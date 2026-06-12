/**
 * Compute aggregated portfolio data from raw trader positions and activity.
 *
 * Site-parameterized: computeAll({ config, dataDir, rootDir }) receives the
 * parsed config/<site>.json, the per-site output dir (out/<site>/data — CI
 * pre-downloads the previous run's live R2 files there) and the repo root
 * (traders csv lives at config.traders_csv relative to it).
 */

import fs from 'fs';
import path from 'path';
import {
  fetchWalletPositions,
  fetchWalletActivity,
  fetchWalletValue,
  fetchUsdcBalance,
  batchFetch
} from './polymarket_api.js';
import { batchScrapeProfilesParallel } from './scrape_profile.js';

const PNL_LOOKUP_VERSION = 2;

/**
 * Parse CSV file
 */
function parseCSV(content) {
  const lines = content.trim().split('\n');
  const headers = lines[0].split(',').map(h => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(',').map(v => v.trim());
    if (values.length >= 2 && values[0]) {
      const row = {};
      headers.forEach((h, idx) => {
        row[h] = values[idx] || '';
      });
      rows.push(row);
    }
  }
  return rows;
}

/**
 * Load traders from the site's CSV (config.traders_csv, relative to rootDir).
 */
export function loadTraders(rootDir, config) {
  const csvPath = path.join(rootDir, config.traders_csv);
  const content = fs.readFileSync(csvPath, 'utf-8');
  const traders = parseCSV(content);
  // Filter out traders without valid wallet addresses
  return traders.filter(t => t.address && t.address.startsWith('0x'));
}

/**
 * Load previous trader portfolios so slow-changing fields (scraped PnL)
 * can be reused. CI downloads the live R2 copy into dataDir before the run.
 */
function loadPreviousTraderPortfolios(dataDir) {
  const prevPath = path.join(dataDir, 'trader_portfolios.json');
  try {
    if (fs.existsSync(prevPath)) {
      return JSON.parse(fs.readFileSync(prevPath, 'utf-8'));
    }
  } catch (e) {
    console.warn('Could not load previous trader portfolios:', e.message);
  }
  return {};
}

function isFreshEnough(timestamp, maxAgeMinutes) {
  if (!timestamp || !Number.isFinite(maxAgeMinutes)) return false;
  const time = new Date(timestamp).getTime();
  if (Number.isNaN(time)) return false;
  return Date.now() - time < maxAgeMinutes * 60 * 1000;
}

/**
 * Compute exposure from position
 */
function computeExposure(position) {
  // Use currentValue if available, otherwise use size * price, or just size
  let exposure = 0;
  if (position.currentValue !== undefined && position.currentValue !== null) {
    exposure = Math.abs(parseFloat(position.currentValue));
  } else if (position.size !== undefined && position.curPrice !== undefined) {
    exposure = Math.abs(parseFloat(position.size) * parseFloat(position.curPrice));
  } else if (position.size !== undefined) {
    exposure = Math.abs(parseFloat(position.size));
  }
  return Number.isFinite(exposure) ? exposure : 0;
}

/**
 * Fetch and process all trader portfolios.
 *
 * PnL scrape-cache: traders whose previously scraped PnL is younger than
 * config.scrape_pnl_interval_minutes reuse the cached value; only stale
 * entries are re-scraped, in parallel batches of config.scrape_concurrency.
 *
 * @returns {Promise<{traderPortfolios: object, pnlStats: {cached: number, scraped: number, failed: number}}>}
 */
export async function fetchAllPortfolios(traders, config, dataDir) {
  console.log(`Fetching portfolios for ${traders.length} traders...`);

  const traderPortfolios = {};
  const concurrency = config.concurrency_limit || 8;
  const positionsLimit = config.positions_limit_per_trader || 1000;
  const scrapeConcurrency = config.scrape_concurrency || 4;
  const pnlMaxAgeMinutes = config.scrape_pnl_interval_minutes ?? 60;
  const previousPortfolios = loadPreviousTraderPortfolios(dataDir);

  const addresses = traders.map(t => t.address);
  const positionsPromise = batchFetch(
    addresses,
    (address, cfg) => fetchWalletPositions(address, positionsLimit, cfg),
    concurrency,
    config
  );
  const valuesPromise = batchFetch(addresses, fetchWalletValue, concurrency, config);

  console.log('Fetching USDC balances from Polygon...');
  const usdcPromise = batchFetch(addresses, fetchUsdcBalance, concurrency, config);

  // Reuse cached PnL when fresh enough; collect the stale rest for scraping.
  const pnlMap = new Map();
  const pnlSourceMap = new Map();
  const staleTraders = [];
  for (const trader of traders) {
    const addr = trader.address.toLowerCase();
    const previous = previousPortfolios[addr];
    const previousPnlTimestamp = previous?.pnlLastScraped || previous?.lastUpdated;
    if (
      previous
      && previous.pnlLookupVersion === PNL_LOOKUP_VERSION
      && isFreshEnough(previousPnlTimestamp, pnlMaxAgeMinutes)
    ) {
      pnlMap.set(addr, {
        pnl: previous.totalPnL || 0,
        amount: previous.tradingVolume || 0
      });
      pnlSourceMap.set(addr, 'cached');
    } else {
      staleTraders.push(trader);
    }
  }

  // Scrape only stale profiles, in parallel (with the API fetches above).
  console.log(`Scraping ${staleTraders.length} stale profile PnL entries (max age ${pnlMaxAgeMinutes}m, concurrency ${scrapeConcurrency})...`);
  const scrapePromise = staleTraders.length > 0
    ? batchScrapeProfilesParallel(staleTraders, scrapeConcurrency)
    : Promise.resolve(new Map());

  const [positionsResults, valuesResults, usdcResults, scrapedResults] = await Promise.all([
    positionsPromise,
    valuesPromise,
    usdcPromise,
    scrapePromise
  ]);

  let failedPnl = 0;
  for (const trader of staleTraders) {
    const addr = trader.address.toLowerCase();
    const pnlData = scrapedResults.get(addr);
    if (pnlData) {
      pnlMap.set(addr, pnlData);
      pnlSourceMap.set(addr, 'scraped');
    } else {
      failedPnl++;
    }
  }

  const pnlStats = {
    cached: [...pnlSourceMap.values()].filter(s => s === 'cached').length,
    scraped: [...pnlSourceMap.values()].filter(s => s === 'scraped').length,
    failed: failedPnl
  };
  console.log(`PnL scrape: reused ${pnlStats.cached}, scraped ${pnlStats.scraped}, failed ${pnlStats.failed}`);

  // Build trader portfolios with scraped PnL
  for (const trader of traders) {
    const addr = trader.address.toLowerCase();
    const posResult = positionsResults.get(addr);
    const valResult = valuesResults.get(addr);
    const usdcResult = usdcResults.get(addr);
    const scrapedPnL = pnlMap.get(addr);

    const positions = posResult?.success ? posResult.data : [];
    // 404-partial results carry data: [] even for numeric fetchers — coerce.
    const totalValue = valResult?.success && Number.isFinite(valResult.data) ? valResult.data : 0;
    const usdcBalance = usdcResult?.success && Number.isFinite(usdcResult.data) ? usdcResult.data : 0;

    // Calculate unrealized PnL from positions (for breakdown)
    let unrealizedPnL = 0;
    for (const pos of positions) {
      unrealizedPnL += parseFloat(pos.cashPnl || 0);
    }

    // Use scraped PnL if available, otherwise use unrealized from positions
    const totalPnL = scrapedPnL?.pnl ?? unrealizedPnL;
    const realizedPnL = totalPnL - unrealizedPnL;

    traderPortfolios[addr] = {
      address: addr,
      label: trader.label,
      tier: trader.tier || '1',
      positions: positions,
      totalValue: totalValue,
      usdcBalance: usdcBalance,
      totalPnL: Math.round(totalPnL * 100) / 100,
      unrealizedPnL: Math.round(unrealizedPnL * 100) / 100,
      realizedPnL: Math.round(realizedPnL * 100) / 100,
      tradingVolume: scrapedPnL?.amount || 0,
      fetchSuccess: posResult?.success && valResult?.success,
      pnlSource: pnlSourceMap.get(addr) || 'calculated',
      pnlLookupVersion: scrapedPnL ? PNL_LOOKUP_VERSION : null,
      pnlLastScraped: pnlSourceMap.has(addr)
        ? (pnlSourceMap.get(addr) === 'cached' ? previousPortfolios[addr]?.pnlLastScraped || previousPortfolios[addr]?.lastUpdated : new Date().toISOString())
        : null,
      lastUpdated: new Date().toISOString()
    };
  }

  return { traderPortfolios, pnlStats };
}

/**
 * Fetch recent activity for all traders (for changes display)
 */
export async function fetchRecentActivity(traders, config) {
  console.log(`Fetching activity for ${traders.length} traders...`);

  const allActivity = [];
  const concurrency = config.concurrency_limit || 8;

  // Calculate timestamp for 30 days ago
  const thirtyDaysAgo = Math.floor(Date.now() / 1000) - (30 * 24 * 60 * 60);
  const activityLimit = config.activity_limit_per_trader || 500;

  const activityResults = await batchFetch(
    traders.map(t => t.address),
    (address, cfg) => fetchWalletActivity(address, thirtyDaysAgo, activityLimit, cfg),
    concurrency,
    config
  );

  // Map trader addresses to labels
  const labelMap = new Map(traders.map(t => [t.address.toLowerCase(), t.label]));

  // Combine all activity
  for (const [address, result] of activityResults) {
    if (result.success && result.data) {
      for (const activity of result.data) {
        allActivity.push({
          ...activity,
          traderAddress: address,
          traderLabel: labelMap.get(address) || address.slice(0, 10)
        });
      }
    }
  }

  // Sort by timestamp descending. The full set feeds the window aggregates
  // (Δ1h / Δ1h→1d / Δ1d→1w) — capping happens only in the recent_changes
  // OUTPUT (processRecentChanges), so a small feed file doesn't silently
  // truncate the day/week windows to a couple of hours.
  allActivity.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
  return allActivity;
}

/**
 * Build 24h change map from activity
 */
// Event types that move a position. REWARD/YIELD/MAKER_REBATE are cash-only
// (no conditionId) and never affect position flow.
const POSITION_EVENT_TYPES = new Set(['TRADE', 'REDEEM', 'MERGE', 'SPLIT', 'CONVERSION', 'CONVERT']);

/**
 * Index of which outcome(s) each trader holds per condition, built from the
 * current AND previous run's portfolios. Used to attribute REDEEM / MERGE /
 * CONVERSION events — the activity API reports them with outcomeIndex 999
 * (unknown side), but the trader's portfolio tells us which side they held.
 */
function buildHeldOutcomesIndex(traderPortfolios, prevTraderPortfolios) {
  const index = new Map();
  for (const source of [prevTraderPortfolios, traderPortfolios]) {
    if (!source) continue;
    for (const [addr, p] of Object.entries(source)) {
      for (const pos of p?.positions || []) {
        if (pos.conditionId === undefined || pos.outcomeIndex === undefined) continue;
        const key = `${addr.toLowerCase()}-${pos.conditionId}`;
        let set = index.get(key);
        if (!set) { set = new Set(); index.set(key, set); }
        set.add(pos.outcomeIndex);
      }
    }
  }
  return index;
}

/**
 * Resolve an activity event to per-outcome USD deltas.
 *
 * TRADE events carry their own outcomeIndex. REDEEM / MERGE / CONVERSION /
 * SPLIT report outcomeIndex 999 — attribute them to the outcome(s) the
 * trader holds (or held last run) for that condition; skip when unknown
 * rather than guess and poison the other side's flow.
 *
 * @returns {Array<{outcomeIndex: number, delta: number}>}
 */
function eventOutcomeDeltas(a, heldOutcomes) {
  if (a.type && !POSITION_EVENT_TYPES.has(a.type)) return [];
  if (!a.conditionId) return [];
  const usd = parseFloat(a.usdcSize || a.size || 0);
  if (!usd) return [];

  if (!a.type || a.type === 'TRADE') {
    const idx = (a.outcomeIndex !== undefined && a.outcomeIndex !== 999)
      ? a.outcomeIndex
      : (a.outcome === 'Yes' ? 1 : 0);
    return [{ outcomeIndex: idx, delta: a.side === 'BUY' ? usd : -usd }];
  }

  // SPLIT mints both sides (+); REDEEM/MERGE/CONVERSION close positions (−).
  const sign = a.type === 'SPLIT' ? 1 : -1;
  const held = heldOutcomes?.get(`${(a.traderAddress || a.proxyWallet || '').toLowerCase()}-${a.conditionId}`);
  if (!held || held.size === 0) return [];
  return Array.from(held).map(outcomeIndex => ({ outcomeIndex, delta: sign * usd }));
}

function build24hChangeMap(activity, heldOutcomes) {
  const now = Math.floor(Date.now() / 1000);
  const cutoff24h = now - 24 * 3600;
  const changeMap = new Map();

  for (const a of activity) {
    if ((a.timestamp || 0) < cutoff24h) continue;
    for (const { outcomeIndex, delta } of eventOutcomeDeltas(a, heldOutcomes)) {
      const key = `${a.conditionId}-${outcomeIndex}`;
      changeMap.set(key, (changeMap.get(key) || 0) + delta);
    }
  }

  return changeMap;
}

/**
 * Build per-position chained window aggregates from the FULL activity set.
 *
 * Windows are non-overlapping so each trade counts in exactly one bucket:
 *   h1 = (now-1h, now], d1 = (now-1d, now-1h], w1 = (now-1w, now-1d]
 * This lets the UI show the evolution window by window instead of three
 * overlapping "vs now" sums.
 *
 * @returns {Map<string, object>} key `${conditionId}-${outcomeIndex}` →
 *   { h1, d1, w1, t1h: Map<trader,delta>, t1d: Map, t1w: Map }
 */
function buildWindowChangesMap(activity, heldOutcomes) {
  const now = Math.floor(Date.now() / 1000);
  const cutoff1h = now - 3600;
  const cutoff1d = now - 24 * 3600;
  const cutoff1w = now - 7 * 24 * 3600;
  const map = new Map();

  for (const a of activity) {
    const ts = a.timestamp || 0;
    if (ts < cutoff1w) continue;

    const trader = a.traderLabel || a.traderAddress?.slice(0, 10);

    for (const { outcomeIndex, delta } of eventOutcomeDeltas(a, heldOutcomes)) {
      const key = `${a.conditionId}-${outcomeIndex}`;

      let entry = map.get(key);
      if (!entry) {
        entry = { h1: 0, d1: 0, w1: 0, t1h: new Map(), t1d: new Map(), t1w: new Map() };
        map.set(key, entry);
      }

      if (ts >= cutoff1h) {
        entry.h1 += delta;
        entry.t1h.set(trader, (entry.t1h.get(trader) || 0) + delta);
      } else if (ts >= cutoff1d) {
        entry.d1 += delta;
        entry.t1d.set(trader, (entry.t1d.get(trader) || 0) + delta);
      } else {
        entry.w1 += delta;
        entry.t1w.set(trader, (entry.t1w.get(trader) || 0) + delta);
      }
    }
  }

  return map;
}

/**
 * Serialize one window-changes entry for JSON output: rounded sums plus
 * per-trader breakdowns sorted by |delta| (capped to keep the file small).
 */
function serializeWindowChanges(entry) {
  const MAX_DETAILS = 8;
  const round2 = v => Math.round(v * 100) / 100;
  const details = m => Array.from(m.entries())
    .map(([trader, change]) => ({ trader, change: round2(change) }))
    .sort((a, b) => Math.abs(b.change) - Math.abs(a.change))
    .slice(0, MAX_DETAILS);

  return {
    h1: round2(entry.h1),
    d1: round2(entry.d1),
    w1: round2(entry.w1),
    h1Details: details(entry.t1h),
    d1Details: details(entry.t1d),
    w1Details: details(entry.t1w)
  };
}

/**
 * Load previous aggregated portfolio for trader-count comparison.
 * Reads the pre-rotation copy in dataDir (the previous run's live output).
 */
function loadPreviousPortfolio(dataDir) {
  const prevPath = path.join(dataDir, 'aggregated_portfolio.json');
  try {
    if (fs.existsSync(prevPath)) {
      const data = JSON.parse(fs.readFileSync(prevPath, 'utf-8'));
      // Build map of position key -> trader count
      const traderCountMap = new Map();
      if (data.positions) {
        for (const pos of data.positions) {
          const key = `${pos.conditionId}-${pos.outcomeIndex}`;
          traderCountMap.set(key, pos.traderCount || 0);
        }
      }
      return traderCountMap;
    }
  } catch (e) {
    console.warn('Could not load previous portfolio:', e.message);
  }
  return new Map();
}

/**
 * Aggregate positions across all traders
 */
export function aggregatePortfolios(traderPortfolios, config, activity = [], dataDir = null, heldOutcomes = null) {
  // Load previous trader counts for comparison
  const prevTraderCounts = dataDir ? loadPreviousPortfolio(dataDir) : new Map();

  // Build 24h change map from activity
  const change24hMap = build24hChangeMap(activity, heldOutcomes);

  // Chained per-window aggregates (Δ1h / Δ1h→1d / Δ1d→1w) from full activity
  const windowChangesMap = buildWindowChangesMap(activity, heldOutcomes);

  // Map: conditionId-outcome -> aggregated data
  const aggregated = new Map();

  for (const [address, portfolio] of Object.entries(traderPortfolios)) {
    if (!portfolio.positions || !portfolio.fetchSuccess) continue;

    for (const pos of portfolio.positions) {
      // Determine outcome index and string
      let outcomeIndex = pos.outcomeIndex;
      let outcomeStr = pos.outcome;

      if (outcomeIndex === undefined) {
        outcomeIndex = outcomeStr === 'Yes' ? 1 : 0;
      }
      if (!outcomeStr) {
        outcomeStr = outcomeIndex === 0 ? 'No' : 'Yes';
      }

      const key = `${pos.conditionId}-${outcomeIndex}`;

      if (!aggregated.has(key)) {
        aggregated.set(key, {
          conditionId: pos.conditionId,
          title: pos.title || 'Unknown Market',
          slug: pos.slug || '',
          icon: pos.icon || '',
          eventSlug: pos.eventSlug || '',
          endDate: pos.endDate || null,
          outcome: outcomeStr,
          outcomeIndex: outcomeIndex,
          traders: [],
          totalExposure: 0,
          positions: [],
          weightedAvgPriceSum: 0,
          weightedAvgSize: 0,
          totalSize: 0,
          curPrice: 0
        });
      }

      const agg = aggregated.get(key);
      const exposure = computeExposure(pos);
      const avgPrice = parseFloat(pos.avgPrice || 0);
      const curPrice = parseFloat(pos.curPrice || 0);
      const size = parseFloat(pos.size || 0);

      agg.traders.push({
        address,
        label: traderPortfolios[address].label,
        exposure,
        size,
        avgPrice,
        curPrice
      });
      agg.totalExposure += exposure;
      agg.positions.push(pos);

      // Track share count and weighted average entry price.
      // Guard against NaN sizes/prices so one bad position can't poison the sums.
      if (Number.isFinite(size) && size > 0) {
        agg.totalSize += size;
        if (size > 0 && Number.isFinite(avgPrice) && avgPrice > 0) {
          agg.weightedAvgPriceSum += avgPrice * size;
          agg.weightedAvgSize += size;
        }
      }
      // Use the most recent curPrice
      if (Number.isFinite(curPrice) && curPrice > 0) {
        agg.curPrice = curPrice;
      }
    }
  }

  // Convert to array with 24h changes and price data
  const positions = Array.from(aggregated.entries()).map(([key, agg]) => {
    // Calculate weighted average entry price
    const avgEntry = agg.weightedAvgSize > 0
      ? agg.weightedAvgPriceSum / agg.weightedAvgSize
      : 0;
    const curPrice = agg.curPrice;

    // Calculate price change percentage from entry
    let priceChangePct = 0;
    if (avgEntry > 0 && curPrice > 0) {
      priceChangePct = ((curPrice - avgEntry) / avgEntry) * 100;
    }

    // Calculate trader count change vs previous snapshot
    const currentTraderCount = agg.traders.length;
    const prevTraderCount = prevTraderCounts.get(key) || 0;
    const traderCountChange = prevTraderCount > 0 ? currentTraderCount - prevTraderCount : 0;

    return {
      conditionId: agg.conditionId,
      title: agg.title,
      slug: agg.slug,
      icon: agg.icon,
      eventSlug: agg.eventSlug,
      endDate: agg.endDate,
      outcome: agg.outcome,
      outcomeIndex: agg.outcomeIndex,
      traderCount: currentTraderCount,
      traderCountChange: traderCountChange,
      traders: agg.traders,
      totalExposure: agg.totalExposure,
      totalSize: Math.round(agg.totalSize * 10000) / 10000,
      change24h: Math.round((change24hMap.get(key) || 0) * 100) / 100,
      avgEntry: Math.round(avgEntry * 100) / 100,
      curPrice: Math.round(curPrice * 100) / 100,
      priceChangePct: Math.round(priceChangePct * 10) / 10,
      // Sparse: only positions with activity in the last week carry this.
      ...(windowChangesMap.has(key)
        ? { windowChanges: serializeWindowChanges(windowChangesMap.get(key)) }
        : {})
    };
  });

  // Sort by total exposure descending
  positions.sort((a, b) => b.totalExposure - a.totalExposure);

  // Compute summary stats
  const totalExposure = positions.reduce((sum, p) => sum + p.totalExposure, 0);
  const totalSize = positions.reduce((sum, p) => sum + (p.totalSize || 0), 0);
  const distinctMarkets = new Set(positions.map(p => p.conditionId)).size;

  // Calculate total USDC balance across all traders
  const totalUsdcBalance = Object.values(traderPortfolios)
    .filter(p => p.fetchSuccess)
    .reduce((sum, p) => sum + (p.usdcBalance || 0), 0);

  // Total capital = exposure + USDC balance
  const totalCapital = totalExposure + totalUsdcBalance;

  // Relative exposure = exposure / (exposure + USDC) × 100
  const relativeExposure = totalCapital > 0 ? (totalExposure / totalCapital) * 100 : 0;

  // Concentration metrics
  let top1Share = 0;
  let top5Share = 0;
  if (totalExposure > 0) {
    top1Share = positions.length > 0 ? positions[0].totalExposure / totalExposure : 0;
    top5Share = positions.slice(0, 5).reduce((sum, p) => sum + p.totalExposure, 0) / totalExposure;
  }

  return {
    positions: positions.filter(p => p.totalExposure >= (config.min_usd_filter || 0)),
    summary: {
      totalExposure,
      totalSize: Math.round(totalSize * 10000) / 10000,
      totalCapital: Math.round(totalCapital * 100) / 100,
      relativeExposure: Math.round(relativeExposure * 100) / 100,
      distinctMarkets,
      top1Share: Math.round(top1Share * 100) / 100,
      top5Share: Math.round(top5Share * 100) / 100,
      netFlow24h: 0 // Will be computed from activity
    }
  };
}

/**
 * Process activity into recent changes format
 */
const NON_TRADE_ACTIONS = {
  REDEEM: 'redeemed',
  MERGE: 'merged',
  SPLIT: 'split',
  CONVERSION: 'converted',
  CONVERT: 'converted'
};

export function processRecentChanges(activity, traderPortfolios, maxEvents = 2000, heldOutcomes = null) {
  const now = Math.floor(Date.now() / 1000);
  const windows = {
    '1h': now - 3600,
    '6h': now - 6 * 3600,
    '24h': now - 24 * 3600,
    '7d': now - 7 * 24 * 3600,
    '30d': now - 30 * 24 * 3600
  };

  const windowSummaries = { '1h': 0, '6h': 0, '24h': 0, '7d': 0, '30d': 0 };

  const changes = activity
    .flatMap(a => {
      const ts = a.timestamp || 0;
      const isTrade = !a.type || a.type === 'TRADE';

      return eventOutcomeDeltas(a, heldOutcomes).map(({ outcomeIndex, delta }) => {
        // Update window summaries
        for (const [window, threshold] of Object.entries(windows)) {
          if (ts >= threshold) {
            windowSummaries[window] += delta;
          }
        }

        // Determine action type
        let action = 'unknown';
        if (isTrade) {
          if (a.side === 'BUY') action = 'increased';
          else if (a.side === 'SELL') action = 'decreased';
        } else {
          action = NON_TRADE_ACTIONS[a.type] || 'unknown';
        }

        // Determine outcome name. Non-trade events report outcome '' /
        // index 999 — leave the name to the event when it has one.
        let outcome = a.outcome || '';
        if (!outcome && isTrade && a.outcomeIndex !== undefined) {
          outcome = a.outcomeIndex === 0 ? 'No' : 'Yes';
        }

        return {
          timestamp: ts,
          trader: a.traderLabel || a.proxyWallet?.slice(0, 10),
          traderAddress: a.traderAddress || a.proxyWallet,
          market: a.title || 'Unknown Market',
          marketSlug: a.slug || '',
          eventSlug: a.eventSlug || '',
          conditionId: a.conditionId || '',
          outcome: outcome,
          outcomeIndex,
          action,
          delta: Math.round(delta * 100) / 100,
          size: parseFloat(a.size || 0),
          price: parseFloat(a.price || 0)
        };
      });
    });

  // Round summaries
  for (const key of Object.keys(windowSummaries)) {
    windowSummaries[key] = Math.round(windowSummaries[key] * 100) / 100;
  }

  // Cap the OUTPUT feed only — windowSummaries above already saw the full
  // activity set (activity arrives sorted by timestamp descending).
  return { changes: changes.slice(0, maxEvents), windowSummaries };
}

/**
 * Main computation function.
 *
 * @param {object} args
 * @param {object} args.config - Parsed config/<site>.json
 * @param {string} args.dataDir - out/<site>/data; holds the previous run's files
 * @param {string} args.rootDir - Repo root; traders csv at config.traders_csv
 * @returns {Promise<{metadata: object, aggregatedPortfolio: object, traderPortfolios: object, recentChanges: object}>}
 */
export async function computeAll({ config, dataDir, rootDir }) {
  const traders = loadTraders(rootDir, config);

  console.log(`Loaded ${traders.length} traders from CSV`);
  console.log('Config:', config);

  // Fetch all data
  const [{ traderPortfolios, pnlStats }, activity] = await Promise.all([
    fetchAllPortfolios(traders, config, dataDir),
    fetchRecentActivity(traders, config)
  ]);

  // Attribution index for REDEEM/MERGE/CONVERSION events (outcomeIndex 999):
  // current + previous run portfolios tell us which side each trader held.
  const heldOutcomes = buildHeldOutcomesIndex(
    traderPortfolios,
    loadPreviousTraderPortfolios(dataDir)
  );

  // Aggregate - pass activity for 24h change calculation
  const aggregatedPortfolio = aggregatePortfolios(traderPortfolios, config, activity, dataDir, heldOutcomes);
  const recentChanges = processRecentChanges(activity, traderPortfolios, config.max_recent_events || 2000, heldOutcomes);

  // Update 24h flow in summary
  aggregatedPortfolio.summary.netFlow24h = recentChanges.windowSummaries['24h'];

  // Generate metadata
  const metadata = {
    last_updated: new Date().toISOString(),
    trader_count: traders.length,
    traders_fetched: Object.values(traderPortfolios).filter(p => p.fetchSuccess).length,
    market_count: aggregatedPortfolio.summary.distinctMarkets,
    total_exposure: aggregatedPortfolio.summary.totalExposure,
    activity_count: activity.length,
    pnl_scrape: pnlStats,
    // Embed trader list for frontend live polling
    traders: traders.map(t => ({ address: t.address.toLowerCase(), label: t.label }))
  };

  return { metadata, aggregatedPortfolio, traderPortfolios, recentChanges };
}

export default {
  loadTraders,
  fetchAllPortfolios,
  fetchRecentActivity,
  aggregatePortfolios,
  processRecentChanges,
  computeAll
};
