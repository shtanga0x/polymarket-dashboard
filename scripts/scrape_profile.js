/**
 * Scrape Polymarket profile page to get accurate PnL data
 * Uses proxy rotation via proxy_manager.js (optional — direct fetch
 * when the PROXIES env var is unset).
 */

import { getProxyAgent } from './proxy_manager.js';

/**
 * Sleep helper
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Fetch profile page and extract PnL data
 * @param {string} addressOrUsername - Wallet address or username
 * @returns {Promise<{amount: number, pnl: number, realized: number, unrealized: number, proxyWallet: string|null}|null>}
 */
export async function scrapeProfilePnL(addressOrUsername) {
  // Try username first (if it looks like a username), otherwise use address
  const isAddress = addressOrUsername.startsWith('0x');
  const url = isAddress
    ? `https://polymarket.com/profile/${addressOrUsername}`
    : `https://polymarket.com/@${addressOrUsername}`;

  try {
    const agent = getProxyAgent();
    const fetchOptions = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml'
      }
    };
    if (agent) fetchOptions.dispatcher = agent;

    const response = await fetch(url, fetchOptions);

    if (!response.ok) {
      console.warn(`Failed to fetch profile ${addressOrUsername}: ${response.status}`);
      return null;
    }

    const html = await response.text();

    // Extract __NEXT_DATA__ JSON
    const match = html.match(/__NEXT_DATA__[^>]*>({.*?})<\/script>/);
    if (!match) {
      console.warn(`Could not find __NEXT_DATA__ for ${addressOrUsername}`);
      return null;
    }

    const data = JSON.parse(match[1]);
    const queries = data.props?.pageProps?.dehydratedState?.queries || [];
    const expectedAddress = isAddress ? addressOrUsername.toLowerCase() : null;
    const pnlHits = [];

    // Find the volume query which contains PnL
    for (const q of queries) {
      const d = q.state?.data;
      if (d && typeof d.pnl === 'number' && typeof d.amount === 'number') {
        const result = {
          amount: d.amount,      // Trading volume
          pnl: d.pnl,            // Profit/Loss
          realized: d.realized || 0,
          unrealized: d.unrealized || 0,
          proxyWallet: d.proxyWallet || null
        };
        const proxyWallet = typeof d.proxyWallet === 'string' ? d.proxyWallet.toLowerCase() : '';
        const queryMatches = Array.isArray(q.queryKey)
          && q.queryKey.some(part => typeof part === 'string' && part.toLowerCase() === expectedAddress);
        if (expectedAddress && (proxyWallet === expectedAddress || queryMatches)) return result;
        pnlHits.push(result);
      }
    }

    if (!expectedAddress && pnlHits.length > 0) return pnlHits[0];

    console.warn(`Could not find PnL data for ${addressOrUsername}`);
    return null;
  } catch (error) {
    console.warn(`Error scraping profile ${addressOrUsername}: ${error.message}`);
    return null;
  }
}

/**
 * Scrape one trader, trying the address first and falling back to the
 * username label (or vice versa when the label is the primary identifier).
 * @param {{address: string, label?: string}} trader
 * @returns {Promise<object|null>} PnL data or null
 */
async function scrapeTraderPnL(trader) {
  const data = await scrapeProfilePnL(trader.address);
  if (data) return data;
  if (trader.label && trader.label !== trader.address) {
    return await scrapeProfilePnL(trader.label);
  }
  return null;
}

/**
 * Batch scrape multiple profiles serially with rate limiting
 * @param {Array<{address: string, label: string}>} traders
 * @param {number} delayMs - Delay between requests
 * @returns {Promise<Map<string, {pnl: number, amount: number}>>}
 */
export async function batchScrapeProfiles(traders, delayMs = 1000) {
  const results = new Map();

  for (const trader of traders) {
    // Try username first if available, otherwise use address
    const identifier = trader.label || trader.address;
    console.log(`Scraping profile for ${identifier}...`);

    const data = await scrapeProfilePnL(identifier);

    if (data) {
      results.set(trader.address.toLowerCase(), data);
    } else {
      // Fallback: try with address if username failed
      if (identifier !== trader.address) {
        const dataByAddr = await scrapeProfilePnL(trader.address);
        if (dataByAddr) {
          results.set(trader.address.toLowerCase(), dataByAddr);
        }
      }
    }

    // Rate limit
    await sleep(delayMs);
  }

  return results;
}

/**
 * Batch scrape multiple profiles in parallel with a concurrency-limited
 * worker pool.
 *
 * N workers each pull the next unscraped trader (shared index) and await
 * it to completion before pulling another, with ~100ms of random jitter
 * before each request start so workers don't fire in lockstep.
 *
 * @param {Array<{address: string, label?: string}>} traders
 * @param {number} concurrency - Max concurrent scrapes (callers typically pass config.scrape_concurrency)
 * @returns {Promise<Map<string, {pnl: number, amount: number}>>} Map of lowercase address -> PnL data (missing entries failed)
 */
export async function batchScrapeProfilesParallel(traders, concurrency = 4) {
  const results = new Map();
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < traders.length) {
      const trader = traders[nextIndex++];
      try {
        // ~100ms jitter between request starts
        await sleep(50 + Math.random() * 100);
        const data = await scrapeTraderPnL(trader);
        if (data) {
          results.set(trader.address.toLowerCase(), data);
        }
      } catch (e) {
        // A malformed trader entry must not kill the whole batch.
        console.warn(`Failed to scrape ${trader?.address ?? JSON.stringify(trader)}: ${e.message}`);
      }
    }
  }

  const workerCount = Math.max(1, Math.min(concurrency, traders.length));
  await Promise.all(Array.from({ length: workerCount }, () => worker()));

  return results;
}

export default {
  scrapeProfilePnL,
  batchScrapeProfiles,
  batchScrapeProfilesParallel
};
