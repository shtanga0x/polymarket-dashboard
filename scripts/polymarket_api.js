/**
 * Polymarket API wrapper with retry logic, rate limiting, and proxy rotation.
 *
 * Proxy support is optional: getProxyAgent() returns null when the PROXIES
 * env var is unset, in which case all requests are made directly.
 */

import { getProxyAgent } from './proxy_manager.js';

const DATA_API_BASE = 'https://data-api.polymarket.com';
const LEADERBOARD_API_BASE = 'https://lb-api.polymarket.com';
const POLYGON_RPC = 'https://polygon-bor-rpc.publicnode.com';

// Collateral token contracts on Polygon
const USDC_NATIVE = '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359'; // Native USDC
const USDC_BRIDGED = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'; // USDC.e (bridged)
const PUSD = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB'; // pUSD (Polymarket v2 collateral, 6 decimals)

/**
 * Sleep helper
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * HTTP error carrying the response status code so callers can
 * distinguish e.g. 404 (no data for wallet) from real failures.
 */
export class HttpError extends Error {
  constructor(status, statusText, url) {
    super(`HTTP ${status}: ${statusText} (${url})`);
    this.name = 'HttpError';
    this.status = status;
  }
}

/**
 * Fetch with retry, exponential backoff, and proxy rotation.
 * A fresh proxy agent is picked for each attempt (round-robin); when no
 * proxies are configured the request is made directly.
 *
 * @param {string} url - URL to fetch
 * @param {object} options - fetch() options
 * @param {object} config - Config object (retry_attempts, retry_base_delay_ms)
 * @returns {Promise<any>} Parsed JSON response
 * @throws {HttpError} On non-retryable HTTP errors (e.g. 404)
 */
async function fetchWithRetry(url, options = {}, config = {}) {
  const maxRetries = config.retry_attempts || 3;
  const baseDelay = config.retry_base_delay_ms || 1000;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    // Get a fresh proxy agent each attempt (round-robin rotation)
    const agent = getProxyAgent();
    const fetchOptions = {
      ...options,
      headers: { 'Accept': 'application/json', ...options.headers }
    };
    if (agent) {
      fetchOptions.dispatcher = agent;
    }

    try {
      const response = await fetch(url, fetchOptions);

      if (response.status === 429) {
        // Rate limited - wait and retry
        const delay = baseDelay * Math.pow(2, attempt);
        console.warn(`Rate limited on ${url}, waiting ${delay}ms before retry...`);
        await sleep(delay);
        continue;
      }

      if (response.status >= 500) {
        // Server error - retry
        const delay = baseDelay * Math.pow(2, attempt);
        console.warn(`Server error ${response.status}, waiting ${delay}ms before retry...`);
        await sleep(delay);
        continue;
      }

      if (response.status === 403) {
        // Cloudflare blocks individual (datacenter) IPs with 403 — retrying
        // gets a freshly rotated proxy agent, so treat it as transient.
        const delay = baseDelay * Math.pow(2, attempt);
        console.warn(`Blocked (403) on ${url}, rotating proxy and retrying in ${delay}ms...`);
        await sleep(delay);
        continue;
      }

      if (!response.ok) {
        throw new HttpError(response.status, response.statusText, url);
      }

      return await response.json();
    } catch (error) {
      // Client errors (4xx) are not transient - don't retry them
      if (error instanceof HttpError) throw error;
      if (attempt === maxRetries - 1) throw error;
      const delay = baseDelay * Math.pow(2, attempt);
      console.warn(`Request failed (${url}): ${error.message}, retrying in ${delay}ms...`);
      await sleep(delay);
    }
  }

  throw new Error(`Request failed after ${maxRetries} attempts: ${url}`);
}

/**
 * Fetch current positions for a wallet
 * @param {string} address - Wallet address (0x...)
 * @param {number} limit - Max results (default 1000)
 * @param {object} config - Config object
 * @returns {Promise<Array>} Array of position objects
 */
export async function fetchWalletPositions(address, limit = 1000, config = {}) {
  const url = `${DATA_API_BASE}/positions?user=${address.toLowerCase()}&limit=${limit}`;
  const data = await fetchWithRetry(url, {}, config);
  return data || [];
}

/**
 * Fetch wallet activity/trades
 * @param {string} address - Wallet address
 * @param {number} since - Unix timestamp to fetch activity since (optional)
 * @param {number} limit - Max results per page
 * @param {object} config - Config object
 * @returns {Promise<Array>} Array of activity objects
 */
export async function fetchWalletActivity(address, since = null, limit = 100, config = {}) {
  let url = `${DATA_API_BASE}/activity?user=${address.toLowerCase()}&limit=${limit}`;
  if (since) {
    url += `&start=${since}`;
  }
  const data = await fetchWithRetry(url, {}, config);
  return data || [];
}

/**
 * Fetch total portfolio value for a wallet
 * @param {string} address - Wallet address
 * @param {object} config - Config object
 * @returns {Promise<number>} Total value in USD
 */
export async function fetchWalletValue(address, config = {}) {
  const url = `${DATA_API_BASE}/value?user=${address.toLowerCase()}`;
  const data = await fetchWithRetry(url, {}, config);
  if (!data || data.length === 0) {
    return 0;
  }
  try {
    return parseFloat(data[0]?.value || 0);
  } catch {
    return 0;
  }
}

/**
 * Fetch trades for a wallet
 * @param {string} address - Wallet address
 * @param {number} limit - Max results
 * @param {object} config - Config object
 * @returns {Promise<Array>} Array of trade objects
 */
export async function fetchWalletTrades(address, limit = 500, config = {}) {
  const url = `${DATA_API_BASE}/trades?user=${address.toLowerCase()}&limit=${limit}`;
  const data = await fetchWithRetry(url, {}, config);
  return data || [];
}

/**
 * Fetch USDC balance from Polygon blockchain
 * (Polygon RPC is public — no proxy needed)
 * @param {string} address - Wallet address
 * @param {object} config - Config object
 * @returns {Promise<number>} USDC balance in USD
 */
export async function fetchUsdcBalance(address, config = {}) {
  const addr = address.toLowerCase().replace('0x', '');
  // balanceOf(address) function selector = 0x70a08231
  const data = '0x70a08231000000000000000000000000' + addr;

  async function getBalance(tokenContract) {
    try {
      const response = await fetch(POLYGON_RPC, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          jsonrpc: '2.0',
          method: 'eth_call',
          params: [{ to: tokenContract, data }, 'latest'],
          id: 1
        })
      });
      const result = await response.json();
      if (result.result && result.result !== '0x') {
        // USDC has 6 decimals
        return parseInt(result.result, 16) / 1e6;
      }
      return 0;
    } catch (error) {
      console.warn(`Failed to fetch USDC balance from ${tokenContract}: ${error.message}`);
      return 0;
    }
  }

  // Fetch native USDC, bridged USDC.e, and pUSD (Polymarket v2 collateral)
  const [nativeBalance, bridgedBalance, pusdBalance] = await Promise.all([
    getBalance(USDC_NATIVE),
    getBalance(USDC_BRIDGED),
    getBalance(PUSD)
  ]);

  return Math.round((nativeBalance + bridgedBalance + pusdBalance) * 100) / 100;
}

/**
 * Batch fetch with concurrency limit.
 *
 * Uses a worker pool: N workers each pull the next unprocessed address
 * (shared index) and await its fetch to completion before pulling another,
 * so at most `concurrency` requests are in flight and no polling is needed.
 *
 * @param {Array<string>} addresses - Array of wallet addresses
 * @param {Function} fetchFn - Function (address, config) to call for each address
 * @param {number} concurrency - Max concurrent requests (callers typically pass config.concurrency_limit)
 * @param {object} config - Config object
 * @returns {Promise<Map>} Map of address -> result:
 *   { success: true, data }                  on success
 *   { success: true, partial: true, data: [] } when the API returned 404 (no data for wallet)
 *   { success: false, error }                on failure
 */
export async function batchFetch(addresses, fetchFn, concurrency = 8, config = {}) {
  const results = new Map();
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < addresses.length) {
      const address = addresses[nextIndex++];
      try {
        const result = await fetchFn(address, config);
        results.set(address, { success: true, data: result });
      } catch (error) {
        if (error instanceof HttpError && error.status === 404) {
          // No data for this wallet - treat as empty rather than failing the trader
          console.warn(`No data for ${address} (HTTP 404), recording empty result`);
          results.set(address, { success: true, partial: true, data: [] });
        } else {
          console.error(`Failed to fetch for ${address}: ${error.message}`);
          results.set(address, { success: false, error: error.message });
        }
      }
    }
  }

  const workerCount = Math.max(1, Math.min(concurrency, addresses.length));
  await Promise.all(Array.from({ length: workerCount }, () => worker()));

  return results;
}

/**
 * Fetch all activity history for a wallet with pagination
 * @param {string} address - Wallet address
 * @param {object} config - Config object
 * @returns {Promise<Array>} All activity objects
 */
export async function fetchAllActivity(address, config = {}) {
  const allActivity = [];
  let endTimestamp = Math.floor(Date.now() / 1000);
  const maxIterations = config.activity_max_pages || 30;

  for (let i = 0; i < maxIterations; i++) {
    const url = `${DATA_API_BASE}/activity?user=${address.toLowerCase()}&limit=1000&end=${endTimestamp}`;

    try {
      const data = await fetchWithRetry(url, {}, config);
      if (!data || data.length === 0) break;

      allActivity.push(...data);

      // Get oldest timestamp for next page
      const timestamps = data.map(a => a.timestamp).filter(t => t && t > 0);
      if (timestamps.length === 0) break;

      const oldest = Math.min(...timestamps);
      if (oldest >= endTimestamp) break;

      endTimestamp = oldest - 1;

      if (data.length < 1000) break; // Last page
    } catch (error) {
      console.warn(`Failed to fetch activity page ${i + 1}: ${error.message}`);
      break;
    }
  }

  return allActivity;
}

/**
 * Calculate PnL from activity history and current positions
 * @param {Array} activity - All activity events
 * @param {Array} positions - Current open positions
 * @returns {object} PnL breakdown
 */
export function calculatePnLFromActivity(activity, positions) {
  let totalBuys = 0;
  let totalSells = 0;
  let totalRedemptions = 0;
  let totalYieldsRewards = 0;

  activity.forEach(a => {
    const type = a.type || 'TRADE';
    const usdc = parseFloat(a.usdcSize || a.size || 0);

    if (type === 'TRADE') {
      if (a.side === 'BUY') totalBuys += usdc;
      else if (a.side === 'SELL') totalSells += usdc;
    } else if (type === 'REDEEM') {
      totalRedemptions += usdc;
    } else if (type === 'YIELD' || type === 'REWARD') {
      totalYieldsRewards += usdc;
    }
  });

  // Current position value
  let currentValue = 0;
  let unrealizedPnL = 0;
  positions.forEach(p => {
    currentValue += parseFloat(p.currentValue || 0);
    unrealizedPnL += parseFloat(p.cashPnl || 0);
  });

  // Total PnL = Money Out - Money In
  const moneyOut = totalSells + totalRedemptions + totalYieldsRewards + currentValue;
  const moneyIn = totalBuys;
  const totalPnL = moneyOut - moneyIn;

  return {
    totalPnL: Math.round(totalPnL * 100) / 100,
    unrealizedPnL: Math.round(unrealizedPnL * 100) / 100,
    realizedPnL: Math.round((totalPnL - unrealizedPnL) * 100) / 100,
    totalBuys,
    totalSells,
    totalRedemptions,
    totalYieldsRewards,
    currentValue
  };
}

/**
 * Fetch profit leaderboard (contains all-time PnL)
 * @param {number} limit - Max results (default 5000)
 * @param {object} config - Config object
 * @returns {Promise<Map>} Map of address -> profit amount
 */
export async function fetchProfitLeaderboard(limit = 5000, config = {}) {
  const url = `${LEADERBOARD_API_BASE}/profit?window=all&limit=${limit}`;
  try {
    const data = await fetchWithRetry(url, {}, config);
    const profitMap = new Map();
    for (const user of data) {
      profitMap.set(user.proxyWallet.toLowerCase(), user.amount);
    }
    return profitMap;
  } catch (error) {
    console.warn(`Failed to fetch leaderboard: ${error.message}`);
    return new Map();
  }
}

export default {
  fetchWalletPositions,
  fetchWalletActivity,
  fetchWalletValue,
  fetchWalletTrades,
  fetchUsdcBalance,
  fetchAllActivity,
  calculatePnLFromActivity,
  fetchProfitLeaderboard,
  batchFetch,
  HttpError
};
