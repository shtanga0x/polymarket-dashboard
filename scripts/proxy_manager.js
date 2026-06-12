/**
 * Proxy manager for rotating datacenter proxies
 *
 * Proxies are loaded from the PROXIES environment variable (GitHub Secret).
 * Format per line: host:port:username:password
 *
 * Usage:
 *   import { getProxyAgent, proxyCount } from './proxy_manager.js';
 *   const agent = getProxyAgent(); // returns undici ProxyAgent or null
 */

import { ProxyAgent } from 'undici';

let proxies = [];
let currentIndex = 0;

/**
 * Parse proxy list from env string
 * Format: "host:port:user:pass" one per line
 */
function parseProxyList(raw) {
  return raw
    .split('\n')
    .map(line => line.trim())
    .filter(line => line && line.split(':').length >= 4)
    .map(line => {
      const parts = line.split(':');
      const host = parts[0];
      const port = parts[1];
      const user = parts[2];
      const pass = parts.slice(3).join(':'); // Handle passwords with colons
      return { url: `http://${user}:${pass}@${host}:${port}`, host, port };
    });
}

// Load proxies from environment variable on module import
if (process.env.PROXIES) {
  proxies = parseProxyList(process.env.PROXIES);
  console.log(`Loaded ${proxies.length} proxies for rotation`);
} else {
  console.log('No PROXIES env var found — making direct API calls (no proxy)');
}

/**
 * Total number of configured proxies
 */
export const proxyCount = proxies.length;

/**
 * Get the next proxy agent in round-robin rotation.
 * Returns null if no proxies are configured.
 */
export function getProxyAgent() {
  if (proxies.length === 0) return null;
  const proxy = proxies[currentIndex % proxies.length];
  currentIndex++;
  return new ProxyAgent(proxy.url);
}

/**
 * Get a random proxy agent.
 * Returns null if no proxies are configured.
 */
export function getRandomProxyAgent() {
  if (proxies.length === 0) return null;
  const proxy = proxies[Math.floor(Math.random() * proxies.length)];
  return new ProxyAgent(proxy.url);
}

export default { getProxyAgent, getRandomProxyAgent, proxyCount };
