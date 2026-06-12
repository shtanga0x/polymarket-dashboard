/**
 * Cloudflare Worker — GitHub Actions trigger proxy
 *
 * Dispatches the unified polymarket-dashboard repo. One workflow run updates
 * BOTH sites (core + watch) as parallel matrix jobs.
 *
 * Requires:
 *   - Secret: GITHUB_PAT  (fine-grained PAT with Actions: write on polymarket-dashboard)
 *
 * Endpoints:
 *   POST /trigger-update            → dispatches a refresh (both sites)
 *   POST /trigger-update?repo=...   → legacy param accepted, same effect
 *
 * Cron:
 *   Every 2 minutes. The full run completes well under that with the PnL
 *   cache warm, so the Actions concurrency queue stays empty (no cancelled
 *   runs piling up like the old 1-minute cadence caused).
 */

const COOLDOWN_MS = 60 * 1000; // 1 minute, for manual triggers

const REPO = 'shtanga0x/polymarket-dashboard';

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(triggerRepo(env, { rateLimit: false }));
  },

  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return corsResponse('', 204);
    }

    if (url.pathname === '/trigger-update' && request.method === 'POST') {
      const result = await triggerRepo(env, { rateLimit: true });
      return corsResponse(JSON.stringify(result), result.httpStatus);
    }

    return corsResponse(JSON.stringify({ error: 'Not found' }), 404);
  },
};

async function triggerRepo(env, { rateLimit } = { rateLimit: false }) {
  const cacheKey = new Request('https://pmw-trigger.local/rate-limit/dashboard');
  if (rateLimit) {
    const cached = await caches.default.match(cacheKey);
    if (cached) {
      return {
        status: 'rate_limited',
        cooldown_remaining_sec: COOLDOWN_MS / 1000,
        httpStatus: 429,
      };
    }
  }

  const ghRes = await fetch(
    `https://api.github.com/repos/${REPO}/dispatches`,
    {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${env.GITHUB_PAT}`,
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/json',
        'User-Agent': 'polymarket-dashboard-worker/1.0',
      },
      body: JSON.stringify({ event_type: 'manual-refresh' }),
    }
  );

  if (!ghRes.ok) {
    const detail = await ghRes.text();
    console.error('GitHub dispatch failed:', ghRes.status, detail);
    return { status: 'error', detail, httpStatus: 502 };
  }

  if (rateLimit) {
    await caches.default.put(
      cacheKey,
      new Response('1', { headers: { 'Cache-Control': `max-age=${COOLDOWN_MS / 1000}` } }),
    );
  }
  return { status: 'triggered', httpStatus: 200 };
}

function corsResponse(body, status) {
  return new Response(body, {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    },
  });
}
