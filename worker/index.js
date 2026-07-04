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
 *   Every minute, ADAPTIVE: before dispatching, the tick asks GitHub whether an
 *   update-data run is already queued or in progress and skips if so. Healthy
 *   runs (~40-60s with proxies) → fresh data every minute; a slow/hung run
 *   (proxy flakiness tail, 2-5 min) → the next tick backs off instead of
 *   queueing behind the per-site concurrency group (the old 1-min mass-cancel
 *   email storm). Requires the PAT to have Actions on the repo (write covers
 *   the read used here).
 */

const COOLDOWN_MS = 60 * 1000; // 1 minute, for manual triggers

const REPO = 'shtanga0x/polymarket-dashboard';

// True if an update-data run is already queued or executing. On a GitHub API
// blip, report NOT busy — dispatching anyway is the old (safe) behaviour.
async function updateRunActive(env) {
  for (const status of ['queued', 'in_progress']) {
    const r = await fetch(
      `https://api.github.com/repos/${REPO}/actions/workflows/update-data.yml/runs?status=${status}&per_page=1`,
      {
        headers: {
          'Authorization': `Bearer ${env.GITHUB_PAT}`,
          'Accept': 'application/vnd.github+json',
          'User-Agent': 'polymarket-dashboard-worker/1.0',
        },
      },
    );
    if (!r.ok) { console.warn('run-status check failed:', r.status); return false; }
    const j = await r.json();
    if ((j.total_count ?? 0) > 0) return true;
  }
  return false;
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      if (await updateRunActive(env)) {
        console.log('skip dispatch: update-data run already queued/in progress');
        return;
      }
      await triggerRepo(env, { rateLimit: false });
    })());
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
