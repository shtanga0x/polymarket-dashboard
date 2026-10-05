/**
 * pm-share-history — per-trader share-count history for the core/watch boards.
 *
 * WHAT IT TRACKS
 *   A market (conditionId) enters tracking the first time one of its outcomes
 *   is in a site's top 200 portfolio rows (the exposure-sorted
 *   aggregated_portfolio positions — the same ranking as the alert bot's #N).
 *   From then on every outcome of that market is followed for every tracked
 *   trader, even after it drops out of the top 200, until the market is
 *   resolved (a holder's position turns `redeemable`) or Gamma reports it
 *   `closed`. Closed markets stay in D1 as the archive; they are never
 *   re-tracked.
 *
 * HOW (cost model)
 *   No extra Polymarket fetches and no R2 writes: each cron tick reads the
 *   site's metadata.json pointer and, when it points at a NEW snapshot, that
 *   snapshot's aggregated_portfolio.json (2 R2 class-B reads). It diffs the
 *   per-trader sizes against the `current` table and writes a `holdings` row
 *   only when a size CHANGED — so storage and D1 writes scale with trading
 *   activity, not with the sample rate. The sample rate is therefore the data
 *   pipeline's own cadence (~90s average); a faster cron would see nothing new.
 *
 * READ API (no public route — reached via the polymarket-site-router service
 * binding, behind the member gate):
 *   GET /history?site=core&cid=0x…&range=1d|1w|1m|max
 *   GET /markets?site=core&status=active|closed     (archive listing)
 *   GET /status
 *   GET /holders-pnl?cid=0x…&n=10|20|50   top-n holders per outcome (Polymarket-wide, not
 *                              just tracked traders) with each wallet's
 *                              ACCOUNT-WIDE PnL over 24h / 7d / 30d / all
 */

const SITES = ['core', 'watch'];
const TOP_N = 200;
const GAMMA = 'https://gamma-api.polymarket.com';
const GAMMA_EVERY_S = 15 * 60;   // closed-market check cadence per site
const GAMMA_CHUNK = 50;          // conditionIds per Gamma request
const MIN_FETCH_RATIO = 0.5;     // ignore a snapshot where most trader fetches failed

const RANGES = { '1d': 86400, '1w': 7 * 86400, '1m': 30 * 86400, max: null };

const round2 = (v) => Math.round(v * 100) / 100;

// ─── Ingest ──────────────────────────────────────────────────────────────────

async function readJSON(bucket, key) {
  const obj = await bucket.get(key);
  return obj ? obj.json() : null;
}

/** Multi-row INSERT statements, chunked under D1's 100-bound-parameter cap. */
function multiInsert(db, head, tail, rows) {
  if (!rows.length) return [];
  const cols = rows[0].length;
  const per = Math.floor(100 / cols);
  const ph = '(' + Array(cols).fill('?').join(',') + ')';
  const out = [];
  for (let i = 0; i < rows.length; i += per) {
    const chunk = rows.slice(i, i + per);
    out.push(db.prepare(`${head} VALUES ${chunk.map(() => ph).join(',')} ${tail}`).bind(...chunk.flat()));
  }
  return out;
}

/** cid → { outcomes: Map(oi → Map(addr → size)), info } from one snapshot. */
function indexSnapshot(agg) {
  const markets = new Map();
  const entry = (cid) => {
    let m = markets.get(cid);
    if (!m) { m = { outcomes: new Map(), info: null, names: {}, redeemable: false }; markets.set(cid, m); }
    return m;
  };
  for (const p of agg.positions || []) {
    if (!p.conditionId) continue;
    const m = entry(p.conditionId);
    m.info ??= { title: p.title || '', slug: p.slug || '', eventSlug: p.eventSlug || '', endDate: p.endDate || null };
    m.names[p.outcomeIndex] = p.outcome || '';
    if (p.redeemable) m.redeemable = true;
    const holders = new Map();
    for (const t of p.traders || []) holders.set(t.address.toLowerCase(), t.size || 0);
    m.outcomes.set(p.outcomeIndex, holders);
  }
  for (const [cid, oi, list] of agg.holdingsTail || []) {
    const m = entry(cid);
    m.outcomes.set(oi, new Map(list.map(([a, s]) => [a.toLowerCase(), s || 0])));
  }
  return markets;
}

async function gammaClosed(cids) {
  const closed = new Set();
  for (let i = 0; i < cids.length; i += GAMMA_CHUNK) {
    const q = cids.slice(i, i + GAMMA_CHUNK).map((c) => 'condition_ids=' + c).join('&');
    const r = await fetch(`${GAMMA}/markets?closed=true&limit=${GAMMA_CHUNK * 2}&${q}`, {
      headers: { 'User-Agent': 'pm-share-history/1.0' },
      signal: AbortSignal.timeout(15_000),
    });
    if (!r.ok) throw new Error(`gamma ${r.status}`);
    for (const m of await r.json()) if (m.closed && m.conditionId) closed.add(m.conditionId);
  }
  return closed;
}

async function ingestSite(env, site) {
  const db = env.DB;
  const cursor = await db.prepare('SELECT * FROM cursors WHERE site = ?').bind(site).first();
  const meta = await readJSON(env.DATA_BUCKET, `${site}/metadata.json`);
  if (!meta?.last_updated) return { site, skipped: 'no_metadata' };

  const snap = meta.snapshot || meta.last_updated;
  const ts = Math.floor(Date.parse(meta.last_updated) / 1000);
  const nowS = Math.floor(Date.now() / 1000);
  const gammaDue = !cursor?.gamma_ts || nowS - cursor.gamma_ts >= GAMMA_EVERY_S;
  const fresh = cursor?.snapshot !== snap && !(cursor?.ts >= ts);
  if (!fresh && !gammaDue) return { site, skipped: 'same_snapshot' };

  const stmts = [];
  let changes = 0, added = 0, closedN = 0;
  let gammaTs = cursor?.gamma_ts ?? null;

  if (fresh) {
    const agg = (meta.snapshot && await readJSON(env.DATA_BUCKET, `${site}/${meta.snapshot}/aggregated_portfolio.json`))
      || await readJSON(env.DATA_BUCKET, `${site}/aggregated_portfolio.json`);
    const healthy = agg && Array.isArray(agg.failedTraders)
      && (meta.traders_fetched || 0) >= (meta.trader_count || 0) * MIN_FETCH_RATIO;
    if (!healthy) {
      // Old-format snapshot (pipeline not yet emitting failedTraders /
      // holdingsTail) or a mostly-failed fetch: a diff against it would record
      // phantom exits. Skip it but advance the cursor.
      console.warn(`${site}: skip snapshot ${snap} (${agg ? 'degraded/old format' : 'missing aggregate'})`);
    } else {
      const res = await diffSnapshot(env, site, agg, ts, stmts);
      changes = res.changes; added = res.added; closedN += res.closed;
    }
  }

  if (gammaDue) {
    try {
      const active = (await db.prepare(`SELECT id, cid FROM markets WHERE site = ? AND status = 'active'`)
        .bind(site).all()).results;
      const closed = await gammaClosed(active.map((m) => m.cid));
      for (const m of active) {
        if (!closed.has(m.cid)) continue;
        stmts.push(db.prepare(`UPDATE markets SET status = 'closed', close_reason = 'closed', closed_ts = ? WHERE id = ? AND status = 'active'`).bind(fresh ? ts : nowS, m.id));
        stmts.push(db.prepare('DELETE FROM current WHERE mid = ?').bind(m.id));
        closedN++;
      }
      gammaTs = nowS;
    } catch (err) {
      console.warn(`${site}: gamma closed-check failed: ${err.message}`); // retried next tick
    }
  }

  stmts.push(db.prepare(
    `INSERT INTO cursors (site, snapshot, ts, gamma_ts, ingests, rows_total) VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(site) DO UPDATE SET snapshot = excluded.snapshot, ts = excluded.ts, gamma_ts = excluded.gamma_ts,
       ingests = cursors.ingests + ?, rows_total = cursors.rows_total + ?`
  ).bind(site, fresh ? snap : cursor.snapshot, fresh ? ts : cursor.ts, gammaTs,
    fresh ? 1 : 0, changes, fresh ? 1 : 0, changes));

  await db.batch(stmts); // one transaction: holdings + current + market status + cursor
  return { site, snapshot: fresh ? snap : null, changes, added, closed: closedN };
}

async function diffSnapshot(env, site, agg, ts, stmts) {
  const db = env.DB;
  const snapMarkets = indexSnapshot(agg);
  const failed = new Set((agg.failedTraders || []).map((a) => a.toLowerCase()));

  // 1. Register top-200 markets not tracked yet (OR IGNORE keeps closed ones closed).
  const activeBefore = (await db.prepare(`SELECT id, cid, outcomes FROM markets WHERE site = ? AND status = 'active'`)
    .bind(site).all()).results;
  const activeCids = new Set(activeBefore.map((m) => m.cid));
  const newRows = [];
  for (const p of (agg.positions || []).slice(0, TOP_N)) {
    const cid = p.conditionId;
    if (!cid || activeCids.has(cid)) continue;
    const m = snapMarkets.get(cid);
    if (m.redeemable) continue;
    activeCids.add(cid);
    newRows.push([site, cid, m.info.title, m.info.slug, m.info.eventSlug, m.info.endDate, JSON.stringify(m.names), ts]);
  }
  if (newRows.length) {
    await db.batch(multiInsert(db,
      'INSERT OR IGNORE INTO markets (site, cid, title, slug, event_slug, end_date, outcomes, first_ts)', '', newRows));
  }
  const active = newRows.length
    ? (await db.prepare(`SELECT id, cid, outcomes FROM markets WHERE site = ? AND status = 'active'`).bind(site).all()).results
    : activeBefore;
  const added = active.length - activeBefore.length;

  // 2. Trader dictionary.
  const holders = new Set();
  for (const m of active) {
    for (const h of snapMarkets.get(m.cid)?.outcomes.values() || []) for (const a of h.keys()) holders.add(a);
  }
  let traders = (await db.prepare('SELECT id, addr FROM traders').all()).results;
  const known = new Set(traders.map((t) => t.addr));
  const missing = [...holders].filter((a) => !known.has(a)).map((a) => [a]);
  if (missing.length) {
    await db.batch(multiInsert(db, 'INSERT OR IGNORE INTO traders (addr)', '', missing));
    traders = (await db.prepare('SELECT id, addr FROM traders').all()).results;
  }
  const tidOf = new Map(traders.map((t) => [t.addr, t.id]));
  const addrOf = new Map(traders.map((t) => [t.id, t.addr]));

  // 3. Diff against the current table.
  const curRows = (await db.prepare(
    `SELECT c.mid, c.oi, c.tid, c.size FROM current c JOIN markets m ON m.id = c.mid
      WHERE m.site = ? AND m.status = 'active'`).bind(site).all()).results;
  const curByMid = new Map();
  for (const r of curRows) {
    let mm = curByMid.get(r.mid);
    if (!mm) { mm = new Map(); curByMid.set(r.mid, mm); }
    mm.set(`${r.oi}|${r.tid}`, r.size);
  }

  const hist = [], upserts = [];
  let closed = 0;
  for (const m of active) {
    const snapM = snapMarkets.get(m.cid);
    const cur = curByMid.get(m.id) || new Map();

    if (snapM?.redeemable) {
      stmts.push(db.prepare(`UPDATE markets SET status = 'closed', close_reason = 'resolved', closed_ts = ? WHERE id = ?`).bind(ts, m.id));
      stmts.push(db.prepare('DELETE FROM current WHERE mid = ?').bind(m.id));
      closed++;
      continue;
    }

    // Learn outcome names that appeared after registration (one side held later).
    if (snapM) {
      const names = JSON.parse(m.outcomes || '{}');
      let grew = false;
      for (const [oi, n] of Object.entries(snapM.names)) if (n && names[oi] !== n) { names[oi] = n; grew = true; }
      if (grew) stmts.push(db.prepare('UPDATE markets SET outcomes = ? WHERE id = ?').bind(JSON.stringify(names), m.id));
    }

    const seen = new Set();
    for (const [oi, h] of snapM?.outcomes || []) {
      for (const [addr, rawSize] of h) {
        if (failed.has(addr)) continue;               // carry their last size forward
        const tid = tidOf.get(addr);
        if (tid === undefined) continue;
        const k = `${oi}|${tid}`;
        const size = round2(rawSize);
        seen.add(k);
        const prev = cur.get(k);
        if (prev === undefined ? size <= 0 : Math.abs(prev - size) < 0.01) continue;
        if (size <= 0) {
          hist.push([m.id, oi, tid, ts, 0]);
          stmts.push(db.prepare('DELETE FROM current WHERE mid = ? AND oi = ? AND tid = ?').bind(m.id, oi, tid));
        } else {
          hist.push([m.id, oi, tid, ts, size]);
          upserts.push([m.id, oi, tid, size]);
        }
      }
    }
    // Held last time, gone now → full exit (unless the trader's fetch failed).
    for (const [k] of cur) {
      if (seen.has(k)) continue;
      const [oi, tid] = k.split('|').map(Number);
      if (failed.has(addrOf.get(tid))) continue;
      hist.push([m.id, oi, tid, ts, 0]);
      stmts.push(db.prepare('DELETE FROM current WHERE mid = ? AND oi = ? AND tid = ?').bind(m.id, oi, tid));
    }
  }

  stmts.push(...multiInsert(db, 'INSERT OR REPLACE INTO holdings (mid, oi, tid, ts, size)', '', hist));
  stmts.push(...multiInsert(db, 'INSERT INTO current (mid, oi, tid, size)',
    'ON CONFLICT(mid, oi, tid) DO UPDATE SET size = excluded.size', upserts));
  return { changes: hist.length, added, closed };
}

// ─── Top holders PnL ─────────────────────────────────────────────────────────
// Holders come from data-api /holders (ranked by shares on this market). PnL is
// the wallet's account-wide figure from user-pnl-api — the same series the
// Polymarket profile chart and the dashboard's "All Time PnL" column use:
// a window's PnL = last point − first point of that interval's series; all-time
// = last point. Results are cached in D1 (wallet 15 min, market 5 min) so a
// popular market costs Polymarket ≤ 1 + 3×100 calls per 15 minutes.

const DATA_API = 'https://data-api.polymarket.com';
const PNL_API = 'https://user-pnl-api.polymarket.com/user-pnl';
const HOLDERS_N = 50;
const WALLET_TTL_S = 15 * 60;
const MARKET_TTL_S = 5 * 60;
// 1w/1h serves both 24h (last − point 24h earlier) and 7d; 30d and all-time
// each need their own series. 3 calls per wallet.
const PNL_SERIES = [['week', '1w', '1h'], ['m1', '1m', '1d'], ['al', 'all', '1d']];
const N_CHOICES = [10, 20, 50];

class UpstreamError extends Error {
  constructor(kind, msg, retryAfter = null) { super(msg); this.kind = kind; this.retryAfter = retryAfter; }
}

/**
 * GET JSON with retries. Returns { data, age } — `age` is the CDN Age header:
 * Polymarket caches user-pnl series for 30 min and holders for 2 min, so the
 * data can be that much older than our fetch. Errors are classified for the
 * refresh UI: rate_limited (429, with Retry-After), timeout (Polymarket's own
 * "context deadline exceeded" 500s, or our 8 s cap), upstream (anything else).
 */
async function getJSONMeta(url, tries = 3, timeoutMs = 8_000) {
  let last;
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'pm-share-history/1.0' }, signal: AbortSignal.timeout(timeoutMs) });
      if (r.ok) {
        // Data time: Last-Modified is when Polymarket computed it. The CDN Age
        // header over-reports (seen 648 s on a 175-s-old object), so it is only
        // a fallback.
        const lm = Date.parse(r.headers.get('last-modified') || '');
        const age = Number.isFinite(lm) ? Math.max(0, Math.round((Date.now() - lm) / 1000))
          : parseInt(r.headers.get('age') || '0', 10) || 0;
        return { data: await r.json(), age };
      }
      const body = (await r.text()).slice(0, 200);
      if (r.status === 429) last = new UpstreamError('rate_limited', 'HTTP 429', parseInt(r.headers.get('retry-after') || '60', 10) || 60);
      else if (r.status >= 500) last = new UpstreamError(/deadline|timeout/i.test(body) ? 'timeout' : 'upstream', `HTTP ${r.status} ${body}`);
      else throw new UpstreamError('upstream', `HTTP ${r.status}`);
    } catch (err) {
      if (err instanceof UpstreamError && err.kind === 'upstream' && /HTTP 4/.test(err.message)) throw err;
      last = err instanceof UpstreamError ? err
        : new UpstreamError(err?.name === 'TimeoutError' ? 'timeout' : 'upstream', err?.message || String(err));
    }
    if (i < tries - 1) await new Promise((res) => setTimeout(res, (last?.kind === 'rate_limited' ? 2000 : 300) * 2 ** i));
  }
  throw last;
}

async function getJSON(url, tries = 3) {
  return (await getJSONMeta(url, tries)).data;
}

async function pool(items, size, fn) {
  const queue = [...items];
  await Promise.all(Array.from({ length: Math.min(size, queue.length) }, async () => {
    while (queue.length) await fn(queue.shift());
  }));
}

/** One wallet's 4 windows. `bust` adds a cache-buster so Polymarket recomputes
 *  instead of serving its ≤30-min CDN copy. asOf = oldest data time of the 3 series. */
async function walletPnl(addr, { bust = false } = {}) {
  const out = { d1: null, w1: null, m1: null, al: null, asOf: Math.floor(Date.now() / 1000), err: null };
  const nowS = out.asOf;
  await Promise.all(PNL_SERIES.map(async ([key, interval, fidelity]) => {
    let s;
    try {
      // Forced recomputes get 20 s: Polymarket's slow-but-successful cold computes run to ~15 s.
      const r = await getJSONMeta(`${PNL_API}?user_address=${addr}&interval=${interval}&fidelity=${fidelity}${bust ? `&_=${nowS}` : ''}`,
        bust ? 2 : 3, bust ? 20_000 : 8_000);
      s = r.data;
      out.asOf = Math.min(out.asOf, nowS - r.age);
    } catch (err) {
      out.err = out.err === 'rate_limited' ? out.err : (err.kind || 'upstream');
      if (err.retryAfter) out.retryAfter = err.retryAfter;
      return;
    }
    if (!Array.isArray(s) || !s.length) {             // no PnL history = 0
      if (key === 'week') { out.d1 = 0; out.w1 = 0; } else out[key] = 0;
      return;
    }
    const last = s[s.length - 1];
    if (key === 'week') {
      let dayAgo = s[0];
      for (const pt of s) if (pt.t <= last.t - 86400) dayAgo = pt;
      out.d1 = last.p - dayAgo.p;
      out.w1 = last.p - s[0].p;
    } else {
      out[key] = key === 'al' ? last.p : last.p - s[0].p;
    }
  }));
  return out;
}

// Holder ranking for a market: top 50 per outcome, cached in D1.
async function fetchRanking(env, cid, nowS, { bust = false } = {}) {
  const tokens = await getJSON(`${DATA_API}/holders?market=${cid}&limit=${HOLDERS_N}${bust ? `&_=${nowS}` : ''}`);
  const outcomes = tokens.map((t) => {
    const holders = [...(t.holders || [])].sort((a, b) => b.amount - a.amount).slice(0, HOLDERS_N);
    return {
      oi: holders[0]?.outcomeIndex ?? null,
      holders: holders.map((h) => ({ addr: h.proxyWallet.toLowerCase(), name: h.name || h.pseudonym || '', shares: h.amount })),
    };
  }).filter((o) => o.oi !== null).sort((a, b) => a.oi - b.oi);
  await env.DB.prepare('INSERT OR REPLACE INTO holders_pnl_cache (cid, ts, body) VALUES (?, ?, ?)')
    .bind(cid, nowS, JSON.stringify({ outcomes })).run();
  return outcomes;
}

async function cachedWalletPnl(env, addrs, minTs) {
  const out = new Map();
  for (let i = 0; i < addrs.length; i += 90) {
    const chunk = addrs.slice(i, i + 90);
    const rows = (await env.DB.prepare(
      `SELECT * FROM pnl_cache WHERE ts > ? AND addr IN (${chunk.map(() => '?').join(',')})`
    ).bind(minTs, ...chunk).all()).results;
    for (const r of rows) out.set(r.addr, { ts: r.ts, d1: r.d1, w1: r.w1, m1: r.m1, al: r.al });
  }
  return out;
}

/** Fetch wallets' PnL from Polymarket and cache the complete rows. */
/**
 * Fetch wallets' PnL and cache the complete rows (ts = Polymarket's data time).
 * With a deadline, wallets not yet started when it passes are left as `skipped`
 * (the caller keeps their previous values). Returns { got, failed, skipped }.
 */
async function refreshWallets(env, addrs, nowS, { bust = false, deadline = Infinity, noFallback = false } = {}) {
  const got = new Map(), failed = [], skipped = [];
  let fellBack = 0;
  await pool(addrs, 16, async (a) => {
    if (Date.now() > deadline) { skipped.push(a); return; }
    let p = await walletPnl(a, { bust });
    // A forced recompute often hits Polymarket's own deadline on cold wallets
    // (~25% in testing). Fall back to its CDN copy (≤30 min old) rather than
    // reporting nothing; asOf then carries the true (older) data time.
    if (p.err && bust && !noFallback && p.err !== 'rate_limited') {
      const cdn = await walletPnl(a);
      if (!cdn.err) { p = cdn; fellBack++; }
    }
    if (p.err) failed.push({ addr: a, reason: p.err, retryAfter: p.retryAfter });
    got.set(a, { ...p, ts: p.asOf });
  });
  const rows = [...got].filter(([, p]) => !p.err).map(([a, p]) => [a, p.ts, p.d1, p.w1, p.m1, p.al]);
  if (rows.length) await env.DB.batch(multiInsert(env.DB, 'INSERT OR REPLACE INTO pnl_cache (addr, ts, d1, w1, m1, al)', '', rows));
  return { got, failed, skipped, fellBack };
}

// Stale-while-revalidate: Polymarket computes a cold wallet's PnL series in
// seconds, so a fully cold top-10 can take ~20s. Anything cached within
// SERVE_STALE_S is served at once (refreshed in the background when older than
// the fresh TTL); only never-seen wallets are fetched inline. The cron warmer
// keeps tracked markets' top 10 cached, so most opens never wait.
const SERVE_STALE_S = 2 * 3600;
const RANKING_STALE_S = 3600;

async function holdersPnl(env, url, ctx) {
  const cid = url.searchParams.get('cid') || '';
  const n = Number(url.searchParams.get('n') || 10);
  if (!/^0x[0-9a-fA-F]{64}$/.test(cid) || !N_CHOICES.includes(n)) return json({ error: 'bad_request' }, 400);
  const nowS = Math.floor(Date.now() / 1000);
  const bg = [];

  const cached = await env.DB.prepare('SELECT ts, body FROM holders_pnl_cache WHERE cid = ?').bind(cid).first();
  let outcomes, rankedAt;
  if (cached && nowS - cached.ts < RANKING_STALE_S) {
    outcomes = JSON.parse(cached.body).outcomes;
    rankedAt = cached.ts;
    if (nowS - cached.ts >= MARKET_TTL_S) bg.push(fetchRanking(env, cid, nowS));
  } else {
    try {
      outcomes = await fetchRanking(env, cid, nowS);
      rankedAt = nowS;
    } catch (err) {
      console.warn('holders fetch failed:', err.message);
      if (!cached) return json({ error: 'upstream' }, 502);
      outcomes = JSON.parse(cached.body).outcomes;        // stale ranking beats no answer
      rankedAt = cached.ts;
    }
  }

  for (const o of outcomes) o.holders = o.holders.slice(0, n);
  const addrs = [...new Set(outcomes.flatMap((o) => o.holders.map((h) => h.addr)))];
  const pnl = await cachedWalletPnl(env, addrs, nowS - SERVE_STALE_S);
  const missing = addrs.filter((a) => !pnl.has(a));
  if (missing.length) {
    // Never-seen wallets: CDN fetch, but a page load waits at most ~6 s — the
    // rest finish in the background and show up on the next poll.
    const work = refreshWallets(env, missing, nowS).then(({ got }) => { for (const [a, p] of got) if (!p.err) pnl.set(a, p); });
    const done = await Promise.race([work.then(() => true), new Promise((res) => setTimeout(() => res(false), 6000))]);
    if (!done) bg.push(work);
  }
  const stale = addrs.filter((a) => pnl.get(a)?.ts < nowS - WALLET_TTL_S);
  if (stale.length) bg.push(refreshWallets(env, stale, nowS));
  if (bg.length) ctx.waitUntil(Promise.allSettled(bg));

  let asOf = nowS;
  for (const o of outcomes) for (const h of o.holders) {
    const p = pnl.get(h.addr);
    if (p?.ts) asOf = Math.min(asOf, p.ts);
    h.pnl = p ? { d1: p.d1, w1: p.w1, m1: p.m1, al: p.al } : null;
  }
  const refresh = await env.DB.prepare('SELECT * FROM holders_refresh WHERE cid = ?').bind(cid).first();
  console.log(`holders-pnl ${cid.slice(0, 10)} n=${n} wallets=${addrs.length} inline=${missing.length} bg=${stale.length}`);
  const pending = addrs.filter((a) => !pnl.has(a)).length;
  const refreshState = await refreshInfo(env, refresh, nowS);
  const cacheable = !pending && refreshState?.status !== 'in_progress';
  return json({ cid, n, rankedAt, asOf, pending, outcomes, refresh: refreshState }, 200, cacheable ? 30 : 0);
}

// ─── Member-triggered refresh (queued) ───────────────────────────────────────
// A refresh bypasses Polymarket's ≤30-min CDN copy (cache-buster) so Polymarket
// recomputes each wallet. Measured 2026-10-05: a cold recompute takes 2-15 s,
// ~25% hit Polymarket's own "context deadline exceeded", and ~130 concurrent
// forced calls draw HTTP 429s. So forced work goes through ONE site-wide wallet
// queue with a fixed concurrency, instead of inside the click's HTTP request:
//   • POST /holders-pnl/refresh queues the market's top-n wallets that are not
//     already fresh (< WALLET_FRESH_S — so 10 ⊂ 20 ⊂ 50, and wallets shared by
//     several markets, are computed once), works the queue inline for
//     REFRESH_INLINE_MS, and returns progress.
//   • The minute cron keeps draining the queue (REFRESH_CRON_MS per tick).
//   • A 429 pauses the whole queue for Polymarket's Retry-After.
//   • A wallet that fails twice gets Polymarket's CDN copy (≤30 min old) and
//     leaves the queue; so does one queued for longer than QUEUE_MAX_AGE_S.
// Statuses for the UI: in_progress (with done/total, paused-until), ok/partial,
// cooldown (same market refreshed < REFRESH_COOLDOWN_S ago, same or larger n),
// busy (queue already longer than QUEUE_MAX), upstream_error / rate_limited.

const REFRESH_COOLDOWN_S = 180;
const WALLET_FRESH_S = 300;
const RANKING_FRESH_S = 120;
const REFRESH_INLINE_MS = 20_000;   // background (waitUntil) head start; waitUntil allows ~30 s
const REFRESH_CRON_MS = 35_000;
const QUEUE_CONCURRENCY = 12;
const QUEUE_MAX = 400;
const QUEUE_MAX_AGE_S = 15 * 60;
const QUEUE_RATE_PER_MIN = 30;       // ETA only; measured 20-40 wallets/min (Polymarket cold recompute 2-15 s)

async function queuePausedUntil(env) {
  const r = await env.DB.prepare(`SELECT gamma_ts FROM cursors WHERE site = '_pnl_queue_pause'`).first();
  return r?.gamma_ts || 0;
}

/**
 * Work the wallet queue until the deadline with a continuous pool (a slow
 * wallet never holds up the others). Wallets are CLAIMED atomically first, so
 * an overlapping cron tick or a concurrent click never fetches the same wallet
 * twice; a claim expires after CLAIM_S if its worker died. `onlyAddrs` limits
 * the drain to one market's wallets (the inline part of a click).
 */
const CLAIM_S = 60;
async function claim(env, nowS, onlyAddrs, limit) {
  const where = onlyAddrs ? `addr IN (${onlyAddrs.map(() => '?').join(',')}) AND` : '';
  const sql = `UPDATE pnl_queue SET claimed = ? WHERE addr IN (
      SELECT addr FROM pnl_queue WHERE ${where} claimed < ? ORDER BY enq_ts LIMIT ?) RETURNING addr, tries`;
  return (await env.DB.prepare(sql).bind(nowS, ...(onlyAddrs || []), nowS - CLAIM_S, limit).all()).results;
}

async function drainQueue(env, deadline, onlyAddrs = null) {
  const db = env.DB;
  let nowS = Math.floor(Date.now() / 1000);
  if ((await queuePausedUntil(env)) > nowS) return { processed: 0, paused: true };
  // Expire stragglers: CDN copy instead of yet another forced attempt.
  const old = (await db.prepare('SELECT addr FROM pnl_queue WHERE enq_ts < ? AND claimed < ? LIMIT 40')
    .bind(nowS - QUEUE_MAX_AGE_S, nowS - CLAIM_S).all()).results.map((r) => r.addr);
  if (old.length) {
    await refreshWallets(env, old, nowS);
    await db.batch(old.map((a) => db.prepare('DELETE FROM pnl_queue WHERE addr = ?').bind(a)));
  }
  let processed = 0, rateLimited = null;
  const only = onlyAddrs ? onlyAddrs.slice(0, 90) : null;
  let work = await claim(env, nowS, only, onlyAddrs ? 90 : QUEUE_CONCURRENCY * 6);
  const worker = async () => {
    while (work.length && Date.now() < deadline && !rateLimited) {
      const { addr, tries } = work.shift();
      const res = await refreshWallets(env, [addr], Math.floor(Date.now() / 1000), { bust: true, noFallback: true });
      const f = res.failed[0];
      if (!f) { await db.prepare('DELETE FROM pnl_queue WHERE addr = ?').bind(addr).run(); processed++; continue; }
      if (f.reason === 'rate_limited') { rateLimited = f.retryAfter || 60; break; }
      if (tries + 1 >= 2) {
        await refreshWallets(env, [addr], Math.floor(Date.now() / 1000));          // CDN copy
        await db.prepare('DELETE FROM pnl_queue WHERE addr = ?').bind(addr).run();
        processed++;
      } else {
        // Release the claim so a later pass (or tick) retries it.
        await db.prepare('UPDATE pnl_queue SET tries = tries + 1, claimed = 0 WHERE addr = ?').bind(addr).run();
      }
      if (!work.length && !onlyAddrs && Date.now() < deadline - 5000) {
        work = await claim(env, Math.floor(Date.now() / 1000), null, QUEUE_CONCURRENCY * 6);
      }
    }
  };
  await Promise.all(Array.from({ length: QUEUE_CONCURRENCY }, worker));
  // Unstarted claims go back to the queue immediately.
  if (work.length) await db.batch(work.map(({ addr }) => db.prepare('UPDATE pnl_queue SET claimed = 0 WHERE addr = ?').bind(addr)));
  if (rateLimited) {
    await db.prepare(`INSERT INTO cursors (site, gamma_ts) VALUES ('_pnl_queue_pause', ?)
      ON CONFLICT(site) DO UPDATE SET gamma_ts = excluded.gamma_ts`).bind(Math.floor(Date.now() / 1000) + rateLimited).run();
  }
  return { processed, rateLimited };
}

/** Progress of a market refresh: its wallets still queued vs total. */
async function refreshProgress(env, r) {
  const detail = r.detail ? JSON.parse(r.detail) : {};
  const addrs = detail.addrs || [];
  let queued = 0;
  for (let i = 0; i < addrs.length; i += 90) {
    const chunk = addrs.slice(i, i + 90);
    queued += (await env.DB.prepare(`SELECT COUNT(*) AS c FROM pnl_queue WHERE addr IN (${chunk.map(() => '?').join(',')})`)
      .bind(...chunk).first()).c;
  }
  const ahead = queued
    ? (await env.DB.prepare('SELECT COUNT(*) AS c FROM pnl_queue WHERE enq_ts < ?').bind(r.started_ts).first()).c : 0;
  return { total: detail.total || addrs.length, alreadyFresh: detail.alreadyFresh || 0, queued, ahead, done: (detail.total || addrs.length) - queued };
}

/** Close a market refresh once none of its wallets are queued any more. */
async function finalizeRefresh(env, r, nowS) {
  const prog = await refreshProgress(env, r);
  if (prog.queued) return { ...prog, finished: false };
  const detail = JSON.parse(r.detail || '{}');
  // Wallets whose cached data predates the refresh = Polymarket gave us its CDN copy.
  const addrs = detail.addrs || [];
  const rows = await cachedWalletPnl(env, addrs, 0);
  const cdnCopies = addrs.filter((a) => !rows.has(a) || rows.get(a).ts < r.started_ts - 60).length;
  const status = cdnCopies ? 'partial' : 'ok';
  // Finished = when its last wallet landed, not when somebody next looked.
  const lastTs = Math.max(r.started_ts, ...[...rows.values()].map((p) => p.ts).filter((t) => t >= r.started_ts - 60));
  const finishedTs = Math.min(nowS, lastTs);
  const fin = { total: prog.total, alreadyFresh: prog.alreadyFresh, refreshed: addrs.length - cdnCopies, cdnCopies, tookS: finishedTs - r.started_ts };
  await env.DB.prepare('UPDATE holders_refresh SET finished_ts = ?, status = ?, detail = ? WHERE cid = ? AND finished_ts IS NULL')
    .bind(finishedTs, status, JSON.stringify({ ...fin, addrs: [] }), r.cid).run();
  return { ...prog, finished: true, finishedTs, status, ...fin };
}

async function refreshInfo(env, r, nowS) {
  if (!r) return null;
  const pausedUntil = await queuePausedUntil(env);
  if (!r.finished_ts) {
    const p = await finalizeRefresh(env, r, nowS);
    if (!p.finished) {
      return { status: 'in_progress', startedAt: r.started_ts, n: r.n, ...p,
        pausedFor: pausedUntil > nowS ? pausedUntil - nowS : 0,
        etaS: Math.ceil(((p.ahead + p.queued) / QUEUE_RATE_PER_MIN) * 60) };
    }
    r = { ...r, finished_ts: p.finishedTs, status: p.status, detail: JSON.stringify(p) };
  }
  return { status: r.status, startedAt: r.started_ts, finishedAt: r.finished_ts, n: r.n,
    detail: JSON.parse(r.detail || '{}'), cooldownLeft: Math.max(0, REFRESH_COOLDOWN_S - (nowS - r.finished_ts)) };
}

async function refreshHoldersPnl(env, url, ctx) {
  const cid = url.searchParams.get('cid') || '';
  const n = Number(url.searchParams.get('n') || 10);
  if (!/^0x[0-9a-fA-F]{64}$/.test(cid) || !N_CHOICES.includes(n)) return json({ status: 'bad_request' }, 400);
  const db = env.DB;
  const t0 = Date.now();
  const nowS = Math.floor(t0 / 1000);

  const prev = await db.prepare('SELECT * FROM holders_refresh WHERE cid = ?').bind(cid).first();
  if (prev && !prev.finished_ts && prev.n >= n) {
    return json(await refreshInfo(env, prev, nowS), 202);         // already running: report its progress
  }
  if (prev?.finished_ts && nowS - prev.finished_ts < REFRESH_COOLDOWN_S && prev.n >= n)
    return json({ status: 'cooldown', retryAfter: REFRESH_COOLDOWN_S - (nowS - prev.finished_ts), finishedAgo: nowS - prev.finished_ts }, 429);
  const qlen = (await db.prepare('SELECT COUNT(*) AS c FROM pnl_queue').first()).c;
  if (qlen >= QUEUE_MAX)
    return json({ status: 'busy', queued: qlen, retryAfter: Math.ceil((qlen - QUEUE_MAX / 2) / QUEUE_RATE_PER_MIN * 60) }, 429);
  const pausedUntil = await queuePausedUntil(env);
  if (pausedUntil > nowS) return json({ status: 'rate_limited', retryAfter: pausedUntil - nowS }, 429);

  let outcomes;
  try {
    const cached = await db.prepare('SELECT ts, body FROM holders_pnl_cache WHERE cid = ?').bind(cid).first();
    outcomes = cached && nowS - cached.ts < RANKING_FRESH_S
      ? JSON.parse(cached.body).outcomes
      : await fetchRanking(env, cid, nowS, { bust: true });
  } catch (err) {
    const rl = err.kind === 'rate_limited';
    return json({ status: rl ? 'rate_limited' : 'upstream_error', retryAfter: err.retryAfter || null }, rl ? 429 : 502);
  }
  const addrs = [...new Set(outcomes.flatMap((o) => o.holders.slice(0, n).map((h) => h.addr)))];
  const fresh = await cachedWalletPnl(env, addrs, nowS - WALLET_FRESH_S);
  const todo = addrs.filter((a) => !fresh.has(a));

  const stmts = multiInsert(db, 'INSERT OR IGNORE INTO pnl_queue (addr, enq_ts, tries, claimed)', '', todo.map((a) => [a, nowS, 0, 0]));
  stmts.push(db.prepare(
    `INSERT INTO holders_refresh (cid, started_ts, finished_ts, n, status, detail) VALUES (?, ?, NULL, ?, 'in_progress', ?)
     ON CONFLICT(cid) DO UPDATE SET started_ts = excluded.started_ts, finished_ts = NULL, n = excluded.n, status = 'in_progress', detail = excluded.detail`
  ).bind(cid, nowS, n, JSON.stringify({ addrs, total: addrs.length, alreadyFresh: fresh.size })));
  await db.batch(stmts);

  // Answer at once with progress; work this market's wallets in the background
  // right away (waitUntil), and the minute cron finishes whatever is left.
  if (todo.length) ctx.waitUntil(drainQueue(env, t0 + REFRESH_INLINE_MS, [...todo]).catch((err) => console.warn('inline drain:', err.message)));
  const r = await db.prepare('SELECT * FROM holders_refresh WHERE cid = ?').bind(cid).first();
  const info = await refreshInfo(env, r, Math.floor(Date.now() / 1000));
  console.log(`holders-pnl refresh ${cid.slice(0, 10)} n=${n} ${info.status} ${JSON.stringify({ ...info, detail: undefined })}`);
  return json(info, info.status === 'in_progress' ? 202 : 200);
}

/** Cron: keep the top 10 of a few tracked markets warm per tick, oldest first. */
const WARM_MARKETS_PER_TICK = 3;
async function warmHoldersPnl(env) {
  const nowS = Math.floor(Date.now() / 1000);
  const due = (await env.DB.prepare(
    `SELECT m.cid FROM (SELECT DISTINCT cid FROM markets WHERE status = 'active') m
       LEFT JOIN holders_pnl_cache c ON c.cid = m.cid
      WHERE c.ts IS NULL OR c.ts < ?
      ORDER BY COALESCE(c.ts, 0) LIMIT ?`
  ).bind(nowS - 30 * 60, WARM_MARKETS_PER_TICK).all()).results;
  for (const { cid } of due) {
    try {
      const outcomes = await fetchRanking(env, cid, nowS);
      const addrs = [...new Set(outcomes.flatMap((o) => o.holders.slice(0, 10).map((h) => h.addr)))];
      const fresh = await cachedWalletPnl(env, addrs, nowS - WALLET_TTL_S);
      await refreshWallets(env, addrs.filter((a) => !fresh.has(a)), nowS);
    } catch (err) {
      console.warn(`warm ${cid.slice(0, 10)} failed: ${err.message}`);
    }
  }
}

// ─── Read API ────────────────────────────────────────────────────────────────

function json(body, status = 200, maxAge = 0) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': maxAge ? `private, max-age=${maxAge}` : 'no-store',
    },
  });
}

async function history(env, url) {
  const site = url.searchParams.get('site');
  const cid = url.searchParams.get('cid') || '';
  const range = url.searchParams.get('range') || '1d';
  if (!SITES.includes(site) || !/^0x[0-9a-fA-F]{64}$/.test(cid) || !(range in RANGES))
    return json({ error: 'bad_request' }, 400);

  const db = env.DB;
  const m = await db.prepare('SELECT * FROM markets WHERE site = ? AND cid = ?').bind(site, cid).first();
  if (!m) return json({ tracked: false }, 200, 30);

  const cursor = await db.prepare('SELECT ts FROM cursors WHERE site = ?').bind(site).first();
  const asof = m.status === 'closed' ? m.closed_ts : Math.max(cursor?.ts || m.first_ts, m.first_ts);
  const span = RANGES[range];
  const since = span ? Math.max(asof - span, m.first_ts) : m.first_ts;

  const [baseline, rows, traders] = await Promise.all([
    since > m.first_ts
      ? db.prepare(`SELECT oi, tid, size, MAX(ts) AS ts FROM holdings WHERE mid = ? AND ts < ? GROUP BY oi, tid`)
        .bind(m.id, since).all()
      : { results: [] },
    db.prepare('SELECT ts, oi, tid, size FROM holdings WHERE mid = ? AND ts >= ? ORDER BY ts').bind(m.id, since).all(),
    db.prepare('SELECT id, addr FROM traders').all(),
  ]);

  const used = new Set([...baseline.results, ...rows.results].map((r) => r.tid));
  return json({
    tracked: true,
    market: {
      cid: m.cid, title: m.title, slug: m.slug, eventSlug: m.event_slug, endDate: m.end_date,
      outcomes: JSON.parse(m.outcomes || '{}'), status: m.status, closeReason: m.close_reason,
      firstTs: m.first_ts, closedTs: m.closed_ts,
    },
    range, since, asof,
    traders: Object.fromEntries(traders.results.filter((t) => used.has(t.id)).map((t) => [t.id, t.addr])),
    baseline: baseline.results.filter((r) => r.size > 0).map((r) => [r.oi, r.tid, r.size]),
    rows: rows.results.map((r) => [r.ts, r.oi, r.tid, r.size]),
  }, 200, 30);
}

async function listMarkets(env, url) {
  const site = url.searchParams.get('site');
  const status = url.searchParams.get('status') || 'active';
  if (!SITES.includes(site) || !['active', 'closed'].includes(status)) return json({ error: 'bad_request' }, 400);
  const r = await env.DB.prepare(
    `SELECT m.cid, m.title, m.event_slug AS eventSlug, m.end_date AS endDate, m.status, m.close_reason AS closeReason,
            m.first_ts AS firstTs, m.closed_ts AS closedTs,
            (SELECT COUNT(*) FROM holdings h WHERE h.mid = m.id) AS rows
       FROM markets m WHERE m.site = ? AND m.status = ? ORDER BY COALESCE(m.closed_ts, m.first_ts) DESC LIMIT 1000`
  ).bind(site, status).all();
  return json({ site, status, markets: r.results }, 200, 60);
}

async function status(env) {
  const [cursors, counts, rows] = await Promise.all([
    env.DB.prepare('SELECT * FROM cursors').all(),
    env.DB.prepare('SELECT site, status, COUNT(*) AS n FROM markets GROUP BY site, status').all(),
    env.DB.prepare('SELECT COUNT(*) AS n FROM holdings').first(),
  ]);
  return json({ cursors: cursors.results, markets: counts.results, holdingRows: rows.n });
}

export default {
  async scheduled(event, env, ctx) {
    for (const site of SITES) {
      try {
        const r = await ingestSite(env, site);
        if (!r.skipped) console.log(JSON.stringify(r));
      } catch (err) {
        console.error(`${site}: ingest failed: ${err.stack || err.message}`);
      }
    }
    try {
      const q = await drainQueue(env, Date.now() + REFRESH_CRON_MS);
      if (q.processed || q.rateLimited) console.log(`pnl queue: ${JSON.stringify(q)}`);
      const open = (await env.DB.prepare('SELECT * FROM holders_refresh WHERE finished_ts IS NULL').all()).results;
      for (const r of open) await finalizeRefresh(env, r, Math.floor(Date.now() / 1000));
    } catch (err) { console.error(`queue drain failed: ${err.message}`); }
    try { await warmHoldersPnl(env); } catch (err) { console.error(`warm failed: ${err.message}`); }
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/holders-pnl/refresh' && request.method === 'POST') return refreshHoldersPnl(env, url, ctx);
    if (request.method !== 'GET') return json({ error: 'method' }, 405);
    if (url.pathname === '/history') return history(env, url);
    if (url.pathname === '/markets') return listMarkets(env, url);
    if (url.pathname === '/status') return status(env);
    if (url.pathname === '/holders-pnl') return holdersPnl(env, url, ctx);
    return json({ error: 'not_found' }, 404);
  },
};
