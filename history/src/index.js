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

async function getJSON(url, tries = 3) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'pm-share-history/1.0' }, signal: AbortSignal.timeout(8_000) });
      if (r.ok) return r.json();
      if (r.status !== 429 && r.status < 500) throw new Error(`HTTP ${r.status}`);
    } catch (err) {
      if (i === tries - 1) throw err;
    }
    await new Promise((res) => setTimeout(res, 300 * 2 ** i));
  }
  throw new Error('retries exhausted');
}

async function pool(items, size, fn) {
  const queue = [...items];
  await Promise.all(Array.from({ length: Math.min(size, queue.length) }, async () => {
    while (queue.length) await fn(queue.shift());
  }));
}

async function walletPnl(addr) {
  const out = { d1: null, w1: null, m1: null, al: null };
  await Promise.all(PNL_SERIES.map(async ([key, interval, fidelity]) => {
    let s;
    try { s = await getJSON(`${PNL_API}?user_address=${addr}&interval=${interval}&fidelity=${fidelity}`); }
    catch { return; }
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
async function fetchRanking(env, cid, nowS) {
  const tokens = await getJSON(`${DATA_API}/holders?market=${cid}&limit=${HOLDERS_N}`);
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
async function refreshWallets(env, addrs, nowS) {
  const got = new Map();
  await pool(addrs, 16, async (a) => { got.set(a, { ts: nowS, ...(await walletPnl(a)) }); });
  const rows = [...got].map(([a, p]) => [a, nowS, p.d1, p.w1, p.m1, p.al])
    .filter((r) => r.slice(2).every((v) => v !== null));       // only cache complete rows
  if (rows.length) await env.DB.batch(multiInsert(env.DB, 'INSERT OR REPLACE INTO pnl_cache (addr, ts, d1, w1, m1, al)', '', rows));
  return got;
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
  if (missing.length) for (const [a, p] of await refreshWallets(env, missing, nowS)) pnl.set(a, p);
  const stale = addrs.filter((a) => pnl.get(a)?.ts < nowS - WALLET_TTL_S);
  if (stale.length) bg.push(refreshWallets(env, stale, nowS));
  if (bg.length) ctx.waitUntil(Promise.allSettled(bg));

  let asOf = nowS;
  for (const o of outcomes) for (const h of o.holders) {
    const p = pnl.get(h.addr);
    if (p?.ts) asOf = Math.min(asOf, p.ts);
    h.pnl = p ? { d1: p.d1, w1: p.w1, m1: p.m1, al: p.al } : null;
  }
  console.log(`holders-pnl ${cid.slice(0, 10)} n=${n} wallets=${addrs.length} inline=${missing.length} bg=${stale.length}`);
  return json({ cid, n, rankedAt, asOf, outcomes }, 200, 60);
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
    try { await warmHoldersPnl(env); } catch (err) { console.error(`warm failed: ${err.message}`); }
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method !== 'GET') return json({ error: 'method' }, 405);
    if (url.pathname === '/history') return history(env, url);
    if (url.pathname === '/markets') return listMarkets(env, url);
    if (url.pathname === '/status') return status(env);
    if (url.pathname === '/holders-pnl') return holdersPnl(env, url, ctx);
    return json({ error: 'not_found' }, 404);
  },
};
