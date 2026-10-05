-- pm-share-history D1 schema. Change-only share history for markets that have
-- appeared in a site's top-200 portfolio rows. Apply with:
--   npx wrangler d1 execute pm-share-history --remote --file schema.sql

-- One row per (site, market). Tracking starts the first time the market shows
-- up in the site's top 200 and never restarts: once closed it stays archived.
CREATE TABLE IF NOT EXISTS markets (
  id           INTEGER PRIMARY KEY,
  site         TEXT    NOT NULL,           -- 'core' | 'watch'
  cid          TEXT    NOT NULL,           -- conditionId
  title        TEXT,
  slug         TEXT,
  event_slug   TEXT,
  end_date     TEXT,
  outcomes     TEXT,                       -- JSON {"<outcomeIndex>": "<name>"}
  status       TEXT    NOT NULL DEFAULT 'active',  -- 'active' | 'closed'
  close_reason TEXT,                       -- 'resolved' (redeemable) | 'closed' (Gamma)
  first_ts     INTEGER NOT NULL,           -- unix s, first snapshot tracked
  closed_ts    INTEGER,
  UNIQUE (site, cid)
);
CREATE INDEX IF NOT EXISTS markets_site_status ON markets (site, status);

-- Trader address dictionary (keeps history rows narrow).
CREATE TABLE IF NOT EXISTS traders (
  id   INTEGER PRIMARY KEY,
  addr TEXT NOT NULL UNIQUE
);

-- The archive: one row whenever a trader's share count on a tracked outcome
-- CHANGES (including the baseline at tracking start and 0 on a full exit).
-- The holding is a step function — the size holds until the next row.
CREATE TABLE IF NOT EXISTS holdings (
  mid  INTEGER NOT NULL,
  oi   INTEGER NOT NULL,
  tid  INTEGER NOT NULL,
  ts   INTEGER NOT NULL,
  size REAL    NOT NULL,
  PRIMARY KEY (mid, oi, tid, ts)
) WITHOUT ROWID;

-- Latest known size per tracked (market, outcome, trader) — the diff base.
-- Rows are dropped when their market closes; `holdings` keeps the history.
CREATE TABLE IF NOT EXISTS current (
  mid  INTEGER NOT NULL,
  oi   INTEGER NOT NULL,
  tid  INTEGER NOT NULL,
  size REAL    NOT NULL,
  PRIMARY KEY (mid, oi, tid)
) WITHOUT ROWID;

-- Per-site ingest cursor: last snapshot processed + last Gamma closed-check.
CREATE TABLE IF NOT EXISTS cursors (
  site       TEXT PRIMARY KEY,
  snapshot   TEXT,
  ts         INTEGER,                      -- data time of that snapshot
  gamma_ts   INTEGER,
  ingests    INTEGER NOT NULL DEFAULT 0,
  rows_total INTEGER NOT NULL DEFAULT 0
);

-- Top-holders PnL (/holders-pnl): account-wide PnL per wallet (15-min TTL) and
-- the assembled per-market result (5-min TTL).
CREATE TABLE IF NOT EXISTS pnl_cache (
  addr TEXT PRIMARY KEY,
  ts   INTEGER NOT NULL,
  d1   REAL, w1 REAL, m1 REAL, al REAL       -- NULL = that window failed to load
);
CREATE TABLE IF NOT EXISTS holders_pnl_cache (
  cid  TEXT PRIMARY KEY,
  ts   INTEGER NOT NULL,
  body TEXT NOT NULL
);
-- Member-triggered holders-PnL refreshes: per-market lock + last outcome (drives cooldown / status UI).
CREATE TABLE IF NOT EXISTS holders_refresh (
  cid         TEXT PRIMARY KEY,
  started_ts  INTEGER NOT NULL,
  finished_ts INTEGER,
  n           INTEGER,
  status      TEXT,
  detail      TEXT
);
CREATE INDEX IF NOT EXISTS holders_refresh_started ON holders_refresh (started_ts);
-- Site-wide queue of wallets awaiting a forced (cache-busted) PnL recompute.
CREATE TABLE IF NOT EXISTS pnl_queue (
  addr   TEXT PRIMARY KEY,
  enq_ts INTEGER NOT NULL,
  tries  INTEGER NOT NULL DEFAULT 0,
  claimed INTEGER NOT NULL DEFAULT 0       -- unix s of a worker's claim (0 = free); added live via ALTER 2026-10-05
);
CREATE INDEX IF NOT EXISTS pnl_queue_enq ON pnl_queue (enq_ts);
