# Share-history backfill (one-off, run 2026-10-04)

Rebuilds per-trader share history BEFORE a market's live baseline (`markets.first_ts`)
by replaying roster activity backward from the baseline snapshot. Run from a working
dir holding `meta_{core,watch}.json` (R2 metadata) and `d_{markets,traders,holdings}.json`
(D1 dumps), then import `backfill.sql` with `wrangler d1 execute pm-share-history --remote --file`.

1. `node fetch_act.cjs 90` — page each roster trader's /activity back N days via the `end=`
   cursor (offset= is capped at 5000; end= is not). Heavy market makers: ~6k events/day.
2. `node extend.cjs 180` — extend cached files further back; `node topup.cjs` — up to now.
3. `node recon.cjs` — per market: TRADE (±size on outcome), SPLIT (+both), MERGE (−both).
   Accurate start = latest of: fetch window, last pre-baseline CONVERSION in the market's
   event by a roster trader (negRisk converts move sibling markets; the feed can't say
   which), and the last point where a backward replay goes negative beyond
   max(1 share, 0.2% of the key's peak) (= missing/transfer events). Nothing earlier is written.
4. `node tosql.cjs` → `backfill.sql` (rows strictly before the baseline + new first_ts).

2026-10-04 result (180-day window): core 117/181 markets clean for the full 180 days,
watch 92/177; 22 core / 32 watch markets < 7 days (mostly negRisk conversion-heavy events).
Verified: 0 discontinuities at the baseline join, 0 negative sizes.
