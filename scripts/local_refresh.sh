#!/usr/bin/env bash
#
# Local rescue pipeline — replicates one update-data CI run on this machine.
#
# Used when GitHub Actions is down (the 2026-08-06 outage): the refresh runner
# (scripts/refresh_runner.js, launchd com.polymarket.refreshrunner) spawns this
# on POST /run from the site-router's /api/refresh endpoint.
#
# Same steps and the same atomic upload order as .github/workflows/update-data.yml:
# stage traders.csv → previous state down from R2 → fetch → wm_index →
# immutable snap/ → flat copies → metadata.json pointer LAST.
#
# Uploads go through wrangler's cached OAuth (the runner CI's R2 S3 keys live
# only in GitHub secrets). SNAPSHOT_ID = newest existing snap id + 1 so numeric
# ordering vs GitHub run ids survives and CI's keep-newest-5 prune stays correct.
# No pruning here — the next healthy CI run prunes.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

WRANGLER="npx --yes wrangler"
TRADERS_DIR="${TRADERS_DIR:-$HOME/projects/polymarket-traders}"
LOG_PREFIX() { date -u '+%Y-%m-%dT%H:%M:%SZ'; }
say() { echo "[$(LOG_PREFIX)] $*"; }

# ── trader list (private repo; pull fresh, clone on first use) ────────────────
if [ -d "$TRADERS_DIR/.git" ]; then
  git -C "$TRADERS_DIR" pull -q || say "traders pull failed — using existing checkout"
else
  gh repo clone shtanga0x/polymarket-traders "$TRADERS_DIR" -- -q
fi
mkdir -p data
cp "$TRADERS_DIR/traders.csv" data/traders.csv

# ── snapshot id: newest referenced snap + 1 (keeps ids monotonic vs CI) ───────
max_id=0
for s in core watch; do
  id=$($WRANGLER r2 object get "polymarket-data/$s/metadata.json" --remote --pipe 2>/dev/null \
      | node -e "let d='';process.stdin.on('data',c=>d+=c).on('end',()=>{try{const m=JSON.parse(d).snapshot;console.log(m?m.split('/')[1]:0)}catch{console.log(0)}})")
  if [ "${id:-0}" -gt "$max_id" ] 2>/dev/null; then max_id=$id; fi
done
export SNAPSHOT_ID=$((max_id + 1))
say "snapshot id: $SNAPSHOT_ID"

put() { # put <key> <file> <cache-control>
  $WRANGLER r2 object put "$1" --file "$2" --remote \
    --content-type application/json --cache-control "$3" >/dev/null 2>&1 \
    || { say "UPLOAD FAILED: $1"; return 1; }
}

for SITE in core watch; do
  say "── $SITE: previous state"
  mkdir -p "out/$SITE/data"
  for f in aggregated_portfolio.json trader_portfolios.json; do
    $WRANGLER r2 object get "polymarket-data/$SITE/$f" --remote --pipe \
      > "out/$SITE/data/$f" 2>/dev/null || say "no previous $f (first run?)"
  done

  say "── $SITE: fetch"
  NODE_OPTIONS='--max-old-space-size=1024' node scripts/fetch_data.js --site "$SITE"

  say "── $SITE: watermark index"
  node scripts/wm_index.js --site "$SITE" || say "wm_index skipped"

  say "── $SITE: upload"
  SRC="out/$SITE/data"
  for f in aggregated_portfolio.json trader_portfolios.json recent_changes.json wm_marks.json wm_offsets.json; do
    if [ -f "$SRC/$f" ]; then put "polymarket-data/$SITE/snap/$SNAPSHOT_ID/$f" "$SRC/$f" "public, max-age=31536000, immutable"; fi
  done
  for f in aggregated_portfolio.json trader_portfolios.json recent_changes.json bot_feed.json previous_portfolio.json wm_marks.json wm_offsets.json; do
    if [ -f "$SRC/$f" ]; then put "polymarket-data/$SITE/$f" "$SRC/$f" "public, max-age=60"; fi
  done
  # metadata LAST — the atomic pointer flip
  put "polymarket-data/$SITE/metadata.json" "$SRC/metadata.json" "public, max-age=15"
  say "── $SITE: published"
done

say "local refresh complete"
