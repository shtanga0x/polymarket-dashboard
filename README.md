# polymarket-dashboard

Unified codebase for the Polymarket trader dashboards:

| Site | Tiers shown | Data feed |
|---|---|---|
| [core.shtanga.xyz](https://core.shtanga.xyz) | tier 1 | `data.shtanga.xyz/core` |
| [watch.shtanga.xyz](https://watch.shtanga.xyz) | tier 1 + 2 | `data.shtanga.xyz/watch` |

One codebase, two deployments, **one trader list**. Everything is shared except
per-site config. Supersedes the archived `polymarket_core` and `polymarket_watch`
repos (their git histories were 99% five-minute data-snapshot commits, 1.9–3.9 GB).

### Trader tiers (single source of truth)

`data/traders.csv` is the only trader list. Each row's `tier` column decides where
it shows; each site picks its tiers via `include_tiers` in `config/<site>.json`:

- **tier 1** → core (and watch)
- **tier 2** → watch only
- **tier 0** → delisted: kept in the file as a record, excluded from every
  dashboard and all accounting

## Layout

```
config/core.json, watch.json   per-site config: branding, data URLs, R2 buckets,
                               polling/concurrency limits, referral code
data/traders.csv               single unified trader list; tier column gates each site (the ONLY regularly edited data file)
site/                          shared frontend (index.html, app.js, style.css)
site/assets/site-config.js     generated at deploy from config/<site>.json — never edit
scripts/                       shared data pipeline (fetch → aggregate → upload)
worker/                        Cloudflare worker: cron + manual refresh dispatcher
.github/workflows/
  update-data.yml              data refresh, matrix over [core, watch] in parallel
  deploy-site.yml              site deploy to both R2 buckets on site/config changes
```

## How changes flow

- **Edit frontend or pipeline code** → push to `main` → `deploy-site.yml` deploys the
  identical bundle to BOTH buckets (with per-site `site-config.js` injected). One fix,
  both sites. No more hand-porting between repos.
- **Add/remove a trader** → edit `data/traders.csv` (set its `tier`) → picked up by
  the next data refresh (≤2 min). No site deploy involved. Set `tier` to `0` to
  delist a trader from both dashboards while keeping the row as a record.
- **Change a site setting** (poll rate, branding, referral, limits) → edit
  `config/<site>.json` → site deploy + next data run pick it up.
- **Data refresh cadence**: Cloudflare worker cron dispatches every 2 min; workflow
  cron every 5 min as fallback; the site's refresh button hits the worker's
  `/trigger-update` (1-min cooldown). One run updates both sites as parallel jobs.

## Data publishing (atomic + cache-friendly)

Each run uploads to R2:
1. `…/<site>/snap/<run_id>/*.json` — immutable snapshot, `max-age=31536000, immutable`
2. `…/<site>/*.json` — flat copies (backward compat + next run's previous-state source)
3. `…/<site>/metadata.json` — written **last**, `max-age=15`; contains `snapshot: "snap/<run_id>"`

The frontend polls only the tiny `metadata.json`; when the snapshot id changes it fetches
the big files once from the immutable snapshot URLs (browser-cached, no re-downloads).
Readers can never observe a half-written update — the metadata pointer flips only after
the full snapshot is uploaded. Previous-run state (recent-changes diffing, PnL
cache) is downloaded from R2 at the start of each run.

## Secrets

| Where | Name | Purpose |
|---|---|---|
| GitHub repo | `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` | R2 uploads |
| GitHub repo | `PROXIES` (optional) | datacenter proxies, one `host:port:user:pass` per line |
| Worker | `GITHUB_PAT` | fine-grained PAT, Actions: write on this repo |

## Local development

```bash
npm install
node scripts/fetch_data.js --site watch     # writes out/watch/data/
node scripts/build_site_config.js watch     # writes site/assets/site-config.js
# then serve site/ with any static server and symlink out/watch/data → site/data
```
