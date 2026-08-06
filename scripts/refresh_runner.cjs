#!/usr/bin/env node
/**
 * Refresh runner — tiny local HTTP service behind pm-refresh.shtanga.xyz
 * (Cloudflare tunnel → 127.0.0.1:8902; launchd com.polymarket.refreshrunner).
 *
 * The site-router's POST /api/refresh forwards here (member-gated + 2-min
 * freshness check happen THERE; this end only checks the shared key). One run
 * at a time via a pid lockfile that survives runner restarts.
 *
 *   POST /run  (X-Refresh-Key)  → 202 {status:'started'} | 409 already_running
 *   GET  /health                → 200 {ok, running}
 *
 * Env: REFRESH_KEY (required), PORT (default 8902).
 */

'use strict';

const http = require('http');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

const PORT = Number(process.env.PORT || 8902);
const KEY = process.env.REFRESH_KEY || '';
const REPO = path.resolve(__dirname, '..');
const SCRIPT = path.join(REPO, 'scripts', 'local_refresh.sh');
const LOCK = path.join(os.tmpdir(), 'pm_local_refresh.pid');
const LOG_DIR = path.join(REPO, 'logs');
const LOG = path.join(LOG_DIR, 'local_refresh.log');

if (!KEY) { console.error('REFRESH_KEY unset — refusing to start'); process.exit(1); }
fs.mkdirSync(LOG_DIR, { recursive: true });

function runningPid() {
  try {
    const pid = Number(fs.readFileSync(LOCK, 'utf8').trim());
    if (pid > 0) { process.kill(pid, 0); return pid; }   // throws if gone
  } catch (_) { /* no lock or stale */ }
  return null;
}

function json(res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/health') {
    return json(res, 200, { ok: true, running: runningPid() !== null });
  }
  if (req.method !== 'POST' || req.url !== '/run') return json(res, 404, { error: 'not_found' });
  if (req.headers['x-refresh-key'] !== KEY) return json(res, 403, { error: 'forbidden' });

  if (runningPid() !== null) return json(res, 409, { status: 'already_running' });

  const out = fs.openSync(LOG, 'a');
  fs.writeSync(out, `\n===== refresh started ${new Date().toISOString()} =====\n`);
  const child = spawn('/bin/bash', [SCRIPT], {
    cwd: REPO,
    detached: true,
    stdio: ['ignore', out, out],
    env: { ...process.env, HOME: os.homedir() },
  });
  fs.writeFileSync(LOCK, String(child.pid));
  child.on('exit', (code) => {
    fs.writeSync(out, `===== refresh exited code=${code} ${new Date().toISOString()} =====\n`);
    fs.closeSync(out);
    try { fs.unlinkSync(LOCK); } catch (_) {}
  });
  child.unref();
  json(res, 202, { status: 'started' });
});

server.listen(PORT, '127.0.0.1', () => console.log(`refresh runner on 127.0.0.1:${PORT}`));
