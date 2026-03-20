import 'dotenv/config';
import cron from 'node-cron';
import http from 'http';
import { readSheet, appendRows } from './sheets.js';
import { parseGeoComplyExport } from './geocomply.js';
import { scorePairs } from './score.js';
import { notifyPendingCheck, notifyFraudResult, notifyStatus } from './notify.js';

const FRAUD_SHEET_ID = process.env.FRAUD_SHEET_ID;
const ANALYZED_SHEET_ID = process.env.ANALYZED_SHEET_ID;
const SCORE_THRESHOLD = parseInt(process.env.SCORE_THRESHOLD) || 70;
const PORT = parseInt(process.env.PORT) || 3000;
const BATCH_SIZE = 500;

// In-memory state
let pendingTradingData = [];
let userQueue = [];       // full list of suspicious users, batched
let currentBatchIndex = 0; // which batch we're on

function parseRow(r) {
  return {
    user_id: r.user_id,
    promo_code: r.promo_code || '',
    market_id: r.market_id,
    market_description: r.market_description,
    outcome_side: r.outcome_side,
    user_market_exposure: parseFloat(r.user_market_exposure) || 0,
    num_trades: parseInt(r.num_trades) || 0,
    first_trade_in_market: r.first_trade_in_market,
    placed_at_list: r.placed_at_list,
    outcome_statuses: r.outcome_statuses,
    total_pnl: parseFloat(r.total_pnl) || 0,
    any_settled: r.any_settled,
    users_in_market: parseInt(r.users_in_market) || 0,
    balance: parseFloat(r.balance) || 0,
    counterparty_user_ids: r.counterparty_user_ids || '',
    counterparty_types: r.counterparty_types || '',
    is_direct_counterparty_in_sheet: String(r.is_direct_counterparty_in_sheet || '').toLowerCase() === 'true'
  };
}

// ── Send the current batch to Slack ──
async function sendCurrentBatch() {
  if (userQueue.length === 0) return;

  const totalBatches = Math.ceil(userQueue.length / BATCH_SIZE);
  const start = currentBatchIndex * BATCH_SIZE;
  const batch = userQueue.slice(start, start + BATCH_SIZE);

  if (batch.length === 0) {
    console.log('All batches processed!');
    await notifyStatus(`All ${totalBatches} batches processed. ${userQueue.length} total users checked.`);
    userQueue = [];
    currentBatchIndex = 0;
    return;
  }

  console.log(`Sending batch ${currentBatchIndex + 1}/${totalBatches} (${batch.length} users)`);
  await notifyPendingCheck(batch, currentBatchIndex + 1, totalBatches);
}

// ── Check Hex sheet for new users ──
async function checkForNewUsers() {
  console.log(`[${new Date().toISOString()}] Checking Hex sheet for new users...`);

  let analyzedKeys = new Set();
  try {
    const analyzedRows = await readSheet(ANALYZED_SHEET_ID);
    for (const r of analyzedRows) {
      if (r.user_id && r.market_id) analyzedKeys.add(`${r.user_id}|${r.market_id}`);
    }
    console.log(`Loaded ${analyzedKeys.size} already-analyzed keys`);
  } catch {
    console.log('No existing analyzed data');
  }

  const rawRows = await readSheet(FRAUD_SHEET_ID);
  console.log(`Read ${rawRows.length} rows from fraud sheet`);

  const filtered = rawRows.filter(r => {
    if (!r.user_id || !r.market_id) return false;
    if (String(r.is_locked || '').toLowerCase() === 'locked') return false;
    if (analyzedKeys.has(`${r.user_id}|${r.market_id}`)) return false;
    return true;
  });

  if (filtered.length === 0) {
    console.log('No new rows to analyze.');
    return;
  }

  console.log(`${filtered.length} new rows found`);
  const rows = filtered.map(parseRow);
  pendingTradingData = rows;

  // Write ALL rows to AlreadyAnalyzed
  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
  const seen = new Set();
  const analyzedOutput = [];
  for (const r of rows) {
    const key = `${r.user_id}|${r.market_id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    analyzedOutput.push({ user_id: r.user_id, market_id: r.market_id, analyzed_at: now, fraud_score: 0 });
  }
  await appendRows(ANALYZED_SHEET_ID, analyzedOutput);
  console.log(`Wrote ${analyzedOutput.length} rows to AlreadyAnalyzed`);

  // Pre-filter: find suspicious users
  const byMarket = {};
  for (const r of rows) {
    if (!byMarket[r.market_id]) byMarket[r.market_id] = [];
    byMarket[r.market_id].push(r);
  }

  const suspiciousUserIds = new Set();
  for (const [, marketRows] of Object.entries(byMarket)) {
    if (marketRows.length < 2) continue;
    const sides = {};
    for (const r of marketRows) {
      const side = String(r.outcome_side || '').toLowerCase();
      if (!sides[side]) sides[side] = [];
      sides[side].push(r);
    }
    const sideKeys = Object.keys(sides);
    if (sideKeys.length < 2) continue;

    for (let si = 0; si < sideKeys.length; si++) {
      for (let sj = si + 1; sj < sideKeys.length; sj++) {
        for (const u1 of sides[sideKeys[si]]) {
          for (const u2 of sides[sideKeys[sj]]) {
            if (u1.user_id === u2.user_id) continue;
            const samePromo = u1.promo_code === u2.promo_code && u1.promo_code !== '';
            if (!samePromo) continue;

            const ts1 = String(u1.placed_at_list || '').split(',').map(s => new Date(s.trim() + ' UTC')).filter(d => !isNaN(d));
            const ts2 = String(u2.placed_at_list || '').split(',').map(s => new Date(s.trim() + ' UTC')).filter(d => !isNaN(d));
            let minDelta = Infinity;
            for (const a of ts1) for (const b of ts2) minDelta = Math.min(minDelta, Math.abs(a - b) / 1000);

            const timingClose = minDelta <= 300;
            const bothDrained = u1.balance < 1 && u2.balance < 1;
            const exposureMatch = Math.abs(u1.user_market_exposure - u2.user_market_exposure) <= 5;
            const bothSingle = u1.num_trades === 1 && u2.num_trades === 1;

            const signalCount = (timingClose ? 1 : 0) + (bothDrained ? 1 : 0) + (exposureMatch ? 1 : 0) + (bothSingle ? 1 : 0);
            if (signalCount >= 1) {
              suspiciousUserIds.add(u1.user_id);
              suspiciousUserIds.add(u2.user_id);
            }
          }
        }
      }
    }
  }

  const suspiciousList = [...suspiciousUserIds];
  console.log(`${suspiciousList.length} suspicious users to check in GeoComply`);

  if (suspiciousList.length === 0) {
    console.log('No suspicious trading patterns found.');
    return;
  }

  // Set up the queue and send first batch
  userQueue = suspiciousList;
  currentBatchIndex = 0;
  await sendCurrentBatch();
}

// ── Process uploaded GeoComply xlsx ──
async function processGeoComplyFile(buffer) {
  const geoData = parseGeoComplyExport(buffer);
  console.log(`Parsed ${geoData.records.length} GeoComply records`);
  console.log(`Shared devices found: ${Object.keys(geoData.sharedDevices).length}`);

  if (pendingTradingData.length === 0) {
    console.log('No pending trading data. Running fresh sheet check...');
    await checkForNewUsers();
  }

  if (pendingTradingData.length === 0) {
    return { error: 'No trading data to cross-reference', confirmed: 0, suspicious: 0 };
  }

  const results = scorePairs(pendingTradingData, geoData);
  console.log(`Scored ${results.length} pairs`);

  const confirmed = results.filter(r => r.score >= SCORE_THRESHOLD);
  const suspicious = results.filter(r => r.score >= 50 && r.score < SCORE_THRESHOLD);
  console.log(`Confirmed (${SCORE_THRESHOLD}+): ${confirmed.length}`);
  console.log(`Suspicious (50-${SCORE_THRESHOLD - 1}): ${suspicious.length}`);
  console.log(`Clean (<50): ${results.length - confirmed.length - suspicious.length}`);

  for (const result of confirmed) {
    await notifyFraudResult(result);
  }

  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
  const fraudUpdates = [];
  for (const result of confirmed) {
    fraudUpdates.push(
      { user_id: result.user_a.user_id, market_id: result.market_id, analyzed_at: now, fraud_score: result.score },
      { user_id: result.user_b.user_id, market_id: result.market_id, analyzed_at: now, fraud_score: result.score }
    );
  }
  if (fraudUpdates.length > 0) await appendRows(ANALYZED_SHEET_ID, fraudUpdates);

  // Advance to next batch and send it
  const totalBatches = Math.ceil(userQueue.length / BATCH_SIZE);
  currentBatchIndex++;
  const hasMoreBatches = currentBatchIndex < totalBatches;

  if (hasMoreBatches) {
    console.log(`Batch ${currentBatchIndex}/${totalBatches} done. Sending next batch...`);
    await sendCurrentBatch();
  } else if (userQueue.length > 0) {
    console.log('All batches complete!');
    await notifyStatus(`All ${totalBatches} batches processed. Queue complete.`);
    userQueue = [];
    currentBatchIndex = 0;
  }

  return {
    confirmed: confirmed.length,
    suspicious: suspicious.length,
    total_pairs: results.length,
    batch_completed: currentBatchIndex,
    total_batches: totalBatches,
    has_more_batches: hasMoreBatches
  };
}

// ── HTTP server ──
function startServer() {
  const server = http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/health') {
      const totalBatches = Math.ceil(userQueue.length / BATCH_SIZE);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        pending_trading_rows: pendingTradingData.length,
        queue_size: userQueue.length,
        current_batch: currentBatchIndex + 1,
        total_batches: totalBatches
      }));
      return;
    }

    if (req.method === 'POST' && req.url === '/upload-geocomply') {
      const chunks = [];
      req.on('data', chunk => chunks.push(chunk));
      req.on('end', async () => {
        try {
          const body = Buffer.concat(chunks);
          let fileBuffer;
          try {
            const json = JSON.parse(body.toString());
            fileBuffer = Buffer.from(json.file_base64 || json.file || '', 'base64');
          } catch {
            fileBuffer = body;
          }

          if (!fileBuffer || fileBuffer.length === 0) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'No file data received' }));
            return;
          }

          console.log(`Received GeoComply file: ${fileBuffer.length} bytes`);
          const result = await processGeoComplyFile(fileBuffer);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'processed', ...result }));
        } catch (e) {
          console.error('Upload processing error:', e);
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }

    if (req.method === 'POST' && req.url === '/refresh') {
      try {
        await checkForNewUsers();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          status: 'refreshed',
          queue_size: userQueue.length,
          total_batches: Math.ceil(userQueue.length / BATCH_SIZE)
        }));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
      return;
    }

    res.writeHead(404);
    res.end('Not found');
  });

  server.listen(PORT, () => console.log(`HTTP server listening on port ${PORT}`));
}

// ── Main ──
if (process.argv.includes('--once')) {
  checkForNewUsers().catch(e => { console.error('Run failed:', e); process.exit(1); });
} else {
  startServer();
  cron.schedule('*/30 * * * *', () => {
    checkForNewUsers().catch(e => console.error('Sheet check failed:', e));
  });
  checkForNewUsers().catch(e => console.error('Initial check failed:', e));
  console.log('Fraud detector (manual mode) running.');
  console.log(`  - HTTP server: port ${PORT}`);
  console.log('  - Hex sheet check: every 30 minutes');
  console.log('  - Batch size: 500 users per GeoComply check');
}
