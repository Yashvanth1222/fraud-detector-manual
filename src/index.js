import 'dotenv/config';
import cron from 'node-cron';
import http from 'http';
import { readSheet, appendRows } from './sheets.js';
import { parseGeoComplyExport } from './geocomply.js';
import { scorePairs } from './score.js';
import { notifyPendingCheck, notifyFraudResult } from './notify.js';

const FRAUD_SHEET_ID = process.env.FRAUD_SHEET_ID;
const ANALYZED_SHEET_ID = process.env.ANALYZED_SHEET_ID;
const SCORE_THRESHOLD = parseInt(process.env.SCORE_THRESHOLD) || 70;
const PORT = parseInt(process.env.PORT) || 3000;

// In-memory state
let pendingTradingData = [];

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

// ── Check Hex sheet for new users, notify Slack ──
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
  const uniqueUserIds = [...new Set(rows.map(r => r.user_id))];
  console.log(`${uniqueUserIds.length} unique new user IDs`);

  pendingTradingData = rows;

  // Write to AlreadyAnalyzed
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

  // Send user IDs to Slack (ops channel) via n8n
  await notifyPendingCheck(uniqueUserIds);
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

  // Send confirmed fraud to alerts channel via n8n
  for (const result of confirmed) {
    await notifyFraudResult(result);
  }

  // Update AlreadyAnalyzed with fraud scores
  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
  const fraudUpdates = [];
  for (const result of confirmed) {
    fraudUpdates.push(
      { user_id: result.user_a.user_id, market_id: result.market_id, analyzed_at: now, fraud_score: result.score },
      { user_id: result.user_b.user_id, market_id: result.market_id, analyzed_at: now, fraud_score: result.score }
    );
  }
  if (fraudUpdates.length > 0) await appendRows(ANALYZED_SHEET_ID, fraudUpdates);

  return { confirmed: confirmed.length, suspicious: suspicious.length, total_pairs: results.length };
}

// ── HTTP server to receive xlsx uploads from n8n ──
function startServer() {
  const server = http.createServer(async (req, res) => {
    // Health check
    if (req.method === 'GET' && req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', pending_users: pendingTradingData.length }));
      return;
    }

    // Receive xlsx file from n8n
    if (req.method === 'POST' && req.url === '/upload-geocomply') {
      const chunks = [];
      req.on('data', chunk => chunks.push(chunk));
      req.on('end', async () => {
        try {
          const body = Buffer.concat(chunks);

          // Check if it's JSON with base64 file, or raw binary
          let fileBuffer;
          try {
            const json = JSON.parse(body.toString());
            if (json.file_base64) {
              fileBuffer = Buffer.from(json.file_base64, 'base64');
            } else if (json.file) {
              fileBuffer = Buffer.from(json.file, 'base64');
            }
          } catch {
            // Raw binary upload
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

    // Force refresh trading data
    if (req.method === 'POST' && req.url === '/refresh') {
      try {
        await checkForNewUsers();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'refreshed', pending_users: pendingTradingData.length }));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
      return;
    }

    res.writeHead(404);
    res.end('Not found');
  });

  server.listen(PORT, () => {
    console.log(`HTTP server listening on port ${PORT}`);
  });
}

// ── Main ──
if (process.argv.includes('--once')) {
  checkForNewUsers().catch(e => { console.error('Run failed:', e); process.exit(1); });
} else {
  startServer();

  // Check for new users every 30 minutes
  cron.schedule('*/30 * * * *', () => {
    checkForNewUsers().catch(e => console.error('Sheet check failed:', e));
  });

  // Run immediately on startup
  checkForNewUsers().catch(e => console.error('Initial check failed:', e));

  console.log('Fraud detector (manual mode) running.');
  console.log(`  - HTTP server: port ${PORT}`);
  console.log('  - Hex sheet check: every 30 minutes');
  console.log('  - POST /upload-geocomply to process xlsx');
}
