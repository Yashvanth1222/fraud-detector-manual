import { usersShareDevice } from './geocomply.js';

function parseTimestamps(str) {
  if (!str) return [];
  return String(str).split(',').map(s => s.trim()).filter(Boolean).map(s => {
    const d = new Date(s.includes('UTC') || s.includes('+') || s.includes('-') ? s : s + ' UTC');
    return isNaN(d.getTime()) ? null : d;
  }).filter(Boolean);
}

function minTimingDelta(ts1, ts2) {
  let min = Infinity;
  for (const a of ts1) {
    for (const b of ts2) {
      const delta = Math.abs(a.getTime() - b.getTime()) / 1000;
      if (delta < min) min = delta;
    }
  }
  return min === Infinity ? null : min;
}

export function scorePairs(tradingRows, geoData) {
  // Group trading rows by market
  const byMarket = {};
  for (const r of tradingRows) {
    if (!byMarket[r.market_id]) byMarket[r.market_id] = [];
    byMarket[r.market_id].push(r);
  }

  const results = [];
  const seenPairs = new Set();

  for (const [marketId, marketRows] of Object.entries(byMarket)) {
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

            const pairKey = [u1.user_id, u2.user_id].sort().join('|') + '|' + marketId;
            if (seenPairs.has(pairKey)) continue;
            seenPairs.add(pairKey);

            // Compute all signals
            const deviceCheck = usersShareDevice(geoData, u1.user_id, u2.user_id);

            const ts1 = parseTimestamps(u1.placed_at_list);
            const ts2 = parseTimestamps(u2.placed_at_list);
            const timingDelta = minTimingDelta(ts1, ts2);

            const exposureDiff = Math.abs(u1.user_market_exposure - u2.user_market_exposure);
            const samePromo = u1.promo_code === u2.promo_code && u1.promo_code !== '';

            const u1Counterparties = String(u1.counterparty_user_ids || '').split(',').map(s => s.trim()).filter(Boolean);
            const u2Counterparties = String(u2.counterparty_user_ids || '').split(',').map(s => s.trim()).filter(Boolean);
            const isDirectCounterparty = u1Counterparties.includes(u2.user_id) || u2Counterparties.includes(u1.user_id);

            const bothBalanceDrained = u1.balance < 1 && u2.balance < 1;
            const bothSingleTrade = u1.num_trades === 1 && u2.num_trades === 1;
            const exposureInPromoRange = u1.user_market_exposure >= 45 && u1.user_market_exposure <= 60 && u2.user_market_exposure >= 45 && u2.user_market_exposure <= 60;
            const exposureMatch = exposureDiff <= 5;

            // Composite score
            let score = 0;

            // Device sharing — strongest signal (30 pts)
            if (deviceCheck.shared) score += 30;

            // Direct counterparty (25 pts)
            if (isDirectCounterparty) score += 25;

            // Opposite sides same market (10 pts) — always true here since we're iterating opposite sides
            score += 10;

            // Same promo (8 pts)
            if (samePromo) score += 8;

            // Timing (8 pts max)
            if (timingDelta !== null) {
              if (timingDelta <= 10) score += 8;
              else if (timingDelta <= 60) score += 5;
              else if (timingDelta <= 300) score += 2;
            }

            // Balance drained (5 pts)
            if (bothBalanceDrained) score += 5;

            // Single trade (5 pts)
            if (bothSingleTrade) score += 5;

            // Exposure match (5 pts)
            if (exposureMatch || exposureInPromoRange) score += 5;

            // Blocked user/device bonus (4 pts)
            const eitherBlocked = geoData.blockedUsers.has(u1.user_id) || geoData.blockedUsers.has(u2.user_id)
              || [...(geoData.userToDevices[u1.user_id] || [])].some(d => geoData.blockedDevices.has(d))
              || [...(geoData.userToDevices[u2.user_id] || [])].some(d => geoData.blockedDevices.has(d));
            if (eitherBlocked) score += 4;

            // Cap at 100
            score = Math.min(score, 100);

            results.push({
              market_id: marketId,
              market_description: u1.market_description || u2.market_description,
              user_a: {
                user_id: u1.user_id,
                promo_code: u1.promo_code,
                side: u1.outcome_side,
                exposure: u1.user_market_exposure,
                balance: u1.balance,
                num_trades: u1.num_trades,
                pnl: u1.total_pnl
              },
              user_b: {
                user_id: u2.user_id,
                promo_code: u2.promo_code,
                side: u2.outcome_side,
                exposure: u2.user_market_exposure,
                balance: u2.balance,
                num_trades: u2.num_trades,
                pnl: u2.total_pnl
              },
              score,
              signals: {
                shared_device: deviceCheck.shared,
                shared_device_uuids: deviceCheck.devices,
                direct_counterparty: isDirectCounterparty,
                same_promo: samePromo,
                promo_code: samePromo ? u1.promo_code : null,
                timing_delta_seconds: timingDelta !== null ? Math.round(timingDelta) : null,
                both_balance_drained: bothBalanceDrained,
                both_single_trade: bothSingleTrade,
                exposure_diff: Math.round(exposureDiff * 100) / 100,
                exposure_in_promo_range: exposureInPromoRange,
                either_blocked: eitherBlocked
              }
            });
          }
        }
      }
    }
  }

  // Sort by score descending
  results.sort((a, b) => b.score - a.score);
  return results;
}
