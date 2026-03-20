const WEBHOOK_URL = process.env.N8N_WEBHOOK_URL;

// C0ANANSNVTK = ops channel (user IDs to check + xlsx upload)
// C0AMRE7RFJP = alerts channel (confirmed fraud results)

export async function notifyPendingCheck(userIds) {
  if (!WEBHOOK_URL || userIds.length === 0) return;

  const payload = {
    type: 'pending_geocomply_check',
    channel: 'C0ANANSNVTK',
    user_ids: userIds,
    user_id_list: userIds.join('\n'),
    count: userIds.length
  };

  try {
    const res = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!res.ok) console.error(`Pending check webhook failed (${res.status})`);
    else console.log(`Sent pending check for ${userIds.length} users to ops channel`);
  } catch (e) {
    console.error('Pending check webhook error:', e.message);
  }
}

export async function notifyFraudResult(result) {
  if (!WEBHOOK_URL) return;

  const payload = {
    type: 'confirmed_fraud',
    channel: 'C0AMRE7RFJP',
    market_description: result.market_description,
    market_id: result.market_id,
    score: result.score,
    user_ids: [result.user_a.user_id, result.user_b.user_id],
    signals: result.signals,
    user_a: result.user_a,
    user_b: result.user_b,
    reasoning: buildReasoning(result)
  };

  try {
    const res = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!res.ok) console.error(`Fraud result webhook failed (${res.status})`);
    else console.log(`Sent fraud alert: ${result.market_description} (score ${result.score})`);
  } catch (e) {
    console.error('Fraud result webhook error:', e.message);
  }
}

function buildReasoning(result) {
  const s = result.signals;
  const parts = [];

  if (s.shared_device) parts.push(`SHARED DEVICE (${s.shared_device_uuids.join(', ')})`);
  if (s.direct_counterparty) parts.push('direct counterparty (traded against each other)');
  if (s.same_promo) parts.push(`same promo code (${s.promo_code})`);
  if (s.timing_delta_seconds !== null) parts.push(`${s.timing_delta_seconds}s timing between trades`);
  if (s.both_balance_drained) parts.push(`both balances drained ($${result.user_a.balance.toFixed(3)} and $${result.user_b.balance.toFixed(3)})`);
  if (s.both_single_trade) parts.push('both made exactly 1 trade');
  if (s.exposure_in_promo_range) parts.push(`exposures in promo max range ($${result.user_a.exposure.toFixed(2)} vs $${result.user_b.exposure.toFixed(2)})`);
  if (s.either_blocked) parts.push('user or device blocked by GeoComply');

  return `Score ${result.score}/100. Signals: ${parts.join(', ')}.`;
}
