import XLSX from 'xlsx';

export function parseGeoComplyExport(buffer) {
  // Debug: log file signature to verify it's a valid xlsx
  const header = buffer.slice(0, 4).toString('hex');
  console.log(`File header bytes: ${header} (expect 504b0304 for xlsx)`);
  console.log(`File size: ${buffer.length} bytes`);

  let wb;
  try {
    wb = XLSX.read(buffer, { type: 'buffer' });
  } catch (e) {
    console.error('XLSX.read failed with buffer, trying array:', e.message);
    try {
      wb = XLSX.read(new Uint8Array(buffer), { type: 'array' });
    } catch (e2) {
      console.error('XLSX.read failed with array too:', e2.message);
      return { records: [], deviceToUsers: {}, userToDevices: {}, sharedDevices: {}, blockedUsers: new Set(), blockedDevices: new Set() };
    }
  }

  console.log(`Sheets found: ${wb.SheetNames}`);
  const ws = wb.Sheets[wb.SheetNames[0]];

  // First try with default headers (row 1)
  let rawRows = XLSX.utils.sheet_to_json(ws, { defval: '' });
  console.log(`Raw rows parsed: ${rawRows.length}`);
  if (rawRows.length > 0) console.log(`First row keys: ${Object.keys(rawRows[0]).join(', ')}`);

  // If headers are empty (__EMPTY), the real headers are in row 2 — skip row 1
  if (rawRows.length > 0 && Object.keys(rawRows[0]).some(k => k.startsWith('__EMPTY'))) {
    console.log('Detected blank first row — re-parsing with header row offset');
    const allRows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
    // Find the actual header row (first row with "User ID" in it)
    let headerIdx = -1;
    for (let i = 0; i < Math.min(allRows.length, 5); i++) {
      if (allRows[i].some(cell => String(cell).includes('User ID'))) {
        headerIdx = i;
        break;
      }
    }
    if (headerIdx >= 0) {
      const headers = allRows[headerIdx].map(h => String(h).trim());
      console.log(`Found headers at row ${headerIdx}: ${headers.join(', ')}`);
      rawRows = [];
      for (let i = headerIdx + 1; i < allRows.length; i++) {
        const obj = {};
        headers.forEach((h, idx) => { obj[h] = allRows[i][idx] ?? ''; });
        rawRows.push(obj);
      }
      console.log(`Re-parsed ${rawRows.length} rows with correct headers`);
      if (rawRows.length > 0) console.log(`First row keys: ${Object.keys(rawRows[0]).join(', ')}`);
    }
  }

  const records = [];
  for (const row of rawRows) {
    const userId = row['User ID'] || row['user_id'] || '';
    if (!userId) continue;

    records.push({
      user_id: String(userId).trim(),
      transaction_count: parseInt(row['User Total Transactions Count']) || 0,
      user_blocked: String(row['User Block Status'] || '').toLowerCase() === 'yes',
      device_uuid: String(row['Device UUID'] || '').trim(),
      device_solution: String(row['Device Solution'] || '').trim(),
      device_os: String(row["Device's Operation System"] || row['Device OS'] || '').trim(),
      device_blocked: String(row['Device Block Status'] || '').toLowerCase() === 'yes',
      mac_address: String(row['Device MAC Address'] || '').trim(),
      first_seen: row['First Seen'] || '',
      last_seen: row['Last Seen'] || ''
    });
  }

  // Build device-to-users map
  const deviceToUsers = {};
  for (const r of records) {
    if (!r.device_uuid) continue;
    if (!deviceToUsers[r.device_uuid]) deviceToUsers[r.device_uuid] = new Set();
    deviceToUsers[r.device_uuid].add(r.user_id);
  }

  // Build user-to-devices map
  const userToDevices = {};
  for (const r of records) {
    if (!userToDevices[r.user_id]) userToDevices[r.user_id] = new Set();
    if (r.device_uuid) userToDevices[r.user_id].add(r.device_uuid);
  }

  // Find shared devices (2+ users on same device)
  const sharedDevices = {};
  for (const [uuid, users] of Object.entries(deviceToUsers)) {
    if (users.size >= 2) {
      sharedDevices[uuid] = [...users];
    }
  }

  // Build blocked sets
  const blockedUsers = new Set(records.filter(r => r.user_blocked).map(r => r.user_id));
  const blockedDevices = new Set(records.filter(r => r.device_blocked).map(r => r.device_uuid));

  return {
    records,
    deviceToUsers,
    userToDevices,
    sharedDevices,
    blockedUsers,
    blockedDevices
  };
}

export function usersShareDevice(geoData, userA, userB) {
  const devicesA = geoData.userToDevices[userA];
  const devicesB = geoData.userToDevices[userB];
  if (!devicesA || !devicesB) return { shared: false, devices: [] };

  const shared = [];
  for (const d of devicesA) {
    if (devicesB.has(d)) shared.push(d);
  }
  return { shared: shared.length > 0, devices: shared };
}
