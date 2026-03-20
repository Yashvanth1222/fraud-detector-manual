import XLSX from 'xlsx';

export function parseGeoComplyExport(buffer) {
  const wb = XLSX.read(buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rawRows = XLSX.utils.sheet_to_json(ws, { defval: '' });

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
