// scripts/fetch_sales.js
//
// Pulls Projected & Actual totals from 7shifts for a given week,
// then writes to Firestore at workspaces/{ws}/weeks/{weekISO}.
//
// Env needed:
//   FIREBASE_SA_JSON  (secret)
//   SEVENSHIFTS_TOKEN (secret)
//   WORKSPACE, COMPANY_ID, LOCATION_ID
//   INPUT_WEEK_OF (optional YYYY-MM-DD; defaults to current Monday)

const admin = require('firebase-admin');
const axios = require('axios');
const dayjs = require('dayjs');

function getMondayISO(d = dayjs()) {
  const monday = d.startOf('week').add(1, 'day'); // week starts Sunday in dayjs; +1 = Monday
  return monday.format('YYYY-MM-DD');
}

function parseServiceAccount() {
  const raw = process.env.FIREBASE_SA_JSON;
  if (!raw) throw new Error('FIREBASE_SA_JSON is missing');
  try { return JSON.parse(raw); }
  catch { throw new Error('FIREBASE_SA_JSON is not valid JSON'); }
}

async function fetch7shiftsTotals({ token, companyId, locationId, weekOf }) {
  const client = axios.create({
    baseURL: 'https://api.7shifts.com/v2',
    headers: { Authorization: `Bearer ${token}` },
    timeout: 30000,
  });

  // === Projected totals (Forecast) ===
  // Endpoint variations exist; this one is commonly available:
  // GET /company/{companyId}/forecasts?week_of=YYYY-MM-DD&location_id=...
  // If your account uses /budgets instead, 7shifts will return the same
  // shape; the code below accepts both and maps to { mon..sun,total }.

  const params = { week_of: weekOf, location_id: locationId };

  // Try forecasts, fall back to budgets
  let proj;
  try {
    const r = await client.get(`/company/${companyId}/forecasts`, { params });
    proj = r.data;
  } catch {
    const r = await client.get(`/company/${companyId}/budgets`, { params });
    proj = r.data;
  }

  // === Actual totals ===
  // Many orgs expose actuals via budgets; if your tenant exposes a
  // dedicated sales endpoint, this still works because we only need
  // daily totals. The API generally returns an array with daily rows.

  let act;
  try {
    const r = await client.get(`/company/${companyId}/budgets`, { params });
    act = r.data;
  } catch (e) {
    throw new Error('Could not fetch Actuals from 7shifts (budgets). ' + e.message);
  }

  // Normalize to our structure
  const normalize = (payload) => {
    // Accept either {data:[{date, projected, actual}...]} or similar
    const days = { mon: 0, tue: 0, wed: 0, thu: 0, fri: 0, sat: 0, sun: 0 };
    const monday = dayjs(weekOf);
    const byDate = Array.isArray(payload?.data) ? payload.data : payload;

    for (const row of byDate || []) {
      const d = dayjs(row.date || row.day || row.dt || row.on || row.for);
      if (!d.isValid()) continue;
      const idx = d.diff(monday, 'day');
      if (idx < 0 || idx > 6) continue;
      const key = ['mon','tue','wed','thu','fri','sat','sun'][idx];
      // prefer “projected” for proj set, “actual” for act set
      days[key] = Number(row.projected ?? row.total ?? row.amount ?? 0);
    }
    const total = Object.values(days).reduce((a,b)=>a+Number(b||0),0);
    return { ...Object.fromEntries(Object.entries(days).map(([k,v])=>[k, (Number(v)||0).toFixed(2)])), total: total.toFixed(2) };
  };

  // If the APIs already returned a ready daily object, keep it
  const projNorm = proj?.data && (proj.data.mon || proj.data['mon_hours_open']) ? proj.data
                 : normalize(proj);
  const actNorm  = act?.data && (act.data.mon || act.data['mon_hours_open']) ? act.data
                 : normalize(act);

  return { projected: projNorm, actual: actNorm };
}

async function main() {
  const sa = parseServiceAccount();
  if (admin.apps.length === 0) {
    admin.initializeApp({ credential: admin.credential.cert(sa) });
  }
  const db = admin.firestore();

  const ws = process.env.WORKSPACE || 'tulia';
  const companyId = process.env.COMPANY_ID;
  const locationId = process.env.LOCATION_ID;
  const token = process.env.SEVENSHIFTS_TOKEN;
  if (!companyId || !locationId || !token) {
    throw new Error('COMPANY_ID, LOCATION_ID or SEVENSHIFTS_TOKEN missing');
  }

  const weekOf = (process.env.INPUT_WEEK_OF && dayjs(process.env.INPUT_WEEK_OF).isValid())
    ? dayjs(process.env.INPUT_WEEK_OF).format('YYYY-MM-DD')
    : getMondayISO();

  const { projected, actual } = await fetch7shiftsTotals({ token, companyId, locationId, weekOf });

  const docRef = db.collection('workspaces').doc(ws).collection('weeks').doc(weekOf);

  // Merge just the sales block; your web app calculates budgets from these.
  await docRef.set({
    workspace: ws,
    weekOf,
    data: {
      proj_sales: projected,
      actual_sales: actual,
    },
    // not used by app logic, but useful for debugging
    fetched_at: admin.firestore.FieldValue.serverTimestamp(),
    source: '7shifts',
  }, { merge: true });

  console.log(`✔ Wrote sales for ${weekOf} to workspaces/${ws}/weeks/${weekOf}`);
}

main().catch(err => {
  console.error('❌ fetch_sales failed:', err.stack || err.message || err);
  process.exit(1);
});

