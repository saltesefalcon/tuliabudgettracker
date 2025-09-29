// scripts/fetch_sales.js
// Node 18+ required (GitHub Actions runner is fine). Uses built-in fetch.
// Secrets required: SEVENSHIFTS_TOKEN, FIREBASE_SA_JSON

const WORKSPACE = process.env.WORKSPACE || "tulia";
const COMPANY_ID = process.env.COMPANY_ID || "283376";
const LOCATION_ID = process.env.LOCATION_ID || "351442";
const WEEK_OF_ENV = process.env.WEEK_OF || ""; // YYYY-MM-DD (Monday) or blank

const TOKEN = process.env.SEVENSHIFTS_TOKEN;
const SA_JSON = process.env.FIREBASE_SA_JSON;

if (!TOKEN) {
  console.error("Missing SEVENSHIFTS_TOKEN env.");
  process.exit(2);
}
if (!SA_JSON) {
  console.error("Missing FIREBASE_SA_JSON env.");
  process.exit(2);
}

/* ---------- time helpers ---------- */
function parseISO(s) {
  const [y, m, d] = s.split("-").map(Number);
  const dt = new Date(Date.UTC(y, (m || 1) - 1, (d || 1)));
  dt.setUTCHours(0, 0, 0, 0);
  return dt;
}
function toISO(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}
function startOfWeekMonday(d) {
  const copy = new Date(d.getTime());
  // JS getUTCDay: 0=Sun..6=Sat. We want Monday as 0.
  const dow = copy.getUTCDay();
  const diff = (dow + 6) % 7; // days since Monday
  copy.setUTCDate(copy.getUTCDate() - diff);
  copy.setUTCHours(0, 0, 0, 0);
  return copy;
}
function addDays(d, n) {
  const out = new Date(d.getTime());
  out.setUTCDate(out.getUTCDate() + n);
  out.setUTCHours(0, 0, 0, 0);
  return out;
}
function money(n) {
  if (n == null || isNaN(n)) return "0.00";
  return (Math.round(Number(n) * 100) / 100).toFixed(2);
}

/* ---------- build week range ---------- */
const today = new Date();
const weekStart = WEEK_OF_ENV ? startOfWeekMonday(parseISO(WEEK_OF_ENV)) : startOfWeekMonday(today);
const weekEnd = addDays(weekStart, 6);
const WEEK_OF = toISO(weekStart);
const START = toISO(weekStart);
const END = toISO(weekEnd);

console.log(`Week: ${WEEK_OF}  Range: ${START} → ${END}`);
console.log(`Company: ${COMPANY_ID}  Location: ${LOCATION_ID}`);

/* ---------- 7shifts fetch ---------- */
const headers = {
  Authorization: `Bearer ${TOKEN}`,
  Accept: "application/json",
};

async function tryFetch(url) {
  try {
    const r = await fetch(url, { headers });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    const text = await r.text();
    try {
      return JSON.parse(text);
    } catch {
      throw new Error("Non-JSON response");
    }
  } catch (e) {
    console.warn(`Fetch failed for ${url}: ${e.message}`);
    return null;
  }
}

/**
 * Normalize various potential shapes to a map:
 * {
 *   'YYYY-MM-DD': { actual: number|null, proj: number|null }
 * }
 */
function normalizeSales(json) {
  const out = {};
  if (!json) return out;

  // Extract array-ish rows from common containers
  let rows = null;
  if (Array.isArray(json)) rows = json;
  else if (Array.isArray(json.data)) rows = json.data;
  else if (Array.isArray(json.items)) rows = json.items;
  else if (Array.isArray(json.sales)) rows = json.sales;
  else if (json.result && Array.isArray(json.result)) rows = json.result;

  if (!rows) return out;

  const dateKeys = ["date", "day", "business_date", "businessDate"];
  const actualKeys = ["actual", "actuals", "actual_sales", "net_sales", "netSales", "sales", "total"];
  const projKeys = ["projected", "projected_sales", "forecast", "forecasted", "forecast_sales", "proj"];

  for (const row of rows) {
    // date
    let dt = null;
    for (const k of dateKeys) {
      if (row[k]) {
        dt = String(row[k]).substring(0, 10);
        break;
      }
    }
    if (!dt) continue;

    // actual value
    let actual = null;
    for (const k of actualKeys) {
      if (row[k] != null) {
        actual = Number(row[k]);
        break;
      }
    }

    // projected value
    let proj = null;
    for (const k of projKeys) {
      if (row[k] != null) {
        proj = Number(row[k]);
        break;
      }
    }

    // If the row is a combined object like { date, actual: X, projected: Y }
    // we already captured both. If it’s a one-sided stream (e.g., forecast only),
    // we’ll still capture what exists.
    out[dt] = { actual, proj };
  }
  return out;
}

async function fetchWeek() {
  // Try a combined daily sales endpoint first (common pattern)
  const candidates = [
    // daily sales (often returns actual + projected fields)
    `https://api.7shifts.com/v2/company/${COMPANY_ID}/locations/${LOCATION_ID}/sales?start=${START}&end=${END}&group_by=day`,
    // forecast-only (if present)
    `https://api.7shifts.com/v2/company/${COMPANY_ID}/locations/${LOCATION_ID}/forecast?start=${START}&end=${END}&group_by=day`,
    // legacy projected endpoint (if present)
    `https://api.7shifts.com/v2/company/${COMPANY_ID}/locations/${LOCATION_ID}/projected_sales?start=${START}&end=${END}&group_by=day`,
  ];

  let combined = {};
  for (const url of candidates) {
    const json = await tryFetch(url);
    if (!json) continue;
    const map = normalizeSales(json);
    Object.assign(combined, map);
  }

  // Build day-wise rows for the 7-day window
  const days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
  const projRow = { mon: "", tue: "", wed: "", thu: "", fri: "", sat: "", sun: "", total: "" };
  const actRow  = { mon: "", tue: "", wed: "", thu: "", fri: "", sat: "", sun: "", total: "" };

  let projTot = 0, actTot = 0;
  for (let i = 0; i < 7; i++) {
    const dISO = toISO(addDays(weekStart, i));
    const key = days[i];
    const rec = combined[dISO] || {};
    if (rec.proj != null && !isNaN(rec.proj)) { projRow[key] = money(rec.proj); projTot += Number(rec.proj); }
    if (rec.actual != null && !isNaN(rec.actual)) { actRow[key]  = money(rec.actual); actTot  += Number(rec.actual); }
  }
  projRow.total = money(projTot);
  actRow.total  = money(actTot);

  return { projRow, actRow };
}

/* ---------- Firestore admin ---------- */
const admin = await (async () => {
  const adminMod = await import("firebase-admin");
  return adminMod.default || adminMod;
})();

if (!admin.apps.length) {
  const creds = JSON.parse(SA_JSON);
  admin.initializeApp({
    credential: admin.credential.cert(creds),
  });
}
const db = admin.firestore();

/* ---------- Run ---------- */
(async () => {
  console.log("Fetching 7shifts daily sales…");
  const { projRow, actRow } = await fetchWeek();

  console.log("Projected:", projRow);
  console.log("Actual:   ", actRow);

  const docRef = db.collection("workspaces").doc(WORKSPACE).collection("weeks").doc(WEEK_OF);

  const payload = {
    meta: { weekOf: WEEK_OF, lastAutoPull: new Date().toISOString() },
    data: {
      proj_sales: projRow,
      actual_sales: actRow,
    },
  };

  await docRef.set(payload, { merge: true });
  console.log(`Firestore updated: workspaces/${WORKSPACE}/weeks/${WEEK_OF}`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
