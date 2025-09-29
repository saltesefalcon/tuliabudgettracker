/**
 * Pulls 7shifts projected + actual sales for a week and writes to:
 *   workspaces/{workspace}/weeks/{weekOf}
 *
 * Env:
 *   FIREBASE_SA_JSON (secret)
 *   SEVENSHIFTS_TOKEN (secret)
 *
 * Args:
 *   --week-of YYYY-MM-DD  (Monday; default = current Monday)
 *   --workspace tulia     (default = tulia)
 *   --company   283376
 *   --location  351442
 */
const axios = require("axios");
const admin = require("firebase-admin");
const dayjs = require("dayjs");
const isoWeek = require("dayjs/plugin/isoWeek");
dayjs.extend(isoWeek);

function arg(name, def = "") {
  const i = process.argv.indexOf(name);
  return i > -1 && process.argv[i + 1] ? process.argv[i + 1] : def;
}

// ---------- args & dates ----------
const WORKSPACE = arg("--workspace", "tulia");
const COMPANY_ID = arg("--company");
const LOCATION_ID = arg("--location");
const inputWeek = arg("--week-of", "");
const now = dayjs();
const monday = inputWeek
  ? dayjs(inputWeek)
  : now.isoWeekday() === 1
  ? now.startOf("day")
  : now.isoWeekday(1).startOf("day");
const weekOf = monday.format("YYYY-MM-DD");
const startDate = weekOf;
const endDate = monday.add(6, "day").format("YYYY-MM-DD");

// ---------- firebase init ----------
const saJson = process.env.FIREBASE_SA_JSON;
if (!saJson) throw new Error("Missing FIREBASE_SA_JSON secret");
const creds = JSON.parse(saJson);
if (!admin.apps.length) {
  admin.initializeApp({ credential: admin.credential.cert(creds) });
}
const db = admin.firestore();

// ---------- 7shifts helper ----------
const token = process.env.SEVENSHIFTS_TOKEN;
if (!token) throw new Error("Missing SEVENSHIFTS_TOKEN secret");
const api = axios.create({
  baseURL: "https://api.7shifts.com/v2",
  headers: { Authorization: `Bearer ${token}` },
  timeout: 25000,
});

// Map daily array -> keyed object with totals
function daysToRow(days) {
  const keys = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
  const row = {};
  let total = 0;
  for (let i = 0; i < 7; i++) {
    const v = Number(days[i] || 0);
    row[keys[i]] = v.toFixed(2);
    total += v;
  }
  row.total = total.toFixed(2);
  return row;
}

// NOTE: Endpoints here mirror what you were calling from PowerShell.
// If your 7shifts plan exposes different routes we can swap them in one place.
async function fetchProjected() {
  // Example: Sales projections summary for a week by location
  const r = await api.get(
    `/analytics/sales/projections`,
    { params: { company_id: COMPANY_ID, location_id: LOCATION_ID, start_date: startDate, end_date: endDate } }
  );
  // Expecting numbers per day, in order Mon..Sun
  return daysToRow(r.data?.data?.daily || []);
}

async function fetchActuals() {
  // Example: Actual sales summary for a week by location
  const r = await api.get(
    `/analytics/sales/actuals`,
    { params: { company_id: COMPANY_ID, location_id: LOCATION_ID, start_date: startDate, end_date: endDate } }
  );
  return daysToRow(r.data?.data?.daily || []);
}

async function run() {
  console.log(`Fetching sales for ${weekOf} (location ${LOCATION_ID})…`);
  const [proj, act] = await Promise.all([fetchProjected(), fetchActuals()]);

  // prepare delta row
  const keys = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
  const delta = {};
  let tot = 0;
  keys.forEach((k) => {
    const v = Number(act[k] || 0) - Number(proj[k] || 0);
    delta[k] = v.toFixed(2);
    tot += v;
  });
  delta.total = tot.toFixed(2);

  const payload = {
    meta: { weekOf },
    data: {
      proj_sales: proj,
      actual_sales: act,
      sales_delta: delta,
    },
    fetched_at: admin.firestore.FieldValue.serverTimestamp(),
  };

  const ref = db
    .collection("workspaces")
    .doc(WORKSPACE)
    .collection("weeks")
    .doc(weekOf);

  await ref.set(payload, { merge: true });
  console.log("Wrote:", ref.path);
}

run().catch((e) => {
  console.error(e?.response?.data || e);
  process.exit(1);
});
