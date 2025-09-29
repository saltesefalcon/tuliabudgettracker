// scripts/pull-7shifts-sales.mjs
// Pull daily Projected & Actual sales from 7shifts and upsert to Firestore
// Uses: FIREBASE_SA_JSON (secret), SEVENSHIFTS_TOKEN (secret),
//       COMPANY_ID, LOCATION_ID, WORKSPACE, TIMEZONE (repository variables)

import { initializeApp, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { DateTime } from "luxon";

const {
  FIREBASE_SA_JSON,
  SEVENSHIFTS_TOKEN,
  COMPANY_ID,
  LOCATION_ID,
  WORKSPACE = "tulia",
  TIMEZONE = "America/Toronto",
} = process.env;

if (!FIREBASE_SA_JSON) throw new Error("FIREBASE_SA_JSON missing");
if (!SEVENSHIFTS_TOKEN) throw new Error("SEVENSHIFTS_TOKEN missing");
if (!COMPANY_ID) throw new Error("COMPANY_ID missing");
if (!LOCATION_ID) throw new Error("LOCATION_ID missing");

// ---------- Time helpers (Monday..Sunday in your local timezone) ----------
const now = DateTime.now().setZone(TIMEZONE);
const monday = now.startOf("week").plus({ days: 1 }).startOf("day"); // Mon
const week = Array.from({ length: 7 }, (_, i) => monday.plus({ days: i }));
const startISO = week[0].toISODate();
const endISO = week[6].toISODate();
const mondayISO = startISO;

const DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
const dayKeyFromISO = (iso) => {
  const d = DateTime.fromISO(iso, { zone: TIMEZONE });
  const idx = (d.weekday + 6) % 7; // Mon=1..Sun=7 => 0..6
  return DAY_KEYS[idx];
};

// ---------- Firestore init ----------
const sa = JSON.parse(FIREBASE_SA_JSON);
initializeApp({ credential: cert(sa) });
const db = getFirestore();

// ---------- 7shifts fetch ----------
const authHeader = { Authorization: `Bearer ${SEVENSHIFTS_TOKEN}` };

// We try a small set of known/seen paths to be robust across tenants.
const candidateUrls = [
  // most common
  `https://api.7shifts.com/v2/company/${COMPANY_ID}/engage/sales?start=${startISO}&end=${endISO}&location_id=${LOCATION_ID}`,
  // alternates if the above 404s on some tenants
  `https://api.7shifts.com/v2/company/${COMPANY_ID}/sales?start=${startISO}&end=${endISO}&location_id=${LOCATION_ID}`,
  `https://api.7shifts.com/v2/company/${COMPANY_ID}/engage/sales?start_date=${startISO}&end_date=${endISO}&location_id=${LOCATION_ID}`,
];

async function fetchFirst200(urls) {
  for (const url of urls) {
    const r = await fetch(url, { headers: authHeader });
    if (r.ok) {
      const j = await r.json().catch(() => ({}));
      return { url, json: j };
    }
  }
  throw new Error("All 7shifts sales endpoints returned non-200.");
}

function getArrayPayload(json) {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.data)) return json.data;
  if (Array.isArray(json?.items)) return json.items;
  if (Array.isArray(json?.result)) return json.result;
  return [];
}

function pickNumber(obj, paths) {
  for (const path of paths) {
    let cur = obj;
    for (const seg of path.split(".")) {
      if (cur == null) break;
      cur = cur[seg];
    }
    const n = Number(cur);
    if (!Number.isNaN(n)) return n;
  }
  return 0;
}

function money(n) {
  return (Math.round(Number(n || 0) * 100) / 100).toFixed(2);
}

function blankRow() {
  return { mon: "", tue: "", wed: "", thu: "", fri: "", sat: "", sun: "", total: "" };
}

async function run() {
  console.log(`Pulling 7shifts sales for ${startISO}..${endISO} (workspace=${WORKSPACE})`);
  const { url, json } = await fetchFirst200(candidateUrls);
  console.log(`OK ${url}`);

  const rows = getArrayPayload(json);
  if (!rows.length) throw new Error("7shifts response did not contain any rows.");

  // Heuristic mapping: { date, projected, actual } with a few fallbacks
  const proj = blankRow();
  const act  = blankRow();
  let sumP = 0, sumA = 0;

  for (const it of rows) {
    const dateISO =
      it.date || it.business_date || it.sales_date || it.day || it.dt || it.start || it.start_date;
    if (!dateISO) continue;

    const k = dayKeyFromISO(String(dateISO).substring(0, 10));
    if (!k) continue;

    const projected = pickNumber(it, [
      "projected",
      "projection",
      "forecast",
      "projected.total",
      "projected.net_sales",
      "forecast.total",
      "forecasted",
    ]);

    const actual = pickNumber(it, [
      "actual",
      "actuals",
      "actual_sales",
      "actual.total",
      "actual.net_sales",
      "net",
      "sales",
      "total",
    ]);

    proj[k] = money(projected);
    act[k]  = money(actual);
  }

  // Totals
  sumP = DAY_KEYS.reduce((a, d) => a + Number(proj[d] || 0), 0);
  sumA = DAY_KEYS.reduce((a, d) => a + Number(act[d]  || 0), 0);
  proj.total = money(sumP);
  act.total  = money(sumA);

  // Upsert to Firestore
  const ref = db.collection("workspaces").doc(WORKSPACE).collection("weeks").doc(mondayISO);
  await ref.set(
    {
      meta: { weekOf: mondayISO, tz: TIMEZONE, source: "7shifts" },
      data: {
        proj_sales: proj,   // raw totals from 7shifts
        actual_sales: act,  // raw totals from 7shifts
      },
      updatedAt: DateTime.now().toISO(),
    },
    { merge: true }
  );

  console.log("Firestore updated:", ref.path);
}

run().catch((e) => {
  console.error("FAILED:", e?.message || e);
  process.exit(1);
});
