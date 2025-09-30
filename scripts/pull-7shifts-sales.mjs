// scripts/pull-7shifts-sales.mjs
//
// Pulls Projected & Actual totals from 7shifts for a given week,
// then writes to Firestore at workspaces/{WORKSPACE}/weeks/{weekISO}.
//
// ENV (set via GitHub Secrets/Variables or local):
//   FIREBASE_SA_JSON      (secret)  -> the full service account JSON string
//   SEVENSHIFTS_TOKEN     (secret)  -> OAuth2 access token ("Bearer ...")
//   COMPANY_ID            (var)     -> your 7shifts company id
//   LOCATION_ID           (var)     -> your 7shifts location id
//   WORKSPACE             (var)     -> e.g. "tulia"  (defaults to "tulia")
//   INPUT_WEEK_OF         (optional)-> any date in the target week (YYYY-MM-DD)
//   RUN_PREV_WEEK         (optional)-> "true" to also refresh the previous week
//   SALES_ARE_DOLLARS     (optional)-> "true" if your API returns dollars (skip /100)
//
// CLI override: --week=YYYY-MM-DD  (any date in target week)

import axios from "axios";
import admin from "firebase-admin";

// --- Helpers ---
const DAYS = ["mon","tue","wed","thu","fri","sat","sun"];
function pad(n){ return String(n).padStart(2,"0"); }
function toISO(d){ return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`; }
function mondayOf(dateStr){ 
  const d = dateStr ? new Date(dateStr) : new Date();
  const day = d.getDay();              // 0 Sun..6 Sat
  const diff = (day + 6) % 7;          // days since Monday
  d.setDate(d.getDate() - diff);
  d.setHours(0,0,0,0);
  return d;
}
function addDays(d, n){ const x = new Date(d); x.setDate(x.getDate()+n); x.setHours(0,0,0,0); return x; }
function money(n){ return (Math.round(n*100)/100).toFixed(2); }

// --- Env & setup ---
const FIREBASE_SA_JSON  = process.env.FIREBASE_SA_JSON;
const TOKEN             = process.env.SEVENSHIFTS_TOKEN;
const COMPANY_ID        = process.env.COMPANY_ID;
const LOCATION_ID       = process.env.LOCATION_ID;
const WORKSPACE         = process.env.WORKSPACE || "tulia";
const INPUT_WEEK_OF     = (process.env.INPUT_WEEK_OF || "").trim() || null;
const CLI_WEEK          = (process.argv.find(a=>a.startsWith("--week="))||"").split("=")[1] || null;
const RUN_PREV_WEEK     = String(process.env.RUN_PREV_WEEK||"").toLowerCase()==="true";
const SALES_ARE_DOLLARS = String(process.env.SALES_ARE_DOLLARS||"").toLowerCase()==="true";

if (!FIREBASE_SA_JSON) throw new Error("Missing FIREBASE_SA_JSON");
if (!TOKEN)            throw new Error("Missing SEVENSHIFTS_TOKEN");
if (!COMPANY_ID)       throw new Error("Missing COMPANY_ID");
if (!LOCATION_ID)      throw new Error("Missing LOCATION_ID");

const sa = JSON.parse(FIREBASE_SA_JSON);
if (!admin.apps.length){
  admin.initializeApp({ credential: admin.credential.cert(sa) });
}
const db = admin.firestore();

// --- 7shifts fetch ---
async function fetchDailySalesAndLabor({ companyId, locationId, startISO, endISO }){
  const url = "https://api.7shifts.com/v2/reports/daily_sales_and_labor"; // GET
  const res = await axios.get(url, {
    headers: { Authorization: `Bearer ${TOKEN}` },
    params: {
      company_id: companyId,
      location_id: locationId,
      start_date: startISO,
      end_date: endISO
    },
    timeout: 30000
  });
  const rows = res.data?.data || [];
  const byDate = new Map();
  for (const r of rows){
    // 7shifts commonly returns currency as integer "cents" (see sample in docs).
    // If your tenant returns dollars, set SALES_ARE_DOLLARS=true to skip /100.
    const toDollars = (x)=> SALES_ARE_DOLLARS ? Number(x||0) : Number(x||0)/100;
    byDate.set(r.date, {
      projected: toDollars(r.projected_sales),
      actual:    toDollars(r.actual_sales),
    });
  }
  return byDate;
}

// --- Build week rows for our front-end ---
function emptyRow(){ return { mon:"",tue:"",wed:"",thu:"",fri:"",sat:"",sun:"", total:"" }; }

function buildWeekRows(weekMon, byDate){
  const rowProj = emptyRow();
  const rowAct  = emptyRow();
  let totP = 0, totA = 0;

  for (let i=0;i<7;i++){
    const d = addDays(weekMon, i);
    const iso = toISO(d);
    const entry = byDate.get(iso) || { projected: 0, actual: 0 };
    const key = DAYS[i];
    rowProj[key] = money(entry.projected);
    rowAct[key]  = money(entry.actual);
    totP += entry.projected;
    totA += entry.actual;
  }
  rowProj.total = money(totP);
  rowAct.total  = money(totA);
  return { rowProj, rowAct };
}

// --- Firestore write ---
async function writeWeek({ workspace, weekISO, rowProj, rowAct }){
  const ref = db.collection("workspaces").doc(workspace).collection("weeks").doc(weekISO);
  await ref.set({
    data: {
      proj_sales:  rowProj,
      actual_sales:rowAct
    },
    meta: { weekOf: weekISO }
  }, { merge: true });
  console.log(`✔ Wrote ${workspace}/weeks/${weekISO}`);
}

// --- Orchestrate one week ---
async function runForWeek(weekAnyDateISO){
  const weekMon = mondayOf(weekAnyDateISO);
  const weekISO = toISO(weekMon);
  const startISO = weekISO;
  const endISO   = toISO(addDays(weekMon, 6));

  console.log(`Fetching 7shifts sales for ${startISO} → ${endISO} (company ${COMPANY_ID}, location ${LOCATION_ID})`);
  const byDate = await fetchDailySalesAndLabor({
    companyId: COMPANY_ID,
    locationId: LOCATION_ID,
    startISO, endISO
  });
  const { rowProj, rowAct } = buildWeekRows(weekMon, byDate);
  await writeWeek({ workspace: WORKSPACE, weekISO, rowProj, rowAct });
}

// --- Main ---
(async ()=>{
  const baseISO = toISO(mondayOf(CLI_WEEK || INPUT_WEEK_OF || toISO(new Date())));
  // current week
  await runForWeek(baseISO);
  // optionally also refresh previous week (helps finish late POS syncs on Mondays)
  if (RUN_PREV_WEEK){
    const prevISO = toISO(addDays(new Date(baseISO), -7));
    await runForWeek(prevISO);
  }
  process.exit(0);
})().catch(err=>{
  console.error("✖ pull-7shifts-sales failed:", err.response?.data || err);
  process.exit(1);
});


