#!/usr/bin/env python3
"""
Pull daily projected & actual sales from 7shifts and upsert the current week's
doc in Firestore at: workspaces/{WORKSPACE}/weeks/{YYYY-MM-DD}

- Uses SERVICE ACCOUNT creds (admin) via FIREBASE_SA_JSON secret written by the workflow
- 7shifts token from SEVENSHIFTS_TOKEN secret
- COMPANY_ID & LOCATION_ID from workflow env (strings)
- Optional: FOOD_PCT_ENABLED / FOOD_PCT — not needed by the app (the UI can
  display a food % view), but included here in case you later want to write a
  derived row.
"""

import os, json, sys, decimal, datetime as dt
from decimal import Decimal
from zoneinfo import ZoneInfo
import requests

from google.cloud import firestore

API_ROOT = "https://api.7shifts.com/v2"

def env(name, default=None):
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def money(x):
    try:
        return f"{Decimal(x).quantize(Decimal('0.01'))}"
    except Exception:
        try:
            return f"{Decimal(str(x)).quantize(Decimal('0.01'))}"
        except Exception:
            return "0.00"

def start_of_week_monday(now_local: dt.datetime) -> dt.date:
    # Monday=0 .. Sunday=6
    return (now_local - dt.timedelta(days=(now_local.weekday()))).date()

def to_iso(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def fetch_sales(token: str, company_id: str, location_id: str,
                start_date: str, end_date: str) -> list[dict]:
    """
    Returns a list of daily rows between start_date..end_date inclusive.
    We try the common 7shifts sales endpoint shape:
      GET /v2/company/{company_id}/locations/{location_id}/sales?start=YYYY-MM-DD&end=YYYY-MM-DD
    Fallbacks handle small field-name variations (actual/projected keys).
    """
    # A few tenants use a slightly different path. Try the most common one first.
    urls = [
        f"{API_ROOT}/company/{company_id}/locations/{location_id}/sales?start={start_date}&end={end_date}",
        f"{API_ROOT}/locations/{location_id}/sales?start={start_date}&end={end_date}",
    ]

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    last_error = None
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=60)
            if r.status_code == 200:
                js = r.json()
                # Normalize out: prefer js["data"] list, else treat top-level as list
                if isinstance(js, dict) and "data" in js and isinstance(js["data"], list):
                    return js["data"]
                if isinstance(js, list):
                    return js
                # Some responses nest under "sales"
                if isinstance(js, dict) and "sales" in js and isinstance(js["sales"], list):
                    return js["sales"]
                raise ValueError(f"Unexpected response shape from {url}: {str(js)[:200]}")
            else:
                last_error = f"{url} -> HTTP {r.status_code} {r.text[:200]}"
        except Exception as e:
            last_error = f"{url} -> {e}"
    raise RuntimeError(f"All sales endpoints failed. Last error: {last_error}")

def pick(row: dict, *keys, default=0):
    """Return the first present numeric value from keys."""
    for k in keys:
        if k in row and row[k] is not None:
            try:
                return float(row[k])
            except Exception:
                try:
                    return float(str(row[k]))
                except Exception:
                    pass
    return float(default)

def build_week_shape(monday: dt.date, day_map: dict[str, float]) -> dict:
    keys = ["mon","tue","wed","thu","fri","sat","sun"]
    out = {}
    total = Decimal("0")
    for i, k in enumerate(keys):
        d = (monday + dt.timedelta(days=i))
        iso = to_iso(d)
        val = Decimal(str(day_map.get(iso, 0.0)))
        out[k] = money(val)
        total += val
    out["total"] = money(total)
    return out

def main():
    # --- config from env ---
    token       = env("SEVENSHIFTS_TOKEN")
    company_id  = env("COMPANY_ID")
    location_id = env("LOCATION_ID")
    workspace   = env("WORKSPACE", "tulia")
    tzname      = env("LOCAL_TZ", "America/Toronto")
    if not token or not company_id or not location_id:
        print("Missing env: SEVENSHIFTS_TOKEN, COMPANY_ID, LOCATION_ID", file=sys.stderr)
        sys.exit(2)

    # current local time & week window (Mon..Sun) in your local tz
    now_local = dt.datetime.now(ZoneInfo(tzname))
    monday    = start_of_week_monday(now_local)
    sunday    = monday + dt.timedelta(days=6)
    start_iso = to_iso(monday)
    end_iso   = to_iso(sunday)

    print(f"Week window: {start_iso} .. {end_iso}")

    # --- pull from 7shifts ---
    rows = fetch_sales(token, company_id, location_id, start_iso, end_iso)

    # rows usually look like: { "date":"YYYY-MM-DD", "actual":123.45, "projected": 234.56, ... }
    actual_map = {}
    proj_map   = {}
    for r in rows:
        # find a date field
        date_s = r.get("date") or r.get("business_date") or r.get("day") or r.get("sales_date")
        if not date_s:
            continue
        # find numeric fields (be forgiving with names)
        actual_v = pick(r, "actual", "actual_sales", "actual_total", "actuals", "actuals_total", default=0)
        proj_v   = pick(r, "projected", "projected_sales", "projected_total", "forecast", "forecast_total", default=0)
        actual_map[date_s] = actual_map.get(date_s, 0.0) + actual_v
        proj_map[date_s]   = proj_map.get(date_s, 0.0) + proj_v

    proj_week   = build_week_shape(monday, proj_map)
    actual_week = build_week_shape(monday, actual_map)

    # Optional derived (not required by the app; the UI computes deltas)
    delta = {}
    for k in ["mon","tue","wed","thu","fri","sat","sun"]:
        delta[k] = money(Decimal(actual_week.get(k, "0.00")) - Decimal(proj_week.get(k, "0.00")))
    delta["total"] = money(Decimal(actual_week["total"]) - Decimal(proj_week["total"]))

    # --- Firestore upsert ---
    client = firestore.Client()  # uses GOOGLE_APPLICATION_CREDENTIALS set by workflow

    doc_path = f"workspaces/{workspace}/weeks/{start_iso}"
    doc_ref  = client.document(doc_path)

    # merge only sales-related rows; leave purchases/settings intact
    payload = {
        "meta": {
            "updatedAt": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "source": "7shifts",
        },
        "data": {
            "proj_sales":   proj_week,
            "actual_sales": actual_week,
            "sales_delta":  delta,
        }
    }

    print("Upserting:", doc_path)
    doc_ref.set(payload, merge=True)
    print("Done.")

if __name__ == "__main__":
    main()
