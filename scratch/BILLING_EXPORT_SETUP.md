# GCP billing export → BigQuery setup

Goal: pipe your daily GCP billing data into a BigQuery dataset, so you can
query SKU-level costs (including the elusive "egress" SKUs) instead of
trusting the Cloud Console UI which only shows top-level services.

Cost of setup: **£0**.
Cost of querying the resulting table: pennies at most — the table is
single-digit MB per month. The query at the bottom of this doc scans
under 10 MB, well within the BigQuery free tier (1 TB/month).

---

## Step 1 — Enable the export (Cloud Console)

In your browser:

1. Open <https://console.cloud.google.com/billing>
2. Click your billing account name (it'll be something like
   "My Billing Account" or your project name).
3. In the left sidebar, click **"Billing export"**.
4. Under the **"BigQuery export"** tab, find the **"Standard usage cost"**
   section and click **"Edit settings"**.
5. Choose:
   - **Project**: your existing GCP project (the one running the VPS)
   - **Dataset**: create a new one called `billing_export`
     (location: `EU` or `US` — match wherever you usually keep BQ data)
6. Click **Save**.

Repeat steps 4-6 for **"Detailed usage cost"** if it's available — it
gives extra columns (per-resource breakdown) at no extra cost. Some
billing accounts don't expose it; if you don't see it, skip.

You'll need **Billing Account Administrator** permission to do this. On a
personal project where you set up the billing account, you already have it.

---

## Step 2 — Wait ~24 hours

The first export populates roughly a day after enabling. You can come
back to this and check whether data has flowed.

To verify (from the VPS or any machine with `bq` installed):

```bash
bq ls billing_export
```

You should see a table like `gcp_billing_export_v1_<YOUR-BILLING-ID>`.
If the table doesn't exist yet, wait another 12h.

---

## Step 3 — The cost-check query

Save this as `scratch/billing_check.sql` (already created — see below).
Run it any time, especially during/after the WN2 backfill:

```bash
bq query --use_legacy_sql=false --maximum_bytes_billed=10000000 \
  < scratch/billing_check.sql
```

The `--maximum_bytes_billed=10000000` (10 MB) cap is a safety net — the
query should scan well under that, but if it ever exceeds, BQ will refuse
to run rather than charging you.

Expected output: one row per (service, SKU) combo over the last 7 days
with total cost. If WeatherNext egress is genuinely sponsor-paid, you'll
see Cloud Storage rows with `cost = 0` even when usage is multi-GiB.
**Any row with a non-zero "Network Egress" or "Multi-region" SKU is the
thing to investigate immediately.**

---

## Step 4 — Optional: nightly Telegram alert

Once the backfill is running, add a daily cron on the VPS:

```bash
# Edit crontab
crontab -e

# Add line (runs 09:00 UTC daily, after the previous day's billing has flowed)
0 9 * * * cd ~/Polyweather && source .venv/bin/activate && python scripts/billing_alert.py
```

If you want this, ping me and I'll write `scripts/billing_alert.py` —
it'd query the table, sum yesterday's Cloud Storage cost, and ping
Telegram if > $0.10.

---

## What you're looking for during the backfill

When you start the WN2 historic backfill (~5 TiB for 1 year), run the
query each morning. Expected SKUs that **should appear and should cost 0**:

- `Storage Bytes Egress: Standard Storage US Multi-region`
- `Network Internet Egress from Americas to Americas`
- (possibly) `Class A Operations` (read ops, normally pennies)

If the egress SKU shows non-zero cost, **stop the backfill immediately**:

```bash
# On the VPS, kill the backfill process
pkill -f "backfill_weathernext"
```

Then investigate before resuming.

---

## Reference

- Billing export docs: <https://cloud.google.com/billing/docs/how-to/export-data-bigquery>
- Schema (what columns the table has): <https://cloud.google.com/billing/docs/how-to/export-data-bigquery-tables>
- Pricing: the export itself is free; the BQ storage costs $0.02/GiB-month
  for active storage. Your billing table will be <10 MB → effectively $0.
