-- Last 7 days of SKU-level billing for the project.
-- Run with:
--   bq query --use_legacy_sql=false --maximum_bytes_billed=10000000 < scratch/billing_check.sql
--
-- IMPORTANT: replace `YOUR-PROJECT.billing_export.gcp_billing_export_v1_XXXXXX`
-- with your actual table name. Find it with: bq ls billing_export

SELECT
  service.description     AS service,
  sku.description         AS sku,
  SUM(usage.amount)       AS usage_amount,
  usage.unit              AS usage_unit,
  ROUND(SUM(cost), 4)     AS cost_usd,
  currency,
  ROUND(SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)), 4) AS credits_usd,
  ROUND(SUM(cost + IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)), 4) AS net_cost_usd
FROM
  `YOUR-PROJECT.billing_export.gcp_billing_export_v1_XXXXXX`
WHERE
  _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY
  service, sku, usage_unit, currency
HAVING
  SUM(usage.amount) > 0
ORDER BY
  net_cost_usd DESC,
  service,
  sku;
