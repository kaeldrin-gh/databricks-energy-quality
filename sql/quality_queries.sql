-- Quality-dashboard queries (Databricks SQL).
-- Replace ${catalog}.${schema} with your values (defaults: workspace.energy_quality).

-- 1) Latest check results
SELECT check_name, status, detail, checked_at
FROM ${catalog}.${schema}.quality_report
WHERE checked_at = (SELECT max(checked_at) FROM ${catalog}.${schema}.quality_report)
ORDER BY check_name;

-- 2) Check history (last 30 runs, one row per check and run)
SELECT checked_at, check_name, status, metric
FROM ${catalog}.${schema}.quality_report
ORDER BY checked_at DESC
LIMIT 120;

-- 3) Freshness in hours (should stay under the 26h SLA)
SELECT round((unix_timestamp(current_timestamp()) - unix_timestamp(max(delivery_ts))) / 3600, 1)
       AS age_hours
FROM ${catalog}.${schema}.silver_prices;

-- 4) Daily prices with negative-hour flags
SELECT day, hours, avg_price_eur_mwh, min_price_eur_mwh, max_price_eur_mwh, negative_hours
FROM ${catalog}.${schema}.gold_daily
WHERE day >= current_date() - INTERVAL 30 DAYS
ORDER BY day DESC;

-- 5) Silver uniqueness invariant: expect zero rows
SELECT region, delivery_ts, count(*) AS n
FROM ${catalog}.${schema}.silver_prices
GROUP BY region, delivery_ts
HAVING count(*) > 1
LIMIT 20;
