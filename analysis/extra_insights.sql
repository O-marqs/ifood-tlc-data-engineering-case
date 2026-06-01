-- Extra analytical insights for the iFood NYC TLC case.
-- Scope: yellow taxi only, using gold yellow datasets.
--
-- If the views are not registered yet, create them from parquet first:
--
-- CREATE OR REPLACE TEMP VIEW gold_yellow_trips
-- USING parquet
-- OPTIONS (path "data/gold/gold_yellow_trips");
--
-- 1. Receita media por hora do dia.
-- Helps identify hours with higher average ticket and revenue concentration.
-- Spark SQL AVG and SUM ignore null total_amount values by default.
SELECT
  reference_year,
  reference_month,
  pickup_hour,
  ROUND(AVG(total_amount), 2) AS avg_total_amount,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  COUNT(*) AS total_trips
FROM gold_yellow_trips
GROUP BY reference_year, reference_month, pickup_hour
ORDER BY reference_year, reference_month, pickup_hour;

-- 2. Volume de corridas por dia.
-- Shows daily demand behavior and can reveal seasonality or operational anomalies.
SELECT
  pickup_date,
  COUNT(*) AS total_trips
FROM gold_yellow_trips
GROUP BY pickup_date
ORDER BY pickup_date;

-- 3. Distribuicao de corridas por forma de pagamento.
-- Useful to understand payment mix and prioritize reconciliation or product analysis.
SELECT
  reference_year,
  reference_month,
  payment_type,
  COUNT(*) AS total_trips,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY reference_year, reference_month), 2)
    AS trip_share_percentage
FROM gold_yellow_trips
GROUP BY reference_year, reference_month, payment_type
ORDER BY reference_year, reference_month, total_trips DESC;

-- 4. Relacao agregada entre distancia e valor total por faixas de distancia.
-- Gives a simple view of pricing behavior by distance band without modeling complexity.
SELECT
  CASE
    WHEN trip_distance IS NULL THEN 'unknown'
    WHEN trip_distance < 1 THEN '00_<1_mile'
    WHEN trip_distance < 3 THEN '01_1_to_3_miles'
    WHEN trip_distance < 5 THEN '02_3_to_5_miles'
    WHEN trip_distance < 10 THEN '03_5_to_10_miles'
    ELSE '04_10_plus_miles'
  END AS distance_bucket,
  COUNT(*) AS total_trips,
  ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
  ROUND(AVG(total_amount), 2) AS avg_total_amount,
  ROUND(SUM(total_amount), 2) AS total_revenue
FROM gold_yellow_trips
GROUP BY
  CASE
    WHEN trip_distance IS NULL THEN 'unknown'
    WHEN trip_distance < 1 THEN '00_<1_mile'
    WHEN trip_distance < 3 THEN '01_1_to_3_miles'
    WHEN trip_distance < 5 THEN '02_3_to_5_miles'
    WHEN trip_distance < 10 THEN '03_5_to_10_miles'
    ELSE '04_10_plus_miles'
  END
ORDER BY distance_bucket;

-- 5. Top horarios por receita total.
-- Highlights the strongest revenue windows for operational and business planning.
SELECT
  reference_year,
  reference_month,
  pickup_hour,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  COUNT(*) AS total_trips
FROM gold_yellow_trips
GROUP BY reference_year, reference_month, pickup_hour
ORDER BY total_revenue DESC
LIMIT 20;

-- 6. Duracao media das corridas por mes.
-- Tracks trip duration profile and can help detect traffic, routing or data quality changes.
-- AVG ignores null trip_duration_minutes values by default.
SELECT
  reference_year,
  reference_month,
  ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes,
  COUNT(*) AS total_trips
FROM gold_yellow_trips
GROUP BY reference_year, reference_month
ORDER BY reference_year, reference_month;
