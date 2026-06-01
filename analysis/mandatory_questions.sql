-- Mandatory iFood case questions for yellow taxi.
-- These queries use only gold yellow datasets.
--
-- If the tables/views are not registered yet, create temporary views first:
--
-- CREATE OR REPLACE TEMP VIEW gold_monthly_yellow_kpis
-- USING parquet
-- OPTIONS (path "data/gold/gold_monthly_yellow_kpis");
--
-- CREATE OR REPLACE TEMP VIEW gold_hourly_passenger_kpis
-- USING parquet
-- OPTIONS (path "data/gold/gold_hourly_passenger_kpis");

-- Question 1:
-- Qual a media de valor total recebido em um mes considerando todos os yellow taxis da frota?
--
-- This query uses gold_monthly_yellow_kpis.
-- Spark SQL AVG ignores null total_amount values by default. The aggregation in gold
-- keeps this behavior. No additional total_amount IS NOT NULL filter is applied here
-- because null handling is already explicit in the KPI definition.
-- total_trips counts all yellow records available in each month group.
SELECT
  reference_year,
  reference_month,
  ROUND(avg_total_amount, 2) AS avg_total_amount,
  total_trips,
  ROUND(total_revenue, 2) AS total_revenue
FROM gold_monthly_yellow_kpis
ORDER BY reference_year, reference_month;

-- Question 2:
-- Qual a media de passageiros por cada hora do dia que pegaram taxi no mes de maio
-- considerando todos os taxis da frota?
--
-- This query uses gold_hourly_passenger_kpis.
-- Spark SQL AVG ignores null passenger_count values by default. The aggregation in gold
-- keeps this behavior. No additional passenger_count IS NOT NULL filter is applied here
-- because null handling is already explicit in the KPI definition.
-- total_trips counts all yellow records grouped in each pickup_hour.
SELECT
  pickup_hour,
  ROUND(avg_passenger_count, 2) AS avg_passenger_count,
  total_trips
FROM gold_hourly_passenger_kpis
WHERE reference_year = 2023
  AND reference_month = '05'
ORDER BY pickup_hour;
