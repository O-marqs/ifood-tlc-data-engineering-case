# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Case Analysis
# MAGIC
# MAGIC Answers the two mandatory iFood case questions from the gold yellow datasets.

# COMMAND ----------

import os
import sys
from pathlib import Path


PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/dbfs/FileStore/ifood_tlc_case/project")
DATA_DIR = os.getenv("DATA_DIR", "/dbfs/FileStore/ifood_tlc_case/data")

SRC_PATH = str(Path(PROJECT_ROOT) / "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

os.environ["DATA_DIR"] = DATA_DIR

print(f"PROJECT_ROOT={PROJECT_ROOT}")
print(f"DATA_DIR={DATA_DIR}")

# COMMAND ----------

from ifood_tlc.paths import get_gold_path


gold_root = get_gold_path()
monthly_path = gold_root / "gold_monthly_yellow_kpis"
hourly_path = gold_root / "gold_hourly_passenger_kpis"

gold_monthly_yellow_kpis = spark.read.parquet(str(monthly_path))
gold_hourly_passenger_kpis = spark.read.parquet(str(hourly_path))

gold_monthly_yellow_kpis.createOrReplaceTempView("gold_monthly_yellow_kpis")
gold_hourly_passenger_kpis.createOrReplaceTempView("gold_hourly_passenger_kpis")

print(f"Loaded monthly KPIs from: {monthly_path}")
print(f"Loaded hourly passenger KPIs from: {hourly_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 1
# MAGIC Qual a média de valor total recebido em um mês considerando todos os yellow táxis da frota?
# MAGIC
# MAGIC Spark SQL `AVG` ignores null `total_amount` values in the gold aggregation.

# COMMAND ----------

spark.sql(
    """
    SELECT
      reference_year,
      reference_month,
      ROUND(avg_total_amount, 2) AS avg_total_amount,
      total_trips,
      ROUND(total_revenue, 2) AS total_revenue
    FROM gold_monthly_yellow_kpis
    ORDER BY reference_year, reference_month
    """
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 2
# MAGIC Qual a média de passageiros por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?
# MAGIC
# MAGIC Spark SQL `AVG` ignores null `passenger_count` values in the gold aggregation.

# COMMAND ----------

spark.sql(
    """
    SELECT
      pickup_hour,
      ROUND(avg_passenger_count, 2) AS avg_passenger_count,
      total_trips
    FROM gold_hourly_passenger_kpis
    WHERE reference_year = 2023
      AND reference_month = '05'
    ORDER BY pickup_hour
    """
).show(24, truncate=False)
