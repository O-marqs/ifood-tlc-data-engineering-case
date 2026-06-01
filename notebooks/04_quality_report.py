# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Quality Report
# MAGIC
# MAGIC Reads and summarizes the yellow taxi quality report generated in gold.
# MAGIC Run notebook 02 before this one.

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


quality_path = get_gold_path() / "quality" / "quality_yellow_trips_report"
quality_report = spark.read.parquet(str(quality_path))
quality_report.createOrReplaceTempView("quality_yellow_trips_report")

print(f"Loaded quality report from: {quality_path}")

# COMMAND ----------

# Show all quality rules by period.
spark.sql(
    """
    SELECT
      reference_year,
      reference_month,
      rule_name,
      total_records,
      failed_records,
      failed_percentage,
      created_at
    FROM quality_yellow_trips_report
    ORDER BY reference_year, reference_month, rule_name
    """
).show(200, truncate=False)

# COMMAND ----------

# Highlight the rules with the highest failed percentage.
spark.sql(
    """
    SELECT
      reference_year,
      reference_month,
      rule_name,
      failed_records,
      failed_percentage
    FROM quality_yellow_trips_report
    WHERE failed_records > 0
    ORDER BY failed_percentage DESC, failed_records DESC
    LIMIT 50
    """
).show(50, truncate=False)
