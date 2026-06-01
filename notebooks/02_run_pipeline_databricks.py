# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Run Pipeline on Databricks
# MAGIC
# MAGIC Orchestrates landing -> bronze -> silver -> gold -> quality report for yellow taxi.
# MAGIC The implementation remains in `src/`; this notebook only wires the steps together.

# COMMAND ----------

import os
import sys
from pathlib import Path


# Adjust these paths to your Databricks workspace/DBFS layout.
# Optionally install the package with `%pip install -e /dbfs/FileStore/ifood_tlc_case/project`.
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/dbfs/FileStore/ifood_tlc_case/project")
DATA_DIR = os.getenv("DATA_DIR", "/dbfs/FileStore/ifood_tlc_case/data")

SRC_PATH = str(Path(PROJECT_ROOT) / "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

os.environ["DATA_DIR"] = DATA_DIR

print(f"PROJECT_ROOT={PROJECT_ROOT}")
print(f"DATA_DIR={DATA_DIR}")

# COMMAND ----------

from ifood_tlc.config import load_pipeline_config
from ifood_tlc.ingestion.bronze import ingest_config_months
from ifood_tlc.quality.yellow_checks import run_quality_checks
from ifood_tlc.transformations.gold_yellow import build_gold
from ifood_tlc.transformations.silver_yellow import transform_config_months


config = load_pipeline_config(Path(PROJECT_ROOT) / "configs" / "pipeline.yml")
year = int(config["default_year"])
service_type = "yellow"

print(f"Pipeline scope: service_type={service_type}, year={year}, months={config['months']}")
print("Databricks Community note: focus on yellow taxi Jan-May; avoid heavy FHVHV processing here.")

# COMMAND ----------

# Step 1: landing -> bronze.
# This reads the organized landing parquet files and writes partitioned bronze parquet.
bronze_results = ingest_config_months(
    spark=spark,
    service_type=service_type,
    year=year,
    write_mode="overwrite",
)
print(bronze_results)

# COMMAND ----------

# Step 2: bronze -> silver.
# This standardizes types and enriches yellow trips without destructive filters.
silver_results = transform_config_months(
    spark=spark,
    year=year,
    write_mode="overwrite",
)
print(silver_results)

# COMMAND ----------

# Step 3: silver -> gold.
# This creates detail and KPI datasets for analytical consumption.
gold_results = build_gold(
    spark=spark,
    year=year,
    months=[int(month) for month in config["months"]],
    write_mode="overwrite",
)
print(gold_results)

# COMMAND ----------

# Step 4: quality report.
# This writes data quality metrics to gold/quality without blocking or deleting records.
quality_rows = run_quality_checks(
    spark=spark,
    write_mode="overwrite",
    year=year,
    months=[int(month) for month in config["months"]],
)
print(f"Quality report rows written: {quality_rows}")
