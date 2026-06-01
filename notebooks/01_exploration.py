# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Landing Exploration
# MAGIC
# MAGIC Use this notebook to inspect NYC TLC parquet files already organized in the landing zone.
# MAGIC It calls project code from `src/` and does not transform or write data.

# COMMAND ----------

# Databricks Community setup:
# 1. Upload this repository to DBFS, Repos, or Workspace files.
# 2. Adjust PROJECT_ROOT to the folder that contains `src/` and `configs/`.
# 3. Adjust DATA_DIR to the DBFS folder that contains `landing/`, `bronze/`, `silver/`, `gold/`.
# 4. Optionally install the package with `%pip install -e /dbfs/FileStore/ifood_tlc_case/project`.
#
# Example DBFS layout:
# /dbfs/FileStore/ifood_tlc_case/project/src
# /dbfs/FileStore/ifood_tlc_case/project/configs
# /dbfs/FileStore/ifood_tlc_case/data/landing/tlc/yellow/year=2023/month=01/yellow_tripdata_2023-01.parquet

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

from ifood_tlc.config import load_pipeline_config
from ifood_tlc.paths import build_landing_file_path
from ifood_tlc.utils.inspect_landing_schemas import (
    build_markdown_summary,
    inspect_parquet,
)


config = load_pipeline_config(Path(PROJECT_ROOT) / "configs" / "pipeline.yml")
year = int(config["default_year"])
months = [int(month) for month in config["months"]]
service_type = "yellow"

print(f"Exploring {service_type} files for year={year}, months={months}")

# COMMAND ----------

inspections = []
missing_files = []

for month in months:
    path = build_landing_file_path(year=year, month=month, service_type=service_type)
    if not path.exists():
        print(f"WARNING: missing landing file: {path}")
        missing_files.append(path)
        continue

    inspections.append(
        inspect_parquet(
            spark=spark,
            path=path,
            service_type=service_type,
            year=year,
            month=month,
        )
    )

# COMMAND ----------

summary = build_markdown_summary(inspections, missing_files)
print(summary)
