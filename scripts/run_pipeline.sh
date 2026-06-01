#!/usr/bin/env bash
set -e

echo "== iFood TLC case: running yellow taxi pipeline =="
echo "DATA_DIR=${DATA_DIR:-/app/data}"

echo "== Step 1/5: bronze ingestion =="
python -m ifood_tlc.ingestion.bronze --service-type yellow --year 2023 --all-config-months

echo "== Step 2/5: silver transformation =="
python -m ifood_tlc.transformations.silver_yellow --year 2023 --all-config-months

echo "== Step 3/5: quality report =="
python -m ifood_tlc.quality.yellow_checks --year 2023 --all-config-months

echo "== Step 4/5: gold build =="
python -m ifood_tlc.transformations.gold_yellow --year 2023

echo "== Step 5/5: export mandatory case outputs =="
python -m analysis.export_case_outputs

echo "== Pipeline finished successfully =="
