#!/usr/bin/env bash
set -e

echo "== Environment check: Python =="
python --version

echo "== Environment check: Java =="
java -version

echo "== Environment check: PySpark =="
python -c "import pyspark; print(pyspark.__version__)"

echo "== Environment check: ifood_tlc package =="
python -c "import ifood_tlc; print('ifood_tlc import OK')"

echo "== Environment check: compile source =="
python -m compileall src

echo "== Environment check: pytest =="
pytest

echo "== Environment check: data volume =="
ls -la data

echo "== Environment check finished successfully =="
