#!/usr/bin/env bash
# Headless end-to-end run: 99_reset then 00 -> 04 via nbconvert --execute.
# Usage: ./run_all.sh [DEMO_SCALE_GB]
set -uo pipefail
cd "$(dirname "$0")"
export DEMO_SCALE_GB="${1:-1}"
export PYTHONPATH="/root/OBS:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
export PYSPARK_PYTHON=/opt/jupyter/venv/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/jupyter/venv/bin/python
V=/opt/jupyter/venv/bin
STATUS=0
for nb in 99_reset 00_smoke 01_spark_on_objectscale 02_s3_throttler \
          03_s3_tables_iceberg 04_summary; do
  echo "=== $nb ==="
  if ! "$V/jupyter" nbconvert --to notebook --execute --inplace \
      --ExecutePreprocessor.timeout=900 "notebooks/$nb.ipynb" 2>&1 | tail -3; then
    STATUS=1
  fi
done
echo "=== result: $STATUS ==="
exit $STATUS
