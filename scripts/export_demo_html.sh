#!/usr/bin/env bash
# Execute all notebooks at demo scale and save HTML backups under
# reports/demo_backup/ — fallback if the live demo cannot run.
set -uo pipefail
cd "$(dirname "$0")/.."
export DEMO_SCALE_GB="${1:-1}"
export PYTHONPATH="/root/OBS:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
export PYSPARK_PYTHON=/opt/jupyter/venv/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/jupyter/venv/bin/python
V=/opt/jupyter/venv/bin
OUT=reports/demo_backup
mkdir -p "$OUT"
"$V/jupyter" nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.timeout=900 notebooks/99_reset.ipynb || true
for nb in 00_smoke 01_spark_on_objectscale 02_s3_throttler \
          03_s3_tables_iceberg 04_summary; do
  "$V/jupyter" nbconvert --to notebook --execute \
      --ExecutePreprocessor.timeout=900 \
      --output "/tmp/$nb.exec.ipynb" "notebooks/$nb.ipynb"
  "$V/jupyter" nbconvert --to html "/tmp/$nb.exec.ipynb" --output "$OUT/$nb.html"
  echo "saved $OUT/$nb.html"
done
/opt/jupyter/venv/bin/python scripts/report.py || true
cp -f reports/PoC_Report.html "$OUT/" 2>/dev/null || true
echo "done -> $OUT/"
