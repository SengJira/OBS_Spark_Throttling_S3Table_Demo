#!/usr/bin/env bash
# Headless end-to-end run: 99_reset then 00 -> 04 via nbconvert --execute.
# Usage: ./run_all.sh [DEMO_SCALE_GB]
# Executed copies (with outputs) go to reports/executed/ — the notebooks in
# notebooks/ stay output-free. One POC_RUN_ID is exported for the whole suite.
set -uo pipefail
ROOT="${POC_ROOT:-$(cd "$(dirname "$0")" && pwd)}"
cd "$ROOT"
export POC_ROOT="$ROOT"
export DEMO_SCALE_GB="${1:-${DEMO_SCALE_GB:-1}}"
export POC_RUN_ID="${POC_RUN_ID:-demo-$(date +%Y%m%d-%H%M%S)}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
V="${POC_VENV:-/opt/jupyter/venv}/bin"
PY4J="$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip | head -1)"
export SPARK_HOME PYTHONPATH="$ROOT:$SPARK_HOME/python/lib/pyspark.zip:$PY4J"
export PYSPARK_PYTHON="$V/python" PYSPARK_DRIVER_PYTHON="$V/python"
OUT=reports/executed
mkdir -p "$OUT"
echo "run id: $POC_RUN_ID"
STATUS=0
for nb in 99_reset 00_smoke 01_spark_on_objectscale 02_s3_throttler \
          03_s3_tables_iceberg 04_summary; do
  echo "=== $nb ($(date +%H:%M:%S)) ==="
  "$V/jupyter" nbconvert --to notebook --execute \
      --ExecutePreprocessor.timeout=1200 \
      --output-dir "$OUT" "notebooks/$nb.ipynb" > "$OUT/$nb.log" 2>&1
  if [ $? -ne 0 ]; then
    STATUS=1
    echo "FAILED — last lines of $OUT/$nb.log:"; tail -25 "$OUT/$nb.log"
  else
    tail -1 "$OUT/$nb.log"
  fi
done
"$V/python" scripts/report.py || STATUS=1
echo "=== result: $STATUS (run $POC_RUN_ID, outputs in $OUT/) ==="
exit $STATUS
