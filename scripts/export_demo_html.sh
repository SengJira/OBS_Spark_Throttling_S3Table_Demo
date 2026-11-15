#!/usr/bin/env bash
# Execute all notebooks at demo scale and save HTML backups under
# reports/demo_backup/ — fallback if the live demo cannot run.
set -uo pipefail
ROOT="${POC_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"
V="${POC_VENV:-/opt/jupyter/venv}/bin"
OUT=reports/demo_backup
mkdir -p "$OUT"
# run_all.sh exports one POC_RUN_ID for the suite and writes reports/executed/
./run_all.sh "${1:-1}"
STATUS=$?
for nb in 99_reset 00_smoke 01_spark_on_objectscale 02_s3_throttler \
          03_s3_tables_iceberg 04_summary; do
  [ -f "reports/executed/$nb.ipynb" ] || continue
  "$V/jupyter" nbconvert --to html "reports/executed/$nb.ipynb" \
      --output-dir "$OUT" >/dev/null 2>&1 && echo "saved $OUT/$nb.html"
done
cp -f reports/PoC_Report.html "$OUT/" 2>/dev/null || true
echo "done -> $OUT/ (suite exit $STATUS)"
exit $STATUS
