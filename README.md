# Spark 4.2 × Dell ObjectScale 4.4 PoC

Automated, re-runnable PoC proving Spark 4.2 interoperability with ObjectScale 4.4:
standard S3 (S3A), the S3 Throttler (QoS), and native S3 Tables via Apache Iceberg.

## Layout

- `notebooks/` — demo suite (`99_reset`, `00`–`04`). Run in JupyterLab on
  `http://192.168.1.40:8888` (token in `/root/jupyter_token.txt`).
- `poc/` — engine modules (config, obs_admin, s3util, loadgen, spark_helpers,
  results, reset, viz, demo_data). Notebooks are thin wrappers.
- `scripts/` — `prep_obs.py` (ObjectScale provisioning), `render_conf.py`
  (spark-defaults.conf from `conf/spark-defaults.conf.j2` + `.env`),
  `build_iceberg.sh` (Iceberg 4.2 runtime from source), `report.py`
  (`reports/PoC_Report.html`), `export_demo_html.sh`, `gen_notebooks.py`.
- `reports/` — `environment.md` (verified API/port/schema findings),
  `throttle_backup.json`, `raw/TC*.json`, `PoC_Report.html`.
- `run_all.sh` — headless `99_reset` + `00`–`04` via nbconvert.

## Operator quick start

```bash
export OBS_ROOT_PASSWORD='…'      # env only, never written to disk
export DEMO_SCALE_GB=1            # demo scale (all cells < 60 s)
export THROTTLE_PHASE_S=60        # nb02 phase seconds (enforced phase = 2x)
./run_all.sh                      # reset + full suite, ~8-10 min
```

Or run in JupyterLab: `99_reset.ipynb`, then `00` → `04`. Re-runnable; each run
uses a `demo-*` prefix cleaned by reset.

Offline tests (mocked S3, no ObjectScale needed):

```bash
PYTHONPATH=/root/OBS pytest -q tests/
```

## Security

- Root password: process env only (`OBS_ROOT_PASSWORD`); never persisted.
- IAM keys: `/root/.poc/credentials` (0600), `.env` (0600, gitignored).
- Rendered `/opt/spark/conf/spark-defaults.conf` is 0600.
- Pre-commit: nbstripout + secret grep (`.pre-commit-config.yaml`).

## Known environment notes

See `reports/environment.md` for the fully verified endpoint/schema map.
Two lab-build caveats documented there: throttle rule enforcement needs a
datahead reload not exposed via the management API (notebooks measure live and
report `enforcement_active: false` honestly), and OBS SigV4 for `s3tables`
double-encodes the canonical path — handled by a one-line patch in the
locally-built Iceberg 4.2 runtime (`scripts/build_iceberg.sh`).
