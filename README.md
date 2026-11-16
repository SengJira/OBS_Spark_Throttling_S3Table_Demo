# Spark 4.2 × Dell ObjectScale 4.4 PoC

Automated, re-runnable PoC proving Spark 4.2 interoperability with ObjectScale 4.4:
standard S3 (S3A), the S3 Throttler (QoS), and native S3 Tables via Apache Iceberg.

## Layout

- `notebooks/` — demo suite (`99_reset`, `00`–`04`). **Generated** by
  `scripts/gen_notebooks.py` and committed without outputs — change the
  generator, never the `.ipynb` files.
- `poc/` — engine modules (config, obs_admin, s3util, loadgen, noisy_agent,
  throttle_eval, spark_helpers, results, reset, viz, demo_data, throughput).
  Notebooks are thin wrappers.
- `scripts/` — `prep_obs.py` (ObjectScale provisioning + one-time throttler
  backup), `render_conf.py` (cluster-wide `spark-defaults.conf`),
  `build_iceberg.sh` (Iceberg 4.2 runtime from source, ObjectScale-specific
  patch), `report.py` (`reports/PoC_Report.html`), `export_demo_html.sh`,
  `gen_notebooks.py`.
- `reports/` — `environment.md` (verified API/port/schema findings),
  `throttle_backup.json` (pre-demo throttler state); generated and gitignored:
  `raw/TC*.json`, `executed/` (notebooks with outputs), `PoC_Report.html`,
  `current_run_id`.
- `tests/` — offline unit tests (mocked S3 / management API).
- `run_all.sh` — headless `99_reset` + `00`–`04` via nbconvert.

## Operator quick start

```bash
export OBS_ROOT_PASSWORD='…'      # env only, never written to disk
./run_all.sh                      # reset + full suite, ≈ 25 min
```

Executed notebooks land in `reports/executed/` (the ones in `notebooks/` stay
output-free), the report in `reports/PoC_Report.html`. Or run in JupyterLab:
`99_reset.ipynb`, then `00` → `04`.

| Variable | Default | Meaning |
|---|---|---|
| `DEMO_SCALE_GB` | `1` | dataset size for notebook 01: ≈ 8 M rows per unit (1 GiB of uncompressed row data, ≈ 0.09 GB of Parquet on ObjectScale) |
| `THROTTLE_PHASE_S` | `60` | notebook 02 phase length (the enforced phase is 2×) |
| `THROTTLE_SETTLE_S` | `120` | how long to wait for the mode switch to become visible |
| `CRITICAL_RATE` | `0` | critical GET offer rate in notebook 02 (req/s); `0` = as fast as possible. Pacing is experimental — it needs enough threads for the rate |
| `NOISY_THREADS` | `6` | noisy PUT worker threads (local or on U02) |
| `NOISY_OBJECT_KB` | `64` | KiB per noisy PUT |
| `PAYLOAD_RANDOM` | `0` | `1` = random (uncompressible) bytes per worker |
| `CRITICAL_OBJECTS` | `1` | seed objects critical GETs read at random |
| `CRITICAL_OBJECT_KB` | `64` | KiB per critical seed object |
| `POC_RUN_ID` | `demo-<timestamp>` | one run ID for the whole suite |
| `NOISY_HOST` | empty | `host:port` of `poc/noisy_agent.py` on the second client host; empty keeps noisy PUTs local on U01 |
| `NOISY_AGENT_TOKEN` | empty | shared `X-POC-Token` header for U01 ↔ U02 control traffic |

Approximate run time at the defaults (2-vCPU lab host): `99_reset` 1–3 min,
`00` 0.5 min, `01` ≈ 3 min, `02` ≈ 12 min (60 s baseline + 60 s contention +
60 s dry-run + mode switch + 120 s enforced + 60 s recovery + 60 s
post-baseline + prefix bursts + two Spark writes), `03` ≈ 3 min,
`04` < 0.5 min — **≈ 22 min in total**.

Run `99_reset` at least **5 minutes** before notebook 02: it stages the two
throttle rules with the throttler disabled, and ObjectScale needs up to
300 s (`rules_config_cache_expiration_time`) before a stored rule is
enforced. Notebook 02 shows a countdown only if the rules are still younger
than 300 s — normally there is none.

### Remote noisy agent (optional second client host)

Notebook 02 can move **only** the tenant-noisy PUT workload to a second host
(U02, `192.168.1.41`) while Jupyter, Spark, critical GETs, phases, verdicts
and the live chart stay on U01 (`192.168.1.40`). ObjectScale is unchanged.

- U01: set `NOISY_HOST=192.168.1.41:8765` and `NOISY_AGENT_TOKEN=…` in `.env`.
- U02: install the repo, Python 3.10+ with boto3, the ObjectScale CA bundle,
  and chrony synced to the same source as U01. The agent env file
  `/etc/obs-poc/noisy-agent.env` (0600) contains **only** the tenant-noisy
  keys plus `OBS_S3_ENDPOINT`, `OBS_CA_BUNDLE`, `OBS_REGION`, `OBS_NAMESPACE`,
  `BUCKET_THROTTLE`, `NOISY_AGENT_TOKEN`, `NOISY_AGENT_BIND=192.168.1.41`,
  `NOISY_AGENT_PORT=8765`. No admin/root/critical/table credentials.
- Service: `systemd/noisy-agent.service` → `python3 -m poc.noisy_agent`.
  It binds only to the U02 address, requires `X-POC-Token`, and stops noisy
  workers if U01 stops polling for `NOISY_AGENT_IDLE_S` (default 60 s).
- Firewall: allow TCP 8765 only from `192.168.1.40`.
- The control plane is plain HTTP on the lab LAN: the `X-POC-Token` header
  is sent unencrypted. Mitigations: the firewall rule above, a random
  per-deployment token in 0600 env files, and the agent refusing to start
  without an explicit bind address and token. For anything beyond a lab,
  put the agent behind TLS (or an SSH tunnel) — the agent does not serve
  TLS itself.
- Notebook 02 Step 1 refuses to run if `/health` reports a U01/U02 clock
  offset over 50 ms, if the agent cannot reach ObjectScale on 9021, or if the
  remote PUT/GET probe does not run as `tenant-noisy`. It also stops any
  leftover remote noisy load before starting, so a re-run cannot contaminate
  the baseline.
- Remote events are pulled continuously and flushed before every verdict, so
  the slow queued PUTs and 429s are not lost at phase boundaries. The clock
  offset is re-measured at each phase boundary; a phase fails if it exceeds
  50 ms or if event pulls are lost for more than 5 s.

For the customer comparison, run notebook 02 once with `NOISY_HOST` empty
(local noisy on U01) and once with it set (remote noisy on U02), keeping the
same phase lengths. Compare `client_cpu_pct`, contention critical p95 and
recovery critical p95 in `reports/raw/TC2.*.json` — the remote run removes
the single-host CPU-contention term documented in `reports/environment.md`.

### Run IDs and the scorecard

Every result file carries the run ID it belongs to and a status of `PASS`,
`FAIL` or `INCONCLUSIVE`. The run ID comes from `POC_RUN_ID`, else the persisted
`reports/current_run_id`, else `RUN_ID` in `.env`, else a timestamp; it always
starts with `demo-` because it is also the S3 key prefix that `99_reset` cleans.
`run_all.sh` exports one ID for the suite; in JupyterLab `99_reset` starts a new
one.

The scorecard (notebook 04, `PoC_Report.html`) only counts results of the
current run. An objective is `INCOMPLETE` if any of its test cases is missing,
else `FAIL` if any failed, else `INCONCLUSIVE` if the evidence did not decide
one of them, else `PASS`. `INCONCLUSIVE` is never shown as a pass.

### Tests and lint

```bash
PYTHONPATH=. pytest -q tests/        # offline, no ObjectScale needed
pyflakes poc scripts tests
python3 scripts/gen_notebooks.py     # regenerate notebooks after generator changes
```

## Spark configuration

`poc/spark_helpers.build_session()` is the single source of truth for
per-session settings: the S3A options, the Iceberg `os` catalog and the
identities. `conf/spark-defaults.conf.j2` (rendered by `scripts/render_conf.py`)
only holds cluster-wide, non-secret settings — master, event log/history
server, SQL defaults and the redaction regex.

- **Identities are explicit per session**, not environment variables:
  `build_session(tables_user=…, s3a_user=…)` sets `fs.s3a.access.key/secret.key`
  and the catalog properties `rest.access-key-id`, `rest.secret-access-key`,
  `s3.access-key-id`, `s3.secret-access-key`. Switching identity in one kernel
  (`spark.stop()` then `build_session(...)`) is supported and verified:
  `sh.identities(spark)` reports what the running session really uses.
- **Secrets never reach the command line.** A fresh JVM reads them from a 0600
  `--properties-file` under `POC_SECRETS_DIR`; a reused JVM gets them as
  builder options.
- S3A choices: ETag change detection (`change.detection.mode=server`),
  `experimental.input.fadvise=random`, TLS on, throttle retries
  (`retry.throttle.limit=20`, `interval=500ms`), `committer.name=magic`.
- **Committers:** the magic committer is used for non-Parquet output
  (verified: JSON `_SUCCESS` manifest reports `committer: magic`). Parquet
  writes use Spark's rename-based `ParquetOutputCommitter` on this cluster,
  because `spark-hadoop-cloud` is not installed; to use an S3A committer for
  Parquet too, add `spark-hadoop-cloud_2.13-4.2.0.jar` and set
  `spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol`
  and
  `spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter`.

## Security

- Root password: process env only (`OBS_ROOT_PASSWORD`); never persisted by
  the PoC.
- IAM keys: `<POC_SECRETS_DIR>/credentials` and `.env` (0600, gitignored); files
  holding secrets are created with mode 0600 from the first byte.
- TLS verification is on everywhere (S3 9021, TableHead 19021, management
  4443) against the ObjectScale CA in the system bundle (`OBS_CA_BUNDLE`).
  Only hostname matching is relaxed, and only for 4443, whose certificate
  names `localhost`.
- The rendered `spark-defaults.conf` contains no secrets.
- Throttler safety: `prep_obs.py` writes `reports/throttle_backup.json` once,
  and only from a clean pre-demo state (no `demo-*` rules, mode Disabled);
  `--force-backup` overrides. Notebook 02 refuses to start without the backup
  and restores it at the end and at kernel exit.
- `99_reset` deletes only what the PoC created; run its dry-run cell first.
- Pre-commit: nbstripout, pyflakes, secret grep (`.pre-commit-config.yaml`).

### Running Jupyter as a non-root user

`systemd/jupyter.service` runs JupyterLab as the unprivileged user `poc`.
The lab host was installed with everything under `/root`, so switching needs
these one-time host steps (as root) — until then keep using the installed
root unit:

```bash
useradd --system --create-home --home-dir /var/lib/obs-poc --shell /usr/sbin/nologin poc
usermod -aG spark poc
install -d -o poc -g poc -m 0700 /var/lib/obs-poc/secrets /var/lib/obs-poc/jupyter
# repo out of /root, owned by poc
cp -a /root/OBS /opt/obs-poc && chown -R poc:poc /opt/obs-poc
# IAM keys: move, keep 0600
install -o poc -g poc -m 0600 /root/.poc/credentials /var/lib/obs-poc/secrets/credentials
# management password: readable by systemd only
install -d -m 0700 /etc/obs-poc
( umask 077; printf 'OBS_ROOT_PASSWORD=%s\n' '…' > /etc/obs-poc/mgmt.env )
# event log directory: group-writable for the spark group
chmod 2775 /var/log/spark-events
# existing Jupyter config/token and kernelspec (PYTHONPATH -> /opt/obs-poc)
cp -a /opt/jupyter/jupyter_lab_config /var/lib/obs-poc/jupyter/config
sed -i 's#/root/OBS#/opt/obs-poc#' /opt/jupyter/venv/share/jupyter/kernels/pyspark-obs/kernel.json
chown -R poc:poc /var/lib/obs-poc
install -m 0644 /opt/obs-poc/systemd/jupyter.service /etc/systemd/system/jupyter.service
systemctl daemon-reload && systemctl restart jupyter
```

`POC_ROOT` and `POC_SECRETS_DIR` tell the code where the repo and the
credential files live; nothing in `poc/` or `scripts/` hardcodes `/root`.

## Known environment notes

See `reports/environment.md` for the verified endpoint/schema map and the live
findings. In short:

- **Throttler behaviour:** with client retries off, ObjectScale 4.4 both
  delays and rejects — accepted requests are queued to hold the rate at the
  limit, the overflow gets HTTP 429 `TooManyRequests`. SDK retries hide those
  429s, so the load generator disables them.
- **Rule propagation:** rules are cached for up to 300 s; `99_reset` stages
  them minutes ahead and notebook 02 measures enforcement only once they are
  old enough (a countdown appears only if they are still younger than 300 s).
- **Iceberg runtime:** OBS SigV4 for `s3tables` double-encodes the canonical
  path — handled by a one-line patch in the locally built Iceberg 4.2 runtime
  (`scripts/build_iceberg.sh`, which verifies checksum, patch and jar). The jar
  is ObjectScale-specific.
