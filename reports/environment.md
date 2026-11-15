# Environment Report — Phase 0

_Generated 2026-11-12 on U01 (192.168.1.40) against ObjectScale at 192.168.1.31_

## U01 (Spark / Jupyter host)

| Item | Value |
|---|---|
| OS | Ubuntu 22.04.5 LTS |
| Kernel | Linux 5.15.0-203-generic |
| CPU cores | **2** (constrained — see sizing note) |
| RAM | 7.8 GiB total / ~5.3 GiB available |
| Disk /, /opt | 62 GB total, **46 GB free** (single LV) |
| Python (system) | 3.10.12 (Python 3.11 installed via apt for the Jupyter venv) |
| Internet | Maven Central, PyPI, archive.apache.org, dlcdn.apache.org all HTTP 200 |
| NTP | chrony installed + synced (required for SigV4 <5 min skew) |

**Sizing note:** the box has only 2 cores, so "reserve 2 cores for OS" is not
feasible. Worker is configured for 1 core / 3.5 GiB; master 1 GiB; the OS +
Jupyter share the remainder. DEMO_SCALE_GB=1 workloads run fine on this.

## Network U01 → OBS (192.168.1.31)

- ping: ~0.3–2 ms RTT, 0% loss
- Open ports (`nc -zv`): **9020, 9021, 9022, 9024, 4443, 443, 19020, 19021**; 3211 refused

| Port | What answers | Use |
|---|---|---|
| 443 | ObjectScale management UI (React SPA); cert SAN=localhost only | operator UI |
| 4443 | ECS-lineage management API (`X-SDS-AUTH-TOKEN`), IAM Query API (`/iam`), config API (`/config`, `/config-admin`), throttle API (`/throttle/config`) | all admin calls |
| 9020 | S3 HTTP (403 XML unauthenticated) | S3 data (HTTP) |
| 9021 | S3 HTTPS (403 XML unauthenticated, self-signed `CN=DataService`, SAN includes IP) | S3 data (TLS) |
| 9022 | Jetty service, HTTP only, 500 NPE on plain requests | aux service (not needed) |
| 9024 | **OpenStack Swift**, not S3 Tables (misidentified during discovery) | not used |
| 19020 | **TableHead** — AWS S3 Tables API (`/buckets` …) + Iceberg REST (`/iceberg/v1/…`); HTTP, SigV4 name `s3tables` | S3 Tables control plane + Iceberg REST |
| 19021 | TableHead HTTPS twin | S3 Tables (TLS) |

## OBS (ObjectScale)

- Version: **4.4.0.0.143516.a68cbda81306** (from `GET /vdc/nodes`)
- Login: `GET https://192.168.1.31:4443/login` (HTTP basic, root) → `X-SDS-AUTH-TOKEN` header, max-age 28800 s
- Existing namespace: `ns1`; vpool/storage pool: `sp1` = `urn:storageos:VirtualArray:d115b202-e5cd-40b6-a6f4-a548b347758b`
- TLS: self-signed certs on 9021 (`CN=DataService`) and 4443 (`CN=localhost`) — imported into system CA store + Spark truststore (see below); TLS verification stays **on**
- Region string accepted by SigV4/S3A: `us-east-1` (OBS accepts any; we pin `us-east-1`)

## Discovered APIs (verified live, not just documented)

### S3 Throttler (new in 4.4, "mvp" feature flag)

| Endpoint | Method | Body / notes |
|---|---|---|
| `/throttle/config` | GET | current config; **404 when no rules exist** (treat 404 = empty) |
| `/throttle/config` | PUT | `{"throttle":{"rules":[…]}}` — replaces **all** rules |
| `/throttle/config?includeAuditInfo=true` | GET | rules + audit info |
| `/throttle/config/rule/{id}` | PUT | update single rule |
| `/throttle/config/rules` | DELETE | delete rules |
| `/throttle/config/actions` | GET | `["s3:GetObject","s3:GetObjectVersion","s3:PutObject","s3:DeleteObject","s3:DeleteObjectVersion","s3:CreateBucket","s3:DeleteBucket","s3:ListBucket","s3:ListBucketVersions","*"]` |
| `/config/<name>` | GET | read CMF config param (has `<configured_value>` once set) |
| `/config-admin` | PUT | `{"list_of_params":[{"name":…,"value":…}]}` — sets CMF params |

Throttle CMF params (set via `/config-admin`):

- `com.emc.ecs.common.request.throttle.mvp.state` = `Enabled` / `Dry_Run` / `Disabled`
- `com.emc.ecs.common.request.throttle.mvp.response_code` = `429` or `503` (default 503)
- `com.emc.ecs.common.request.throttle.mvp.rules_config_cache_expiration_time` = **300 s, read-only** — rule changes take up to 5 min to reach the data path.

Rule JSON schema — **from `GET /throttle/template` (server-shipped examples)**:

```json
{"throttle":{"rules":[{
  "id": "string", "description": "string", "isDisabled": false,
  "rateUnit": "TPS"|"TPM",
  "limit": {"value": <int>},
  "filters": {
    "principals": ["*"|"user:<name>"|"iam::user:<id>"|
                   "urn:ecs:iam::<ns>:user/<name>"],  // all forms accepted
    "ips":   {"matcher": "exact|wildcard4|cidr4", "values": [...]},
    "exclude_ips":   {"matcher": ..., "values": [...]},
    "paths": {"matcher": "exact|wildcard", "values": ["<namespace>/<bucket-glob>"]},
    "exclude_paths": {"matcher": ..., "values": [...]},
    "verbs":   ["GET","POST","PUT","DELETE","HEAD","*"],   // XOR actions
    "actions": ["s3:PutObject", ...]
  },
  "scoping": {"per": ["principal","action","ip","path","verb"]}   // lowercase
}]}}
```

Also verified: `/throttle/template` (GET, worked examples), `PUT /throttle/config/rule/{id}`,
`DELETE /throttle/config` (empties all rules — PUT requires ≥1 rule).

**Enforcement behavior (verified live):** this build throttles by **delay**, not
rejection — when a rule is fully propagated, noisy ok-TPS collapses to the
configured limit while every request still returns HTTP 200 (queued server-side).
No 429/503 was ever observed; `response_code` appears to govern a reject path
that this build does not exercise. Propagation is gated by
`rules_config_cache_expiration_time` (**300 s, read-only**): mid-propagation
the limit is partially applied (ok-TPS falls toward the limit, p99 latency
spikes, p50 flat). The notebook detects enforcement by either signature —
`enforcement_mode: "reject"` (429/SlowDown seen) or `"delay"` (ok-TPS pinned
at ≤ limit×1.2 with contention ≥ 2× limit and noisy tail latency ≥ 2×
contention) — and reports INCONCLUSIVE while neither holds. UI metrics use
Flux (`POST /flux/query`, bucket `monitoring_main`, counters `matched_counter`,
`throttle_counter`, `dryrun_counter`) — the Flux backend returns 503 on this
node, so the notebooks collect their own client-side metrics instead.

### IAM Query API (4443, management token)

`GET/POST https://192.168.1.31:4443/iam?Action=<X>&Version=2010-05-08` — AWS IAM query-protocol compatible.
Managed policies present: `ECSDenyAll`, `ECSS3FullAccess`, `ECSS3ReadOnlyAccess`, `IAMFullAccess`, `IAMReadOnlyAccess`, **`S3TablesFullAccess`**, **`S3TablesReadOnlyAccess`** (`urn:ecs:iam:::policy/<name>`).

### ECS object API (4443, management token)

- `GET /object/namespaces` / `POST /object/namespaces/namespace`
- `GET/POST /object/users` (+ `/object/user-secret-keys` for keys)
- `GET/POST /object/bucket`

### S3 Tables / Iceberg (TableHead, port 19020)

- `x-emc-namespace: poc` header required on **s3tables control-plane** calls (boto3 `before-call` event injects it); unsigned → 500 "no namespace is provided for anonymous access".
- Control plane (boto3 `s3tables`): `PUT /buckets` create, `GET /buckets` list, namespaces/tables under `tableBucketARN`. Table bucket `poc-lakehouse` → `arn:aws:s3tables:::bucket/poc-lakehouse`.
- Iceberg REST: `GET /iceberg/v1/config?warehouse=<urlencoded ARN>` (warehouse param required) advertises `/v1/{prefix}/namespaces|tables…`.
- **`{prefix}` is the tableBucketARN, percent-encoded** (`arn%3Aaws%3As3tables%3A%3A%3Abucket%2Fpoc-lakehouse`); set as `spark.sql.catalog.os.prefix`.
- **SigV4 is verified with S3-style double-URL-encoding** of the canonical path (proven: request validates only when `%`→`%25` in canonical path). Stock Iceberg `Aws4Signer` single-encodes → `SignatureDoesNotMatch`. Fix: patch `RESTSigV4AuthSession` to set `Aws4SignerParams.doubleUrlEncode(true)` when `rest.signing-name=s3tables`, rebuild `iceberg-spark-runtime-4.2_2.13-1.12.0.jar` from the 1.12.0 source release — `scripts/build_iceberg.sh`.
- Table data lives in per-table buckets `s3://<table-uuid>--table-s3` on the normal S3 endpoint (9021) — `S3FileIO` with `s3.endpoint` + `s3.path-style-access`.
- **Managed policy split:** `S3TablesFullAccess` grants only `s3tables:*` (control plane). Table data I/O needs an S3 data-plane policy on `--table-s3` buckets → attach `ECSS3FullAccess` to tables-rw and `ECSS3ReadOnlyAccess` to tables-ro (ro list OK / put 403 — desired).

## CA / truststore

- `scripts/fetch_obs_certs.sh` pulls the 9021 + 4443 chains with `openssl s_client -showcerts`
- Imported into `/usr/local/share/ca-certificates/obs-*.crt` + `update-ca-certificates`
- Java truststore: `$JAVA_HOME/lib/security/cacerts` (imported as `obs-s3`, `obs-mgmt`) **and** a dedicated `/opt/spark/conf/obs-truststore.jks` referenced by `spark-defaults.conf` (`spark.ssl.trustStore`-style + `fs.s3a` SSL on)

## Verified substitutions vs. spec

| Spec asked | Reality | Decision |
|---|---|---|
| `iceberg-spark-runtime-4.2_2.13:1.12.0` | not on Maven Central (4.0/4.1 only); the `spark/v4.2` module **is** in the 1.12.0 source release | **built from source** per spec — `scripts/build_iceberg.sh`; also carries the `doubleUrlEncode` patch required by OBS SigV4 (verified: the 4.1 jar crashes with `IncompatibleClassChangeError` on Spark 4.2) |
| `/throttle/operation-mode` PUT endpoint | exists in the UI bundle but **404 on 4443** | mode set via `/config-admin` CMF param (same backend the UI uses) |
| GET `/config/general/<name>` | 404 | read via `/config/<name>` |
| `/throttle/audit-events`, `/monitor/events/*`, `/flux/query` | 404 / 503 backend-down | rule metrics not retrievable on this build; notebooks measure client-side |
| Throttle enforcement | not observed on lab build (config stored fine) | manual operator step — see "Enforcement caveat" above |
