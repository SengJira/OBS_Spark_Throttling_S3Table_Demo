# Environment Report — Phase 0

_Generated 2026-11-12 on U01 (192.168.1.40) against ObjectScale at 192.168.1.31; throttler, TLS, S3 Tables and sizing sections re-verified live 2026-11-15 (review fixes)._

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

Rule JSON schema — from `GET /throttle/template` (server-shipped examples) and
**confirmed live** by PUTting one-rule configs while the throttler was Disabled
(every "rejected" below is an HTTP 400 from `PUT /throttle/config`):

```json
{"throttle":{"rules":[{
  "id": "string",                       // required
  "description": "string", "isDisabled": false,      // optional
  "rateUnit": "TPS"|"TPM",              // required; BPS and others rejected
  "limit": {"value": <int >= 0>},       // negative rejected; 0 accepted (effect not tested)
  "filters": {                          // at least one filter, {} rejected
    "principals": ["*"|"user:<name>"|"iam::user:<id>"|
                   "urn:ecs:iam::<ns>:user/<name>"],
    "ips":   {"matcher": "exact|wildcard4|cidr4", "values": [...]},
    "exclude_ips":   {...},             // only together with "ips"
    "paths": {"matcher": "exact|wildcard", "values": ["<namespace>/<bucket>/<key-glob>"]},
    "exclude_paths": {...},             // only together with "paths"
    "verbs":   ["GET","POST","PUT","DELETE","HEAD","OPTIONS","*"],   // XOR actions
    "actions": ["s3:PutObject", ...]    // must be in GET /throttle/config/actions
  },
  "scoping": {"per": ["principal","action","ip","path","verb"]}   // optional
}]}}
```

| Checked live | Result |
|---|---|
| `rateUnit` `TPS`, `TPM` | accepted |
| `rateUnit` `BPS` (or anything else) | rejected: "rateUnit value BPS not valid" |
| `filters.size` | rejected: "Unknown field 'size'" — there is no size filter |
| unknown filter / top-level field | rejected: "Unknown field" |
| `paths.matcher` `exact`, `wildcard` | accepted |
| `paths.matcher` `prefix` | rejected: "Invalid path matcher 'prefix'" |
| `ips.matcher` `exact`, `wildcard4`, `cidr4` | accepted; `prefix` rejected |
| `exclude_paths` / `exclude_ips` without `paths` / `ips` | rejected: "requires a corresponding …" |
| `verbs` and `actions` in one rule | rejected: "cannot have both" |
| action not in `/throttle/config/actions` | rejected: "action value … not valid" |
| verb outside GET/POST/PUT/DELETE/HEAD/OPTIONS/* | rejected |
| `scoping.per` value outside the five dimensions | rejected |
| `scoping.per` dimension without its filter (all five) | rejected: "scoping.per dimension '<d>' requires a corresponding filter" |
| neither `verbs` nor `actions`; no `scoping`; no `isDisabled` | accepted |
| `limit.value` 0 / "50" (string) / -1 | accepted / accepted (coerced) / rejected |

`poc/throttle_eval.verify_rule()` implements exactly this table and looks the
actions up on the live server.

**What `paths` matches (tested with real PUT traffic, limit 10 TPS, retries off):**

| Rule value (matcher) | Traffic | Result |
|---|---|---|
| `poc/poc-throttle/<prefix>/*` (wildcard) | PUTs under `<prefix>/` | throttled (≈7 ok TPS + 429s); PUTs under a sibling prefix by the same user: **not** throttled (≈46 TPS) |
| `poc/poc-throttle/*` (wildcard) | any PUT to the bucket | throttled |
| `poc/poc-thr*` (wildcard) | any PUT to the bucket | throttled — `*` also spans `/` |
| `poc/poc-throttle` (exact) | object PUTs to the bucket | **not** throttled — a bucket-level `exact` value does not match object requests |
| `poc-throttle/<prefix>/*` (wildcard, **no namespace**) | PUTs under `<prefix>/` | **not** throttled (≈90–125 TPS) — the API accepts the value but it never matches |
| `poc/poc-throttle/<prefix>/*`, `scoping.per: ["principal","path"]` | PUTs spread over 80 keys | **not** throttled (≈93 TPS) |
| same rule shape, `scoping.per: ["principal","path"]` | PUTs to **one** key | throttled (≈9 ok TPS + 429s) |

So the value is `<namespace>/<bucket>/<key-glob>`, matched against the full
request path; the namespace is mandatory and `*` spans `/`. An `exact` value
naming only the bucket did not match object PUTs (an exact object path was not
tested). `scoping.per: ["path"]` gives **every distinct object key its own
counter**, so a prefix rule scoped per path does not limit traffic spread over
many keys — scope prefix rules per `principal`. The API does not validate any
of this: a value that can never match is stored without complaint.

Also verified: `/throttle/template` (GET, worked examples), `PUT /throttle/config/rule/{id}`,
`DELETE /throttle/config` (empties all rules — PUT requires ≥1 rule).

**Enforcement behaviour (verified live with client retries OFF — `max_attempts=1`):**
ObjectScale 4.4 throttles by **delay and rejection together**.

- Accepted requests are queued server-side so the accepted rate sits just
  under the limit: 50 TPS rule → ≈45–47 accepted PUT/s (≈290–370 before).
  Accepted-request latency: p50 unchanged (≈15 ms), p95 ≈27 → ≈690 ms,
  p99 ≈40 → ≈1100 ms.
- The overflow is rejected with **HTTP 429, S3 code `TooManyRequests`** — about
  6 % of the requests that complete (≈3 per second with 6 PUT threads). The
  rejections themselves arrive late (p50 ≈ 500 ms, p95 ≈ 1.2 s): a request
  waits in the queue first. With `response_code` = 429 no 503 was seen.
- The other tenant is untouched: 0 throttle responses on critical GETs, whose
  rate rose and p95 fell once the rule was active.
- Server-side throttle counters are not reachable on this node (Flux
  `POST /flux/query` → 503; no `/throttle/stats`-style endpoint), so the
  evidence is client-side.

An earlier revision of this file said this build "throttles by delay, not
rejection … no 429 was ever observed". That was an artefact of the client:
boto3's standard retry mode retries 429/503 with backoff (8 attempts), so
almost every rejection was retried until it succeeded and showed up as a slow
HTTP 200. `poc/loadgen.py` now uses `max_attempts=1` and records
`RetryAttempts` on every request.

**Propagation:** `rules_config_cache_expiration_time` is 300 s (read-only), so a
rule change can take up to 5 min to reach the data path. In four probes the
rule took effect 35–60 s after `PUT /throttle/config`, but that is luck of the
cache cycle, not a guarantee. Notebook 02 stores the rules first and measures
enforcement only once they are `PROPAGATION_S` (default 300 s) old; the time
from switching the mode to Enabled until throttling is visible is recorded as
`seconds_to_effect` in TC2.4.

### IAM Query API (4443, management token)

`GET/POST https://192.168.1.31:4443/iam?Action=<X>&Version=2010-05-08` — AWS IAM query-protocol compatible.
Managed policies present: `ECSDenyAll`, `ECSS3FullAccess`, `ECSS3ReadOnlyAccess`, `IAMFullAccess`, `IAMReadOnlyAccess`, **`S3TablesFullAccess`**, **`S3TablesReadOnlyAccess`** (`urn:ecs:iam:::policy/<name>`).

### ECS object API (4443, management token)

- `GET /object/namespaces` / `POST /object/namespaces/namespace`
- `GET/POST /object/users` (+ `/object/user-secret-keys` for keys)
- `GET/POST /object/bucket`

### S3 Tables / Iceberg (TableHead, HTTPS 19021 / HTTP 19020)

- `x-emc-namespace: poc` header required on **s3tables control-plane** calls (boto3 `before-call` event injects it); unsigned → 500 "no namespace is provided for anonymous access".
- Control plane (boto3 `s3tables`): `PUT /buckets` create, `GET /buckets` list, namespaces/tables under `tableBucketARN`. Table bucket `poc-lakehouse` → `arn:aws:s3tables:::bucket/poc-lakehouse`.
- Iceberg REST: `GET /iceberg/v1/config?warehouse=<urlencoded ARN>` (warehouse param required) advertises `/v1/{prefix}/namespaces|tables…`.
- **`{prefix}` is the tableBucketARN, percent-encoded** (`arn%3Aaws%3As3tables%3A%3A%3Abucket%2Fpoc-lakehouse`); set as `spark.sql.catalog.os.prefix`.
- **SigV4 is verified with S3-style double-URL-encoding** of the canonical path (proven: request validates only when `%`→`%25` in canonical path). Stock Iceberg `Aws4Signer` single-encodes → `SignatureDoesNotMatch`. Fix: patch `RESTSigV4AuthSession` to set `Aws4SignerParams.doubleUrlEncode(true)` when `rest.signing-name=s3tables`, rebuild `iceberg-spark-runtime-4.2_2.13-1.12.0.jar` from the 1.12.0 source release — `scripts/build_iceberg.sh`.
- Table data lives in per-table buckets `s3://<table-uuid>--table-s3` on the normal S3 endpoint (9021) — `S3FileIO` with `s3.endpoint` + `s3.path-style-access`.
- **Managed policy split:** `S3TablesFullAccess` grants only `s3tables:*` (control plane). Table data I/O needs an S3 data-plane policy on `--table-s3` buckets → attach `ECSS3FullAccess` to tables-rw and `ECSS3ReadOnlyAccess` to tables-ro (ro list OK / put 403 — desired).

- **TLS:** TableHead answers HTTPS on **19021** with the same ObjectScale
  certificate as 9021 (CN=DataService, SAN IP 192.168.1.31). Verified live with
  certificate verification on: boto3 `s3tables` (`verify=` system bundle) and
  Spark's Iceberg REST client (`SHOW NAMESPACES`, SELECT, INSERT through
  `https://192.168.1.31:19021/iceberg`, JVM truststore). It is the default
  (`OBS_TABLES_ENDPOINT`); 19020 is the same service over plain HTTP.
- **Identity per session:** the Iceberg catalog takes credentials from the
  catalog properties `rest.access-key-id` / `rest.secret-access-key` (REST
  SigV4 signer) and `s3.access-key-id` / `s3.secret-access-key` (S3FileIO).
  Verified in ONE driver JVM: tables-rw INSERT ok → `spark.stop()` → tables-ro
  session: SELECT ok, INSERT fails with S3 `AccessDeniedException … Status
  Code: 403` (data plane), CREATE/DROP TABLE fail with Iceberg
  `ForbiddenException: Not authorized to make this request` (control plane) →
  tables-rw session again: INSERT ok. No environment variables involved.
- **Table data buckets:** `get_table` returns
  `warehouseLocation: s3://<table-uuid>--table-s3`; the UUID is the last
  segment of `tableARN`. After `delete_table` / `DROP TABLE`, ObjectScale
  empties and removes that bucket itself, asynchronously (an empty table's
  bucket was gone after ≈80 s; buckets with data take longer), and rejects
  client deletes meanwhile with `EmptyBucketInProgress`. `99_reset` therefore
  reports such buckets as pending and only ever touches buckets it attributed
  to a PoC table through `get_table`.
- Object `Owner` in S3 listings is the namespace root
  (`urn:ecs:iam::poc:root`) for every IAM user — it cannot be used to tell
  which user wrote an object.

## Client-side contention (limits of this lab)

Both tenants are driven by `poc/loadgen.py` from the single 2-vCPU host U01.
Measured with 2 critical GET threads (25 s per row, throttler disabled):

| Situation | client CPU | critical GET TPS | p50 | p95 |
|---|---|---|---|---|
| critical alone | 52 % | 570 | 3.1 ms | 5.5 ms |
| + 6 noisy PUT threads in a **separate process** | 59 % | 332 | 4.9 ms | 11.3 ms |
| + 6 noisy PUT threads in the same process | 54 % | 284 | 5.7 ms | 14.2 ms |
| no S3 noise, one CPU-busy local process | 74 % | 285 | 3.8 ms | 13.9 ms |
| critical alone again | 54 % | 545 | 3.2 ms | 5.8 ms |

A purely local CPU disturbance — no storage traffic at all — degrades the
critical GETs as much as the noisy tenant does. So on this host the *size* of
the contention effect (TC2.3) and of the recovery (TC2.5) cannot be attributed
to ObjectScale alone: part of it is the load generator competing with itself
for client CPU. What the throttler does on the server — rate held at the
limit, 429s, only the targeted principal/prefix affected — does not depend on
this. To make the latency-isolation numbers attributable, drive the two
tenants from separate client hosts. Each phase records `client_cpu_pct`.

The live chart used to rescan every recorded event on each redraw inside the
load generator's process; it now keeps incremental per-bin state so a redraw
does not stall the worker threads.

### Paced recovery measurement (notebook 02 v2)

Critical GETs are now open-loop paced (`CRITICAL_RATE`, default 100 req/s, 4
threads) instead of 2 threads as fast as possible (≈570 GET/s), and latency is
measured from the scheduled send time. A phase with more than 1 % missed slots
is INCONCLUSIVE because the client could not keep up.

The phase order is baseline → contention → simulation → propagation (noisy
paused) → pre-baseline → reference → enforced → recovery → optional
enforced-24 → post-baseline. `reference` paces the noisy tenant at the rule
limit (50 PUT/s) in dry-run: it is the best result any throttler could give.
`recovery_vs_reference` is the scored isolation verdict; `baseline_stable`
(post-baseline p95 ≤ 1.25 × max(baseline, pre-baseline)) guards against
environment drift; `recovery_p95_vs_baseline` is information only.

With `NOISY_HOST` set, only the tenant-noisy PUTs run on the second host. The
agent (`poc/noisy_agent.py`) reuses `LoadGen`, so timing/status telemetry is
identical; U01 pulls events by request-start timestamp and flushes in-flight
requests before every `phase_stats()` used for a verdict. The U01/U02 clock
offset is measured by request round trip and re-checked at each phase
boundary (fail above 50 ms); a loss of event contact for more than 5 s fails
the phase. Local mode (`NOISY_HOST` empty) is unchanged.

## Demo dataset sizing

`poc/demo_data.orders_df` generates rows on the Spark side from the row id
(xxhash64), 30 daily partitions from `DEMO_BASE_DATE` (default 2026-01-01).
`DEMO_SCALE_GB` × `DEMO_ROWS_PER_GB` (default 8,000,000) rows. One unit is
about 1 GiB of uncompressed row data in Spark (≈125 bytes per row as
UnsafeRow); the columns are low-cardinality, so it lands as ≈89 MB of snappy
Parquet (measured: 8,000,000 rows → 89,252,418 bytes in 60 objects,
11.2 bytes/row ≈ 96 M rows per GB *of Parquet*). On the 2-vCPU host with one
executor core: count 1.1 s, partitioned Parquet write 35 s, read-back count
2.2 s. A full GB of Parquet (≈96 M rows) would take ≈7 min to write here, which
is why the unit is defined on row data, not on stored bytes.

## CA / truststore

- The 9021 and 4443 certificate chains were pulled with `openssl s_client -showcerts`
  and installed as `/usr/local/share/ca-certificates/obs-s3.crt` / `obs-mgmt.crt`
  + `update-ca-certificates` (system bundle `/etc/ssl/certs/ca-certificates.crt`,
  used by boto3 and the management client via `OBS_CA_BUNDLE`).
- Java: the same two certificates are in `$JAVA_HOME/lib/security/cacerts`
  (aliases `obs-s3`, `obs-mgmt`), which is what S3A, S3FileIO and the Iceberg
  REST client use. `/opt/spark/conf/obs-truststore.jks` also exists on the host
  but nothing in the PoC configuration references it.
- 19021 (TableHead) presents the same certificate as 9021, so no extra import
  was needed for HTTPS there.

## Verified substitutions vs. spec

| Spec asked | Reality | Decision |
|---|---|---|
| `iceberg-spark-runtime-4.2_2.13:1.12.0` | not on Maven Central (4.0/4.1 only); the `spark/v4.2` module **is** in the 1.12.0 source release | **built from source** per spec — `scripts/build_iceberg.sh`; also carries the `doubleUrlEncode` patch required by OBS SigV4 (verified: the 4.1 jar crashes with `IncompatibleClassChangeError` on Spark 4.2) |
| `/throttle/operation-mode` PUT endpoint | exists in the UI bundle but **404 on 4443** | mode set via `/config-admin` CMF param (same backend the UI uses) |
| GET `/config/general/<name>` | 404 | read via `/config/<name>` |
| `/throttle/audit-events`, `/monitor/events/*`, `/flux/query` | 404 / 503 backend-down | rule metrics not retrievable on this build; notebooks measure client-side |
| Throttle enforcement | not observed on lab build (config stored fine) | manual operator step — see "Enforcement caveat" above |

## Split-host lab validation (2026-11-15)

Five full `run_all.sh` suites on the lab (DEMO_SCALE_GB=1, CRITICAL_RATE=100):
two local (`NOISY_HOST` empty), two remote (`NOISY_HOST=192.168.1.41:8765`),
one remote with `QUEUE_TEST=1`. Throttler restored to the clean backup
(disabled / 503 / no rules) after every run. All five reported overall FAIL —
the honest result on this hardware, not a loosened check.

Averaged critical GET ok-latency (ms) and noisy PUT ok-TPS:

| phase | local p95 | remote p95 | local p99 | remote p99 | local noisy TPS | remote noisy TPS |
|---|---|---|---|---|---|---|
| baseline | 37.5 | 42.0 | 170 | 142 | — | — |
| contention | 267 | 88 | 471 | 147 | 175 | 229 |
| reference | 121 | 29 | 369 | 82 | 50 | 50 |
| enforced | 3499 | 263 | 3971 | 735 | 15 | 31 |
| recovery | 3614 | 274 | 3992 | 882 | 15 | 29 |
| post-baseline | 1268 | 273 | 2516 | 717 | — | — |

What this shows:

- **Client-side contention dominated the old recovery numbers.** Moving the
  noisy tenant to U02 cut U01 `client_cpu_pct` (≈93% → ≈82% in enforced) and
  dropped critical p95 in enforced/recovery from ≈3.5–3.6 s to ≈263–274 ms.
  The remaining gap to reference (≈29 ms) is real throttler/queue cost plus
  client pacing backlog, not the noisy tenant's CPU on U01.
- **Open-loop pacing + 4 critical threads cannot sustain 100 GET/s once
  latency rises.** ≈25% of critical slots were missed (>50 ms late) in every
  run, so `paced_client_kept_up` is INCONCLUSIVE and latency verdicts that
  depend on it are INCONCLUSIVE. The missed-slot backlog also keeps
  post-baseline p95 elevated (≈273 ms remote), which is why `baseline_stable`
  reports FAIL — treat that as "client could not keep up / backlog carried
  over", not proof of server drift.
- **Accepted noisy TPS under 429 rejection was ≈30/s for a 50 TPS limit**
  (remote), below the ≈45–47/s seen earlier; local mode fell to ≈15/s because
  U01 was CPU-saturated. Enforcement was still observed (≈41% of noisy
  requests got HTTP 429).
- **`QUEUE_TEST=1` (24 vs 6 noisy threads):** critical p95 was 210 ms at 24
  threads vs 293 ms at 6 — no evidence that a deeper ObjectScale throttle
  queue costs the critical tenant, but accepted noisy TPS also fell
  (20 vs 30/s), so the "same accepted TPS" condition was not met. Not a
  clean product finding; recorded as measured.
- **Clock offset:** after switching to the lowest-RTT `/health` sample,
  remote offsets were ≈0.1–8.9 ms (one earlier run showed single-sample
  spikes to ≈−128 ms from HTTP latency, which the guard correctly rejected).

## Simplified demo flow (six phases + staged rules)

Notebook 02 now shows six measured phases — baseline, contention,
simulation (dry-run), enforced (HTTP 429), recovery, post-baseline — then
the prefix test, Spark write, lifecycle check, verdicts and restore. The
in-demo propagation wait, pre-baseline, paced-noisy reference and
`QUEUE_TEST`/`enforced-24` probe were removed to keep the customer flow
simple.

- `99_reset` verifies and stores `demo-noisy-put` (all tenant-noisy PUTs,
  50 TPS) and `demo-raw-prefix` (`demo-*/raw/*`, 10 TPS) with the throttler
  disabled, then records `reports/rules_staged_at`. ObjectScale caches the
  rule set for up to 300 s, so running reset ≥5 min before notebook 02 hides
  the propagation wait; notebook 02 only counts down if the rules are still
  younger than 300 s.
- The rules are run-id agnostic (`demo-*/raw/*`) because `99_reset` runs
  before the notebook's `run_id` exists; both rules come from
  `poc/demo_rules.py` so staging and enforcement cannot drift apart.
- Recovery is judged by `recovery_improvement`: share =
  (contention_p95 − recovery_p95) / (contention_p95 − baseline_p95), PASS at
  ≥ 0.5 with enforcement observed; INCONCLUSIVE on environment drift, no
  enforcement, or < 2 ms of contention. `baseline_stable` compares
  post-baseline to baseline only; `recovery_p95_vs_baseline` is information
  only and does not affect the overall verdict.
- Critical pacing is configurable via `CRITICAL_RATE` (default 100 GET/s,
  open-loop with missed-slot tracking; `0` = as fast as possible). The lab
  validation for this change used `CRITICAL_RATE=0` (unpaced) per operator
  request, so the paced-client guard is omitted on those runs.
