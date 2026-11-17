"""Config loader: reads .env (repo root), never prints secrets."""
import os, pathlib

# repo root: POC_ROOT, else the checkout this module lives in
ROOT = pathlib.Path(os.environ.get("POC_ROOT")
                    or pathlib.Path(__file__).resolve().parent.parent)
ENV_FILE = ROOT / ".env"
# directory holding the 0600 credential files (IAM keys, per-session Spark
# secrets). Override with POC_SECRETS_DIR when Jupyter runs as a non-root user.
SECRETS_DIR = pathlib.Path(os.environ.get("POC_SECRETS_DIR", "/root/.poc"))
CREDS_FILE = SECRETS_DIR / "credentials"
REPORT_DIR = ROOT / "reports"
RAW_DIR = REPORT_DIR / "raw"
BACKUP_FILE = REPORT_DIR / "throttle_backup.json"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def load_env(path=ENV_FILE):
    env = {}
    if pathlib.Path(path).exists():
        for line in pathlib.Path(path).read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def load_credentials(path=CREDS_FILE):
    """Parse /root/.poc/credentials (INI-ish KEY=VAL blocks per user)."""
    creds = {}
    if not pathlib.Path(path).exists():
        return creds
    for line in pathlib.Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        creds[k.strip()] = v.strip()
    return creds


ENV = load_env()
CREDS = load_credentials()


def get(key, default=None):
    if key in os.environ:
        return os.environ[key]
    if key in CREDS:
        return CREDS[key]
    return ENV.get(key, default)


OBS_HOST = get("OBS_HOST", "192.168.1.31")
OBS_MGMT = get("OBS_MGMT", f"https://{OBS_HOST}:4443")
OBS_S3_ENDPOINT = get("OBS_S3_ENDPOINT", f"https://{OBS_HOST}:9021")
# TableHead (S3 Tables + Iceberg REST): HTTPS on 19021 (verified live with the
# OBS CA for both boto3 and the Spark/Iceberg REST client); 19020 is plain HTTP
OBS_TABLES_ENDPOINT = get("OBS_TABLES_ENDPOINT", f"https://{OBS_HOST}:19021")
# CA bundle that contains the ObjectScale CA (system bundle after
# update-ca-certificates); TLS verification is always on
CA_BUNDLE = get("OBS_CA_BUNDLE", "/etc/ssl/certs/ca-certificates.crt")
OBS_REGION = get("OBS_REGION", "us-east-1")
OBS_NAMESPACE = get("OBS_NAMESPACE", "poc")
OBS_ROOT_PASSWORD = os.environ.get("OBS_ROOT_PASSWORD", "")  # never stored
ICEBERG_SIGNING_NAME = get("ICEBERG_SIGNING_NAME", "s3tables")
# table bucket NAME, e.g. 'poc-lakehouse' (an ARN in an old .env is reduced
# to its last path segment); every ARN is built by table_bucket_arn()
TABLE_BUCKET = get("TABLE_BUCKET", "poc-lakehouse").rsplit("/", 1)[-1]


def table_bucket_arn(name=None):
    """S3 Tables ARN of a table bucket — also the Iceberg REST warehouse."""
    return f"arn:aws:s3tables:::bucket/{name or TABLE_BUCKET}"


ICEBERG_WAREHOUSE = table_bucket_arn()
BUCKET_DATA = get("BUCKET_DATA", "poc-spark-data")
BUCKET_THROTTLE = get("BUCKET_THROTTLE", "poc-throttle")
DEMO_SCALE_GB = float(get("DEMO_SCALE_GB", "1"))
# rows of the synthetic orders dataset per DEMO_SCALE_GB unit (measured:
# reports/environment.md) and the fixed first partition date
DEMO_ROWS_PER_GB = int(get("DEMO_ROWS_PER_GB", "0") or 0) or 8_000_000
DEMO_BASE_DATE = get("DEMO_BASE_DATE", "2026-01-01")
RUN_ID = get("RUN_ID")  # optional fixed run id; default demo-<timestamp>
SPARK_MASTER = get("SPARK_MASTER", "spark://192.168.1.40:7077")
SPARK_UI = get("SPARK_UI", "http://192.168.1.40:8080")
SPARK_JARS = get("SPARK_JARS", "/opt/spark/jars")
ICEBERG_REST_URI = OBS_TABLES_ENDPOINT.rstrip("/") + "/iceberg"
OBS_TABLE_BUCKET = TABLE_BUCKET   # alias kept for existing callers
TABLES_NAMESPACE = get("TABLES_NAMESPACE", "pocns")
OBS_BUCKET = BUCKET_DATA
OBS_BUCKET_THROTTLE = BUCKET_THROTTLE

# Optional second load-generator host for the noisy tenant. Empty means the
# noisy PUT workers run inside the Jupyter host's LoadGen (local mode).
NOISY_HOST = get("NOISY_HOST", "")
NOISY_AGENT_TOKEN = get("NOISY_AGENT_TOKEN", "")
# noisy-agent.service on the remote host (poc/noisy_agent.py)
NOISY_AGENT_BIND = get("NOISY_AGENT_BIND", "")
NOISY_AGENT_PORT = int(get("NOISY_AGENT_PORT", "8765") or 8765)
NOISY_AGENT_IDLE_S = float(get("NOISY_AGENT_IDLE_S", "60") or 60)
# Open-loop critical GET rate for notebook 02 (req/s); 0 = as fast as
# possible (the validated default — pacing is experimental and needs enough
# threads for the rate)
CRITICAL_RATE = float(get("CRITICAL_RATE", "0") or 0)

# Optional load shaping for notebook 02 (all default to prior behaviour)
NOISY_THREADS = int(get("NOISY_THREADS", "6") or 6)
NOISY_OBJECT_KB = int(get("NOISY_OBJECT_KB", "64") or 64)
PAYLOAD_RANDOM = int(get("PAYLOAD_RANDOM", "0") or 0)
CRITICAL_OBJECTS = int(get("CRITICAL_OBJECTS", "1") or 1)
CRITICAL_OBJECT_KB = int(get("CRITICAL_OBJECT_KB", "64") or 64)
SPARK_WRITE_ROWS = int(get("SPARK_WRITE_ROWS", "50000") or 50000)
SPARK_WRITE_PARTITIONS = int(get("SPARK_WRITE_PARTITIONS", "4") or 4)
SPARK_WRITE_REPEATS = int(get("SPARK_WRITE_REPEATS", "1") or 1)

USERS = {
    "admin":  (get("POC_ADMIN_AK"),  get("POC_ADMIN_SK")),
    "noisy":  (get("NOISY_AK"),      get("NOISY_SK")),
    "critical": (get("CRITICAL_AK"), get("CRITICAL_SK")),
    "tables-rw": (get("TABLES_RW_AK"), get("TABLES_RW_SK")),
    "tables-ro": (get("TABLES_RO_AK"), get("TABLES_RO_SK")),
}

OBS_MGMT_USER = "root"
OBS_CA_MGMT = "/usr/local/share/ca-certificates/obs-mgmt.crt"
OBS_CA_S3 = "/usr/local/share/ca-certificates/obs-s3.crt"


def mask(v, keep=4):
    """Show only first `keep` chars of a secret."""
    if not v:
        return "<empty>"
    return v[:keep] + "…" if len(v) > keep else v


def masked_users():
    return {u: (mask(ak), "***") for u, (ak, sk) in USERS.items()}
