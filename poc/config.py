"""Config loader: reads .env (repo root), never prints secrets."""
import os, pathlib, re

ROOT = pathlib.Path(os.environ.get("POC_ROOT", "/root/OBS"))
ENV_FILE = ROOT / ".env"
CREDS_FILE = pathlib.Path("/root/.poc/credentials")
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
OBS_TABLES_ENDPOINT = get("OBS_TABLES_ENDPOINT", f"http://{OBS_HOST}:19020")
OBS_REGION = get("OBS_REGION", "us-east-1")
OBS_NAMESPACE = get("OBS_NAMESPACE", "poc")
OBS_ROOT_PASSWORD = os.environ.get("OBS_ROOT_PASSWORD", "")  # never stored
ICEBERG_SIGNING_NAME = get("ICEBERG_SIGNING_NAME", "s3tables")
ICEBERG_WAREHOUSE = get("ICEBERG_WAREHOUSE", "arn:aws:s3tables:::bucket/poc-lakehouse")
TABLE_BUCKET = get("TABLE_BUCKET", ICEBERG_WAREHOUSE)
BUCKET_DATA = get("BUCKET_DATA", "poc-spark-data")
BUCKET_THROTTLE = get("BUCKET_THROTTLE", "poc-throttle")
DEMO_SCALE_GB = float(get("DEMO_SCALE_GB", "1"))
RUN_ID = get("RUN_ID")  # optional fixed run id; default demo-<timestamp>
SPARK_MASTER = get("SPARK_MASTER", "spark://192.168.1.40:7077")
SPARK_UI = get("SPARK_UI", "http://192.168.1.40:8080")
SPARK_JARS = get("SPARK_JARS", "/opt/spark/jars")
ICEBERG_REST_URI = OBS_TABLES_ENDPOINT.rstrip("/") + "/iceberg"
# table bucket NAME (last ARN path segment), e.g. 'poc-lakehouse'
OBS_TABLE_BUCKET = get("TABLE_BUCKET_NAME",
                       ICEBERG_WAREHOUSE.rsplit("/", 1)[-1])
TABLES_NAMESPACE = get("TABLES_NAMESPACE", "pocns")
OBS_BUCKET = BUCKET_DATA
OBS_BUCKET_THROTTLE = BUCKET_THROTTLE

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
