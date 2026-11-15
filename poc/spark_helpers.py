"""SparkSession construction + display helpers shared by all notebooks."""
import os, json, pathlib, urllib.parse
from . import config, results

def _secret_conf_file(tables_user):
    """Per-user Spark conf holding credentials, mode 0600. Referenced via
    --properties-file so keys never appear on the spark-submit command line
    (visible in `ps` and the Spark UI)."""
    ak, sk = config.USERS[tables_user]
    adm_ak, adm_sk = config.USERS["admin"]
    path = pathlib.Path(f"/root/.poc/spark-{tables_user}.conf")
    path.write_text("\n".join([
        f"spark.executorEnv.AWS_ACCESS_KEY_ID {ak}",
        f"spark.executorEnv.AWS_SECRET_ACCESS_KEY {sk}",
        f"spark.executorEnv.AWS_REGION {config.OBS_REGION}",
        "spark.executorEnv.AWS_REQUEST_CHECKSUM_CALCULATION when_required",
        "spark.executorEnv.AWS_RESPONSE_CHECKSUM_VALIDATION when_required",
        f"spark.hadoop.fs.s3a.access.key {adm_ak}",
        f"spark.hadoop.fs.s3a.secret.key {adm_sk}",
    ]) + "\n")
    os.chmod(path, 0o600)
    return path


def build_session(app="obs-poc", tables_user="tables-rw"):
    """SparkSession on the standalone cluster with S3A + Iceberg 'os' catalog."""
    # driver-side env for the Iceberg REST SigV4 signer (executors get executorEnv
    # via the 0600 properties file — not the command line)
    os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"] = config.USERS[tables_user]
    os.environ["AWS_REGION"] = config.OBS_REGION
    os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"
    os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"

    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    gw = getattr(SparkContext, "_gateway", None)
    if gw is not None and getattr(SparkContext, "_active_spark_context", None) is None:
        # a previous session was stopped in this kernel — drop stale handles or
        # getOrCreate() reuses the dead session and dies on a stopped ListenerBus
        SparkSession._instantiatedSession = None
        try:
            SparkSession._activeSession = None
            gw.jvm.SparkSession.clearActiveSession()
            gw.jvm.SparkSession.clearDefaultSession()
        except Exception:
            pass
    if gw is None:
        # fresh JVM: keep keys off the spark-submit command line
        conf_file = _secret_conf_file(tables_user)
        os.environ["PYSPARK_SUBMIT_ARGS"] = \
            f"--properties-file {conf_file} pyspark-shell"

    secret = {}
    if getattr(SparkContext, "_gateway", None) is not None:
        # reused JVM: no new submit happens, so --properties-file is never read;
        # per-user creds must come from builder confs instead (not cmdline-visible)
        ak, sk = config.USERS[tables_user]
        adm_ak, adm_sk = config.USERS["admin"]
        secret = {
            "spark.executorEnv.AWS_ACCESS_KEY_ID": ak,
            "spark.executorEnv.AWS_SECRET_ACCESS_KEY": sk,
            "spark.executorEnv.AWS_REGION": config.OBS_REGION,
            "spark.executorEnv.AWS_REQUEST_CHECKSUM_CALCULATION": "when_required",
            "spark.executorEnv.AWS_RESPONSE_CHECKSUM_VALIDATION": "when_required",
            "spark.hadoop.fs.s3a.access.key": adm_ak,
            "spark.hadoop.fs.s3a.secret.key": adm_sk,
        }

    tb_arn = f"arn:aws:s3tables:::bucket/{config.OBS_TABLE_BUCKET}"
    # OBS Iceberg REST expects the tableBucketARN as {prefix} path segment,
    # percent-encoded; OBS verifies SigV4 with S3-style double encoding,
    # which requires the patched iceberg runtime built by scripts/build_iceberg.sh
    tb_prefix = urllib.parse.quote(tb_arn, safe="")
    b = (SparkSession.builder
         .master(config.SPARK_MASTER).appName(app)
         .config("spark.driver.memory", "1g")
         .config("spark.executor.memory", "1g")
         .config("spark.executor.cores", "1")
         .config("spark.eventLog.enabled", "true")
         .config("spark.eventLog.dir", "file:///var/log/spark-events")
         .config("spark.driver.extraJavaOptions",
                 "-Daws.requestChecksumCalculation=when_required "
                 "-Daws.responseChecksumValidation=when_required")
         .config("spark.executor.extraJavaOptions",
                 "-Daws.requestChecksumCalculation=when_required "
                 "-Daws.responseChecksumValidation=when_required")
         # ---------- S3A (ordinary buckets) — admin identity ----------
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.endpoint", config.OBS_S3_ENDPOINT)
         .config("spark.hadoop.fs.s3a.endpoint.region", config.OBS_REGION)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .config("spark.hadoop.fs.s3a.committer.name", "magic")
         .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
         .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
         .config("spark.hadoop.fs.s3a.connection.maximum", "200")
         .config("spark.hadoop.fs.s3a.threads.max", "64")
         .config("spark.hadoop.fs.s3a.multipart.size", "128M")
         .config("spark.hadoop.fs.s3a.retry.throttle.limit", "20")
         .config("spark.hadoop.fs.s3a.retry.throttle.interval", "500ms")
         # ---------- Iceberg REST catalog 'os' (S3 Tables) ----------
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.os", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.os.type", "rest")
         .config("spark.sql.catalog.os.uri", config.ICEBERG_REST_URI)
         .config("spark.sql.catalog.os.warehouse", tb_arn)
         .config("spark.sql.catalog.os.prefix", tb_prefix)
         .config("spark.sql.catalog.os.rest.sigv4-enabled", "true")
         .config("spark.sql.catalog.os.rest.signing-name", "s3tables")
         .config("spark.sql.catalog.os.rest.signing-region", config.OBS_REGION)
         .config("spark.sql.catalog.os.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config("spark.sql.catalog.os.s3.endpoint", config.OBS_S3_ENDPOINT)
         .config("spark.sql.catalog.os.s3.path-style-access", "true")
         .config("spark.sql.catalog.os.client.region", config.OBS_REGION))
    for k, v in secret.items():
        b = b.config(k, v)
    return b.getOrCreate()


def versions(spark):
    hadoop = spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    java = spark._jvm.java.lang.System.getProperty("java.version")
    obs_ver = "-"
    try:
        from .obs_admin import MgmtApi
        obs_ver = MgmtApi().version()
    except Exception:
        pass
    return {
        "Spark": spark.version, "Hadoop": hadoop, "Iceberg": "1.12.0",
        "Java": java, "ObjectScale": obs_ver,
        "Master": config.SPARK_MASTER,
        "Spark UI": "http://192.168.1.40:8080",
    }


def banner(tc, passed, msg=""):
    """Green/red PASS/FAIL HTML banner; also writes the TC result JSON."""
    from IPython.display import HTML, display
    color = "#1e7d32" if passed else "#b71c1c"
    label = "PASS" if passed else "FAIL"
    display(HTML(
        f'<div style="background:{color};color:#fff;padding:12px 18px;border-radius:8px;'
        f'font-size:18px;font-family:sans-serif"><b>{label}</b> — {tc} {msg}</div>'))
    return {"tc": tc, "passed": passed, "msg": msg}
