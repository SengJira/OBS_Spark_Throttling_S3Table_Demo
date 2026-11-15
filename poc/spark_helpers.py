"""SparkSession construction shared by all notebooks.

build_session() is the single source of truth for per-session Spark settings
(S3A options, the Iceberg `os` catalog and the identities used). The rendered
/opt/spark/conf/spark-defaults.conf only carries cluster-wide, non-secret
settings (see conf/spark-defaults.conf.j2 and README "Spark configuration").

Identities are passed explicitly per session — never through environment
variables, which a reused driver JVM does not see:
  * S3A (ordinary buckets):   fs.s3a.access.key / fs.s3a.secret.key
  * Iceberg REST SigV4 signer: spark.sql.catalog.os.rest.access-key-id / rest.secret-access-key
  * Iceberg S3FileIO (data):   spark.sql.catalog.os.s3.access-key-id / s3.secret-access-key
(property names from Iceberg 1.12 AwsProperties / S3FileIOProperties).

Secrets stay off the spark-submit command line: a fresh JVM reads them from a
0600 --properties-file; a reused in-kernel JVM gets them as builder confs (no
new process is started, so nothing reaches `ps`).
"""
import os, pathlib, urllib.parse

from . import config

CATALOG = "spark.sql.catalog.os"
# also masks '...access-key-id' and AWS_ACCESS_KEY_ID in the UI / event log
REDACTION_REGEX = "(?i)secret|password|token|access[._-]?key"
_AWS_ENV = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
_CHECKSUM_OPTS = ("-Daws.requestChecksumCalculation=when_required "
                  "-Daws.responseChecksumValidation=when_required")


def public_conf():
    """Non-secret per-session settings (identical for every identity)."""
    tb_arn = config.table_bucket_arn()
    https = config.OBS_S3_ENDPOINT.lower().startswith("https://")
    return {
        "spark.driver.memory": "1g",
        "spark.executor.memory": "1g",
        "spark.executor.cores": "1",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "file:///var/log/spark-events",
        "spark.redaction.regex": REDACTION_REGEX,
        "spark.driver.extraJavaOptions": _CHECKSUM_OPTS,
        "spark.executor.extraJavaOptions": _CHECKSUM_OPTS,
        # ---------- S3A (ordinary buckets) ----------
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": config.OBS_S3_ENDPOINT,
        "spark.hadoop.fs.s3a.endpoint.region": config.OBS_REGION,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(https).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # magic committer: used for non-Parquet formats. Parquet keeps Spark's
        # ParquetOutputCommitter (rename-based) unless spark-hadoop-cloud is
        # installed — see README "Spark configuration".
        "spark.hadoop.fs.s3a.committer.name": "magic",
        "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
        "spark.hadoop.fs.s3a.connection.maximum": "200",
        "spark.hadoop.fs.s3a.threads.max": "64",
        "spark.hadoop.fs.s3a.multipart.size": "128M",
        "spark.hadoop.fs.s3a.retry.throttle.limit": "20",
        "spark.hadoop.fs.s3a.retry.throttle.interval": "500ms",
        # ETag-based change detection (Hadoop default) — works on ObjectScale
        "spark.hadoop.fs.s3a.change.detection.mode": "server",
        "spark.hadoop.fs.s3a.change.detection.source": "etag",
        # columnar formats seek a lot: random IO policy
        "spark.hadoop.fs.s3a.experimental.input.fadvise": "random",
        # ---------- Iceberg REST catalog 'os' (S3 Tables) ----------
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        CATALOG: "org.apache.iceberg.spark.SparkCatalog",
        f"{CATALOG}.type": "rest",
        f"{CATALOG}.uri": config.ICEBERG_REST_URI,
        f"{CATALOG}.warehouse": tb_arn,
        # OBS expects the tableBucketARN as the {prefix} path segment,
        # percent-encoded; it verifies SigV4 with S3-style double encoding,
        # which needs the patched runtime from scripts/build_iceberg.sh
        f"{CATALOG}.prefix": urllib.parse.quote(tb_arn, safe=""),
        f"{CATALOG}.rest.sigv4-enabled": "true",
        f"{CATALOG}.rest.signing-name": config.ICEBERG_SIGNING_NAME,
        f"{CATALOG}.rest.signing-region": config.OBS_REGION,
        f"{CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{CATALOG}.s3.endpoint": config.OBS_S3_ENDPOINT,
        f"{CATALOG}.s3.path-style-access": "true",
        f"{CATALOG}.client.region": config.OBS_REGION,
    }


def secret_conf(tables_user="tables-rw", s3a_user="admin"):
    """Credential-bearing settings for one (tables identity, S3A identity)."""
    t_ak, t_sk = config.USERS[tables_user]
    s_ak, s_sk = config.USERS[s3a_user]
    if not (t_ak and t_sk and s_ak and s_sk):
        raise RuntimeError(
            f"missing credentials for {tables_user!r} / {s3a_user!r} — "
            f"run scripts/prep_obs.py (keys live in {config.CREDS_FILE})")
    return {
        "spark.hadoop.fs.s3a.access.key": s_ak,
        "spark.hadoop.fs.s3a.secret.key": s_sk,
        f"{CATALOG}.rest.access-key-id": t_ak,
        f"{CATALOG}.rest.secret-access-key": t_sk,
        f"{CATALOG}.s3.access-key-id": t_ak,
        f"{CATALOG}.s3.secret-access-key": t_sk,
    }


def write_private(path, text):
    """Create/replace `path` with mode 0600 from the first byte (no window in
    which the file is world-readable)."""
    path = pathlib.Path(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(fd, 0o600)          # also tightens a pre-existing file
        os.write(fd, text.encode())
    finally:
        os.close(fd)
    return path


def _secret_conf_file(tables_user, s3a_user):
    """0600 Spark properties file with the session's credentials; passed as
    --properties-file so keys never appear on the spark-submit command line."""
    conf = secret_conf(tables_user, s3a_user)
    path = config.SECRETS_DIR / f"spark-{tables_user}-{s3a_user}.conf"
    return write_private(
        path, "".join(f"{k} {v}\n" for k, v in sorted(conf.items())))


def build_session(app="obs-poc", tables_user="tables-rw", s3a_user="admin"):
    """SparkSession on the standalone cluster.

    tables_user: identity for the Iceberg `os` catalog (REST + S3FileIO).
    s3a_user:    identity for s3a:// paths (ordinary buckets).
    Safe to call again in the same kernel after spark.stop() to switch
    identity — verify with identities(spark)."""
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    # never let the SDK default chain supply an identity behind our back
    for k in _AWS_ENV:
        os.environ.pop(k, None)

    want = {"tables": tables_user, "s3a": s3a_user}
    gw = getattr(SparkContext, "_gateway", None)
    if gw is not None and getattr(SparkContext, "_active_spark_context", None) is not None:
        # a session is already running in this kernel (cell re-run): reuse it
        # only if it has the identity that was asked for
        spark = SparkSession.builder.getOrCreate()
        got = identities(spark)
        if {k: got[k] for k in want} == want:
            return spark
        raise RuntimeError(
            f"a SparkSession with identity {got} is still running in this "
            f"kernel — call spark.stop() before building one for {want}")
    conf = public_conf()
    if gw is None:
        # fresh JVM: secrets via a 0600 properties file, not --conf arguments
        conf_file = _secret_conf_file(tables_user, s3a_user)
        os.environ["PYSPARK_SUBMIT_ARGS"] = \
            f"--properties-file {conf_file} pyspark-shell"
    else:
        # reused JVM (a previous session was stopped in this kernel): no new
        # process, so the properties file is not re-read. Drop every cached
        # handle that still carries the previous identity, then pass the
        # credentials as builder confs.
        SparkSession._instantiatedSession = None
        try:
            SparkSession._activeSession = None
            gw.jvm.SparkSession.clearActiveSession()
            gw.jvm.SparkSession.clearDefaultSession()
        except Exception:
            pass
        try:   # cached S3AFileSystem instances hold the old S3A keys
            gw.jvm.org.apache.hadoop.fs.FileSystem.closeAll()
        except Exception:
            pass
        conf.update(secret_conf(tables_user, s3a_user))

    b = SparkSession.builder.master(config.SPARK_MASTER).appName(app)
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    got = identities(spark)
    if {k: got[k] for k in want} != want:
        spark.stop()
        raise RuntimeError(f"session identity mismatch: wanted {want}, the "
                           f"running session has {got} — restart the kernel")
    return spark


def _user_of(access_key):
    for user, (ak, _) in config.USERS.items():
        if ak and ak == access_key:
            return user
    return "unknown"


def identities(spark):
    """Which PoC identity the RUNNING session really uses (keys masked)."""
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    s3a_ak = hconf.get("fs.s3a.access.key")
    rest_ak = spark.conf.get(f"{CATALOG}.rest.access-key-id", None)
    io_ak = spark.conf.get(f"{CATALOG}.s3.access-key-id", None)
    tables = _user_of(rest_ak)
    if _user_of(io_ak) != tables:
        tables = f"MIXED({tables}/{_user_of(io_ak)})"
    return {"tables": tables, "s3a": _user_of(s3a_ak),
            "tables_key": config.mask(rest_ak), "s3a_key": config.mask(s3a_ak)}


def iceberg_version(spark):
    """Iceberg version of the runtime actually loaded in the driver JVM."""
    try:
        return str(spark._jvm.org.apache.iceberg.IcebergBuild.version())
    except Exception:
        import glob, re
        for jar in glob.glob(f"{config.SPARK_JARS}/iceberg-spark-runtime-*.jar"):
            m = re.search(r"-(\d+\.\d+\.\d+[^/]*)\.jar$", jar)
            if m:
                return m.group(1) + " (from jar name)"
        return "unknown"


def versions(spark):
    hadoop = spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    java = spark._jvm.java.lang.System.getProperty("java.version")
    try:
        from .obs_admin import MgmtApi
        obs_ver = MgmtApi().version() or "unknown"
    except Exception as e:
        obs_ver = f"unavailable ({type(e).__name__})"
    return {
        "Spark": spark.version, "Hadoop": hadoop,
        "Iceberg": iceberg_version(spark),
        "Java": java, "ObjectScale": obs_ver,
        "Master": config.SPARK_MASTER,
        "Spark UI": config.SPARK_UI,
    }


def s3a_committer_stats(bucket, prefix, user="admin"):
    """IOStatistics the S3A committer aggregated from every task into the
    job's _SUCCESS file (non-Parquet writes). Returns the counters relevant to
    throttling, or {} when the file carries no statistics."""
    import json
    from . import s3util
    try:
        body = s3util.s3(user).get_object(
            Bucket=bucket, Key=f"{prefix.rstrip('/')}/_SUCCESS")["Body"].read()
        doc = json.loads(body) if body else {}
    except Exception:
        return {}
    counters = (doc.get("iostatistics") or {}).get("counters") or {}
    keep = ("throttl", "retry", "retries", "ignored_errors", "failures")
    out = {k: v for k, v in counters.items()
           if any(s in k for s in keep) and v}
    out["committer"] = doc.get("committer")
    return out
