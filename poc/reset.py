"""99_reset: remove demo objects/tables/table-bucket contents + restore throttler.

Cleans: objects under any 'demo-*' prefix in poc-spark-data / poc-throttle
(plus seed objects), all demo tables + namespaces in the poc-lakehouse table
bucket, and restores reports/throttle_backup.json.
"""
from . import s3util, config
from .obs_admin import MgmtApi, Throttler

DEMO_PREFIXES = ("demo-",)


def _namespaces(st, tb_arn):
    try:
        return [ns["namespace"][0] for ns in
                st.list_namespaces(tableBucketARN=tb_arn).get("namespaces", [])]
    except Exception:
        return []


def clean_ordinary_buckets():
    """Delete every object whose key starts with a demo prefix, plus seed keys."""
    removed = {}
    for bucket in (config.OBS_BUCKET, config.OBS_BUCKET_THROTTLE):
        cli = s3util.s3("admin")
        n = 0
        try:
            pag = cli.get_paginator("list_objects_v2")
            for page in pag.paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    top = key.split("/", 1)[0]
                    if any(top.startswith(p) for p in DEMO_PREFIXES):
                        cli.delete_object(Bucket=bucket, Key=key)
                        n += 1
            removed[bucket] = n
        except cli.exceptions.NoSuchBucket:
            removed[bucket] = "(missing)"
    return removed


def clean_table_bucket():
    """Delete every table in every namespace of the table bucket (API-level)."""
    st = s3util.s3tables("tables-rw")
    tb_arn = f"arn:aws:s3tables:::bucket/{config.OBS_TABLE_BUCKET}"
    deleted = []
    nss = _namespaces(st, tb_arn)
    for ns in nss:
        try:
            tabs = st.list_tables(tableBucketARN=tb_arn, namespace=ns).get("tables", [])
        except Exception:
            tabs = []
        for t in tabs:
            try:
                st.delete_table(tableBucketARN=tb_arn, namespace=ns, name=t["name"])
                deleted.append(f"{ns}.{t['name']}")
            except Exception as e:
                deleted.append(f"{ns}.{t['name']} (FAILED: {e.__class__.__name__})")
        try:
            st.delete_namespace(tableBucketARN=tb_arn, namespace=ns)
        except Exception:
            pass
    return deleted


def drop_table_bucket():
    """Delete every table + namespace in the table bucket, then the bucket
    itself, then any orphan '<uuid>--table-s3' data buckets."""
    st = s3util.s3tables("tables-rw")
    tb_arn = f"arn:aws:s3tables:::bucket/{config.OBS_TABLE_BUCKET}"
    out = {"tables_removed": clean_table_bucket(),
           "bucket_deleted": False, "data_buckets": []}
    try:
        st.delete_table_bucket(tableBucketARN=tb_arn)
        out["bucket_deleted"] = True
    except Exception as e:
        out["bucket_deleted"] = e.__class__.__name__
    cli = s3util.s3("admin")
    try:
        names = [b["Name"] for b in cli.list_buckets().get("Buckets", [])
                 if b["Name"].endswith("--table-s3")]
    except Exception:
        names = []
    for name in names:
        try:
            pag = cli.get_paginator("list_objects_v2")
            for page in pag.paginate(Bucket=name):
                for o in page.get("Contents", []):
                    cli.delete_object(Bucket=name, Key=o["Key"])
            cli.delete_bucket(Bucket=name)
            out["data_buckets"].append(name)
        except Exception as e:
            out["data_buckets"].append(f"{name} (FAILED: {e.__class__.__name__})")
    return out


def ensure_seed_structure():
    """Namespace + table bucket exist; ordinary buckets have raw/ curated/."""
    st = s3util.s3tables("tables-rw")
    tb_arn = f"arn:aws:s3tables:::bucket/{config.OBS_TABLE_BUCKET}"
    try:
        st.create_table_bucket(name=config.OBS_TABLE_BUCKET)
    except Exception:
        pass
    if config.TABLES_NAMESPACE not in _namespaces(st, tb_arn):
        st.create_namespace(tableBucketARN=tb_arn, namespace=[config.TABLES_NAMESPACE])
    cli = s3util.s3("admin")
    for b in (config.OBS_BUCKET, config.OBS_BUCKET_THROTTLE):
        try:
            cli.create_bucket(Bucket=b)
        except Exception:
            pass
        for p in ("raw/", "curated/"):
            cli.put_object(Bucket=b, Key=p, Body=b"")
    return {"namespaces": [n for n in st.list_namespaces(tableBucketARN=tb_arn).get("namespaces", [])]}


def restore_throttler(backup=None):
    backup = backup or config.BACKUP_FILE
    return Throttler(MgmtApi()).restore(backup)


def full_reset(backup=None):
    out = {}
    out["restore_throttler"] = restore_throttler(backup)
    out["objects_removed"] = clean_ordinary_buckets()
    drop = drop_table_bucket()
    out["tables_removed"] = drop["tables_removed"]
    out["table_bucket_deleted"] = drop["bucket_deleted"]
    out["data_buckets_deleted"] = drop["data_buckets"]
    out["seed"] = ensure_seed_structure()
    return out
