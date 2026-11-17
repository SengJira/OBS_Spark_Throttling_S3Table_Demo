"""99_reset: remove what THIS PoC created, restore the throttler, re-seed.

Deletes only:
  * objects under a 'demo-*' top-level prefix in the two PoC buckets;
  * tables + namespaces inside the PoC table bucket (config.TABLE_BUCKET);
  * the '<table-uuid>--table-s3' data buckets of exactly those tables, taken
    from each table's own metadata (get_table.warehouseLocation) before the
    table is deleted, and remembered in a ledger so an interrupted reset can
    finish later;
  * the PoC table bucket itself (re-created by ensure_seed_structure).
Any other '--table-s3' bucket the admin user can list is reported as
unattributed and left alone — it may belong to another team.

full_reset(dry_run=True) returns the same plan without changing anything.
"""
import json, time

from . import s3util, config, demo_rules
from . import throttle_eval as te
from .obs_admin import MgmtApi, Throttler

DEMO_PREFIXES = ("demo-",)
DATA_BUCKET_SUFFIX = "--table-s3"
LEDGER = config.REPORT_DIR / "owned_table_buckets.json"
BATCH = 1000


# ---------------- helpers ----------------
def _namespaces(st, tb_arn):
    try:
        return [ns["namespace"][0] for ns in
                st.list_namespaces(tableBucketARN=tb_arn).get("namespaces", [])]
    except Exception:
        return []


def _code(e):
    return (getattr(e, "response", None) or {}).get("Error", {}).get(
        "Code", e.__class__.__name__)


def _is_demo_key(key):
    return any(key.split("/", 1)[0].startswith(p) for p in DEMO_PREFIXES)


def _versioned(cli, bucket):
    try:
        return cli.get_bucket_versioning(Bucket=bucket).get("Status") in (
            "Enabled", "Suspended")
    except Exception:
        return False


def _iter_keys(cli, bucket, want=lambda key: True):
    """Yield delete_objects identifiers; every version + delete marker when
    the bucket is (or was) versioned."""
    if _versioned(cli, bucket):
        pag = cli.get_paginator("list_object_versions")
        for page in pag.paginate(Bucket=bucket):
            for o in page.get("Versions", []) + page.get("DeleteMarkers", []):
                if want(o["Key"]):
                    yield {"Key": o["Key"], "VersionId": o["VersionId"]}
    else:
        pag = cli.get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket=bucket):
            for o in page.get("Contents", []):
                if want(o["Key"]):
                    yield {"Key": o["Key"]}


def delete_keys(cli, bucket, want=lambda key: True, dry_run=False):
    """Batched delete (<=1000 keys per call). Returns (deleted, errors)."""
    n, errors, batch = 0, [], []

    def flush():
        nonlocal n
        if not batch:
            return
        if not dry_run:
            r = cli.delete_objects(Bucket=bucket,
                                   Delete={"Objects": list(batch), "Quiet": True})
            errs = r.get("Errors", []) or []
            errors.extend(f"{e.get('Key')}: {e.get('Code')}" for e in errs)
            n += len(batch) - len(errs)
        else:
            n += len(batch)
        batch.clear()

    for ident in _iter_keys(cli, bucket, want):
        batch.append(ident)
        if len(batch) >= BATCH:
            flush()
    flush()
    return n, errors


# ---------------- ledger of data buckets the PoC owns ----------------
def _ledger_read():
    try:
        return sorted(set(json.loads(LEDGER.read_text())))
    except Exception:
        return []


def _ledger_write(names):
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    LEDGER.write_text(json.dumps(sorted(set(names)), indent=1))


# ---------------- ordinary buckets ----------------
def clean_ordinary_buckets(dry_run=False):
    """Delete every object whose top-level prefix is a demo prefix."""
    removed = {}
    cli = s3util.s3("admin")
    for bucket in (config.OBS_BUCKET, config.OBS_BUCKET_THROTTLE):
        try:
            n, errors = delete_keys(cli, bucket, _is_demo_key, dry_run)
            removed[bucket] = n if not errors else f"{n} ({len(errors)} failed)"
        except Exception as e:
            if _code(e) in ("NoSuchBucket", "404"):
                removed[bucket] = "(missing)"
            else:
                raise
    return removed


# ---------------- table bucket ----------------
def table_data_buckets(st=None, tb_arn=None):
    """Data bucket of every table inside the PoC table bucket, read from the
    table's own metadata. Returns [{'table','bucket'}] plus a list of notes
    for tables whose location could not be attributed safely."""
    st = st or s3util.s3tables("tables-rw")
    tb_arn = tb_arn or config.table_bucket_arn()
    found, notes = [], []
    for ns in _namespaces(st, tb_arn):
        try:
            tabs = st.list_tables(tableBucketARN=tb_arn, namespace=ns).get("tables", [])
        except Exception as e:
            notes.append(f"{ns}: list_tables failed ({_code(e)})")
            continue
        for t in tabs:
            name = f"{ns}.{t['name']}"
            try:
                g = st.get_table(tableBucketARN=tb_arn, namespace=ns, name=t["name"])
            except Exception as e:
                notes.append(f"{name}: get_table failed ({_code(e)})")
                continue
            loc = g.get("warehouseLocation") or g.get("metadataLocation") or ""
            bucket = loc[len("s3://"):].split("/", 1)[0] if loc.startswith("s3://") else ""
            table_id = str(g.get("tableARN", "")).rsplit("/", 1)[-1]
            if not bucket.endswith(DATA_BUCKET_SUFFIX):
                notes.append(f"{name}: unexpected location {loc!r} — bucket not touched")
            elif table_id and bucket != f"{table_id}{DATA_BUCKET_SUFFIX}":
                notes.append(f"{name}: location bucket {bucket} does not match "
                             f"table id {table_id} — bucket not touched")
            else:
                found.append({"table": name, "bucket": bucket})
    return found, notes


def clean_table_bucket(dry_run=False):
    """Delete every table + namespace in the PoC table bucket (API-level)."""
    st = s3util.s3tables("tables-rw")
    tb_arn = config.table_bucket_arn()
    deleted = []
    for ns in _namespaces(st, tb_arn):
        try:
            tabs = st.list_tables(tableBucketARN=tb_arn, namespace=ns).get("tables", [])
        except Exception:
            tabs = []
        for t in tabs:
            if dry_run:
                deleted.append(f"{ns}.{t['name']}")
                continue
            try:
                st.delete_table(tableBucketARN=tb_arn, namespace=ns, name=t["name"])
                deleted.append(f"{ns}.{t['name']}")
            except Exception as e:
                deleted.append(f"{ns}.{t['name']} (FAILED: {_code(e)})")
        if not dry_run:
            try:
                st.delete_namespace(tableBucketARN=tb_arn, namespace=ns)
            except Exception:
                pass
    return deleted


PENDING = "pending (ObjectScale is still emptying it)"


def _delete_data_bucket(cli, name):
    """One attempt to empty + delete a PoC-owned data bucket. ObjectScale
    empties and removes a table's data bucket by itself after delete_table
    (observed: ~80 s for an empty table) and rejects changes meanwhile with
    EmptyBucketInProgress — reported as pending, not as a failure."""
    try:
        delete_keys(cli, name)
        cli.delete_bucket(Bucket=name)
        return "deleted"
    except Exception as e:
        code = _code(e)
        if code in ("NoSuchBucket", "404"):
            return "gone"
        if code == "EmptyBucketInProgress":
            return PENDING
        return f"FAILED: {code}"


def drop_table_bucket(dry_run=False, wait_s=120):
    """Delete the PoC tables, their own data buckets, then the table bucket."""
    st = s3util.s3tables("tables-rw")
    tb_arn = config.table_bucket_arn()
    cli = s3util.s3("admin")
    found, notes = table_data_buckets(st, tb_arn)
    owned = sorted(set(_ledger_read()) | {f["bucket"] for f in found})
    try:
        listed = [b["Name"] for b in cli.list_buckets().get("Buckets", [])
                  if b["Name"].endswith(DATA_BUCKET_SUFFIX)]
    except Exception:
        listed = []
    out = {"tables_removed": [], "bucket_deleted": False,
           "data_buckets": {}, "notes": notes,
           "unattributed_left_alone": sorted(set(listed) - set(owned))}
    if dry_run:
        out["tables_removed"] = clean_table_bucket(dry_run=True)
        out["bucket_deleted"] = f"would delete {config.TABLE_BUCKET}"
        out["data_buckets"] = {n: "would delete" for n in owned}
        return out
    _ledger_write(owned)          # before deleting tables: survive a crash
    out["tables_removed"] = clean_table_bucket()
    try:
        st.delete_table_bucket(tableBucketARN=tb_arn)
        out["bucket_deleted"] = True
    except Exception as e:
        out["bucket_deleted"] = _code(e)
    deadline = time.time() + wait_s
    todo = list(owned)
    while todo:
        for name in list(todo):
            state = _delete_data_bucket(cli, name)
            out["data_buckets"][name] = state
            if state != PENDING:
                todo.remove(name)
        if not todo or time.time() >= deadline:
            break
        time.sleep(5)
    _ledger_write([n for n, st_ in out["data_buckets"].items()
                   if st_ not in ("deleted", "gone")])   # next reset retries
    return out


def ensure_seed_structure():
    """Namespace + table bucket exist; ordinary buckets have raw/ curated/."""
    st = s3util.s3tables("tables-rw")
    tb_arn = config.table_bucket_arn()
    try:
        st.create_table_bucket(name=config.TABLE_BUCKET)
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
    return {"namespaces": _namespaces(st, tb_arn)}


def restore_throttler(backup=None):
    return Throttler(MgmtApi()).restore(backup or config.BACKUP_FILE)


RULES_STAGED_AT = config.REPORT_DIR / "rules_staged_at"


def stage_rules():
    """Store the demo rules with the throttler disabled and record the push
    time, so notebook 02 reads the propagation age instead of waiting the
    full rules_config_cache_expiration_time inside the demo."""
    thr = Throttler(MgmtApi())
    rules = demo_rules.demo_rules()
    checks = {r["id"]: te.verify_rule(thr, r) for r in rules}
    ok = all(c["ok"] for c in checks.values())
    if not ok:
        raise RuntimeError("demo rules failed verification: " + json.dumps(
            {k: v["problems"] for k, v in checks.items() if not v["ok"]}))
    keep = [r for r in thr.get_rules() if not demo_rules.is_demo_rule(r)]
    thr.set_mode("disabled")
    thr.put_config(keep + rules)
    RULES_STAGED_AT.write_text(str(time.time()))
    return {"staged": True, "rules": [r["id"] for r in rules],
            "verify": {k: v["ok"] for k, v in checks.items()}}


def full_reset(backup=None, dry_run=False, wait_s=120):
    """Reset the PoC. With dry_run=True nothing is changed; the returned
    structure lists what a real reset would delete."""
    out = {"dry_run": dry_run}
    if dry_run:
        out["restore_throttler"] = f"would restore {backup or config.BACKUP_FILE}"
    else:
        out["restore_throttler"] = restore_throttler(backup)
    out["objects_removed"] = clean_ordinary_buckets(dry_run)
    drop = drop_table_bucket(dry_run, wait_s)
    out["tables_removed"] = drop["tables_removed"]
    out["table_bucket_deleted"] = drop["bucket_deleted"]
    out["data_buckets_deleted"] = drop["data_buckets"]
    out["unattributed_left_alone"] = drop["unattributed_left_alone"]
    out["notes"] = drop["notes"]
    out["seed"] = None if dry_run else ensure_seed_structure()
    out["rules_staged"] = ("would stage demo rules" if dry_run
                           else stage_rules())
    return out
